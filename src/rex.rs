//! Remote ExEx bridge: streams canonical chain notifications to subscribers over gRPC.
//!
//! Mirrors the `reth-rex` binary we run on Ethereum mainnet so the same downstream consumers
//! work against BSC. The payload is a bincode-serialised
//! [`reth_exex_types::serde_bincode_compat::ExExNotification`] parameterised over
//! [`BscPrimitives`]; the bincode `Chain` representation stores blocks as RLP plus senders, so
//! it needs only an RLP codec on the block type, not `SerdeBincodeCompat`.

use crate::node::primitives::BscPrimitives;
use reth_ethereum_primitives::{Block as EthBlock, EthPrimitives};
use reth_execution_types::Chain;
use reth_primitives_traits::RecoveredBlock;
use reth_exex::ExExNotification;
use reth_exex_types::serde_bincode_compat::ExExNotification as BincodeExExNotification;
use rex_proto::{
    remote_ex_ex_server::{RemoteExEx, RemoteExExServer},
    ExExNotification as ProtoExExNotification, SubscribeRequest,
};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status};

/// Re-shape a BSC chain as an Ethereum-primitives chain.
///
/// `BscBlockBody` is `alloy_consensus::BlockBody` plus an extra `sidecars` field, and that field
/// is present in its RLP encoding even when empty. A consumer decoding with Ethereum primitives
/// therefore fails with `ListLengthMismatch` off by one byte.
///
/// Downstream (`rex-sim`) consumes the `BundleState`, receipts, headers and transaction hashes —
/// never blob sidecars — so we drop them here and emit Ethereum-shaped blocks. That keeps the
/// existing Ethereum `rex-cons` consumer working against BSC unmodified.
///
/// **Sidecars are dropped.** If anything downstream ever needs BSC blob sidecars, this conversion
/// must go and the consumer must gain a BSC-aware `NodePrimitives` instead.
///
/// Receipts need no conversion: `BscPrimitives::Receipt` and `EthPrimitives::Receipt` are the same
/// type, so the `ExecutionOutcome` carries across as-is.
fn to_eth_chain(chain: Chain<BscPrimitives>) -> Chain<EthPrimitives> {
    let (blocks, execution_outcome, trie_data) = chain.into_inner();
    let eth_blocks = blocks.into_iter().map(|(_, recovered)| {
        let (sealed, senders) = recovered.split_sealed();
        let bsc_block = sealed.into_block();
        let eth_block = EthBlock { header: bsc_block.header, body: bsc_block.body.inner };
        RecoveredBlock::new_unhashed(eth_block, senders)
    });
    Chain::new(eth_blocks, execution_outcome, trie_data)
}

/// Convert a BSC notification into the Ethereum-shaped notification put on the wire.
fn to_eth_notification(
    notification: &ExExNotification<BscPrimitives>,
) -> ExExNotification<EthPrimitives> {
    match notification {
        ExExNotification::ChainCommitted { new } => {
            ExExNotification::ChainCommitted { new: Arc::new(to_eth_chain((**new).clone())) }
        }
        ExExNotification::ChainReorged { old, new } => ExExNotification::ChainReorged {
            old: Arc::new(to_eth_chain((**old).clone())),
            new: Arc::new(to_eth_chain((**new).clone())),
        },
        ExExNotification::ChainReverted { old } => {
            ExExNotification::ChainReverted { old: Arc::new(to_eth_chain((**old).clone())) }
        }
    }
}

/// Buffer of notifications held for subscribers that fall behind.
const NOTIFICATION_BUFFER: usize = 32;

pub struct ExExService {
    notifications: Arc<broadcast::Sender<ExExNotification<BscPrimitives>>>,
}

impl ExExService {
    pub fn new(notifications: Arc<broadcast::Sender<ExExNotification<BscPrimitives>>>) -> Self {
        Self { notifications }
    }
}

#[tonic::async_trait]
impl RemoteExEx for ExExService {
    type SubscribeStream = ReceiverStream<Result<ProtoExExNotification, Status>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        let mut notifications = self.notifications.subscribe();

        tokio::spawn(async move {
            // A lagging subscriber ends the stream rather than blocking the broadcast; the
            // client reconnects and downstream treats the gap as stale state.
            while let Ok(notification) = notifications.recv().await {
                let eth_notification = to_eth_notification(&notification);
                let bincode_notification: BincodeExExNotification<'_, EthPrimitives> =
                    (&eth_notification).into();
                let data = match bincode::serialize(&bincode_notification) {
                    Ok(data) => data,
                    Err(err) => {
                        tracing::error!(target: "bsc::rex", %err, "failed to serialize notification");
                        break;
                    }
                };
                let size_mb = data.len() as f64 / (1024.0 * 1024.0);
                if tx.send(Ok(ProtoExExNotification { data: data.into() })).await.is_err() {
                    tracing::info!(target: "bsc::rex", "client disconnected");
                    break;
                }
                tracing::debug!(target: "bsc::rex", size_mb = format!("{size_mb:.2}"), "sent to client");
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Build the gRPC server for the remote ExEx.
pub fn server(
    notifications: Arc<broadcast::Sender<ExExNotification<BscPrimitives>>>,
) -> RemoteExExServer<ExExService> {
    RemoteExExServer::new(ExExService::new(notifications))
}

/// Create the broadcast channel shared between the ExEx and the gRPC service.
pub fn channel() -> Arc<broadcast::Sender<ExExNotification<BscPrimitives>>> {
    Arc::new(broadcast::channel(NOTIFICATION_BUFFER).0)
}

/// The ExEx itself.
///
/// Notifications published to subscribers come from the canonical state stream, not from
/// `ctx.notifications`; the latter is drained only to advance the finished-height watermark so
/// reth can prune the ExEx WAL.
/// Feed subscribers straight off the canonical state stream.
///
/// This used to be installed as an ExEx. It no longer is, because the ExEx machinery cost far more
/// than it provided:
///
///   - We never used the ExEx notifications. The payload has always come from
///     `canonical_state_stream()`; `ctx.notifications` was read and discarded purely so the manager
///     could advance past it.
///   - The manager's poll takes notifications in MANY per cycle (WAL-committing each to disk) but
///     hands each ExEx exactly ONE per cycle:
///     `if let Some(n) = buffer.get(idx) && exex.send(cx, n)`, then
///     `buffer.retain(|(id, _)| id >= min_id)`. Intake outruns delivery whenever notifications
///     arrive faster than the manager polls.
///   - On Ethereum that never binds: one notification per 12 s block. On BSC, 0.46 s blocks are
///     ~26x the rate, so the buffer filled to its 1024 cap and stayed there — 1024 retained
///     `Chain`s (blocks, receipts, state) is tens of GB, which is what drove the OOM kills. A
///     faster consumer cannot fix it; delivery is one-per-poll regardless.
///
/// Reading the stream directly keeps exactly the part we use and drops the buffer, the WAL commits,
/// and the engine backpressure. Subscribers already resync from the live tip rather than replaying
/// history, so the ExEx replay guarantees bought us nothing.
pub async fn pump_canonical<P>(
    provider: P,
    notifications: Arc<broadcast::Sender<ExExNotification<BscPrimitives>>>,
) where
    P: reth::providers::CanonStateSubscriptions<Primitives = BscPrimitives>,
{
    let mut canon_stream = provider.canonical_state_stream();

    while let Some(canon_notif) = canon_stream.next().await {
        let exex_notification = match canon_notif {
            reth::providers::CanonStateNotification::Commit { new } => {
                tracing::debug!(target: "bsc::rex", block = new.tip().number, "canon commit");
                ExExNotification::ChainCommitted { new }
            }
            reth::providers::CanonStateNotification::Reorg { old, new } => {
                tracing::info!(
                    target: "bsc::rex",
                    old_block = old.tip().number,
                    new_block = new.tip().number,
                    "canon reorg"
                );
                ExExNotification::ChainReorged { old, new }
            }
        };
        // Lagging subscribers are dropped by the broadcast channel and resync from the tip; the
        // node is never held up waiting for one.
        let _ = notifications.send(exex_notification);
    }

    tracing::warn!(target: "bsc::rex", "canonical state stream ended");
}
