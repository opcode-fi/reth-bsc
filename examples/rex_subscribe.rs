//! Minimal subscriber for the remote ExEx gRPC stream.
//!
//! Verifies the bridge end-to-end without needing the full downstream consumer:
//! connects, subscribes, and reports the size of each notification as it arrives.
//!
//! ```
//! cargo run --release --example rex_subscribe -- http://127.0.0.1:10001
//! ```

use rex_proto::{remote_ex_ex_client::RemoteExExClient, SubscribeRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args().nth(1).unwrap_or_else(|| "http://127.0.0.1:10000".to_string());
    println!("connecting to {url}");

    let mut client = RemoteExExClient::connect(url)
        .await?
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    let mut stream = client.subscribe(SubscribeRequest {}).await?.into_inner();
    println!("subscribed, waiting for notifications");

    let mut count = 0usize;
    let mut bytes = 0usize;
    while let Some(notification) = stream.message().await? {
        count += 1;
        bytes += notification.data.len();
        println!(
            "notification #{count}: {:.2} MB (total {:.2} MB)",
            notification.data.len() as f64 / (1024.0 * 1024.0),
            bytes as f64 / (1024.0 * 1024.0)
        );
        if count >= 5 {
            println!("received {count} notifications, bridge is working");
            break;
        }
    }
    Ok(())
}
