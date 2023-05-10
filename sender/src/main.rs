use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use library::RPCHandle;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = std::env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());

    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

    let channel_a = conn.create_channel().await?;

    let _queue = channel_a
        .queue_declare(
            "hello",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    let _queue = channel_a
        .queue_declare(
            "hello_response",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let handle = RPCHandle::<String, String>::new("", "hello", "hello_response", &channel_a)
        .await
        .unwrap();

    let start = Instant::now();

    let send = async {
        loop {
            println!(r#"[{}] Sending payload."#, start.elapsed().as_secs_f32());
            let result = handle.send(&"Test payload".to_string()).await.unwrap();
            println!(
                r#"[{}] Recieved return "{result}"."#,
                start.elapsed().as_secs_f32()
            );

            tokio::time::sleep(Duration::from_secs_f32(10.0)).await;
        }
    };

    let runner = handle.run();

    let (_send_res, _runner_res) = tokio::join!(send, runner);

    Ok(())
}
