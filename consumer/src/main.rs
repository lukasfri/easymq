use std::time::Instant;

use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use library::{self, RPCHandler};

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
    let start = Instant::now();

    let mut handler = RPCHandler::new("", "hello", &channel_a, |input: String| async move {
        println!(r#"[{}] Recieved "{input}"."#, start.elapsed().as_secs_f32());
        let return_value = "This is the return";

        println!(
            r#"[{}] Sending reply "{return_value}"."#,
            start.elapsed().as_secs_f32()
        );

        Ok(return_value.to_string())
    })
    .await
    .unwrap();

    handler.run().await.unwrap();

    Ok(())
}
