use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use simple_rpc::{self, RPCRoute};

pub const INPUT_STR: &str = "This is the input";
pub const RETURN_STR: &str = "This is the return";

async fn handler(input: String) -> Result<String, ()> {
    assert_eq!(input.as_str(), INPUT_STR);

    Ok(RETURN_STR.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn integration_test_with_external_amqp() {
    let addr = std::env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());

    let conn = Connection::connect(&addr, ConnectionProperties::default())
        .await
        .unwrap();

    let channel_a = conn.create_channel().await.unwrap();
    let channel_b = conn.create_channel().await.unwrap();

    channel_a
        .queue_declare(
            "hello",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    channel_a
        .queue_declare(
            "hello_response",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let route: RPCRoute<_, _> = RPCRoute::new("", "hello", "hello_response");

    let mut handler = route.handler(&channel_a, handler).await.unwrap();

    let mut handle_controller = route.handle(channel_b).await.unwrap();

    let handle = handle_controller.get_handle();

    let input = INPUT_STR.to_string();

    tokio::select! {
        request_result = handle.send(&input) => {
            assert_eq!(request_result.unwrap().unwrap().as_str(), RETURN_STR);
        },
        _handler_result = handler.run() => {
            unreachable!()
        }
        _runner_result = handle_controller.run() => {
            unreachable!()
        }
    };
}
