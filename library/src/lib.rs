use std::{
    marker::PhantomData,
    str::FromStr,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use futures_lite::{Future, StreamExt};
use lapin::{
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions},
    publisher_confirm::Confirmation,
    types::FieldTable,
    BasicProperties, Channel, Consumer,
};
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;

pub struct RPCHandle<'a, Input: Serialize, Output: Serialize>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    exchange_key: &'a str,
    routing_key: &'a str,
    routing_response_key: &'a str,
    marker: PhantomData<(Input, Output)>,
    channel: &'a Channel,
    map: DashMap<Uuid, oneshot::Sender<Delivery>>,
    return_listener: Arc<Mutex<Consumer>>,
}

impl<'a, Input: Serialize, Output: Serialize> RPCHandle<'a, Input, Output>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    pub async fn new(
        exchange_key: &'a str,
        queue_name: &'a str,
        response_queue_name: &'a str,
        channel: &'a Channel,
    ) -> Result<RPCHandle<'a, Input, Output>, ()> {
        let return_listener = channel
            .basic_consume(
                response_queue_name,
                "my_consumer_1",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        Ok(Self {
            exchange_key,
            routing_key: queue_name,
            routing_response_key: response_queue_name,
            marker: PhantomData,
            channel,
            return_listener: Arc::new(Mutex::new(return_listener)),
            map: DashMap::new(),
        })
    }

    pub async fn send<'b>(&self, input: &'b Input) -> Result<Output, ()> {
        let payload = to_allocvec(&input).unwrap();

        let request_id = Uuid::new_v4();

        let confirm = self
            .channel
            .basic_publish(
                self.exchange_key,
                self.routing_key,
                BasicPublishOptions::default(),
                payload.as_slice(),
                BasicProperties::default()
                    .with_reply_to(self.routing_response_key.into())
                    .with_delivery_mode(2)
                    .with_correlation_id(request_id.to_string().into()),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        assert_eq!(confirm, Confirmation::NotRequested);

        let (tx, rx) = oneshot::channel::<Delivery>();

        self.map.insert(request_id, tx);

        let delivery = rx.await.unwrap();

        let return_value = from_bytes::<Output>(&delivery.data).unwrap();

        Ok(return_value)
    }
    #[allow(clippy::await_holding_lock)]

    pub async fn run(&self) -> Result<(), ()> {
        let mut return_listener = self.return_listener.lock().unwrap();

        while let Some(delivery) = return_listener.next().await {
            let delivery = delivery.expect("error in consumer");

            let request_id = Uuid::from_str(
                delivery
                    .properties
                    .correlation_id()
                    .as_ref()
                    .unwrap()
                    .as_str(),
            )
            .unwrap();

            if let Some((_key, tx)) = self.map.remove(&request_id) {
                tx.send(delivery).unwrap();
            };
        }
        Ok(())
    }
}

pub struct RPCHandler<
    'a,
    Input: Serialize,
    Output: Serialize,
    DriverFut: Future<Output = Result<Output, ()>>,
    Driver: FnMut(Input) -> DriverFut,
> where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    exchange_key: &'a str,
    marker: PhantomData<(Input, Output)>,
    channel: &'a Channel,
    listener: Consumer,
    f: Driver,
}

impl<
        'a,
        Input: Serialize,
        Output: Serialize,
        DriverFut: Future<Output = Result<Output, ()>>,
        Driver: FnMut(Input) -> DriverFut,
    > RPCHandler<'a, Input, Output, DriverFut, Driver>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    pub async fn new(
        exchange_key: &'a str,
        queue_name: &'a str,
        channel: &'a Channel,
        f: Driver,
    ) -> Result<RPCHandler<'a, Input, Output, DriverFut, Driver>, ()> {
        let listener = channel
            .basic_consume(
                queue_name,
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        Ok(Self {
            exchange_key,
            marker: PhantomData,
            channel,
            listener,
            f,
        })
    }

    async fn handle_request<'b>(&mut self, incoming: Delivery) -> Result<(), ()> {
        incoming.ack(BasicAckOptions::default()).await.expect("ack");

        let input_data = from_bytes::<Input>(&incoming.data).unwrap();

        let result_value = (self.f)(input_data).await.unwrap();

        let payload = to_allocvec(&result_value).unwrap();

        let routing_key = incoming.properties.reply_to().as_ref().unwrap().as_str();

        let reply_confirm = self
            .channel
            .basic_publish(
                self.exchange_key,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_delivery_mode(2)
                    .with_correlation_id(
                        incoming
                            .properties
                            .correlation_id()
                            .as_ref()
                            .unwrap()
                            .as_str()
                            .into(),
                    ),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        assert_eq!(reply_confirm, Confirmation::NotRequested);

        Ok(())
    }

    pub async fn run<'b>(&mut self) -> Result<(), ()> {
        while let Some(delivery) = self.listener.next().await {
            let delivery = delivery.expect("error in consumer");
            self.handle_request(delivery).await.unwrap();
        }
        Ok(())
    }
}
