use std::{marker::PhantomData, str::FromStr, sync::Arc};

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
use thiserror::Error;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("LapinError")]
    LapinError(lapin::Error),
    #[error("RecvError")]
    RecvError(oneshot::error::RecvError),
    #[error("DidNotRecieveReply")]
    DidNotRecieveReply,
    #[error("SerializationError")]
    SerializationError(postcard::Error),
    #[error("CorrelationIdNotFound")]
    CorrelationIdNotFound,
    #[error("CorrelationIdNotUuid")]
    CorrelationIdNotUuid(uuid::Error),
}

impl From<lapin::Error> for Error {
    fn from(value: lapin::Error) -> Self {
        Self::LapinError(value)
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(value: oneshot::error::RecvError) -> Self {
        Self::RecvError(value)
    }
}

impl From<postcard::Error> for Error {
    fn from(value: postcard::Error) -> Self {
        Self::SerializationError(value)
    }
}

impl From<uuid::Error> for Error {
    fn from(value: uuid::Error) -> Self {
        Self::CorrelationIdNotUuid(value)
    }
}

pub struct RPCHandle<'a, Input: Serialize, Output: Serialize>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    marker: PhantomData<(Input, Output)>,
    exchange_key: &'a str,
    routing_key: &'a str,
    routing_response_key: &'a str,
    channel: Arc<Channel>,
    map: Arc<DashMap<Uuid, oneshot::Sender<Delivery>>>,
}

impl<'a, Input: Serialize, Output: Serialize> RPCHandle<'a, Input, Output>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    pub async fn send<'b>(&self, input: &'b Input) -> Result<Output, Error> {
        let payload = to_allocvec(&input)?;

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
            .await?
            .await?;

        assert_eq!(confirm, Confirmation::NotRequested);

        let (tx, rx) = oneshot::channel::<Delivery>();

        self.map.insert(request_id, tx);

        let delivery = rx.await?;

        let return_value = from_bytes::<Output>(&delivery.data)?;

        Ok(return_value)
    }
}

pub struct RPCHandleController<'a, Input: Serialize, Output: Serialize>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    marker: PhantomData<(Input, Output)>,
    exchange_key: &'a str,
    routing_key: &'a str,
    routing_response_key: &'a str,
    channel: Arc<Channel>,
    map: Arc<DashMap<Uuid, oneshot::Sender<Delivery>>>,
    return_listener: Consumer,
}

impl<'a, Input: Serialize, Output: Serialize> RPCHandleController<'a, Input, Output>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    pub async fn new(
        exchange_key: &'a str,
        queue_name: &'a str,
        response_queue_name: &'a str,
        channel: Channel,
    ) -> Result<RPCHandleController<'a, Input, Output>, Error> {
        let return_listener = channel
            .basic_consume(
                response_queue_name,
                "my_consumer_1",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            exchange_key,
            routing_key: queue_name,
            routing_response_key: response_queue_name,
            marker: PhantomData,
            channel: Arc::new(channel),
            return_listener,
            map: Arc::new(DashMap::new()),
        })
    }

    pub fn get_handle(&self) -> RPCHandle<'a, Input, Output> {
        RPCHandle {
            exchange_key: self.exchange_key,
            routing_key: self.routing_key,
            routing_response_key: self.routing_response_key,
            channel: self.channel.clone(),
            map: self.map.clone(),
            marker: PhantomData,
        }
    }
    #[allow(clippy::await_holding_lock)]

    pub async fn run(&mut self) -> Result<(), Error> {
        while let Some(delivery) = self.return_listener.next().await {
            let delivery = delivery?;

            let request_id = Uuid::from_str(
                delivery
                    .properties
                    .correlation_id()
                    .as_ref()
                    .ok_or(Error::CorrelationIdNotFound)?
                    .as_str(),
            )?;

            if let Some((_key, tx)) = self.map.remove(&request_id) {
                let _ = tx.send(delivery);
            };
        }
        Ok(())
    }
}

pub struct RPCHandler<
    'a,
    'b,
    Input: Serialize,
    Output: Serialize,
    DriverFut: Future<Output = Output>,
    Driver: FnMut(Input) -> DriverFut,
> where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    exchange_key: &'a str,
    marker: PhantomData<(Input, Output)>,
    channel: &'b Channel,
    listener: Consumer,
    f: Driver,
}

impl<
        'a,
        'b,
        Input: Serialize,
        Output: Serialize,
        DriverFut: Future<Output = Output>,
        Driver: FnMut(Input) -> DriverFut,
    > RPCHandler<'a, 'b, Input, Output, DriverFut, Driver>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    pub async fn new(
        exchange_key: &'a str,
        queue_name: &'a str,
        channel: &'b Channel,
        f: Driver,
    ) -> Result<RPCHandler<'a, 'b, Input, Output, DriverFut, Driver>, Error> {
        let listener = channel
            .basic_consume(
                queue_name,
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            exchange_key,
            marker: PhantomData,
            channel,
            listener,
            f,
        })
    }

    async fn handle_request(&mut self, incoming: Delivery) -> Result<(), Error> {
        incoming.ack(BasicAckOptions::default()).await?;

        let input_data = from_bytes::<Input>(&incoming.data)?;

        let result_value = (self.f)(input_data).await;

        let payload = to_allocvec(&result_value)?;

        let routing_key = incoming
            .properties
            .reply_to()
            .as_ref()
            .ok_or(Error::DidNotRecieveReply)?
            .as_str();

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
                            .ok_or(Error::CorrelationIdNotFound)?
                            .clone(),
                    ),
            )
            .await?
            .await?;

        assert_eq!(reply_confirm, Confirmation::NotRequested);

        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        while let Some(delivery) = self.listener.next().await {
            self.handle_request(delivery?).await?;
        }
        Ok(())
    }
}

pub struct RPCRoute<'a, Input: Serialize, Output: Serialize>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    marker: PhantomData<(Input, Output)>,
    exchange_key: &'a str,
    queue_name: &'a str,
    response_queue_name: &'a str,
}

impl<'a, Input: Serialize, Output: Serialize> RPCRoute<'a, Input, Output>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    pub const fn new(
        exchange_key: &'a str,
        queue_name: &'a str,
        response_queue_name: &'a str,
    ) -> Self {
        Self {
            marker: PhantomData,
            exchange_key,
            queue_name,
            response_queue_name,
        }
    }
}

impl<'a, Input: Serialize, Output: Serialize> RPCRoute<'a, Input, Output>
where
    for<'d> Input: Deserialize<'d>,
    for<'d> Output: Deserialize<'d>,
{
    pub async fn handler<
        'b,
        DriverFut: Future<Output = Output>,
        Driver: FnMut(Input) -> DriverFut,
    >(
        &self,
        channel: &'b Channel,
        handler: Driver,
    ) -> Result<RPCHandler<'a, 'b, Input, Output, DriverFut, Driver>, Error> {
        RPCHandler::new(self.exchange_key, self.queue_name, channel, handler).await
    }

    pub async fn handle(
        &self,
        channel: Channel,
    ) -> Result<RPCHandleController<'a, Input, Output>, Error> {
        RPCHandleController::new(
            self.exchange_key,
            self.queue_name,
            self.response_queue_name,
            channel,
        )
        .await
    }
}
