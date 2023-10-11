use std::{fmt::Debug, pin::Pin};

use futures_lite::{Stream, StreamExt};
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel as LapinChannel, Consumer as LapinLibConsumer, Queue as LapinQueue,
};

use crate::{Consumer, Producer, QueueDeclaration};

// Extend the QueueDeclaration struct with lapin-related methods
impl<'a, T> QueueDeclaration<'a, T> {
    async fn create_lapin_consumer(
        &self,
        channel: &LapinChannel,
        consumer_tag: &str,
    ) -> Result<LapinLibConsumer, lapin::Error> {
        channel
            .basic_consume(
                self.queue_name,
                consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
    }

    pub async fn declare_lapin_queue(
        &self,
        channel: &LapinChannel,
    ) -> Result<LapinQueue, lapin::Error> {
        channel
            .queue_declare(
                self.queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
    }
}

pub struct LapinProducer<'a, 'c, T, S: Fn(T) -> Vec<u8>> {
    channel: &'c LapinChannel,
    queue_declaration: QueueDeclaration<'a, T>,
    serializer: S,
}
impl<'a, 'c, T: Send + Sync, S: (Fn(T) -> Vec<u8>) + Send + Sync> LapinProducer<'a, 'c, T, S> {
    pub async fn new(
        channel: &'c LapinChannel,
        queue_declaration: QueueDeclaration<'a, T>,
        serializer: S,
    ) -> Result<LapinProducer<'a, 'c, T, S>, lapin::Error> {
        queue_declaration.declare_lapin_queue(channel).await?;

        Ok(Self {
            channel,
            queue_declaration,
            serializer,
        })
    }
}

#[async_trait::async_trait]
impl<'a, 'c, T: Send + Sync, S: (Fn(T) -> Vec<u8>) + Send + Sync> Producer<T>
    for LapinProducer<'a, 'c, T, S>
{
    type Error = lapin::Error;

    async fn publish(&self, value: T) -> Result<(), Self::Error> {
        let payload = (self.serializer)(value);

        let _confirm = self
            .channel
            .basic_publish(
                self.queue_declaration.exchange,
                self.queue_declaration.routing_key,
                BasicPublishOptions::default(),
                payload.as_slice(),
                BasicProperties::default().with_delivery_mode(2),
            )
            .await?
            .await?;

        Ok(())
    }
}

pub struct LapinConsumer<T, DError, D: Fn(Vec<u8>) -> Result<T, DError>> {
    consumer: LapinLibConsumer,
    deserializer: D,
}

impl<'a, T, DError, D: Fn(Vec<u8>) -> Result<T, DError>> LapinConsumer<T, DError, D> {
    pub async fn new(
        channel: &LapinChannel,
        queue_declaration: QueueDeclaration<'a, T>,
        deserializer: D,
        consumer_tag: &str,
    ) -> Result<LapinConsumer<T, DError, D>, lapin::Error> {
        let consumer = queue_declaration
            .create_lapin_consumer(channel, consumer_tag)
            .await?;

        Ok(Self {
            consumer,
            deserializer,
        })
    }
}

impl<'a, T: Send + Sync, DError: Debug, D: Fn(Vec<u8>) -> Result<T, DError> + Send + Sync>
    Consumer<'a, T> for LapinConsumer<T, DError, D>
where
    Self: 'a,
{
    type Error = lapin::Error;
    type Stream = Pin<Box<dyn Stream<Item = Option<Result<T, Self::Error>>> + Send + 'a>>;

    fn to_stream(&'a mut self) -> Self::Stream {
        Box::pin(async_stream::stream! {
          loop {
              let delivery = self.consumer.next().await;

              let Some(delivery) = delivery else {
                yield None;
                continue;
              };

              let delivery = match delivery {
                Ok(delivery) => delivery,
                Err(err) => {

                  yield Some(Err(err));
                  continue;

                }
              };

              match delivery.ack(BasicAckOptions::default()).await {
                Ok(()) => (),
                Err(ack) => {
                  yield Some(Err(ack));
                  continue;
                }
              };

              let value = (self.deserializer)(delivery.data).unwrap();

              yield Some(Ok(value));
          }
        })
    }
}
