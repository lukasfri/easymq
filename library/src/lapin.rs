use std::pin::Pin;

use futures_lite::{Stream, StreamExt};
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel as LapinChannel, Consumer as LapinLibConsumer, Queue as LapinQueue,
};

use crate::{AmqpConsumerError, AmqpQueueDeclaration, AmqpQueueInformation, Consumer, Producer};

// Extend the AmqpQueueDeclaration struct with lapin-related methods
impl<'a> AmqpQueueInformation<'a> {
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
    queue_information: AmqpQueueInformation<'a>,
    serializer: S,
    marker: std::marker::PhantomData<T>,
}
impl<'a, 'c, T: Send + Sync> LapinProducer<'a, 'c, T, fn(T) -> Vec<u8>> {
    pub async fn new<DError>(
        channel: &'c LapinChannel,
        queue_declaration: AmqpQueueDeclaration<'a, T, DError>,
    ) -> Result<LapinProducer<'a, 'c, T, fn(T) -> Vec<u8>>, lapin::Error> {
        queue_declaration
            .information
            .declare_lapin_queue(channel)
            .await?;

        Ok(Self {
            channel,
            queue_information: queue_declaration.information,
            serializer: queue_declaration.serializer,
            marker: std::marker::PhantomData,
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
                self.queue_information.exchange,
                self.queue_information.routing_key,
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

impl<'a, T, DError> LapinConsumer<T, DError, fn(Vec<u8>) -> Result<T, DError>> {
    pub async fn new(
        channel: &LapinChannel,
        queue_declaration: AmqpQueueDeclaration<'a, T, DError>,
        consumer_tag: &str,
    ) -> Result<LapinConsumer<T, DError, fn(Vec<u8>) -> Result<T, DError>>, lapin::Error> {
        let consumer = queue_declaration
            .information
            .create_lapin_consumer(channel, consumer_tag)
            .await?;

        Ok(Self {
            consumer,
            deserializer: queue_declaration.deserializer,
        })
    }
}

impl<
        'a,
        T: Send + Sync,
        DError: Send + Sync,
        D: Fn(Vec<u8>) -> Result<T, DError> + Send + Sync,
    > Consumer<'a, T, DError> for LapinConsumer<T, DError, D>
where
    Self: 'a,
{
    type Error = lapin::Error;
    type Stream = Pin<
        Box<
            dyn Stream<Item = Option<Result<T, AmqpConsumerError<Self::Error, DError>>>>
                + Send
                + 'a,
        >,
    >;

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

                  yield Some(Err(AmqpConsumerError::ConsumerError(err)));
                  continue;

                }
              };

              match delivery.ack(BasicAckOptions::default()).await {
                Ok(()) => (),
                Err(err) => {
                  yield Some(Err(AmqpConsumerError::ConsumerError(err)));
                  continue;
                }
              };

              let value = match (self.deserializer)(delivery.data) {
                Ok(value) => value,
                Err(err) => {

                  yield Some(Err(AmqpConsumerError::DeserializationError(err)));
                  continue;

                }
              };

              yield Some(Ok(value));
          }
        })
    }
}
