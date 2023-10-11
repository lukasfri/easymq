use async_trait::async_trait;
use futures_lite::Stream;
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub struct AmqpQueueInformation<'a> {
    pub queue_name: &'a str,
    pub exchange: &'a str,
    pub routing_key: &'a str,
}

pub struct AmqpQueueDeclaration<'a, T, DError> {
    pub information: AmqpQueueInformation<'a>,
    pub serializer: fn(T) -> Vec<u8>,
    pub deserializer: fn(Vec<u8>) -> Result<T, DError>,
}

#[derive(Debug, Error)]
pub enum AmqpConsumerError<CError, DError> {
    #[error("Consumer error: {0}")]
    ConsumerError(CError),
    #[error("Deserialization error: {0}")]
    DeserializationError(DError),
}

#[cfg(feature = "lapin")]
pub mod lapin;

#[async_trait]
pub trait Producer<T> {
    type Error;

    async fn publish(&self, value: T) -> Result<(), Self::Error>;
}

pub trait Consumer<'a, T, DError> {
    type Error;
    type Stream: Stream<Item = Option<Result<T, AmqpConsumerError<Self::Error, DError>>>> + 'a
    where
        Self: 'a;

    fn to_stream(&'a mut self) -> Self::Stream;
}
