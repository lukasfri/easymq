use std::marker::PhantomData;

use async_trait::async_trait;
use futures_lite::Stream;

#[cfg(feature = "lapin")]
pub mod lapin;

#[derive(Debug, Clone, Copy)]
pub struct QueueDeclaration<'a, T> {
    queue_name: &'a str,
    exchange: &'a str,
    routing_key: &'a str,
    marker: PhantomData<T>,
}

#[async_trait]
pub trait Producer<T> {
    type Error;

    async fn publish(&self, value: T) -> Result<(), Self::Error>;
}

pub trait Consumer<'a, T> {
    type Error;
    type Stream: Stream<Item = Option<Result<T, Self::Error>>> + 'a
    where
        Self: 'a;

    fn to_stream(&'a mut self) -> Self::Stream;
}
