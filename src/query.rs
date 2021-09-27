use crate::{Error,Point,Value};
use async_std::{stream::Stream};
use std::marker::Unpin;

pub type QStream<P,V> = Box<dyn Stream<Item=Result<(P,V),Error>>+Send+Unpin>;

pub fn from_queries<P:Point,V:Value>(queries: Vec<QStream<P,V>>) -> Result<QStream<P,V>,Error> {
  Ok(Box::new(futures::stream::select_all(queries.into_iter())))
}
