use crate::{Error,Point,Value,tree::TreeRef};
use async_std::{stream::Stream};
use std::marker::Unpin;

pub type QStream<P,V> = Box<dyn Stream<Item=Result<(P,V),Error>>+Send+Unpin>;
pub trait QTrace<P: Point>: Send+Sync+'static {
  fn trace(&mut self, tr: TreeRef<P>);
}

impl<F,P> QTrace<P> for F where F: FnMut(TreeRef<P>)+Send+Sync+'static, P: Point {
  fn trace(&mut self, tr: TreeRef<P>) {
    (self)(tr)
  }
}
pub fn from_queries<P:Point,V:Value>(queries: Vec<QStream<P,V>>) -> Result<QStream<P,V>,Error> {
  Ok(Box::new(futures::stream::select_all(queries.into_iter())))
}
