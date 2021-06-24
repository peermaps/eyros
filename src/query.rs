use crate::{Error,Point,Value};
use async_std::{prelude::*,stream::Stream,sync::{Arc,Mutex}};
use crate::unfold::unfold;
use std::marker::Unpin;

type Out<P,V> = Option<Result<(P,V),Error>>;
pub type QStream<P,V> = Box<dyn Stream<Item=Result<(P,V),Error>>+Send+Unpin>;

pin_project_lite::pin_project! {
  pub struct QueryStream<P,V> where P: Point, V: Value {
    index: Arc<Mutex<usize>>,
    #[pin] queries: Arc<Mutex<Vec<QStream<P,V>>>>,
  }
}

impl<P,V> QueryStream<P,V> where P: Point, V: Value {
  pub fn from_queries(queries: Vec<QStream<P,V>>) -> Result<QStream<P,V>,Error> {
    let qs = Self {
      index: Arc::new(Mutex::new(0)),
      queries: Arc::new(Mutex::new(queries)),
    };
    Ok(Box::new(unfold(qs, async move |mut qs| {
      qs.get_next().await.map(|p| (p,qs))
    })))
  }
  async fn get_next(&mut self) -> Out<P,V> {
    let mut qs = self.queries.lock().await;
    while !qs.is_empty() {
      let len = qs.len();
      {
        let mut ix = self.index.lock().await;
        let q = &mut qs[*ix];
        let result = q.next().await;
        if result.is_some() {
          *ix = ((*ix)+1) % len;
          return result;
        }
      }
      {
        let mut ix = self.index.lock().await;
        let _ = qs.remove(*ix);
        if !qs.is_empty() {
          *ix %= qs.len();
        }
      }
    }
    None
  }
}
