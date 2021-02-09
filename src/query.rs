use crate::{Error,Point,Value};
use async_std::{prelude::*,stream::Stream,sync::{Arc,Mutex}};
use crate::unfold::unfold;
use std::marker::Unpin;

type Out<P,V> = Option<Result<(P,V),Error>>;
pub type QStream<P,V> = Box<dyn Stream<Item=Result<(P,V),Error>>+Unpin>;

pin_project_lite::pin_project! {
  pub struct QueryStream<P,V> where P: Point, V: Value {
    index: usize,
    #[pin] queries: Vec<Arc<Mutex<QStream<P,V>>>>,
  }
}

impl<P,V> QueryStream<P,V> where P: Point, V: Value {
  pub fn from_queries(queries: Vec<Arc<Mutex<QStream<P,V>>>>) -> Result<QStream<P,V>,Error> {
    let qs = Self {
      index: 0,
      queries,
    };
    Ok(Box::new(unfold(qs, async move |mut qs| {
      let res = qs.get_next().await;
      match res {
        Some(p) => Some((p,qs)),
        None => None
      }
    })))
  }
  async fn get_next(&mut self) -> Out<P,V> {
    while !self.queries.is_empty() {
      let len = self.queries.len();
      {
        let ix = self.index;
        let q = &mut self.queries[ix];
        let next = {
          let result = q.lock().await.next().await;
          match result {
            Some(Err(e)) => return Some(Err(e.into())),
            _ => {}
          };
          result
        };
        match next {
          Some(result) => {
            self.index = (self.index+1) % len;
            return Some(result);
          },
          None => {}
        }
      }
      let ix = self.index;
      self.queries.remove(ix);
      if self.queries.len() > 0 {
        self.index = self.index % self.queries.len();
      }
    }
    None
  }
}
