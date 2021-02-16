use async_std::{prelude::*,task::JoinHandle};
#[cfg(not(feature="wasm"))] use async_std::task::spawn;
#[cfg(feature="wasm")] use async_std::task::{spawn_local as spawn};
use std::future::Future;

pub type Error = Box<dyn std::error::Error+Sync+Send>;

pub struct Join<T> where T: Send+'static {
  tasks: Vec<JoinHandle<Result<T,Error>>>,
}
impl<T> Join<T> where T: Send+'static {
  pub fn new() -> Self {
    Self { tasks: vec![] }
  }
  pub fn push<F>(&mut self, future: F) -> () where F: Future<Output=Result<T,Error>>+Send+'static {
    self.tasks.push(spawn(future));
  }
  pub async fn try_join(&mut self) -> Result<(),Error> {
    let mut itasks = self.tasks.iter_mut();
    loop {
      let a = itasks.next();
      if a.is_none() { break }
      let b = itasks.next();
      if b.is_none() {
        a.unwrap().await?;
        break;
      }
      let c = itasks.next();
      if c.is_none() {
        a.unwrap().try_join(b.unwrap()).await?;
        break;
      }
      let d = itasks.next();
      if d.is_none() {
        a.unwrap().try_join(b.unwrap()).try_join(c.unwrap()).await?;
        break;
      }
      a.unwrap()
        .try_join(b.unwrap())
        .try_join(c.unwrap().try_join(d.unwrap()))
        .await?;
    }
    Ok(())
  }
}
