use crate::{QTrace,Point,TreeRef};
use async_std::channel::Sender;

pub struct JsTrace<P: Point> {
  rpc: Sender<TreeRef<P>>,
}

impl<P> JsTrace<P> where P: Point {
  pub fn new(rpc: Sender<TreeRef<P>>) -> Self {
    Self { rpc }
  }
}

impl<P> QTrace<P> for JsTrace<P> where P: Point {
  fn trace(&mut self, tr: TreeRef<P>) {
    self.rpc.try_send(tr).unwrap();
  }
}
