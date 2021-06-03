use crate::Debugger;
use async_std::channel::Sender;

pub struct JsDebug {
  rpc: Sender<String>,
}

impl JsDebug {
  pub fn new(rpc: Sender<String>) -> Self {
    Self { rpc }
  }
}

impl Debugger for JsDebug {
  fn send(&mut self, msg: &str) -> () {
    self.rpc.try_send(msg.to_string()).unwrap();
  }
}
