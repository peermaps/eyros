use crate::Debugger;
use wasm_bindgen::prelude::JsValue;
use js_sys::Function;

pub struct JsDebug {
  f: Function
}

impl JsDebug {
  pub fn new(f: Function) -> Self {
    Self { f }
  }
}

// this MAY work only because wasm is single-threaded (in the browser, for now):
unsafe impl Send for JsDebug {}
unsafe impl Sync for JsDebug {}

impl Debugger for JsDebug {
  fn send(&mut self, msg: &str) -> () {
    if let Err(_) = self.f.call1(&JsValue::NULL, &msg.into()) {}
  }
}
