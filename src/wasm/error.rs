use crate::Error;
use wasm_bindgen::prelude::JsValue;

#[derive(Debug)]
pub struct JsError {
  message: String,
}

impl JsError {
  pub fn new(value: JsValue) -> Self {
    Self { message: format!["{:?}", value] }
  }
  pub fn wrap<T>(r: Result<T,JsValue>) -> Result<T,Error> {
    r.map_err(|e| Box::new(Self::new(e)).into())
  }
}

impl std::error::Error for JsError {}

impl std::fmt::Display for JsError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write![f, "{}", self.message]
  }
}
