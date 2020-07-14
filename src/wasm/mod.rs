use crate::{DB,Mix2};
pub mod storage;
use storage::{RandomAccessWeb,StorageWeb};
use wasm_bindgen::prelude::{wasm_bindgen,JsValue,Closure};
use js_sys::Error;

type S = RandomAccessWeb;

#[wasm_bindgen]
pub struct JsDB2 {
  db: DB<S,Mix2<f32,f32>,u32>
}

#[wasm_bindgen]
impl JsDB2 {
  /*
  pub async fn batch(&mut self, rows: &JsValue) -> Result<Error> {
  }
  pub async fn query(&mut self, bbox: &JsValue) -> JsStream {
    JsStream
  }
  */
}

#[wasm_bindgen]
pub async fn open() -> Result<JsDB2,Error> {
  type P = Mix2<f32,f32>;
  type V = u32;
  let db: DB<S,P,V> = DB::open_from_storage(Box::new(StorageWeb {})).await
    .map_err(|_e| Error::new("error"))?;
  Ok(JsDB2 { db })
}

#[wasm_bindgen]
extern "C" {
}
