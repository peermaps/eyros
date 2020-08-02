use crate::{Storage,Error};
use random_access_storage::RandomAccess;
use wasm_bindgen::prelude::JsValue;
use js_sys::{Function,Uint8Array,Reflect::get};
use async_std::sync::{Arc,Mutex};
#[path="./errback.rs"] mod errback;
use errback::ErrBack;

pub struct JsStorage {
  pub storage_fn: Function
}

#[async_trait::async_trait]
impl Storage<JsRandomAccess> for JsStorage {
  async fn open(&mut self, name: &str) -> Result<JsRandomAccess,Error> {
    let errf = |_e| failure::err_msg("error").compat();
    let context = self.storage_fn.call1(&JsValue::NULL, &name.into())
      .map_err(|_e| failure::err_msg("error").compat())?;
    Ok(JsRandomAccess {
      write_fn: get(&context,&"write".into())
        .map_err(errf)?.into(),
      read_fn: get(&context,&"read".into())
        .map_err(errf)?.into(),
      len_fn: get(&context,&"len".into())
        .map_err(errf)?.into(),
    })
  }
}

pub struct JsRandomAccess {
  pub write_fn: Function,
  pub read_fn: Function,
  pub len_fn: Function
}

// this MAY work only because wasm is single-threaded:
unsafe impl Send for JsRandomAccess {}
unsafe impl Sync for JsRandomAccess {}
unsafe impl Send for JsStorage {}
unsafe impl Sync for JsStorage {}

#[async_trait::async_trait]
impl RandomAccess for JsRandomAccess {
  type Error = Box<dyn std::error::Error+Sync+Send>;

  async fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), Self::Error> {
    let mut errback = ErrBack::new();
    self.write_fn.call3(
      &JsValue::NULL,
      &JsValue::from_f64(offset as f64),
      unsafe { &Uint8Array::view(&data) },
      &errback.cb().as_ref()
    ).map_err(|_e| failure::err_msg("error").compat())?;
    errback.await.map_err(|_e| failure::err_msg("error").compat())?;
    Ok(())
  }

  async fn read(&mut self, offset: u64, length: u64) -> Result<Vec<u8>, Self::Error> {
    unimplemented![]
  }

  async fn read_to_writer(&mut self, offset: u64, length: u64,
  buf: &mut (impl futures_io::AsyncWrite + Send)) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn del(&mut self, offset: u64, length: u64) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn truncate(&mut self, length: u64) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn len(&self) -> Result<u64, Self::Error> {
    unimplemented![]
  }

  async fn is_empty(&mut self) -> Result<bool, Self::Error> {
    unimplemented![]
  }

  async fn sync_all(&mut self) -> Result<(), Self::Error> {
    unimplemented![]
  }
}
