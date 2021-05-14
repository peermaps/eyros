use crate::{Storage,Error,wasm::{errback::ErrBack,error::JsError}};
use random_access_storage::RandomAccess;
use wasm_bindgen::prelude::JsValue;
use js_sys::{Function,Uint8Array,Reflect::get};

pub struct JsStorage {
  pub storage_fn: Function,
  pub remove_fn: Function,
}

#[async_trait::async_trait]
impl Storage<JsRandomAccess> for JsStorage {
  async fn open(&mut self, name: &str) -> Result<JsRandomAccess,Error> {
    let context = JsError::wrap(self.storage_fn.call1(&JsValue::NULL, &name.into()))?;
    Ok(JsRandomAccess {
      write_fn: JsError::wrap(get(&context,&"write".into()))?.into(),
      read_fn: JsError::wrap(get(&context,&"read".into()))?.into(),
      len_fn: JsError::wrap(get(&context,&"len".into()))?.into(),
      truncate_fn: JsError::wrap(get(&context,&"truncate".into()))?.into(),
      del_fn: JsError::wrap(get(&context,&"del".into()))?.into(),
      sync_fn: JsError::wrap(get(&context,&"sync".into()))?.into(),
    })
  }
  async fn remove(&mut self, name: &str) -> Result<(),Error> {
    JsError::wrap(self.remove_fn.call1(&JsValue::NULL, &name.into()))?;
    Ok(())
  }
}

pub struct JsRandomAccess {
  pub write_fn: Function,
  pub read_fn: Function,
  pub len_fn: Function,
  pub truncate_fn: Function,
  pub del_fn: Function,
  pub sync_fn: Function,
}

// this MAY work only because wasm is single-threaded (in the browser, for now):
unsafe impl Send for JsRandomAccess {}
unsafe impl Sync for JsRandomAccess {}
unsafe impl Send for JsStorage {}
unsafe impl Sync for JsStorage {}

#[async_trait::async_trait]
impl RandomAccess for JsRandomAccess {
  type Error = Box<dyn std::error::Error+Sync+Send>;
  async fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), Self::Error> {
    let mut errback = ErrBack::new();
    JsError::wrap(self.write_fn.call3(
      &JsValue::NULL,
      &JsValue::from_f64(offset as f64),
      unsafe { &Uint8Array::view(&data) },
      &errback.cb()
    ))?;
    JsError::wrap(errback.await)?;
    Ok(())
  }

  async fn read(&mut self, offset: u64, length: u64) -> Result<Vec<u8>, Self::Error> {
    let mut errback = ErrBack::new();
    JsError::wrap(self.read_fn.call3(
      &JsValue::NULL,
      &JsValue::from_f64(offset as f64),
      &JsValue::from_f64(length as f64),
      &errback.cb()
    ))?;
    Ok(JsError::wrap(errback.await
      .map(|v| { let u: Uint8Array = v.into(); u.to_vec() }))?)
  }

  async fn read_to_writer(&mut self, _offset: u64, _length: u64,
  _buf: &mut (impl futures_io::AsyncWrite + Send)) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn del(&mut self, offset: u64, length: u64) -> Result<(), Self::Error> {
    let mut errback = ErrBack::new();
    JsError::wrap(self.del_fn.call3(
      &JsValue::NULL,
      &JsValue::from_f64(offset as f64),
      &JsValue::from_f64(length as f64),
      &errback.cb()
    ))?;
    JsError::wrap(errback.await)?;
    Ok(())
  }

  async fn truncate(&mut self, length: u64) -> Result<(), Self::Error> {
    let mut errback = ErrBack::new();
    JsError::wrap(self.truncate_fn.call2(
      &JsValue::NULL,
      &JsValue::from_f64(length as f64),
      &errback.cb()
    ))?;
    JsError::wrap(errback.await)?;
    Ok(())
  }

  async fn len(&self) -> Result<u64, Self::Error> {
    let mut errback = ErrBack::new();
    JsError::wrap(self.len_fn.call1(&JsValue::NULL, &errback.cb()))?;
    Ok(JsError::wrap(errback.await.map(|v| v.as_f64().unwrap() as u64))?)
  }

  async fn is_empty(&mut self) -> Result<bool, Self::Error> {
    let mut errback = ErrBack::new();
    JsError::wrap(self.len_fn.call1(&JsValue::NULL, &errback.cb()))?;
    Ok(JsError::wrap(errback.await.map(|v| v.as_f64().unwrap() as u64 == 0))?)
  }

  async fn sync_all(&mut self) -> Result<(), Self::Error> {
    let mut errback = ErrBack::new();
    JsError::wrap(self.sync_fn.call1(&JsValue::NULL, &errback.cb()))?;
    JsError::wrap(errback.await)?;
    Ok(())
  }
}
