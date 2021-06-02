use crate::{Storage,Error,wasm::error::JsError};
use random_access_storage::RandomAccess;
use wasm_bindgen::{prelude::JsValue,closure::Closure};
use wasm_bindgen_futures::spawn_local;
use js_sys::{Function,Uint8Array,Reflect::get};
use async_std::channel::unbounded;

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
    let (sender,receiver) = unbounded();
    {
      let cb = Closure::once_into_js(Box::new(move |err: JsValue| {
        spawn_local(async move {
          let r = sender.send(JsError::wrap(if err.is_truthy() { Err(err) } else { Ok(()) }));
          if let Err(_) = r.await {} // ignore send errors
        });
      }) as Box<dyn FnOnce(JsValue)>);
      JsError::wrap(self.write_fn.call3(
        &JsValue::NULL,
        &JsValue::from_f64(offset as f64),
        unsafe { &Uint8Array::view(&data) },
        &cb
      ))?;
    }
    receiver.recv().await?
  }

  async fn read(&mut self, offset: u64, length: u64) -> Result<Vec<u8>, Self::Error> {
    let (sender,receiver) = unbounded();
    {
      let cb = Closure::once_into_js(Box::new(move |err: JsValue, value: JsValue| {
        spawn_local(async move {
          let r = sender.send(JsError::wrap(
            if err.is_truthy() {
              Err(err)
            } else {
              let u: Uint8Array = value.into();
              Ok(u.to_vec())
            }
          ));
          if let Err(_) = r.await {} // ignore send errors
        });
      }) as Box<dyn FnOnce(JsValue,JsValue)>);
      JsError::wrap(self.read_fn.call3(
        &JsValue::NULL,
        &JsValue::from_f64(offset as f64),
        &JsValue::from_f64(length as f64),
        &cb
      ))?;
    }
    receiver.recv().await?
  }

  async fn read_to_writer(&mut self, _offset: u64, _length: u64,
  _buf: &mut (impl futures_io::AsyncWrite + Send)) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn del(&mut self, offset: u64, length: u64) -> Result<(), Self::Error> {
    let (sender,receiver) = unbounded();
    {
      let cb = Closure::once_into_js(Box::new(move |err: JsValue| {
        spawn_local(async move {
          let r = sender.send(JsError::wrap(if err.is_truthy() { Err(err) } else { Ok(()) }));
          if let Err(_) = r.await {} // ignore send errors
        });
      }) as Box<dyn FnOnce(JsValue)>);
      JsError::wrap(self.del_fn.call3(
        &JsValue::NULL,
        &JsValue::from_f64(offset as f64),
        &JsValue::from_f64(length as f64),
        &cb
      ))?;
    }
    receiver.recv().await?
  }

  async fn truncate(&mut self, length: u64) -> Result<(), Self::Error> {
    let (sender,receiver) = unbounded();
    {
      let cb = Closure::once_into_js(Box::new(move |err: JsValue| {
        spawn_local(async move {
          let r = sender.send(JsError::wrap(if err.is_truthy() { Err(err) } else { Ok(()) }));
          if let Err(_) = r.await {} // ignore send errors
        });
      }) as Box<dyn FnOnce(JsValue)>);
      JsError::wrap(self.truncate_fn.call2(
        &JsValue::NULL,
        &JsValue::from_f64(length as f64),
        &cb
      ))?;
    }
    receiver.recv().await?
  }

  async fn len(&self) -> Result<u64, Self::Error> {
    let (sender,receiver) = unbounded();
    {
      let cb = Closure::once_into_js(Box::new(move |err: JsValue, value: JsValue| {
        spawn_local(async move {
          let r = sender.send(JsError::wrap(
            if err.is_truthy() {
              Err(err)
            } else {
              Ok(value.as_f64().map(|v| v as u64).unwrap_or(0))
            }
          ));
          if let Err(_) = r.await {} // ignore send errors
        });
      }) as Box<dyn FnOnce(JsValue,JsValue)>);
      JsError::wrap(self.len_fn.call1(&JsValue::NULL, &cb))?;
    }
    receiver.recv().await?
  }

  async fn is_empty(&mut self) -> Result<bool, Self::Error> {
    let (sender,receiver) = unbounded();
    {
      let cb = Closure::once_into_js(Box::new(move |err: JsValue, value: JsValue| {
        spawn_local(async move {
          let r = sender.send(JsError::wrap(
            if err.is_truthy() {
              Err(err)
            } else {
              Ok(value.as_f64().map(|v| v as u64 == 0).unwrap_or(true))
            }
          ));
          if let Err(_) = r.await {} // ignore send errors
        });
      }) as Box<dyn FnOnce(JsValue,JsValue)>);
      JsError::wrap(self.len_fn.call1(&JsValue::NULL, &cb))?;
    }
    receiver.recv().await?
  }

  async fn sync_all(&mut self) -> Result<(), Self::Error> {
    let (sender,receiver) = unbounded();
    {
      let cb = Closure::once_into_js(Box::new(move |err: JsValue| {
        spawn_local(async move {
          let r = sender.send(JsError::wrap(if err.is_truthy() { Err(err) } else { Ok(()) }));
          if let Err(_) = r.await {} // ignore send errors
        });
      }) as Box<dyn FnOnce(JsValue)>);
      JsError::wrap(self.sync_fn.call1(&JsValue::NULL, &cb))?;
    }
    receiver.recv().await?
  }
}
