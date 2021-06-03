use crate::{Storage,Error,wasm::error::JsError};
use random_access_storage::RandomAccess;
use wasm_bindgen::{prelude::JsValue,closure::Closure};
use wasm_bindgen_futures::spawn_local;
use js_sys::{Function,Uint8Array,Reflect::get};
use async_std::channel::{unbounded,Sender,Receiver};

pub struct JsStorage {
  pub storage_rpc: Sender<(String,Sender<Result<JsRandomAccess,Error>>)>,
  pub remove_rpc: Sender<(String,Sender<Result<(),Error>>)>,
}

#[async_trait::async_trait]
impl Storage<JsRandomAccess> for JsStorage {
  async fn open(&mut self, name: &str) -> Result<JsRandomAccess,Error> {
    let (sender, receiver) = unbounded();
    self.storage_rpc.send((name.to_string(),sender)).await?;
    receiver.recv().await?
  }
  async fn remove(&mut self, name: &str) -> Result<(),Error> {
    let (sender, receiver) = unbounded();
    self.remove_rpc.send((name.to_string(),sender)).await?;
    receiver.recv().await?
  }
}

pub enum JRequest {
  Write { offset: u64, data: Vec<u8> },
  Read { offset: u64, length: u64 },
  Del { offset: u64, length: u64 },
  Len {},
  Truncate { length: u64 },
  IsEmpty {},
  SyncAll {},
}

pub enum JResponse {
  Unit(),
  Data(Vec<u8>),
  Length(u64),
  Bool(bool),
}

pub struct JsRandomAccess {
  pub rpc: Sender<(JRequest,Sender<Result<JResponse,Error>>)>,
}

impl JsRandomAccess {
  fn new(rpc: Sender<(JRequest,Sender<Result<JResponse,Error>>)>) -> Self {
    Self { rpc }
  }
  pub async fn from_context(context: JsValue) -> Result<Self,Error> {
    let write_fn: Function = JsError::wrap(get(&context,&"write".into()))?.into();
    let read_fn: Function = JsError::wrap(get(&context,&"read".into()))?.into();
    let len_fn: Function = JsError::wrap(get(&context,&"len".into()))?.into();
    let truncate_fn: Function = JsError::wrap(get(&context,&"truncate".into()))?.into();
    let del_fn: Function = JsError::wrap(get(&context,&"del".into()))?.into();
    let sync_fn: Function = JsError::wrap(get(&context,&"sync".into()))?.into();
    let (sender, receiver): (
      Sender<(JRequest,Sender<Result<JResponse,Error>>)>,
      Receiver<(JRequest,Sender<Result<JResponse,Error>>)>,
    ) = unbounded();
    spawn_local(async move {
      while let Ok((msg,s)) = receiver.recv().await {
        match msg {
          JRequest::Write { offset, data } => {
            let cb = Closure::once_into_js(Box::new(move |err: JsValue| {
              spawn_local(async move {
                let r = s.send(JsError::wrap(
                  if err.is_truthy() { Err(err) }
                  else { Ok(JResponse::Unit()) }
                ));
                if let Err(_) = r.await {} // ignore send errors
              });
            }) as Box<dyn FnOnce(JsValue)>);
            write_fn.call3(
              &JsValue::NULL,
              &JsValue::from_f64(offset as f64),
              unsafe { &Uint8Array::view(&data) },
              &cb
            ).unwrap();
          },
          JRequest::Read { offset, length } => {
            let cb = Closure::once_into_js(Box::new(move |err: JsValue, value: JsValue| {
              spawn_local(async move {
                let r = s.send(JsError::wrap(
                  if err.is_truthy() { Err(err) }
                  else {
                    let u: Uint8Array = value.into();
                    Ok(JResponse::Data(u.to_vec()))
                  }
                ));
                if let Err(_) = r.await {} // ignore send errors
              });
            }) as Box<dyn FnOnce(JsValue,JsValue)>);
            read_fn.call3(
              &JsValue::NULL,
              &JsValue::from_f64(offset as f64),
              &JsValue::from_f64(length as f64),
              &cb
            ).unwrap();
          },
          JRequest::Del { offset, length } => {
            let cb = Closure::once_into_js(Box::new(move |err: JsValue| {
              spawn_local(async move {
                let r = s.send(JsError::wrap(
                  if err.is_truthy() { Err(err) }
                  else { Ok(JResponse::Unit()) }
                ));
                if let Err(_) = r.await {} // ignore send errors
              });
            }) as Box<dyn FnOnce(JsValue)>);
            del_fn.call3(
              &JsValue::NULL,
              &JsValue::from_f64(offset as f64),
              &JsValue::from_f64(length as f64),
              &cb
            ).unwrap();
          },
          JRequest::Len {} => {
            let cb = Closure::once_into_js(Box::new(move |err: JsValue, value: JsValue| {
              spawn_local(async move {
                let r = s.send(JsError::wrap(
                  if err.is_truthy() { Err(err) }
                  else { Ok(JResponse::Length(value.as_f64().map(|v| v as u64).unwrap_or(0))) }
                ));
                if let Err(_) = r.await {} // ignore send errors
              });
            }) as Box<dyn FnOnce(JsValue,JsValue)>);
            len_fn.call1(&JsValue::NULL, &cb).unwrap();
          },
          JRequest::Truncate { length } => {
            let cb = Closure::once_into_js(Box::new(move |err: JsValue| {
              spawn_local(async move {
                let r = s.send(JsError::wrap(
                  if err.is_truthy() { Err(err) }
                  else { Ok(JResponse::Unit()) }
                ));
                if let Err(_) = r.await {} // ignore send errors
              });
            }) as Box<dyn FnOnce(JsValue)>);
            truncate_fn.call2(
              &JsValue::NULL,
              &JsValue::from_f64(length as f64),
              &cb
            ).unwrap();
          },
          JRequest::IsEmpty {} => {
            let cb = Closure::once_into_js(Box::new(move |err: JsValue, value: JsValue| {
              spawn_local(async move {
                let r = s.send(JsError::wrap(
                  if err.is_truthy() { Err(err) }
                  else {
                    Ok(JResponse::Bool(value.as_f64()
                      .map(|v| v as u64 == 0).unwrap_or(true)))
                  }
                ));
                if let Err(_) = r.await {} // ignore send errors
              });
            }) as Box<dyn FnOnce(JsValue,JsValue)>);
            len_fn.call1(&JsValue::NULL, &cb).unwrap();
          },
          JRequest::SyncAll {} => {
            let cb = Closure::once_into_js(Box::new(move |err: JsValue| {
              spawn_local(async move {
                let r = s.send(JsError::wrap(
                  if err.is_truthy() { Err(err) }
                  else { Ok(JResponse::Unit()) }
                ));
                if let Err(_) = r.await {} // ignore send errors
              });
            }) as Box<dyn FnOnce(JsValue)>);
            sync_fn.call1(&JsValue::NULL, &cb).unwrap();
          },
        }
      }
    });
    Ok(Self::new(sender))
  }
}

#[async_trait::async_trait]
impl RandomAccess for JsRandomAccess {
  type Error = Box<dyn std::error::Error+Sync+Send>;
  async fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), Self::Error> {
    let (sender,receiver) = unbounded();
    self.rpc.send((JRequest::Write { offset, data: data.to_vec() }, sender)).await?;
    match receiver.recv().await?? {
      JResponse::Unit() => Ok(()),
      _ => panic!["unexpected response"],
    }
  }

  async fn read(&mut self, offset: u64, length: u64) -> Result<Vec<u8>, Self::Error> {
    let (sender,receiver) = unbounded();
    self.rpc.send((JRequest::Read { offset, length }, sender)).await?;
    match receiver.recv().await?? {
      JResponse::Data(data) => Ok(data),
      _ => panic!["unexpected response"],
    }
  }

  async fn read_to_writer(&mut self, _offset: u64, _length: u64,
  _buf: &mut (impl futures_io::AsyncWrite + Send)) -> Result<(), Self::Error> {
    unimplemented![]
  }

  async fn del(&mut self, offset: u64, length: u64) -> Result<(), Self::Error> {
    let (sender,receiver) = unbounded();
    self.rpc.send((JRequest::Del { offset, length }, sender)).await?;
    match receiver.recv().await?? {
      JResponse::Unit() => Ok(()),
      _ => panic!["unexpected response"],
    }
  }

  async fn truncate(&mut self, length: u64) -> Result<(), Self::Error> {
    let (sender,receiver) = unbounded();
    self.rpc.send((JRequest::Truncate { length }, sender)).await?;
    match receiver.recv().await?? {
      JResponse::Unit() => Ok(()),
      _ => panic!["unexpected response"],
    }
  }

  async fn len(&self) -> Result<u64, Self::Error> {
    let (sender,receiver) = unbounded();
    self.rpc.send((JRequest::Len {}, sender)).await?;
    match receiver.recv().await?? {
      JResponse::Length(len) => Ok(len),
      _ => panic!["unexpected response"],
    }
  }

  async fn is_empty(&mut self) -> Result<bool, Self::Error> {
    let (sender,receiver) = unbounded();
    self.rpc.send((JRequest::IsEmpty {}, sender)).await?;
    match receiver.recv().await?? {
      JResponse::Bool(x) => Ok(x),
      _ => panic!["unexpected response"],
    }
  }

  async fn sync_all(&mut self) -> Result<(), Self::Error> {
    let (sender,receiver) = unbounded();
    self.rpc.send((JRequest::SyncAll {}, sender)).await?;
    match receiver.recv().await?? {
      JResponse::Unit() => Ok(()),
      _ => panic!["unexpected response"],
    }
  }
}
