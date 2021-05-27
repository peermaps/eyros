use crate::{DB,Setup,Row,Coord,Value,BatchOptions,Error as E};
mod storage;
pub use storage::{JsStorage,JsRandomAccess};
mod stream;
use desert::{ToBytes,CountBytes,FromBytes};
use core::hash::Hash;
mod error;
mod errback;

use wasm_bindgen::{prelude::{wasm_bindgen,JsValue},JsCast};
use wasm_bindgen_futures::future_to_promise;
use js_sys::{Error,Function,Array,Uint8Array,Promise,Reflect::get};
use async_std::sync::{Arc,Mutex};

type S = JsRandomAccess;

struct GetId {
  pub f: Option<Function>
}
unsafe impl Send for GetId {}
unsafe impl Sync for GetId {}
static mut GETID: GetId = GetId { f: None };

#[wasm_bindgen]
pub fn set_getid(getid_fn: Function) -> () {
  if getid_fn.is_function() {
    unsafe { GETID.f = Some(getid_fn) }
  } else {
    unsafe { GETID.f = None }
  }
}

#[derive(Debug,Clone,Hash)]
pub struct V {
  pub data: Vec<u8>
}

impl Value for V {
  type Id = Vec<u8>;
  fn get_id(&self) -> Self::Id {
    if let Some(f) = unsafe { &GETID.f } {
      let id: Uint8Array = f.call1(
        &JsValue::NULL,
        unsafe { &Uint8Array::view(&self.data) }
      ).unwrap().into();
      return id.to_vec();
    }
    self.data.clone()
  }
}
impl ToBytes for V {
  fn to_bytes(&self) -> Result<Vec<u8>,E> {
    self.data.to_bytes()
  }
}
impl CountBytes for V {
  fn count_from_bytes(src: &[u8]) -> Result<usize,E> {
    <Vec<u8>>::count_from_bytes(src)
  }
  fn count_bytes(&self) -> usize {
    self.data.count_bytes()
  }
}
impl FromBytes for V {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),E> {
    let (size,data) = <Vec<u8>>::from_bytes(src)?;
    Ok((size, Self { data }))
  }
}

macro_rules! def_mix {
  ($C:ident, $Stream:ident, $Tree:ident, $open:ident, $n:literal, ($($T:ty),+), ($($I:tt),+)) => {
    use crate::$Tree;
    pub use stream::$Stream;
    #[wasm_bindgen]
    pub struct $C {
      db: Arc<Mutex<DB<S,$Tree<$($T),+,V>,($(Coord<$T>),+),V>>>
    }
    #[wasm_bindgen]
    impl $C {
      pub fn batch(&self, rows: JsValue, opts: JsValue) -> Promise {
        let errf = |e| Error::new(&format!["{:?}",e]);
        let db_ref = Arc::clone(&self.db);
        future_to_promise(async move {
          let mut r_opts = BatchOptions::new();
          if opts.is_object() {
            if let Some(depth) = get(&opts,&"rebuildDepth".into()).map_err(errf)?.as_f64() {
              r_opts = r_opts.rebuild_depth(depth as usize);
            }
            if let Some(x) = get(&opts,&"errorIfMissing".into()).map_err(errf)?.as_bool() {
              r_opts = r_opts.error_if_missing(x);
            }
          }
          let mut db = db_ref.lock().await;
          let batch = Self::batch_rows(rows)?;
          db.batch_with_options(&batch, &r_opts).await
            .map_err(|e| Error::new(&format!["{:?}",e]))?;
          Ok(JsValue::NULL)
        })
      }
      fn batch_rows(rows: JsValue) -> Result<Vec<Row<($(Coord<$T>),+),V>>,Error> {
        if !Array::is_array(&rows) {
          return Err(Error::new(&"row must be an array").into())
        }
        let errf = |e| Error::new(&format!["{:?}",e]);
        let arows: Array = rows.clone().into();
        let mut batch: Vec<Row<($(Coord<$T>),+),V>> = Vec::with_capacity(arows.length() as usize);
        for row in arows.iter() {
          let t = get(&row,&"type".into()).map_err(errf)?.as_string();
          batch.push(match t.unwrap().as_ref() {
            "insert" => {
              let point: Array = (get(&row,&"point".into()))
                .map(|x| x.into())
                .map_err(errf)?;
              let value: Uint8Array = get(&row,&"value".into())
                .map(|x| x.into())
                .map_err(errf)?;
              let mut data: Vec<u8> = vec![0;value.length() as usize];
              value.copy_to(&mut data);
              Row::Insert(($(
                {
                  let p = point.get($I);
                  match Array::is_array(&p) {
                    true => {
                      let a: Array = p.into();
                      Coord::Interval(
                        a.get(0).as_f64().unwrap() as $T,
                        a.get(1).as_f64().unwrap() as $T
                      )
                    },
                    false => Coord::Scalar(p.as_f64().unwrap() as $T)
                  }
                }
              ),+), V { data })
            },
            "delete" => {
              let point: Array = (get(&row,&"point".into()))
                .map(|x| x.into())
                .map_err(errf)?;
              let id: Uint8Array = get(&row,&"id".into())
                .map(|x| x.into())
                .map_err(errf)?;
              let mut buf: Vec<u8> = vec![0;id.length() as usize];
              id.copy_to(&mut buf);
              Row::Delete(($(
                {
                  let p = point.get($I);
                  match Array::is_array(&p) {
                    true => {
                      let a: Array = p.into();
                      Coord::Interval(
                        a.get(0).as_f64().unwrap() as $T,
                        a.get(1).as_f64().unwrap() as $T
                      )
                    },
                    false => Coord::Scalar(p.as_f64().unwrap() as $T)
                  }
                }
              ),+), buf)
            },
            _ => return Err(Error::new(&"unsupported row type").into())
          });
        }
        Ok(batch)
      }
      pub fn query(&self, bbox_js: JsValue) -> Promise {
        let db_ref = Arc::clone(&self.db);
        future_to_promise(async move {
          if !Array::is_array(&bbox_js) {
            return Err(Error::new(&"provided bbox is not an array").into())
          }
          let bbox_a: Array = bbox_js.into();
          let bbox = (
            ($(
              bbox_a.get($I).as_f64().unwrap() as $T
            ),+),
            ($(
              bbox_a.get($I+$n).as_f64().unwrap() as $T
            ),+)
          );
          let mut db = db_ref.lock().await;
          db.query(&bbox).await
            .map_err(|e| Error::new(&format!["{:?}",e]).into())
            .map(|x| $Stream::new(x).into())
        })
      }
      pub fn sync(&self) -> Promise {
        let db_ref = Arc::clone(&self.db);
        future_to_promise(async move {
          let mut db = db_ref.lock().await;
          db.sync().await.map_err(|e| Error::new(&format!["{:?}",e]))?;
          Ok(JsValue::NULL)
        })
      }
    }
    #[wasm_bindgen]
    pub async fn $open(opts: JsValue) -> Result<$C,Error> {
      let errf = |e| Error::new(&format!["{:?}",e]);
      let storage_fn = match get(&opts,&"storage".into()).map_err(errf)?.dyn_into() {
        Ok(storage) => storage,
        Err(_) => { return Err(Error::new("must provide opts.storage function")) },
      };
      let remove_fn = match get(&opts,&"remove".into()).map_err(errf)?.dyn_into() {
        Ok(remove) => remove,
        Err(_) => { return Err(Error::new("must provide opts.remove function")) },
      };
      let setup = Setup::from_storage(Box::new(JsStorage {
        storage_fn,
        remove_fn,
      }));
      type P = ($(Coord<$T>),+);
      type T = $Tree<$($T),+,V>;
      let db: DB<S,T,P,V> = setup.build().await
        .map_err(|e| Error::new(&format!["{:?}",e]))?;
      Ok($C { db: Arc::new(Mutex::new(db)) })
    }
  }
}

#[cfg(feature="2d")]
def_mix![JsDB2,JsStream2,Tree2,
  open_f32_f32,2,(f32,f32),(0,1)];

#[cfg(feature="3d")]
def_mix![JsDB3,JsStream3,Tree3,
  open_f32_f32_f32,3,(f32,f32,f32),(0,1,2)];

#[cfg(feature="4d")]
def_mix![JsDB4,JsStream4,Tree4,
  open_f32_f32_f32_f32,4,(f32,f32,f32,f32),(0,1,2,3)];

#[cfg(feature="5d")]
def_mix![JsDB5,JsStream5,Tree5,
  open_f32_f32_f32_f32_f32,
  5,(f32,f32,f32,f32,f32),(0,1,2,3,4)];

#[cfg(feature="6d")]
def_mix![JsDB6,JsStream6,Tree6,
  open_f32_f32_f32_f32_f32_f32,
  6,(f32,f32,f32,f32,f32,f32),(0,1,2,3,4,5)];

#[cfg(feature="7d")]
def_mix![JsDB7,JsStream7,Tree7,
  open_f32_f32_f32_f32_f32_f32_f32,
  7,(f32,f32,f32,f32,f32,f32,f32),(0,1,2,3,4,5,6)];

#[cfg(feature="8d")]
def_mix![JsDB8,JsStream8,Tree8,
  open_f32_f32_f32_f32_f32_f32_f32_f32,
  8,(f32,f32,f32,f32,f32,f32,f32,f32),(0,1,2,3,4,5,6,7)];

#[wasm_bindgen]
extern "C" {
  //fn log(msg: &str);
}
