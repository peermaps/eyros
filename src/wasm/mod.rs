use crate::{DB,Row,Location,Mix,Mix2};
mod storage;
pub use storage::{JsStorage,JsRandomAccess};
mod stream;
pub use stream::JsStream;
use wasm_bindgen::prelude::{wasm_bindgen,JsValue};
use js_sys::{Error,Function,Array,Uint8Array,Reflect::get};

type S = JsRandomAccess;
type P2 = Mix2<f32,f32>;
type V = Vec<u8>;

#[wasm_bindgen]
pub struct JsDB2 {
  db: DB<S,P2,V>
}

#[wasm_bindgen]
impl JsDB2 {
  pub async fn batch(mut self, rows: JsValue) -> Result<(),Error> {
    if !Array::is_array(&rows) {
      panic!["must be an array. todo make this fail properly"]
    }
    let errf = |e| Error::new(&format!["{:?}",e]);
    let arows: Array = rows.clone().into();
    let mut batch: Vec<Row<P2,V>> = Vec::with_capacity(arows.length() as usize);
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
          let mut buf: V = vec![0;value.length() as usize];
          value.copy_to(&mut buf);
          let x = {
            let p = point.get(0);
            match Array::is_array(&p) {
              true => {
                let a: Array = p.into();
                Mix::Interval(
                  a.get(0).as_f64().unwrap() as f32,
                  a.get(1).as_f64().unwrap() as f32
                )
              },
              false => Mix::Scalar(p.as_f64().unwrap() as f32)
            }
          };
          let y = {
            let p = point.get(0);
            match Array::is_array(&p) {
              true => {
                let a: Array = p.into();
                Mix::Interval(
                  a.get(0).as_f64().unwrap() as f32,
                  a.get(1).as_f64().unwrap() as f32
                )
              },
              false => Mix::Scalar(p.as_f64().unwrap() as f32)
            }
          };
          Row::Insert(Mix2::new(x,y), buf)
        },
        "delete" => {
          let js_loc: Array = get(&row,&"location".into())
            .map(|x| x.into())
            .map_err(errf)?;
          let loc: Location = (
            js_loc.get(0).as_f64().unwrap() as u64,
            js_loc.get(1).as_f64().unwrap() as u32
          );
          Row::Delete(loc)
        },
        _ => panic!["unknown row type"]
      });
    }
    self.db.batch(&batch).await.map_err(|e| Error::new(&format!["{:?}",e]))
  }
  pub async fn query(mut self, bbox_js: JsValue) -> Result<JsStream,Error> {
    if !Array::is_array(&bbox_js) {
      return Err(Error::new(&"provided bbox is not an array"))
    }
    let bbox_a: Array = bbox_js.into();
    let bbox = (
      (
        bbox_a.get(0).as_f64().unwrap() as f32,
        bbox_a.get(1).as_f64().unwrap() as f32,
      ),
      (
        bbox_a.get(2).as_f64().unwrap() as f32,
        bbox_a.get(3).as_f64().unwrap() as f32,
      )
    );
    log(&format!["bbox={:?}", bbox]);
    Ok(JsStream::new(
      self.db.query(&bbox).await
        .map_err(|e| Error::new(&format!["{:?}",e]))?
    ))
  }
}

#[wasm_bindgen]
pub async fn open(storage_fn: Function) -> Result<JsDB2,Error> {
  let db: DB<S,P2,V> = DB::open_from_storage(Box::new(JsStorage {
    storage_fn
  })).await.map_err(|e| Error::new(&format!["{:?}",e]))?;
  Ok(JsDB2 { db })
}

#[wasm_bindgen]
extern "C" {
  fn log(msg: &str);
}