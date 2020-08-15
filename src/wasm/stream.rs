use crate::{Mix2,Mix,Location,Error};
use async_std::{prelude::*,stream::Stream,sync::{Arc,Mutex}};
use wasm_bindgen::prelude::{wasm_bindgen,JsValue};
use wasm_bindgen_futures::future_to_promise;
use js_sys::{Error as JsError,Array,Uint8Array,Promise};

type P2 = Mix2<f32,f32>;
type V = Vec<u8>;

#[wasm_bindgen]
pub struct JsStream {
  stream: Arc<Mutex<dyn Stream<Item=Result<(P2,V,Location),Error>>+Unpin>>
}

impl JsStream {
  pub fn new(stream: Box<dyn Stream<Item=Result<(P2,V,Location),Error>>+Unpin>) -> Self {
    Self { stream: Arc::new(Mutex::new(stream)) }
  }
}

#[wasm_bindgen]
impl JsStream {
  pub fn next(&self) -> Promise {
    let stream_ref = Arc::clone(&self.stream);
    future_to_promise(async move {
      let mut stream = stream_ref.lock().await;
      match stream.next().await {
        None => Ok(JsValue::NULL),
        Some(Err(e)) => {
          Err(JsError::new(&format!["{:?}",e]).into())
        },
        Some(Ok((point,value,loc))) => {
          let r = Array::new_with_length(3); // point, value, location
          r.set(0, { // point
            let p = Array::new_with_length(2);
            p.set(0, match point.v0 {
              Mix::Scalar(x) => JsValue::from_f64(x as f64),
              Mix::Interval(x0,x1) => {
                let iv = Array::new_with_length(2);
                iv.set(0, JsValue::from_f64(x0 as f64));
                iv.set(1, JsValue::from_f64(x1 as f64));
                iv.into()
              }
            });
            p.set(1, match point.v1 {
              Mix::Scalar(x) => JsValue::from_f64(x as f64),
              Mix::Interval(x0,x1) => {
                let iv = Array::new_with_length(2);
                iv.set(0, JsValue::from_f64(x0 as f64));
                iv.set(1, JsValue::from_f64(x1 as f64));
                iv.into()
              }
            });
            p.into()
          });
          r.set(1, { // value
            let u: Uint8Array = value.as_slice().into();
            u.into()
          });
          r.set(2, {
            let l = Array::new_with_length(2);
            l.set(0, JsValue::from_f64(loc.0 as f64));
            l.set(1, JsValue::from_f64(loc.1 as f64));
            l.into()
          });
          Ok(r.into())
        }
      }
    })
  }
}
