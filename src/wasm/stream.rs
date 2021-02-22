use crate::{Coord,Error,wasm::V};
use async_std::{prelude::*,stream::Stream,sync::{Arc,Mutex}};
use wasm_bindgen::prelude::{wasm_bindgen,JsValue};
use wasm_bindgen_futures::future_to_promise;
use js_sys::{Error as JsError,Array,Uint8Array,Promise};

macro_rules! def_stream {
  ($C:ident, ($($T:tt),+), $n:literal, ($($i:tt),+)) => {
    #[wasm_bindgen]
    pub struct $C {
      stream: Arc<Mutex<dyn Stream<Item=Result<(($(Coord<$T>),+),V),Error>>+Unpin>>
    }
    impl $C {
      pub fn new(stream: Box<dyn Stream<Item=Result<(($(Coord<$T>),+),V),Error>>+Unpin>) -> Self {
        Self { stream: Arc::new(Mutex::new(stream)) }
      }
    }
    #[wasm_bindgen]
    impl $C {
      pub fn next(&self) -> Promise {
        let stream_ref = Arc::clone(&self.stream);
        future_to_promise(async move {
          let mut stream = stream_ref.lock().await;
          match stream.next().await {
            None => Ok(JsValue::NULL),
            Some(Err(e)) => {
              Err(JsError::new(&format!["{:?}",e]).into())
            },
            Some(Ok((point,value))) => {
              let r = Array::new_with_length(2); // point, value
              r.set(0, { // point
                let p = Array::new_with_length($n);
                $(
                  p.set($i, match point.$i {
                    Coord::Scalar(x) => JsValue::from_f64(x as f64),
                    Coord::Interval(x0,x1) => {
                      let iv = Array::new_with_length(2);
                      iv.set(0, JsValue::from_f64(x0 as f64));
                      iv.set(1, JsValue::from_f64(x1 as f64));
                      iv.into()
                    }
                  });
                )+
                p.into()
              });
              r.set(1, { // value
                let u: Uint8Array = value.data.as_slice().into();
                u.into()
              });
              Ok(r.into())
            }
          }
        })
      }
    }
  }
}

#[cfg(feature="2d")] def_stream![JsStream2,(f32,f32),2,(0,1)];
#[cfg(feature="3d")] def_stream![JsStream3,(f32,f32,f32),3,(0,1,2)];
#[cfg(feature="4d")] def_stream![JsStream4,(f32,f32,f32,f32),4,(0,1,2,3)];
#[cfg(feature="5d")] def_stream![JsStream5,(f32,f32,f32,f32,f32),5,(0,1,2,3,4)];
#[cfg(feature="6d")] def_stream![JsStream6,(f32,f32,f32,f32,f32,f32),6,(0,1,2,3,4,5)];
#[cfg(feature="7d")] def_stream![JsStream7,(f32,f32,f32,f32,f32,f32,f32),7,(0,1,2,3,4,5,6)];
#[cfg(feature="8d")] def_stream![JsStream8,(f32,f32,f32,f32,f32,f32,f32,f32),8,(0,1,2,3,4,5,6,7)];
