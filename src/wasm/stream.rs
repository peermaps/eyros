use crate::{Mix,Location,Error};
#[cfg(feature="2d")] use crate::Mix2;
#[cfg(feature="3d")] use crate::Mix3;
#[cfg(feature="4d")] use crate::Mix4;
#[cfg(feature="5d")] use crate::Mix5;
#[cfg(feature="6d")] use crate::Mix6;
#[cfg(feature="7d")] use crate::Mix7;
#[cfg(feature="8d")] use crate::Mix8;
use async_std::{prelude::*,stream::Stream,sync::{Arc,Mutex}};
use wasm_bindgen::prelude::{wasm_bindgen,JsValue};
use wasm_bindgen_futures::future_to_promise;
use js_sys::{Error as JsError,Array,Uint8Array,Promise};

type V = Vec<u8>;

macro_rules! def_stream {
  ($C:ident, $P:ty, $n:literal, ($($v:tt),+), ($($I:tt),+)) => {
    #[wasm_bindgen]
    pub struct $C {
      stream: Arc<Mutex<dyn Stream<Item=Result<($P,V,Location),Error>>+Unpin>>
    }
    impl $C {
      pub fn new(stream: Box<dyn Stream<Item=Result<($P,V,Location),Error>>+Unpin>) -> Self {
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
            Some(Ok((point,value,loc))) => {
              let r = Array::new_with_length(3); // point, value, location
              r.set(0, { // point
                let p = Array::new_with_length($n);
                $(
                  p.set($I, match point.$v {
                    Mix::Scalar(x) => JsValue::from_f64(x as f64),
                    Mix::Interval(x0,x1) => {
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
  }
}

#[cfg(feature="2d")] def_stream![JsStream2,
  Mix2<f32,f32>,2,(v0,v1),(0,1)];

#[cfg(feature="3d")] def_stream![JsStream3,
  Mix3<f32,f32,f32>,3,(v0,v1,v2),(0,1,2)];

#[cfg(feature="4d")] def_stream![JsStream4,
  Mix4<f32,f32,f32,f32>,4,(v0,v1,v2,v3),(0,1,2,3)];

#[cfg(feature="5d")] def_stream![JsStream5,
  Mix5<f32,f32,f32,f32,f32>,5,(v0,v1,v2,v3,v4),(0,1,2,3,4)];

#[cfg(feature="6d")] def_stream![JsStream6,
  Mix6<f32,f32,f32,f32,f32,f32>,6,(v0,v1,v2,v3,v4,v5),(0,1,2,3,4,5)];

#[cfg(feature="7d")] def_stream![JsStream7,
  Mix7<f32,f32,f32,f32,f32,f32,f32>,
  7,(v0,v1,v2,v3,v4,v5,v6),(0,1,2,3,4,5,6)
];

#[cfg(feature="8d")] def_stream![JsStream8,
  Mix8<f32,f32,f32,f32,f32,f32,f32,f32>,
  8,(v0,v1,v2,v3,v4,v5,v6,v7),(0,1,2,3,4,5,6,7)
];
