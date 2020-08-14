// Create a new rust future with a callback field.

use async_std::{future::Future,task::{Context, Poll, Waker}};
use async_std::{sync::{Arc,Mutex},task};
use wasm_bindgen::prelude::{JsValue,Closure};
use std::pin::Pin;
use js_sys::{Error as JsError};
//use crate::wasm::log;

pin_project_lite::pin_project!{
  pub struct ErrBack {
    state: Arc<Mutex<ErrBackState>>
  }
}

pub struct ErrBackState {
  result: Option<Result<JsValue, JsValue>>,
  waker: Option<Waker>
}

// this MAY work only because wasm is single-threaded:
unsafe impl Send for ErrBack {}
unsafe impl Sync for ErrBack {}

impl Future for ErrBack {
  type Output = Result<JsValue, JsValue>;
  fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
    let mut f = self.project().state.lock();
    match (unsafe { Pin::new_unchecked(&mut f) }).poll(ctx) {
      Poll::Ready(mut s) => {
        match &s.result {
          Some(res) => Poll::Ready(res.clone()),
          None => {
            s.waker = Some(ctx.waker().clone());
            Poll::Pending
          }
        }
      },
      Poll::Pending => Poll::Pending
    }
  }
}

impl ErrBack {
  pub fn new() -> Self {
    let state = Arc::new(Mutex::new(ErrBackState {
      result: None,
      waker: None,
    }));
    Self { state }
  }
  pub fn cb(&mut self) -> JsValue {
    let state = Arc::clone(&self.state);
    Closure::once_into_js(Box::new(|err: JsValue, value: JsValue| {
      //log(&format!["called back err={:?} value={:?}", err, value]);
      task::block_on(async {
        Self::call(state, err, value).await;
      });
    }) as Box<dyn FnOnce(JsValue, JsValue)>)
  }
  async fn call(state: Arc<Mutex<ErrBackState>>, err: JsValue, value: JsValue) -> () {
    let mut s = state.lock().await;
    if err.is_falsy() {
      s.result = Some(Ok(value))
    } else {
      s.result = Some(Err(err.into()))
    }
    if let Some(waker) = s.waker.take() {
      waker.wake();
    }
  }
}
