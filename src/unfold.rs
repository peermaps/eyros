// vendored version of futures::stream::unfold
// modified to use async_std

// The original source file from which this is derived is
// Copyright (c) 2016 Alex Crichton
// Copyright (c) 2017 The Tokio Authors

// and released under the MIT or APACHE license:
// https://github.com/rust-lang/futures-rs/blob/master/LICENSE-MIT
// https://github.com/rust-lang/futures-rs/blob/master/LICENSE-APACHE

use core::fmt;
use core::pin::Pin;
use futures_core::ready;
use async_std::future::Future;
use async_std::stream::{Stream};
use async_std::task::{Context, Poll};

pub fn unfold<T, F, Fut, It>(init: T, f: F) -> Unfold<T, F, Fut>
  where F: FnMut(T) -> Fut,
      Fut: Future<Output = Option<(It, T)>>,
{
  Unfold {
    f,
    state: Some(init),
    fut: None,
  }
}

pin_project_lite::pin_project!{
  /// Stream for the [`unfold`] function.
  #[must_use = "streams do nothing unless polled"]
  pub struct Unfold<T, F, Fut> {
    f: F,
    state: Option<T>,
    fut: Option<Pin<Box<Fut>>>,
  }
}

impl<T, F, Fut> fmt::Debug for Unfold<T, F, Fut>
where
  T: fmt::Debug,
  Fut: fmt::Debug,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Unfold")
      .field("state", &self.state)
      .field("fut", &self.fut)
      .finish()
  }
}

impl<T, F, Fut, It> Stream for Unfold<T, F, Fut>
  where F: FnMut(T) -> Fut,
      Fut: Future<Output = Option<(It, T)>>,
{
  type Item = It;

  fn poll_next(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Option<It>> {
    let this = self.project();
    if let Some(state) = this.state.take() {
      let fut = (this.f)(state);
      *this.fut = Some(Box::pin(fut));
    }

    let step = ready!(Pin::new(this.fut.as_mut().unwrap()).poll(cx));
    *this.fut = None;

    if let Some((item, next_state)) = step {
      *this.state = Some(next_state);
      Poll::Ready(Some(item))
    } else {
      Poll::Ready(None)
    }
  }
}
