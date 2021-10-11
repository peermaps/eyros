pub trait Debugger {
  fn send(&mut self, _msg: &str);
}

impl<F> Debugger for F where F: FnMut(&str) {
  fn send(&mut self, msg: &str) {
    (self)(msg)
  }
}
