pub struct BatchFields {
  pub rebuild_depth: usize,
  pub error_if_missing: bool,
}
impl BatchFields {
  pub fn default() -> Self {
    BatchFields {
      rebuild_depth: 2,
      error_if_missing: true,
    }
  }
}

pub struct BatchOptions {
  pub fields: BatchFields,
}

impl BatchOptions {
  pub fn new() -> Self {
    Self { fields: BatchFields::default() }
  }
  pub fn rebuild_depth(mut self, depth: usize) -> Self {
    self.fields.rebuild_depth = depth;
    self
  }
  pub fn error_if_missing(mut self, x: bool) -> Self {
    self.fields.error_if_missing = x;
    self
  }
}

impl Default for BatchOptions {
  fn default() -> Self { Self::new() }
}
