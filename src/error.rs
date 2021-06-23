pub type Error = Box<dyn std::error::Error+Send+Sync>;
use std::backtrace::Backtrace;
use crate::TreeId;

#[derive(Debug)]
pub struct EyrosError {
  kind: EyrosErrorKind,
  backtrace: Backtrace,
}

#[derive(Debug)]
pub enum EyrosErrorKind {
  MetaBitfieldInsufficientBytes {},
  ScalarInBounds {},
  IntervalSides { dimension: usize, min: String, max: String },
  TreeRemoved { id: TreeId },
  TreeEmpty { id: TreeId, file: String },
  RemoveIdsMissing { ids: Vec<String> }
}

impl EyrosErrorKind {
  pub fn raise<T>(self) -> Result<T,Error> {
    Err(Box::new(EyrosError {
      kind: self,
      backtrace: Backtrace::capture(),
    }))
  }
}

impl std::error::Error for EyrosError {
  fn backtrace(&'_ self) -> Option<&'_ Backtrace> {
    Some(&self.backtrace)
  }
}

impl std::fmt::Display for EyrosError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match &self.kind {
      EyrosErrorKind::MetaBitfieldInsufficientBytes {} => {
        write![f, "not enough bytes to construct roots bitfield for Meta"]
      },
      EyrosErrorKind::ScalarInBounds {} => {
        write![f, "scalar found in bounds"]
      },
      EyrosErrorKind::IntervalSides { dimension, min, max } => {
        write![f, "!(min <= max) dimension {} for Coord::Interval({:?},{:?})",
          dimension, min, max]
      },
      EyrosErrorKind::TreeRemoved { id } => {
        write![f, "attempted to load tree scheduled for removal with id={}", id]
      },
      EyrosErrorKind::TreeEmpty { id, file } => {
        write![f, "tree with id={} located at file={} is empty", id, file]
      },
      EyrosErrorKind::RemoveIdsMissing { ids } => {
        write![f, "ids not found during remove(): {}", ids.join(", ")]
      },
    }
  }
}
