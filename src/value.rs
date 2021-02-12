use desert::{ToBytes,CountBytes,FromBytes};
use core::{fmt::Debug,hash::Hash};

pub trait Value: Clone+Hash+Debug+Send+Sync+'static+ToBytes+CountBytes+FromBytes {
  type Id: Clone+Hash+Eq+Debug+Send+Sync+'static;
  fn get_id(&self) -> Self::Id;
}

macro_rules! def_value {
  ($T:ident) => {
    impl Value for $T {
      type Id = $T;
      fn get_id(&self) -> $T { self.clone() }
    }
  }
}

def_value![u8];
def_value![u16];
def_value![u32];
def_value![u64];
def_value![i8];
def_value![i16];
def_value![i32];
def_value![i64];

impl<T> Value for Vec<T> where T: Value+Clone+Eq {
  type Id = Vec<T>;
  fn get_id(&self) -> Self { self.clone() }
}
