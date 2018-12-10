use random_access_storage::RandomAccess;
use failure::Error;
use std::marker::PhantomData;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Tree<S,P,V> where
S: Debug+RandomAccess<Error=Error> {
  _marker0: PhantomData<P>,
  _marker1: PhantomData<V>,
  storage: S
}

impl<S,P,V> Tree<S,P,V> where
S: Debug+RandomAccess<Error=Error> {
  pub fn new (b: usize, storage: S) -> Self {
    Self {
      storage,
      _marker0: PhantomData,
      _marker1: PhantomData
    }
  }
}
