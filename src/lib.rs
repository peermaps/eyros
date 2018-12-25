extern crate random_access_storage;
extern crate failure;
extern crate bincode;
extern crate serde;

mod meta;
mod point;
mod tree;
pub use point::Point;
pub use tree::Tree;

use random_access_storage::RandomAccess;
use failure::Error;
use serde::{Serialize,de::DeserializeOwned};
use meta::Meta;
use std::marker::PhantomData;

pub struct DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point,
V: Serialize+DeserializeOwned+'static {
  open_store: U,
  meta_store: S,
  trees: Vec<Tree<S>>,
  meta: Meta,
  _marker0: PhantomData<P>,
  _marker1: PhantomData<V>
}

impl<S,U,P,V> DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point+Copy,
V: Serialize+DeserializeOwned+'static {
  pub fn open(open_store: U) -> Result<Self,Error> {
    unimplemented!();
  }
  pub fn batch (&mut self, rows: &Vec<(P,V)>) -> Result<(),Error> {
    unimplemented!();
  }
  pub fn query (&mut self, query: (P,P)) -> Result<(),Error> {
    // todo: iterator return type
    unimplemented!();
  }
}
