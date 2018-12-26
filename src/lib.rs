extern crate random_access_storage;
extern crate failure;
extern crate bincode;
extern crate serde;

mod meta;
mod point;
mod tree;
pub use point::{Point,Scalar};
pub use tree::Tree;

use random_access_storage::RandomAccess;
use failure::Error;
use serde::{Serialize,de::DeserializeOwned};
use meta::Meta;
use std::marker::PhantomData;

pub trait Value: Serialize+DeserializeOwned+'static {}
impl<T> Value for T where T: Serialize+DeserializeOwned+'static {}

pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(P,V)
}

pub struct DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  open_store: U,
  meta_store: S,
  trees: Vec<Tree<S,P,V>>,
  meta: Meta,
  _marker: PhantomData<(P,V)>,
}

impl<S,U,P,V> DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  pub fn open(open_store: U) -> Result<Self,Error> {
    unimplemented!();
  }
  pub fn batch (&mut self, rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    unimplemented!();
  }
  pub fn query (&mut self, query: (P,P)) -> Result<(),Error> {
    // todo: iterator return type
    unimplemented!();
  }
}
