extern crate random_access_storage;
extern crate failure;
extern crate bincode;
extern crate serde;

mod meta;
mod point;
mod tree;
mod branch;
//mod staging;
pub use point::{Point,Scalar};
pub use tree::Tree;

use random_access_storage::RandomAccess;
use failure::Error;
use serde::{Serialize,de::DeserializeOwned};
use meta::Meta;
use std::marker::PhantomData;

pub trait Value: Copy+Serialize+DeserializeOwned+'static {}
impl<T> Value for T where T: Copy+Serialize+DeserializeOwned+'static {}

pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(P,V)
}

pub struct DB<'a,S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  open_store: U,
  trees: Vec<Tree<'a,S,P,V>>,
  meta: &'a Meta<'a,S>,
  _marker: PhantomData<(P,V)>,
}

impl<'a,S,U,P,V> DB<'a,S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  pub fn open(open_store: U) -> Result<Self,Error> {
    let meta = Meta::open(Box::leak(Box::new(open_store("meta")?)))?;
    Ok(Self {
      open_store,
      meta: Box::leak(Box::new(meta)),
      trees: vec![],
      _marker: PhantomData
    })
  }
  pub fn batch (&mut self, rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    let mut store = (self.open_store)("tree0")?;
    let bf = 8;
    let order = <Tree<S,P,V>>::pivot_order(bf);
    let inserts = rows.iter()
      .filter(|row| {
        match row { Row::Insert(p,v) => true, _ => false }
      })
      .map(|row| {
        match row {
          Row::Insert(p,v) => (*p,*v),
          _ => panic!("unknown problem")
        }
      })
      .collect()
    ;
    let mut tree = Tree::open(&mut store, bf, 100, &order)?;
    tree.build(&inserts)?;
    Ok(())
  }
  pub fn query (&mut self, bbox: P::BBox) -> QueryIterator<S,P,V> {
    QueryIterator::new(bbox)
  }
}

pub struct QueryIterator<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  _marker: PhantomData<(S,P,V)>
}

impl<S,P,V> QueryIterator<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (bbox: P::BBox) -> Self {
    Self { _marker: PhantomData }
  }
}

impl<S,P,V> Iterator for QueryIterator<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  type Item = Result<(P,V),Error>;
  fn next (&mut self) -> Option<Self::Item> {
    None
  }
}
