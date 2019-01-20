extern crate random_access_storage;
extern crate failure;
extern crate bincode;
extern crate serde;

mod meta;
mod point;
mod tree;
mod branch;
mod staging;
mod planner;

use staging::{Staging,StagingIterator};
use planner::Planner;
pub use point::{Point,Scalar};
pub use tree::{Tree,TreeIterator};

use random_access_storage::RandomAccess;
use failure::Error;
use serde::{Serialize,de::DeserializeOwned};
use meta::Meta;
use std::marker::PhantomData;

enum SubIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  Tree(TreeIterator<'a,'b,S,P,V>),
  Staging(StagingIterator<'a,'b,P,V>)
}

pub trait Value: Copy+Serialize+DeserializeOwned+'static {}
impl<T> Value for T where T: Copy+Serialize+DeserializeOwned+'static {}

#[derive(Clone)]
pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(P,V)
}

pub struct DB<'a,S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  open_store: U,
  order: Vec<usize>,
  trees: Vec<Tree<'a,S,P,V>>,
  staging: Staging<S,P,V>,
  meta: Meta<'a,S>,
  _marker: PhantomData<(P,V)>,
}

impl<'a,S,U,P,V> DB<'a,S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  pub fn open(open_store: U) -> Result<Self,Error> {
    let meta = Meta::open(Box::leak(Box::new(open_store("meta")?)))?;
    let staging = Staging::open(open_store("staging")?)?;
    let bf = 8;
    let order = <Tree<S,P,V>>::pivot_order(bf);
    Ok(Self {
      open_store,
      staging,
      order,
      meta: meta,
      trees: vec![],
      _marker: PhantomData
    })
  }
  pub fn batch (&mut self, rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    /*
    let mut store = (self.open_store)("tree0")?;
    let inserts = rows.iter()
      .filter(|row| {
        match row { Row::Insert(_p,_v) => true, _ => false }
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
    */
    let base = 8_u32.pow(4);
    let n = self.staging.len()? + rows.len();
    if n > base as usize {
      unimplemented![];
    } else {
      self.staging.batch(rows)?;
    }
    Ok(())
  }
  fn get_tree (&'a mut self, index: usize) -> Result<&Tree<'a,S,P,V>,Error> {
    for i in self.trees.len()..index+1 {
      let store = (self.open_store)(&format!("tree{}",i))?;
      self.trees.push(Tree::open(store, 8, 100, &self.order)?);
    }
    Ok(&self.trees[index])
  }
  pub fn query<'b> (&'a mut self, bbox: &'b P::BBox)
  -> Result<QueryIterator<'a,'b,S,P,V>,Error> {
    QueryIterator::new(self, bbox)
  }
}

pub struct QueryIterator<'a,'b,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  index: usize,
  queries: Vec<SubIterator<'a,'b,S,P,V>>
}

impl<'a,'b,S,P,V> QueryIterator<'a,'b,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new<U> (db: &'a mut DB<'a,S,U,P,V>, bbox: &'b P::BBox)
  -> Result<Self,Error>
  where U: (Fn(&str) -> Result<S,Error>) {
    let mut mask: Vec<bool> = vec![];
    for tree in db.trees.iter_mut() {
      mask.push(tree.is_empty()?);
    }
    let mut queries: Vec<SubIterator<'a,'b,S,P,V>>
      = Vec::with_capacity(1+db.trees.len());
    queries.push(SubIterator::Staging(db.staging.query(bbox)));
    let exq: Vec<SubIterator<'a,'b,S,P,V>>
      = db.trees.iter_mut().enumerate()
        .filter(|(i,_tree)| { mask[*i] })
        .map(|(_i,tree)| { SubIterator::Tree(tree.query(bbox)) })
        .collect();
    queries.extend(exq);
    Ok(Self { queries, index: 0 })
  }
}

impl<'a,'b,S,P,V> Iterator for QueryIterator<'a,'b,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  type Item = Result<(P,V),Error>;
  fn next (&mut self) -> Option<Self::Item> {
    while !self.queries.is_empty() {
      let len = self.queries.len();
      {
        let q = &mut self.queries[self.index];
        let next = match q {
          SubIterator::Tree(x) => x.next(),
          SubIterator::Staging(x) => x.next()
        };
        match next {
          Some(result) => {
            self.index = (self.index+1) % len;
            return Some(result);
          },
          None => {}
        }
      }
      self.queries.remove(self.index);
      self.index = self.index % len;
    }
    None
  }
}
