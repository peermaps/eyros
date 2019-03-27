#![feature(int_to_from_bytes)]

extern crate random_access_storage;
extern crate failure;
extern crate bincode;
extern crate serde;

#[macro_use] mod ensure;
mod builder;
mod meta;
mod point;
mod tree;
mod branch;
mod staging;
mod planner;
mod bits;
mod order;
mod data;
mod read_block;
mod pivots;
mod write_cache;

pub use builder::Setup;
use staging::{Staging,StagingIterator};
use planner::plan;
pub use point::{Point,Scalar};
pub use tree::{Tree,TreeIterator};
pub use branch::Branch;
use order::pivot_order;
use data::DataStore;

use random_access_storage::RandomAccess;
use failure::{Error,format_err,ensure};
use serde::{Serialize,de::DeserializeOwned};
use meta::Meta;
use std::fmt::Debug;
use std::cell::RefCell;
use std::rc::Rc;

enum SubIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  Tree(TreeIterator<'a,'b,S,P,V>),
  Staging(StagingIterator<'a,'b,P,V>)
}

pub trait Value: Debug+Copy+Serialize+DeserializeOwned+'static {}
impl<T> Value for T where T: Debug+Copy+Serialize+DeserializeOwned+'static {}

#[derive(Clone,Debug)]
pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(P,V)
}

pub struct DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  open_store: U,
  branch_factor: usize,
  pub trees: Vec<Tree<S,P,V>>,
  order: Rc<Vec<usize>>,
  pub staging: Staging<S,P,V>,
  pub data_store: Rc<RefCell<DataStore<S,P,V>>>,
  max_data_size: usize,
  base_size: usize,
  meta: Meta<S>
}

impl<S,U,P,V> DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  pub fn open(open_store: U) -> Result<Self,Error> {
    Setup::new(open_store).build()
  }
  pub fn open_opts(open_store: U, bf: usize,
  max_data_size: usize, base_size: usize) -> Result<Self,Error> {
    let meta = Meta::open(open_store("meta")?)?;
    let staging = Staging::open(open_store("staging")?)?;
    let data_store = DataStore::open(open_store("data")?)?;
    /*
    let n = bf*2-3;
    if max_data_size <= n {
      bail!["max_data_size must be greater than {} for branch_factor={}", n, bf]
    }
    */
    ensure![base_size > max_data_size,
      "base_size ({}) must be > max_data_size ({})", base_size, max_data_size];
    let mut db = Self {
      open_store,
      branch_factor: bf,
      staging,
      data_store: Rc::new(RefCell::new(data_store)),
      order: Rc::new(pivot_order(bf)),
      meta: meta,
      trees: vec![],
      max_data_size,
      base_size
    };
    for i in 0..db.meta.mask.len() {
      db.create_tree(i)?;
    }
    Ok(db)
  }
  pub fn batch (&mut self, rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    let n = (self.staging.len()? + rows.len()) as u64;
    let base = self.base_size as u64;
    if n <= base {
      self.staging.batch(rows)?;
      self.staging.flush()?;
      return Ok(())
    }
    let count = (n/base)*base;
    let rem = n - count;
    let mut mask = vec![];
    for mut tree in self.trees.iter_mut() {
      mask.push(!tree.is_empty()?);
    }
    let p = plan(
      &bits::num_to_bits(n/base),
      &mask
    );
    let mut offset = 0;
    let slen = self.staging.rows.len();
    for (i,staging,trees) in p {
      let mut irows: Vec<(usize,usize)> = vec![];
      for j in staging {
        let size = (2u64.pow(j as u32) * base) as usize;
        irows.push((offset,offset+size));
        offset += size;
      }
      for t in trees.iter() {
        self.create_tree(*t)?;
      }
      self.create_tree(i)?;
      for _ in self.meta.mask.len()..i+1 {
        self.meta.mask.push(false);
      }
      let mut srows: Vec<Row<P,V>> = vec![];
      for (i,j) in irows {
        for k in i..j {
          srows.push(
            if k < slen { self.staging.rows[k].clone() }
            else { rows[k-slen].clone() }
          );
        }
      }
      if trees.is_empty() {
        self.meta.mask[i] = true;
        self.trees[i].build(&srows)?;
      } else {
        self.meta.mask[i] = true;
        for t in trees.iter() {
          self.meta.mask[*t] = false;
        }
        Tree::merge(&mut self.trees, i, trees, &srows)?;
      }
    }
    ensure_eq!(n-(offset as u64), rem, "offset-n ({}-{}={}) != rem ({}) ",
      offset, n, (offset as u64)-n, rem);
    let mut rem_rows = vec![];
    for k in offset..n as usize {
      rem_rows.push(
        if k < slen { self.staging.rows[k].clone() }
        else { rows[k-slen].clone() }
      );
    }
    ensure_eq!(rem_rows.len(), rem as usize,
      "unexpected number of remaining rows (expected {}, actual {})",
      rem, rem_rows.len());
    self.staging.clear()?;
    self.staging.batch(&rem_rows)?;
    self.staging.flush()?;
    {
      let mut dstore = self.data_store.try_borrow_mut()?;
      dstore.flush()?;
    }
    self.meta.save()?;
    Ok(())
  }
  fn create_tree (&mut self, index: usize) -> Result<(),Error> {
    for i in self.trees.len()..index+1 {
      let store = (self.open_store)(&format!("tree{}",i))?;
      self.trees.push(Tree::open(store, Rc::clone(&self.data_store),
        self.branch_factor, self.max_data_size, Rc::clone(&self.order))?);
    }
    Ok(())
  }
  pub fn query<'a,'b> (&'a mut self, bbox: &'b P::Bounds)
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
  pub fn new<U> (db: &'a mut DB<S,U,P,V>, bbox: &'b P::Bounds)
  -> Result<Self,Error>
  where U: (Fn(&str) -> Result<S,Error>) {
    let mut mask: Vec<bool> = vec![];
    for tree in db.trees.iter_mut() {
      mask.push(!tree.is_empty()?);
    }
    let mut queries: Vec<SubIterator<'a,'b,S,P,V>>
      = Vec::with_capacity(1+db.trees.len());
    queries.push(SubIterator::Staging(db.staging.query(bbox)));
    for (i,tree) in db.trees.iter_mut().enumerate() {
      if !mask[i] { continue }
      queries.push(SubIterator::Tree(tree.query(bbox)?));
    }
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
      if self.queries.len() > 0 {
        self.index = self.index % self.queries.len();
      }
    }
    None
  }
}
