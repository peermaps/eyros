#![recursion_limit="1024"]

#[macro_use] mod ensure;
mod setup;
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
mod take_bytes;

pub use crate::setup::{Setup,SetupFields};
use crate::staging::{Staging,StagingIterator};
use crate::planner::plan;
pub use crate::point::{Point,Scalar};
pub use crate::tree::{Tree,TreeIterator,TreeOpts};
pub use crate::branch::Branch;
use crate::order::pivot_order;
pub use crate::data::{DataStore,DataRange};
use crate::take_bytes::TakeBytes;
use crate::meta::Meta;

use random_access_storage::RandomAccess;
use failure::{Error,format_err};
use serde::{Serialize,de::DeserializeOwned};
use std::fmt::Debug;
use std::cell::RefCell;
use std::rc::Rc;

enum SubIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  Tree(TreeIterator<'a,'b,S,P,V>),
  Staging(StagingIterator<'a,'b,P,V>)
}

pub trait Value: Debug+Clone+TakeBytes+Serialize+DeserializeOwned+'static {}
impl<T> Value for T where T: Debug+Clone+TakeBytes+Serialize+DeserializeOwned+'static {}

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
  pub trees: Vec<Tree<S,P,V>>,
  order: Rc<Vec<usize>>,
  pub staging: Staging<S,P,V>,
  pub data_store: Rc<RefCell<DataStore<S,P,V>>>,
  meta: Meta<S>,
  pub fields: SetupFields,
  pub bincode: Rc<bincode::Config>
}

impl<S,U,P,V> DB<S,U,P,V> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>),
P: Point, V: Value {
  pub fn open(open_store: U) -> Result<Self,Error> {
    Setup::new(open_store).build()
  }
  pub fn open_from_setup(setup: Setup<S,U>) -> Result<Self,Error> {
    let meta = Meta::open((setup.open_store)("meta")?)?;
    let staging = Staging::open((setup.open_store)("staging")?)?;
    let mut bcode = bincode::config();
    bcode.big_endian();
    let r_bcode = Rc::new(bcode);
    let data_store = DataStore::open(
      (setup.open_store)("data")?,
      (setup.open_store)("range")?,
      setup.fields.max_data_size,
      setup.fields.bbox_cache_size,
      setup.fields.data_list_cache_size,
      Rc::clone(&r_bcode)
    )?;
    let bf = setup.fields.branch_factor;
    let mut db = Self {
      open_store: setup.open_store,
      staging,
      bincode: Rc::clone(&r_bcode),
      data_store: Rc::new(RefCell::new(data_store)),
      order: Rc::new(pivot_order(bf)),
      meta: meta,
      trees: vec![],
      fields: setup.fields
    };
    for i in 0..db.meta.mask.len() {
      db.create_tree(i)?;
    }
    Ok(db)
  }
  pub fn batch (&mut self, rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    let n = (self.staging.rows.len() + rows.len()) as u64;
    let base = self.fields.base_size as u64;
    if n <= base {
      self.staging.batch(rows)?;
      self.staging.commit()?;
      return Ok(())
    }
    let count = (n/base)*base;
    let rem = n - count;
    let mut mask = vec![];
    for tree in self.trees.iter_mut() {
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
    self.staging.commit()?;
    {
      let mut dstore = self.data_store.try_borrow_mut()?;
      dstore.commit()?;
    }
    self.meta.save()?;
    Ok(())
  }
  fn create_tree (&mut self, index: usize) -> Result<(),Error> {
    for i in self.trees.len()..index+1 {
      let store = (self.open_store)(&format!("tree{}",i))?;
      self.trees.push(Tree::open(TreeOpts {
        store,
        index,
        data_store: Rc::clone(&self.data_store),
        order: Rc::clone(&self.order),
        bincode: Rc::clone(&self.bincode),
        branch_factor: self.fields.branch_factor,
        max_data_size: self.fields.max_data_size
      })?);
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
