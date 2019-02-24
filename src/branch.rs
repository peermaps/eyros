use point::Point;
use data::DataStore;
use ::{Value};
use std::cmp::Ordering;
use std::mem::size_of;
use std::rc::Rc;
use std::cell::RefCell;
use bincode::{serialize};
use failure::Error;
use random_access_storage::RandomAccess;

#[derive(Clone)]
pub enum Node<'a,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  Empty,
  Branch(Branch<'a,S,P,V>),
  Data(u64)
}

#[derive(Clone)]
pub struct Data<'a,P,V> where P: Point, V: Value {
  pub offset: u64,
  bucket: Vec<usize>,
  rows: &'a Vec<(P,V)>
}

#[derive(Clone)]
pub struct Branch<'a,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub offset: u64,
  level: usize,
  max_data_size: usize,
  order: Rc<Vec<usize>>,
  data_store: Rc<RefCell<DataStore<S,P,V>>>,
  bucket: Vec<usize>,
  buckets: Vec<Vec<usize>>,
  rows: &'a Vec<(P,V)>,
  pivots: Vec<P>,
  sorted: Vec<usize>,
  intersecting: Vec<Vec<usize>>,
  matched: Vec<bool>
}

impl<'a,S,P,V> Branch<'a,S,P,V> where
S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (level: usize, max_data_size: usize,
  order: Rc<Vec<usize>>, data_store: Rc<RefCell<DataStore<S,P,V>>>,
  bucket: Vec<usize>, rows: &'a Vec<(P,V)>)
  -> Self {
    let n = order.len();
    let bf = (n+3)/2;
    let mut sorted: Vec<usize> = (0..bucket.len()).collect();
    sorted.sort_unstable_by(|a,b| {
      rows[bucket[*a]].0.cmp_at(&rows[bucket[*b]].0, level as usize)
    });
    let pivots: Vec<P> = (0..n).map(|k| {
      let m = ((k+2)*sorted.len()/(n+1)).min(sorted.len()-2);
      let a = &rows[bucket[sorted[m+0]]];
      let b = &rows[bucket[sorted[m+1]]];
      a.0.midpoint_upper(&b.0)
    }).collect();
    let mut intersecting = vec![vec![];n];
    let mut matched = vec![false;bucket.len()];
    for i in order.iter() {
      let pivot = pivots[*i];
      for j in sorted.iter() {
        let row = rows[bucket[*j]];
        if matched[*j] { continue }
        if row.0.cmp_at(&pivot, level as usize) == Ordering::Equal {
          matched[*j] = true;
          intersecting[*i].push(*j);
        }
      }
    }
    Self {
      offset: 0,
      max_data_size,
      level,
      order,
      data_store,
      bucket,
      buckets: Vec::with_capacity(bf),
      rows,
      pivots,
      sorted,
      intersecting,
      matched
    }
  }
  pub fn alloc (&mut self, alloc: &mut FnMut (usize) -> u64) -> () {
    self.offset = alloc(self.bytes());
  }
  fn bytes (&self) -> usize {
    let n = self.pivots.len();
    let bf = (n+3)/2;
    4 // len
      + n*P::pivot_size_at(self.level % P::dim()) // P
      + (n+bf+7)/8 // D
      + (n+bf)*size_of::<u64>() // I+B
  }
  pub fn build (&mut self, alloc: &mut FnMut (usize) -> u64)
  -> Result<(Vec<u8>,Vec<Node<'a,S,P,V>>),Error> {
    let order = &self.order;
    let n = order.len();
    let bf = (n+3)/2;
    self.buckets = vec![vec![];bf];
    let mut j = 0;
    let mut pivot = self.pivots[order[bf-2]];
    for i in self.sorted.iter() {
      if self.matched[*i] { continue }
      let row = self.rows[self.bucket[*i]];
      while j < bf-2
      && row.0.cmp_at(&pivot, self.level as usize) != Ordering::Less {
        j = (j+1).min(bf-2);
        if j < bf-2 {
          pivot = self.pivots[order[j+bf-2]];
        }
      }
      self.buckets[j].push(*i);
    }
    let mut nodes = Vec::with_capacity(
      self.buckets.len() + self.intersecting.len());
    let mut bitfield: Vec<bool> = vec![];

    for ref buckets in [&self.intersecting,&self.buckets].iter() {
      for bucket in buckets.iter() {
        if bucket.is_empty() {
          nodes.push(Node::Empty);
          bitfield.push(false);
        } else if bucket.len() < self.max_data_size {
          let mut dstore = self.data_store.try_borrow_mut()?;
          nodes.push(Node::Data(
            dstore.batch(&bucket.iter()
              .map(|b| { &self.rows[*b] }).collect())?
          ));
          bitfield.push(true);
        } else {
          let mut b = Branch::new(
            self.level+1, self.max_data_size,
            Rc::clone(&self.order),
            Rc::clone(&self.data_store),
            bucket.clone(), self.rows
          );
          b.alloc(alloc);
          nodes.push(Node::Branch(b));
          bitfield.push(false);
        }
      }
    }
    assert_eq!(nodes.len(), n+bf, "incorrect number of nodes");
    assert_eq!(self.pivots.len(), n, "incorrect number of pivots");
    let len = self.bytes();
    let mut data: Vec<u8> = Vec::with_capacity(len);
    // length
    data.extend_from_slice(&(len as u32).to_be_bytes());
    // pivots
    for pivot in self.pivots.iter() {
      data.extend(pivot.serialize_at(self.level % P::dim())?);
    }
    // data bitfield
    for i in 0..(n+bf+7)/8 {
      let mut byte = 0u8;
      for j in 0..8 {
        byte += (1 << j) * (bitfield[i*8+j] as u8);
      }
      data.push(byte);
    }
    // intersecting + buckets
    for node in nodes.iter() {
      data.extend(&(match node {
        Node::Branch(b) => b.offset+1,
        Node::Data(d) => *d+1,
        Node::Empty => 0u64
      }).to_be_bytes());
    }
    assert_eq!(data.len(), len, "incorrect data length");
    Ok((data,nodes))
  }
}
