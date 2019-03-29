use point::Point;
use data::DataBatch;
use ::{Value};
use std::cmp::Ordering;
use std::mem::size_of;
use std::rc::Rc;
use std::cell::RefCell;
use failure::{Error,bail,format_err};
use pivots;

#[derive(Clone)]
pub enum Node<'a,D,P,V> where D: DataBatch<P,V>, P: Point, V: Value {
  Empty,
  Branch(Branch<'a,D,P,V>),
  Data(u64)
}

#[derive(Clone)]
pub struct Data<'a,P,V> where P: Point, V: Value {
  pub offset: u64,
  bucket: Vec<usize>,
  rows: &'a Vec<((P,V),u64)>
}

#[derive(Clone)]
pub struct Branch<'a,D,P,V> where D: DataBatch<P,V>, P: Point, V: Value {
  pub offset: u64,
  level: usize,
  max_data_size: usize,
  order: Rc<Vec<usize>>,
  data_batch: Rc<RefCell<D>>,
  bucket: Vec<usize>,
  buckets: Vec<Vec<usize>>,
  rows: &'a Vec<((P,V),u64)>,
  pivots: Vec<P>,
  sorted: Vec<usize>,
  intersecting: Vec<Vec<usize>>,
  matched: Vec<bool>
}

impl<'a,D,P,V> Branch<'a,D,P,V> where D: DataBatch<P,V>, P: Point, V: Value {
  pub fn new (level: usize, max_data_size: usize,
  order: Rc<Vec<usize>>, data_batch: Rc<RefCell<D>>,
  bucket: Vec<usize>, rows: &'a Vec<((P,V),u64)>)
  -> Result<Self,Error> {
    let n = order.len();
    let bf = (n+3)/2;
    if bucket.len() < 2 {
      bail!["not enough records to construct pivots. need at least 2, found {}",
        bucket.len()];
    }
    let mut sorted: Vec<usize> = (0..bucket.len()).collect();
    sorted.sort_unstable_by(|a,b| {
      (rows[bucket[*a]].0).0.cmp_at(&(rows[bucket[*b]].0).0, level)
    });
    let mut pivots: Vec<P> = {
      let z = n.min(sorted.len()-1);
      (0..z).map(|k| {
        let m = (k+1) * sorted.len() / (z+1);
        let a = &(rows[bucket[sorted[m+0]]].0);
        let b = &(rows[bucket[sorted[m+1]]].0);
        a.0.midpoint_upper(&b.0)
      }).collect()
    };
    // sometimes the sorted intervals overlap.
    // sort again to make sure the pivots are always in ascending order
    pivots.sort_unstable_by(|a,b| {
      a.cmp_at(b, level)
    });
    { // remove duplicate pivots in already sorted vec
      let mut i = 0;
      while i < pivots.len()-1 {
        while i < pivots.len()-1
        && pivots[i].cmp_at(&pivots[i+1], level) == Ordering::Equal {
          pivots.remove(i+1);
        }
        i += 1;
      }
    }
    // pad out pivots so there's exactly n elements
    if pivots.len() < n {
      pivots = pivots::pad(&pivots, n);
    }
    let blen = bucket.len();
    Ok(Self {
      offset: 0,
      max_data_size,
      level,
      order,
      data_batch,
      bucket,
      buckets: vec![vec![];bf],
      rows,
      pivots,
      sorted,
      intersecting: vec![vec![];n],
      matched: vec![false;blen]
    })
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
  -> Result<(Vec<u8>,Vec<Node<'a,D,P,V>>),Error> {
    let order = &self.order;
    let n = order.len();
    let bf = (n+3)/2;
    for i in self.order.iter() {
      let pivot = self.pivots[*i];
      for j in self.sorted.iter() {
        let row = self.rows[self.bucket[*j]];
        if self.matched[*j] { continue }
        if (row.0).0.cmp_at(&pivot, self.level) == Ordering::Equal {
          self.matched[*j] = true;
          self.intersecting[*i].push(self.bucket[*j]);
        }
      }
    }
    let mut j = 0;
    for i in self.sorted.iter() {
      if self.matched[*i] { continue }
      let row = self.rows[self.bucket[*i]];
      loop {
        if j == bf-1 { break }
        let pivot = self.pivots[j*2];
        match (row.0).0.cmp_at(&pivot, self.level) {
          Ordering::Less => { break },
          Ordering::Greater => j += 1,
          Ordering::Equal => panic!["bucket interval intersects pivot"]
        }
      }
      self.buckets[j].push(self.bucket[*i]);
    }
    let mut nodes = Vec::with_capacity(bf + n);
    let mut bitfield: Vec<bool> = vec![];

    ensure_eq!(self.intersecting.len(), n, "unexpected intersecting length");
    ensure_eq!(self.buckets.len(), bf, "unexpected bucket length");
    for ref buckets in [&self.intersecting,&self.buckets].iter() {
      for bucket in buckets.iter() {
        let mut size = 0u64;
        for b in bucket.iter() { size += self.rows[*b].1 }

        if bucket.is_empty() {
          nodes.push(Node::Empty);
          bitfield.push(false);
        } else if size as usize <= self.max_data_size || bucket.len() <= 3 {
          let mut dstore = self.data_batch.try_borrow_mut()?;
          nodes.push(Node::Data(
            dstore.batch(&bucket.iter().map(|b| {
              &self.rows[*b].0
            }).collect())?
          ));
          bitfield.push(true);
        } else {
          let mut b = Branch::new(
            self.level+1, self.max_data_size,
            Rc::clone(&self.order),
            Rc::clone(&self.data_batch),
            bucket.clone(), self.rows
          )?;
          b.alloc(alloc);
          nodes.push(Node::Branch(b));
          bitfield.push(false);
        }
      }
    }
    ensure_eq!(nodes.len(), n+bf, "incorrect number of nodes");
    ensure_eq!(self.pivots.len(), n, "incorrect number of pivots");
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
      for j in 0..8.min(n+bf-i*8) {
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
    ensure_eq!(data.len(), len, "incorrect data length");
    Ok((data,nodes))
  }
}
