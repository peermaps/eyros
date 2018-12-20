use random_access_storage::RandomAccess;
use failure::Error;
use bincode::{serialize,deserialize};
use serde::{Serialize};

use meta::Meta;
use ::Coord;

use std::fmt::Debug;
use std::marker::{PhantomData,Copy};
use std::ops::{Div,Add};
use std::mem::size_of;

type PSIZE = u64;

#[derive(Debug)]
pub enum Row3<A,B,C,V> {
  Insert(Coord<A>,Coord<B>,Coord<C>,V),
  Delete(Coord<A>,Coord<B>,Coord<C>,V)
}

#[derive(Debug)]
struct Branch<T> where
T: PartialOrd+Copy+Debug+From<u8>+Div<T,Output=T>+Add<T,Output=T> {
  sorted: Vec<usize>,
  branch_factor: usize,
  pub offset: u64,
  pub intersecting: Vec<(PSIZE,Vec<usize>)>,
  pub pivots: Vec<T>,
  pub buckets: Vec<(PSIZE,Vec<usize>)>,
  pub level: u32
}

impl<T> Branch<T> where
T: Serialize+PartialOrd+Copy+Debug+From<u8>+Div<T,Output=T>+Add<T,Output=T> {
  const MAX_DATA_SIZE: usize = 50;
  pub fn new (offset: u64, branch_factor: usize, level: u32, order: &Vec<usize>,
  rows: &Vec<&Coord<T>>) -> Self {
    let mut sorted: Vec<usize> = (0..rows.len()).collect();
    sorted.sort_unstable_by(|a,b| {
      Self::cmp(rows[*a], rows[*b])
    });
    let n = branch_factor*2-1;
    let pivots: Vec<T> = (0..n).map(|k| {
      let m = (k+1)*sorted.len()/(n+1);
      let a = rows[sorted[m]];
      let b = rows[sorted[m+1]];
      (*(Self::upper(a)) + *(Self::upper(b))) / 2.into()
    }).collect();
    let mut intersecting = vec![(0,vec![]);n];
    let mut buckets = vec![(0,vec![]);branch_factor+1];
    let mut matched = vec![0;(rows.len()+7)/8];
    for i in order {
      let pivot = pivots[*i];
      for j in sorted.iter() {
        let row = rows[*j];
        if (matched[(*j)/8]>>((*j)%8))&1 == 1 { continue }
        match row {
          Coord::Point(_) => {},
          Coord::Range(min,max) => {
            if *min <= pivot && pivot <= *max {
              matched[(*j)/8] |= 1<<((*j)%8);
              intersecting[*i].1.push(*j);
            }
          }
        }
      }
    }
    let mut j = 0;
    for i in sorted.iter() {
      if matched[(*i)/8]>>((*i)%8)&1 == 1 { continue }
      let row = rows[*i];
      for k in j..branch_factor {
        let pivot = pivots[order[order.len()-branch_factor+k]];
        if *Self::upper(&row) < pivot { break }
        j += 1;
      }
      buckets[j].1.push(*i);
    }
    Self {
      branch_factor,
      level,
      offset,
      sorted,
      intersecting,
      pivots,
      buckets
    }
  }
  pub fn frame_size<A,B,C> (count: usize, branch_factor: usize) -> PSIZE where
  A: Serialize+PartialOrd+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
  B: Serialize+PartialOrd+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
  C: Serialize+PartialOrd+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C> {
    if count <= Self::MAX_DATA_SIZE {
      return (size_of::<u32>() + size_of::<(A,B,C)>() * count) as PSIZE;
    }
    let npivots = branch_factor*2-1;
    let nbuckets = branch_factor+1;
    ((size_of::<T>() + size_of::<PSIZE>()) * npivots
      + size_of::<PSIZE>() * nbuckets) as PSIZE
  }
  pub fn write<S,U,A,B,C,V> (&mut self, tree: &mut Tree<S,A,B,C,V>, level: u32,
  rows: &Vec<((Coord<A>,Coord<B>,Coord<C>),V)>) -> Result<(),Error> where
  A: Serialize+PartialOrd+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
  B: Serialize+PartialOrd+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
  C: Serialize+PartialOrd+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
  U: Serialize+PartialOrd+Copy+Debug+From<u8>+Div<U,Output=U>+Add<U,Output=U>,
  V: Serialize,
  S: Debug+RandomAccess<Error=Error> {
    let intersecting = &mut self.intersecting;
    let buckets = &mut self.buckets;
    for i in 0..intersecting.len() {
      let len = intersecting[i].1.len();
      intersecting[i].0 = tree.allocate(<Branch<U>>::frame_size::<A,B,C>(
        len, self.branch_factor));
    }
    for i in 0..buckets.len() {
      let len = buckets[i].1.len();
      buckets[i].0 = tree.allocate(<Branch<U>>::frame_size::<A,B,C>(
        len, self.branch_factor));
    }
    let size = self.pivots.len()*size_of::<T>()
      + intersecting.len()*size_of::<PSIZE>()
      + buckets.len()*size_of::<PSIZE>()
    ;
    let mut data = Vec::with_capacity(size);
    // strip off the leading usize from pivots (constant value)
    // to save bytes
    data.extend_from_slice(
      &serialize(&self.pivots).unwrap()[size_of::<usize>()..]);
    let iaddrs: Vec<PSIZE> = intersecting.iter()
      .map(|b| { b.0 as PSIZE }).collect();
    data.extend_from_slice(
      &serialize(&iaddrs).unwrap()[size_of::<usize>()..]);
    let baddrs: Vec<PSIZE> = buckets.iter()
      .map(|b| { b.0 as PSIZE }).collect();
    data.extend_from_slice(
      &serialize(&baddrs).unwrap()[size_of::<usize>()..]);
    tree.store.write(self.offset as usize, &data)
  }
  fn cmp (a: &Coord<T>, b: &Coord<T>) -> std::cmp::Ordering {
    match (a,b) {
      (Coord::Point(ref pa),Coord::Point(ref pb)) => {
        match pa.partial_cmp(pb) {
          Some(x) => x,
          None => std::cmp::Ordering::Less
        }
      },
      (Coord::Range(_,ref pa),Coord::Range(_,ref pb)) => {
        match pa.partial_cmp(pb) {
          Some(x) => x,
          None => std::cmp::Ordering::Less
        }
      },
      _ => panic!("point/range mismatch")
    }
  }
  fn upper (x: &Coord<T>) -> &T {
    match x {
      Coord::Range(_, ref b) => b,
      Coord::Point(ref p) => p
    }
  }
}

struct Tree<S,A,B,C,V> where
A: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
C: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
S: Debug+RandomAccess<Error=Error> {
  branch_factor: usize,
  store: S,
  size: u64,
  _marker0: PhantomData<A>,
  _marker1: PhantomData<B>,
  _marker2: PhantomData<C>,
  _marker3: PhantomData<V>,
}

impl<S,A,B,C,V> Tree<S,A,B,C,V> where
A: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
C: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
S: Debug+RandomAccess<Error=Error> {
  pub fn new (branch_factor: usize, store: S) -> Self {
    Self {
      size: 0,
      branch_factor,
      store,
      _marker0: PhantomData,
      _marker1: PhantomData,
      _marker2: PhantomData,
      _marker3: PhantomData
    }
  }
  pub fn build(&mut self, rows: &Vec<((Coord<A>,Coord<B>,Coord<C>),V)>)
  -> Result<(),Error> where
  A: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
  B: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
  C: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
  V: Serialize {
    let bf = self.branch_factor;
    let nlevels = ((rows.len() as f32).log(bf as f32)) as u32;
    let order = Self::pivot_order(2*bf-1);
    let offset = self.allocate(
      <Branch<A>>::frame_size::<A,B,C>(rows.len(),bf)
    );
    let first_branch = Branch::new(
      offset, bf, 0, &order,
      &rows.iter().map(|r| { &(r.0).0 }).collect()
    );
    let mut branches0 = vec![first_branch];
    let mut branches1 = vec![];
    let mut branches2 = vec![];

    for level in 0..nlevels {
      match level%3 {
        0 => {
          for i in 0..branches0.len() {
            branches0[i].write::<S,B,A,B,C,V>(self, level, rows)?;
            for bu in branches0[i].buckets.iter() {
              if bu.1.len() <= <Branch<B>>::MAX_DATA_SIZE {
                continue;
              }
              branches1.push(Branch::new(
                bu.0, bf, level+1, &order,
                &bu.1.iter().map(|b| { &(rows[*b].0).1 }).collect())
              );
            }
          }
          branches0.clear();
        },
        1 => {
          for i in 0..branches1.len() {
            branches1[i].write::<S,C,A,B,C,V>(self, level, rows)?;
            for bu in branches1[i].buckets.iter() {
              if bu.1.len() <= <Branch<C>>::MAX_DATA_SIZE {
                continue;
              }
              branches2.push(Branch::new(
                bu.0, bf, level+1, &order,
                &bu.1.iter().map(|b| { &(rows[*b].0).2 }).collect())
              );
            }
          }
          branches1.clear();
        },
        _ => {
          for i in 0..branches2.len() {
            branches2[i].write::<S,A,A,B,C,V>(self, level, rows)?;
            for bu in branches2[i].buckets.iter() {
              if bu.1.len() <= <Branch<A>>::MAX_DATA_SIZE {
                continue;
              }
              branches0.push(Branch::new(
                bu.0, bf, level+1, &order,
                &bu.1.iter().map(|b| { &(rows[*b].0).0 }).collect())
              );
            }
          }
          branches2.clear();
        }
      }
    }
    Ok(())
  }
  fn pivot_order (n: usize) -> Vec<usize> {
    let mut order = Vec::with_capacity(n);
    for i in 0..((((n+1) as f32).log2()) as usize) {
      let m = 2usize.pow(i as u32);
      for j in 0..m {
        order.push(n/(m*2) + j*(n+1)/m);
      }
    }
    order
  }
  pub fn allocate (&mut self, size: PSIZE) -> PSIZE {
    let i = self.size;
    self.size += size;
    i
  }
}

pub struct DB3<S,U,A,B,C,V> where
A: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
C: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
S: Debug+RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  open_store: U,
  meta_store: S,
  trees: Vec<Tree<S,A,B,C,V>>,
  meta: Meta
}

impl<S,U,A,B,C,V> DB3<S,U,A,B,C,V> where
A: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
C: PartialOrd+Serialize+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
V: Serialize,
S: Debug+RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  pub fn open(open_store: U) -> Result<Self,Error> {
    let mut meta_store = open_store("meta")?;
    let branch_factor = 8;
    let meta = match meta_store.is_empty()? {
      true => Meta { staging_size: 0, branch_factor, mask: vec![] },
      false => {
        let len = meta_store.len()?;
        let buf = meta_store.read(0, len)?;
        Meta::from_buffer(&buf)?
      }
    };
    let mut trees = vec![];
    for (i,b) in meta.mask.iter().enumerate() {
      trees.push(Tree::new(
        branch_factor,
        open_store(&format!("tree{}",i))?
      ));
    }
    Ok(Self {
      meta,
      open_store,
      meta_store,
      trees
    })
  }
  fn get_tree(&mut self, i: usize) -> Result<&mut Tree<S,A,B,C,V>,Error> {
    if i < self.trees.len() { return Ok(&mut self.trees[i]) }
    let tree = Tree::new(
      self.meta.branch_factor,
      (self.open_store)(&format!("tree{}",i))?
    );
    self.trees.push(tree);
    return Ok(&mut self.trees[i]);
  }
  pub fn batch(&mut self, rows: &Vec<((Coord<A>,Coord<B>,Coord<C>),V)>)
  -> Result<(),Error> {
    let tree = self.get_tree(0)?;
    tree.build(rows)?;
    Ok(())
  }
  pub fn query(&mut self) -> Result<(),Error> {
    unimplemented!();
  }
}
