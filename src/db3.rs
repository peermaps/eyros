use random_access_storage::RandomAccess;
use failure::Error;

use meta::Meta;
use ::Coord;

use std::fmt::Debug;
use std::marker::{PhantomData,Copy};
use std::ops::{Div,Add};

#[derive(Debug)]
pub enum Row3<A,B,C,V> {
  Insert(Coord<A>,Coord<B>,Coord<C>,V),
  Delete(Coord<A>,Coord<B>,Coord<C>,V)
}

#[derive(Debug)]
struct Branch<T> where
T: PartialOrd+Copy+Debug+From<u8>+Div<T,Output=T>+Add<T,Output=T> {
  sorted: Vec<usize>,
  intersecting: Vec<usize>,
  pivots: Vec<T>,
  pub buckets: Vec<Vec<usize>>
}

impl<T> Branch<T> where
T: PartialOrd+Copy+Debug+From<u8>+Div<T,Output=T>+Add<T,Output=T> {
  pub fn new (branch_factor: usize, order: &Vec<usize>, rows: &Vec<&Coord<T>>)
  -> Self {
    let mut sorted: Vec<usize> = (0..rows.len()).collect();
    sorted.sort_unstable_by(|a,b| {
      Self::cmp(rows[*a], rows[*b])
    });
    let n = branch_factor*2-1;
    let pivots = (0..n).map(|k| {
      let m = (k+1)*sorted.len()/(n+1);
      let a = rows[sorted[m]];
      let b = rows[sorted[m+1]];
      (*(Self::upper(a)) + *(Self::upper(b))) / 2.into()
    }).collect();
    let intersecting = vec![];
    let buckets = vec![];
    Self {
      sorted,
      intersecting,
      pivots,
      buckets
    }
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
A: PartialOrd+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
C: PartialOrd+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
S: Debug+RandomAccess<Error=Error> {
  branch_factor: usize,
  store: S,
  _marker0: PhantomData<A>,
  _marker1: PhantomData<B>,
  _marker2: PhantomData<C>,
  _marker3: PhantomData<V>,
}

impl<S,A,B,C,V> Tree<S,A,B,C,V> where
A: PartialOrd+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
C: PartialOrd+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
S: Debug+RandomAccess<Error=Error> {
  pub fn new (branch_factor: usize, store: S) -> Self {
    Self {
      branch_factor,
      store,
      _marker0: PhantomData,
      _marker1: PhantomData,
      _marker2: PhantomData,
      _marker3: PhantomData
    }
  }
  pub fn build(&mut self, rows: &Vec<((Coord<A>,Coord<B>,Coord<C>),V)>)
  -> Result<(),Error> {
    let nlevels = ((rows.len() as f32).log(self.branch_factor as f32)) as u32;
    let order = Self::pivot_order(rows.len());
    let first_branch = Branch::new(
      self.branch_factor, &order, &rows.iter().map(|r| { &(r.0).0 }).collect()
    );
    let mut branches0 = vec![first_branch];
    let mut branches1 = vec![];
    let mut branches2 = vec![];

    for i in 0..nlevels {
      match i%3 {
        0 => {
          for b in branches0.iter() {
            branches1.extend(b.buckets.iter().map(|bs| {
              Branch::new(self.branch_factor, &order,
                &bs.iter().map(|b| { &(rows[*b].0).1 }).collect())
            }));
          }
          branches0.clear();
        },
        1 => {
          for b in branches1.iter() {
            branches2.extend(b.buckets.iter().map(|bs| {
              Branch::new(self.branch_factor, &order,
                &bs.iter().map(|b| { &(rows[*b].0).2 }).collect())
            }));
          }
          branches1.clear();
        },
        _ => {
          for b in branches2.iter() {
            branches0.extend(b.buckets.iter().map(|bs| {
              Branch::new(self.branch_factor, &order,
                &bs.iter().map(|b| { &(rows[*b].0).0 }).collect())
            }));
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
}

pub struct DB3<S,U,A,B,C,V> where
A: PartialOrd+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
C: PartialOrd+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
S: Debug+RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  open_store: U,
  meta_store: S,
  trees: Vec<Tree<S,A,B,C,V>>,
  meta: Meta
}

impl<S,U,A,B,C,V> DB3<S,U,A,B,C,V> where
A: PartialOrd+Copy+Debug+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Copy+Debug+From<u8>+Div<B,Output=B>+Add<B,Output=B>,
C: PartialOrd+Copy+Debug+From<u8>+Div<C,Output=C>+Add<C,Output=C>,
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
