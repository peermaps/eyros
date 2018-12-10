use std::ops::{Div,Add};
use std::fmt::Debug;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct Data<T>
where T: From<u8>+Div<T,Output=T>+Add<T,Output=T>+Copy+Debug+PartialOrd {
  b: usize,
  size: usize,
  _marker: PhantomData<T>
}

impl<T> Data<T>
where T: From<u8>+Div<T,Output=T>+Add<T,Output=T>+Copy+Debug+PartialOrd {
  pub fn new (b: usize, size: usize) -> Self {
    Self {
      b,
      size,
      _marker: PhantomData
    }
  }
  pub fn alloc (&mut self, size: usize) -> usize {
    let offset = self.size;
    self.size += size;
    offset
  }
  pub fn interval (&mut self, intervals: &Vec<(T,T)>) -> IntervalFrame<T> {
    IntervalFrame::new(intervals, self.b)
  }
}

#[derive(Debug)]
pub struct IntervalFrame<T> {
  pivots: Vec<T>,
  buckets: Vec<Vec<(T,T)>>,
  centers: Vec<Vec<(T,T)>>
}

impl<T> IntervalFrame<T>
where T: From<u8>+Div<T,Output=T>+Add<T,Output=T>+Copy+Debug+PartialOrd {
  // intervals must be sorted by maximum coordinate
  pub fn new (intervals: &Vec<(T,T)>, b: usize) -> Self {
    let npivots = b*2-1;
    let len = intervals.len();
    let mut pivots = vec![0.into();npivots];
    for i in 0..npivots {
      let k = (i+1)*len/(npivots+1);
      let pivot = (intervals[k].1 + intervals[k+1].1) / 2.into();
      //let pivot = intervals[k].1 + T::epsilon();
      pivots[i] = pivot;
    }
    let order = pivot_order(npivots);
    let mut centers: Vec<Vec<(T,T)>> = (0..npivots).map(|_| vec![]).collect();
    let mut buckets: Vec<Vec<(T,T)>> = (0..npivots+1).map(|_| vec![]).collect();
    for iv in intervals {
      let mut matched = false;
      let mut bucket_i = npivots;
      for i in order.iter() {
        let pivot = pivots[*i];
        if iv.0 <= pivot && pivot <= iv.1 {
          centers[*i].push(*iv);
          matched = true;
          break;
        }
        if iv.1 < pivot && *i < bucket_i {
          bucket_i = *i;
        }
      }
      if !matched {
        buckets[bucket_i].push(*iv);
      }
    }
    Self { pivots, buckets, centers }
  }
  pub fn pack (&mut self) -> Vec<u8> {
    /*
    for b in self.buckets[0].iter() { println!("B {:?}", b) }
    for i in 0..self.pivots.len() {
      println!("# PIVOT {:?}", self.pivots[i]);
      for c in self.centers[i].iter() { println!("C {:?}", c) }
      for b in self.buckets[i+1].iter() { println!("B {:?}", b) }
    }
    */
    vec![]
  }
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
