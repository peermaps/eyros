use random_access_storage::RandomAccess;
use failure::{Error,bail};
use std::marker::PhantomData;
use std::cmp::Ordering;

use point::Point;
use ::{Row,Value};

pub struct Tree<'a,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S,
  branch_factor: usize,
  max_data_size: usize,
  order: &'a Vec<usize>,
  _marker: PhantomData<(P,V)>
}

impl<'a,S,P,V> Tree<'a,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (store: S, branch_factor: usize, max_data_size: usize,
  order: &'a Vec<usize>) -> Self {
    Self {
      store,
      order,
      branch_factor,
      max_data_size,
      _marker: PhantomData
    }
  }
  pub fn build (&mut self, rows: &Vec<(P,V)>) -> Result<(),Error> {
    let bf = self.branch_factor;
    if rows.len() < bf*2-1 {
      bail!("tree must have at least {} records", bf*2-1);
    }
    let mut buckets: Vec<Vec<&(P,V)>>
      = vec![rows.iter().map(|row| row).collect()];
    let mut bucket_count = 0;
    let n = self.branch_factor*2-1;
    for level in 0.. {
      if buckets.is_empty() { break }
      let mut nbuckets = vec![];
      for bucket in buckets.iter() {
        if bucket.is_empty() { continue }
        if bucket.len() <= self.max_data_size {
          println!("todo: save {} records", bucket.len());
          continue;
        }
        let mut sorted: Vec<usize> = (0..bucket.len()).collect();
        sorted.sort_unstable_by(|a,b| {
          bucket[*a].0.cmp_at(&bucket[*b].0, level as usize)
        });
        let pivots: Vec<P> = (0..n).map(|k| {
          let m = ((k+2)*sorted.len()/(n+1)).min(sorted.len()-2);
          let a = &bucket[sorted[m+0]];
          let b = &bucket[sorted[m+1]];
          a.0.midpoint_upper(&b.0)
        }).collect();
        let mut intersecting = vec![vec![];n];
        let mut matched = vec![0;(bucket.len()+7)/8];
        for i in self.order.iter() {
          let pivot = pivots[*i];
          for j in sorted.iter() {
            let row = bucket[*j];
            if (matched[(*j)/8]>>((*j)%8))&1 == 1 { continue }
            if row.0.cmp_at(&pivot, level as usize) == Ordering::Equal {
              matched[(*j)/8] |= 1<<((*j)%8);
              intersecting[*i].push(*j);
            }
          }
        }
        let mut nbucket = vec![vec![];n];
        let mut j = 0;
        let mut pivot = pivots[self.order[bf-1]];
        for i in sorted.iter() {
          if matched[(*i)/8]>>((*i)%8)&1 == 1 { continue }
          let row = bucket[*i];
          while j < bf-1
          && row.0.cmp_at(&pivot, level as usize) != Ordering::Less {
            j = (j+1).min(bf-1);
            if j < bf-1 {
              pivot = pivots[self.order[j+bf-1]];
            }
          }
          //println!("{}",j); // use to test output distribution
          nbucket[j].push(bucket[*i]);
        }
        nbuckets.extend(nbucket);
      }
      buckets = nbuckets;
    }
    Ok(())
  }
  pub fn pivot_order (bf: usize) -> Vec<usize> {
    let n = bf*2-1;
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
