use random_access_storage::RandomAccess;
use failure::Error;
use std::marker::PhantomData;

use point::Point;
use ::{Row,Value};

pub struct Tree<'a,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S,
  branch_factor: usize,
  order: &'a Vec<usize>,
  _marker: PhantomData<(P,V)>
}

impl<'a,S,P,V> Tree<'a,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (store: S, branch_factor: usize, order: &'a Vec<usize>) -> Self {
    Self {
      store,
      order,
      branch_factor,
      _marker: PhantomData
    }
  }
  pub fn build (&mut self, rows: &Vec<(P,V)>) -> Result<(),Error> {
    let bf = self.branch_factor;
    let nlevels = ((rows.len() as f32).log(bf as f32)) as u32;
    let buckets = vec![rows];
    for level in 0..nlevels {
      for bucket in buckets.iter() {
        let mut sorted: Vec<usize> = (0..bucket.len()).collect();
        sorted.sort_unstable_by(|a,b| {
          bucket[*a].0.cmp_at(&bucket[*b].0, level as usize)
        });
        let n = self.branch_factor*2-1;
        let pivots: Vec<P> = (0..n).map(|k| {
          let m = (k+1)*sorted.len()/(n+1);
          let a = &bucket[sorted[m]];
          let b = &bucket[sorted[m+1]];
          a.0.midpoint_upper(&b.0)
        }).collect();
        println!("pivots={:?}", pivots);
      }
    }
    Ok(())
  }
  pub fn pivot_order (n: usize) -> Vec<usize> {
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
