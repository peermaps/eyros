use point::Point;
use ::{Value};
use std::cmp::Ordering;

pub enum Node<'a,P,V> where P: Point, V: Value {
  Branch(Branch<'a,P,V>),
  Data(Data<'a,P,V>)
}

pub struct Data<'a,P,V> where P: Point, V: Value {
  offset: u64,
  rows: &'a Vec<(P,V)>
}

impl<'a,P,V> Data<'a,P,V> where P: Point, V: Value {
  pub fn data (&self) -> Vec<u8> {
    vec![]
  }
}

pub struct Branch<'a,P,V> where P: Point, V: Value {
  pub offset: u64,
  level: usize,
  order: &'a Vec<usize>,
  rows: &'a Vec<&'a (P,V)>,
  pivots: Vec<P>,
  sorted: Vec<usize>,
  intersecting: Vec<Vec<usize>>,
  matched: Vec<bool>
}

impl<'a,P,V> Branch<'a,P,V> where P: Point, V: Value {
  pub fn new (level: usize, order: &'a Vec<usize>, rows: &'a Vec<&(P,V)>)
  -> Self {
    let n = order.len();
    let mut sorted: Vec<usize> = (0..rows.len()).collect();
    sorted.sort_unstable_by(|a,b| {
      rows[*a].0.cmp_at(&rows[*b].0, level as usize)
    });
    let pivots: Vec<P> = (0..n).map(|k| {
      let m = ((k+2)*sorted.len()/(n+1)).min(sorted.len()-2);
      let a = &rows[sorted[m+0]];
      let b = &rows[sorted[m+1]];
      a.0.midpoint_upper(&b.0)
    }).collect();
    let mut intersecting = vec![vec![];n];
    let mut matched = vec![false;rows.len()];
    for i in order.iter() {
      let pivot = pivots[*i];
      for j in sorted.iter() {
        let row = rows[*j];
        if matched[*j] { continue }
        if row.0.cmp_at(&pivot, level as usize) == Ordering::Equal {
          matched[*j] = true;
          intersecting[*i].push(*j);
        }
      }
    }
    Self {
      offset: 0,
      level,
      order,
      rows,
      pivots,
      sorted,
      intersecting,
      matched
    }
  }
  pub fn build (&mut self) -> (Vec<u8>,Vec<Node<P,V>>) {
    let n = self.order.len();
    let bf = (n+1)/2;
    let mut nbucket = vec![vec![];n];
    let mut j = 0;
    let mut pivot = self.pivots[self.order[bf-1]];
    for i in self.sorted.iter() {
      if self.matched[*i] { continue }
      let row = self.rows[*i];
      while j < bf-1
      && row.0.cmp_at(&pivot, self.level as usize) != Ordering::Less {
        j = (j+1).min(bf-1);
        if j < bf-1 {
          pivot = self.pivots[self.order[j+bf-1]];
        }
      }
      //println!("{}",j); // use to test output distribution
      nbucket[j].push(row);
    }
    (vec![],vec![])
  }
  pub fn bytes (&self) -> usize {
    50
  }
}
