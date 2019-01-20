use random_access_storage::RandomAccess;
use failure::{Error,bail};
use std::marker::PhantomData;

use ::{Point,Value};

use branch::{Branch,Node};

pub struct TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  tree: &'a mut Tree<'a,S,P,V>,
  bbox: &'b P::BBox
}

impl<'a,'b,S,P,V> TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (tree: &'a mut Tree<'a,S,P,V>, bbox: &'b P::BBox) -> Self {
    Self { tree, bbox }
  }
}

impl<'a,'b,S,P,V> Iterator for TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  type Item = Result<(P,V),Error>;
  fn next (&mut self) -> Option<Self::Item> {
    let store = &mut self.tree.store;
    println!("{:?}", store.read(0,64));
    None
  }
}

pub struct Tree<'a,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S,
  branch_factor: usize,
  size: u64,
  max_data_size: usize,
  order: &'a Vec<usize>,
  _marker: PhantomData<(P,V)>
}

impl<'a,S,P,V> Tree<'a,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (mut store: S, branch_factor: usize,
  max_data_size: usize, order: &'a Vec<usize>) -> Result<Self,Error> {
    let size = store.len()? as u64;
    Ok(Self {
      store,
      size,
      order,
      branch_factor,
      max_data_size,
      _marker: PhantomData
    })
  }
  pub fn clear (&mut self) -> Result<(),Error> {
    self.store.truncate(0)?;
    Ok(())
  }
  pub fn is_empty (&mut self) -> Result<bool,Error> {
    let r = self.store.is_empty()?;
    Ok(r)
  }
  pub fn build (&mut self, rows: &Vec<(P,V)>) -> Result<(),Error> {
    let bf = self.branch_factor;
    if self.size > 0 {
      self.size = 0;
      self.store.truncate(0)?;
    }
    if rows.len() < bf*2-1 {
      bail!("tree must have at least {} records", bf*2-1);
    }
    let rrows = rows.iter().map(|row| row).collect();
    let bucket = (0..rows.len()).collect();
    let b = Branch::new(0, self.max_data_size, &self.order, bucket, &rrows);
    let mut branches = vec![Node::Branch(b)];
    match branches[0] {
      Node::Branch(ref mut b) => {
        let alloc = &mut {|bytes| self.alloc(bytes) };
        b.alloc(alloc);
      },
      _ => panic!("unexpected initial node type")
    };
    for _level in 0.. {
      if branches.is_empty() { break }
      let mut nbranches = vec![];
      for mut branch in branches {
        match branch {
          Node::Empty => {},
          Node::Data(d) => {
            let data = d.build()?;
            self.store.write(d.offset as usize, &data)?;
          },
          Node::Branch(ref mut b) => {
            let (data,nb) = {
              let alloc = &mut {|bytes| self.alloc(bytes) };
              b.build(alloc)?
            };
            self.store.write(b.offset as usize, &data)?;
            nbranches.extend(nb);
          }
        }
      }
      branches = nbranches;
    }
    self.flush()?;
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
  pub fn query<'b> (&'a mut self, bbox: &'b P::BBox) -> TreeIterator<'a,'b,S,P,V> {
    TreeIterator::new(self, bbox)
  }
  fn alloc (&mut self, bytes: usize) -> u64 {
    let addr = self.size;
    self.size += bytes as u64;
    addr
  }
  fn write_frame (&mut self, offset: u64, buf: Vec<u8>) -> Result<(),Error> {
    println!("FRAME {:?}", buf);
    Ok(())
  }
  fn flush (&mut self) -> Result<(),Error> {
    Ok(())
  }
  pub fn merge (trees: Vec<&Self>) -> Result<Self,Error> {
    unimplemented!();
  }
}
