use random_access_storage::RandomAccess;
use failure::{Error,bail};
use std::marker::PhantomData;
use std::cell::RefCell;
use std::mem::size_of;

use ::{Row,Point,Value};

use branch::{Branch,Node};

pub struct TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  tree: &'a mut Tree<S,P,V>,
  bbox: &'b P::BBox,
  cursors: Vec<(u64,usize)>,
  tree_size: u64
}

impl<'a,'b,S,P,V> TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (tree: &'a mut Tree<S,P,V>, bbox: &'b P::BBox)
  -> Result<Self,Error> {
    let tree_size = tree.store.len()? as u64;
    Ok(Self {
      tree,
      tree_size,
      bbox,
      cursors: vec![(0,0)]
    })
  }
}

macro_rules! iwrap {
  ($x:expr) => {
    match $x {
      Err(e) => { return Some(Err(e)) },
      Ok(b) => { b }
    }
  };
}

impl<'a,'b,S,P,V> Iterator for TreeIterator<'a,'b,S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  type Item = Result<(P,V),Error>;
  fn next (&mut self) -> Option<Self::Item> {
    let store = &mut self.tree.store;
    let bf = self.tree.branch_factor;
    let n = bf*2-1;
    let psize = size_of::<P>();
    let size = size_of::<u32>() + n * (psize + size_of::<V>());
    while !self.cursors.is_empty() {
      let (cursor,depth) = self.cursors.pop().unwrap();
      println!("cursor={} tree_size={}", cursor, self.tree_size);
      if cursor >= self.tree_size { continue }
      let buf = iwrap![store.read(cursor as usize, (cursor as usize)+size)];
      let mut pivots: Vec<P> = vec![];
      for i in 0..n {
        let cmp = iwrap![P::cmp_buf(
          &buf[i*psize..(i+1)*psize],
          &self.bbox,
          depth % P::dim()
        )];
        if cmp.0 { println!("LEFT") }
        if cmp.1 { println!("CENTER") }
        if cmp.2 { println!("RIGHT") }
      }
      println!("PIVOTS: {:?}", pivots);
    }
    None
  }
}

pub struct Tree<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub store: S,
  branch_factor: usize,
  size: u64,
  max_data_size: usize,
  order: RefCell<Vec<usize>>,
  _marker: PhantomData<(P,V)>
}

impl<S,P,V> Tree<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn open (mut store: S, branch_factor: usize,
  max_data_size: usize, order: RefCell<Vec<usize>>) -> Result<Self,Error> {
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
  pub fn build (&mut self, rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    let bf = self.branch_factor;
    if self.size > 0 {
      self.size = 0;
      self.store.truncate(0)?;
    }
    if rows.len() < bf*2-1 {
      bail!("tree must have at least {} records", bf*2-1);
    }
    let irows: Vec<(P,V)> = rows.iter()
      .filter(|row| { match row { Row::Insert(_,_) => true, _ => false } })
      .map(|row| { match row {
        Row::Insert(p,v) => (*p,*v),
        _ => panic!("unexpected")
      } })
      .collect();
    let bucket = (0..rows.len()).collect();
    let b = Branch::new(0, self.max_data_size,
      &self.order, bucket, &irows);
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
  pub fn query<'a,'b> (&'a mut self, bbox: &'b P::BBox)
  -> Result<TreeIterator<'a,'b,S,P,V>,Error> {
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
  pub fn merge (trees: &mut Vec<Self>, dst: usize, src: Vec<usize>,
  rows: &Vec<Row<P,V>>) -> Result<(),Error> {
    println!("MERGE {} {:?} {}", dst, src, rows.len());
    // TODO
    for i in src { trees[i].clear()? }
    trees[dst].clear()?;
    Ok(())
  }
}
