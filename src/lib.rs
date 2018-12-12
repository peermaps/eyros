//use std::mem::transmute;
extern crate random_access_storage;
extern crate failure;

use random_access_storage::RandomAccess;
use failure::Error;
use std::marker::PhantomData;
use std::fmt::Debug;

mod tree;
use tree::Tree;
pub use tree::{Row,Coord,Point};

mod meta;
use meta::Meta;

#[derive(Debug)]
pub struct DB<S,U,A,B,C,D,E,F,V> where
A: Debug,
B: Debug,
C: Debug,
D: Debug,
E: Debug,
F: Debug,
V: Debug,
S: Debug+RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  _marker0: PhantomData<A>,
  _marker1: PhantomData<B>,
  _marker2: PhantomData<C>,
  _marker3: PhantomData<D>,
  _marker4: PhantomData<E>,
  _marker5: PhantomData<F>,
  _marker6: PhantomData<V>,
  open_store: U,
  meta_store: S,
  trees: Vec<Tree<S,A,B,C,D,E,F,V>>,
  meta: Meta
}

impl<S,U,A,B,C,D,E,F,V> DB<S,U,A,B,C,D,E,F,V> where
A: Debug,
B: Debug,
C: Debug,
D: Debug,
E: Debug,
F: Debug,
V: Debug,
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
      _marker0: PhantomData,
      _marker1: PhantomData,
      _marker2: PhantomData,
      _marker3: PhantomData,
      _marker4: PhantomData,
      _marker5: PhantomData,
      _marker6: PhantomData,
      meta,
      open_store,
      meta_store,
      trees
    })
  }
  fn get_tree(&mut self, i: usize) -> Result<&Tree<S,A,B,C,D,E,F,V>,Error> {
    if i < self.trees.len() { return Ok(&self.trees[i]) }
    let tree = Tree::new(
      self.meta.branch_factor,
      (self.open_store)(&format!("tree{}",i))?
    );
    self.trees.push(tree);
    return Ok(&self.trees[i]);
  }
  pub fn batch(&mut self, rows: &Vec<Row<A,B,C,D,E,F,V>>) -> Result<(),Error> {
    let mut tree = self.get_tree(0);
    println!("tree={:?}", tree);
    Ok(())
  }
  pub fn query(&mut self) -> Result<(),Error> {
    unimplemented!();
  }
}
