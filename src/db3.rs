use random_access_storage::RandomAccess;
use failure::Error;

use meta::Meta;
use ::Coord;

use std::fmt::Debug;
use std::marker::PhantomData;

#[derive(Debug)]
pub enum Row3<A,B,C,V> {
  Insert(Coord<A>,Coord<B>,Coord<C>,V),
  Delete(Coord<A>,Coord<B>,Coord<C>,V)
}

struct Tree<S,A,B,C,V> where
S: Debug+RandomAccess<Error=Error> {
  branch_factor: usize,
  store: S,
  _marker0: PhantomData<A>,
  _marker1: PhantomData<B>,
  _marker2: PhantomData<C>,
  _marker3: PhantomData<V>,
}

impl<S,A,B,C,V> Tree<S,A,B,C,V> where
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
  pub fn build(&mut self, rows: &Vec<Row3<A,B,C,V>>) -> Result<(),Error> {
    unimplemented!();
  }
}

pub struct DB3<S,U,A,B,C,V> where
S: Debug+RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  open_store: U,
  meta_store: S,
  trees: Vec<Tree<S,A,B,C,V>>,
  meta: Meta
}

impl<S,U,A,B,C,V> DB3<S,U,A,B,C,V> where
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
  pub fn batch(&mut self, rows: &Vec<Row3<A,B,C,V>>) -> Result<(),Error> {
    let tree = self.get_tree(0)?;
    tree.build(rows)?;
    Ok(())
  }
  pub fn query(&mut self) -> Result<(),Error> {
    unimplemented!();
  }
}
