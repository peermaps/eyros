//use std::mem::transmute;
extern crate random_access_storage;
extern crate failure;

use random_access_storage::RandomAccess;
use failure::Error;
use std::marker::PhantomData;
use std::fmt::Debug;

mod tree;
use tree::Tree;

mod meta;
use meta::Meta;

#[derive(Debug)]
pub struct DB<S,U,P,V> where
S: Debug+RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  _marker0: PhantomData<P>,
  _marker1: PhantomData<V>,
  open_store: U,
  meta_store: S,
  tree_stores: Vec<S>,
  meta: Meta
}

impl<S,U,P,V> DB<S,U,P,V> where
S: Debug+RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  pub fn open(open_store: U) -> Result<Self,Error> {
    let mut meta_store = open_store("meta")?;
    let meta = match meta_store.is_empty()? {
      true => Meta { staging_size: 0, branch_factor: 8, mask: vec![] },
      false => {
        let len = meta_store.len()?;
        let buf = meta_store.read(0, len)?;
        Meta::from_buffer(&buf)?
      }
    };
    let mut tree_stores = vec![];
    for (i,b) in meta.mask.iter().enumerate() {
      tree_stores.push(open_store(&format!("tree{}",i))?);
    }
    Ok(Self {
      _marker0: PhantomData,
      _marker1: PhantomData,
      meta,
      open_store,
      meta_store,
      tree_stores
    })
  }
  pub fn batch(&mut self, rows: &Vec<(P,V)>) -> Result<(),Error> {
    //let tree = Tree::open(self.open_store("test"));
    println!("meta={:?}", self.meta);
    Ok(())
  }
  pub fn query(&mut self) -> Result<(),Error> {
    unimplemented!();
  }
}
