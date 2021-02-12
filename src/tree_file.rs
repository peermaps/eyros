use lru::{LruCache as LRU};
use crate::{Tree,TreeId,Error,Point,Value,Storage,RA,SetupFields};
use std::collections::{HashMap,HashSet};
use async_std::{sync::{Arc,Mutex}};
#[path="./join.rs"] mod join;
use join::Join;

#[derive(Debug)]
pub struct TreeFile<S,T,P,V> where T: Tree<P,V>, P: Point, V: Value, S: RA {
  //fields: Arc<SetupFields>,
  cache: LRU<TreeId,Arc<Mutex<T>>>,
  storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  updated: HashMap<TreeId,Arc<Mutex<T>>>,
  removed: HashSet<TreeId>,
  _marker: std::marker::PhantomData<(P,V)>,
}

impl<S,T,P,V> TreeFile<S,T,P,V> where T: Tree<P,V>, P: Point, V: Value, S: RA {
  pub fn new(fields: Arc<SetupFields>, storage: Arc<Mutex<Box<dyn Storage<S>>>>) -> Self {
    let cache = LRU::new(fields.tree_cache_size);
    Self {
      //fields,
      cache,
      storage,
      updated: HashMap::new(),
      removed: HashSet::new(),
      _marker: std::marker::PhantomData,
    }
  }
  pub async fn get(&mut self, id: &TreeId) -> Result<Arc<Mutex<T>>,Error> {
    //eprintln!["file={}", get_file(id)];
    if let Some(t) = self.updated.get(id) {
      return Ok(Arc::clone(t));
    }
    if self.removed.contains(id) {
      return Err(Box::new(failure::err_msg(format![
        "attempted to load tree scheduled for removal. id={}", id
      ]).compat()));
    }
    match &self.cache.get(id) {
      Some(t) => Ok(Arc::clone(t)),
      None => {
        let file = get_file(id);
        let mut s = self.storage.lock().await.open(&file).await?;
        let len = s.len().await?;
        if len == 0 {
          return Err(Box::new(failure::err_msg(format![
            "tree empty id={} file={}", id, file
          ]).compat()));
        }
        let bytes = s.read(0, len).await?;
        let t = Arc::new(Mutex::new(T::from_bytes(&bytes)?.1));
        self.cache.put(*id, Arc::clone(&t));
        Ok(t)
      }
    }
  }
  pub async fn put(&mut self, id: &TreeId, t: Arc<Mutex<T>>) -> () {
    self.cache.put(*id, Arc::clone(&t));
    self.updated.insert(*id, Arc::clone(&t));
    self.removed.remove(id);
  }
  pub async fn remove(&mut self, id: &TreeId) -> () {
    self.cache.pop(id);
    self.updated.remove(id);
    self.removed.insert(*id);
  }
  pub async fn sync(&mut self) -> Result<(),Error> {
    let mut join = Join::new();
    for (id,t) in self.updated.iter() {
      let file = get_file(id);
      let tree = Arc::clone(t);
      let storage = Arc::clone(&self.storage);
      join.push(async move {
        let bytes = tree.lock().await.to_bytes()?;
        let mut s = storage.lock().await.open(&file).await?;
        s.write(0, &bytes).await?;
        s.sync_all().await?;
        let res: Result<(),Error> = Ok(());
        res
      });
    }
    for id in self.removed.iter() {
      let file = get_file(id);
      let storage = self.storage.clone();
      join.push(async move {
        // ignore errors
        match storage.lock().await.remove(&file).await {
          Ok(()) => {},
          Err(_err) => {}
        }
        Ok(())
      });
    }
    join.try_join().await?;
    self.updated.clear();
    self.removed.clear();
    Ok(())
  }
}

fn get_file(id: &TreeId) -> String {
  format![
    "t/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}",
    (id >> (8*7)) % 0x100,
    (id >> (8*6)) % 0x100,
    (id >> (8*5)) % 0x100,
    (id >> (8*4)) % 0x100,
    (id >> (8*3)) % 0x100,
    (id >> (8*2)) % 0x100,
    (id >> (8*1)) % 0x100,
    (id >> (8*0)) % 0x100,
  ]
}
