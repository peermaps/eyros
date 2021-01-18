use lru::{LruCache as LRU};
use crate::{Tree,TreeId,Error,Point,Value,Storage,RA,SetupFields};
use std::collections::{HashMap,HashSet};
use async_std::{sync::{Arc,Mutex},task::spawn};
use async_std::prelude::*;

pub struct TreeFile<S,T,P,V> where T: Tree<P,V>, P: Point, V: Value, S: RA {
  fields: Arc<SetupFields>,
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
      fields,
      cache,
      storage,
      updated: HashMap::new(),
      removed: HashSet::new(),
      _marker: std::marker::PhantomData,
    }
  }
  pub async fn get(&mut self, id: &TreeId) -> Result<Arc<Mutex<T>>,Error> {
    if let Some(t) = self.updated.get(id) {
      return Ok(Arc::clone(t));
    }
    match &self.cache.get(id) {
      Some(t) => Ok(Arc::clone(t)),
      None => {
        let file = get_file(id);
        let mut s = self.storage.lock().await.open(&file).await?;
        let len = s.len().await?;
        if len == 0 {
          return Err(Box::new(failure::err_msg(
            format!["tree empty id={} file={}", id, file]).compat()
          ));
        }
        let bytes = s.read(0, len).await?;
        let t = Arc::new(Mutex::new(T::from_bytes(&bytes)?.1));
        self.cache.put(*id, Arc::clone(&t));
        Ok(t)
      }
    }
  }
  pub async fn put(&mut self, id: &TreeId, t: Arc<Mutex<T>>) -> () {
    //eprintln!["put {}", id];
    self.cache.put(*id, Arc::clone(&t));
    self.updated.insert(*id, Arc::clone(&t));
    self.removed.remove(id);
  }
  pub async fn remove(&mut self, id: &TreeId) -> () {
    //eprintln!["remove {}", id];
    self.cache.pop(id);
    self.updated.remove(id);
    self.removed.insert(*id);
  }
  pub async fn flush(&mut self) -> Result<(),Error> {
    let mut tasks = vec![];
    for (id,t) in self.updated.iter() {
      let file = get_file(id);
      let tree = Arc::clone(t);
      let storage = Arc::clone(&self.storage);
      tasks.push(spawn(async move {
        let bytes = tree.lock().await.to_bytes()?;
        let mut s = storage.lock().await.open(&file).await?;
        s.write(0, &bytes).await
      }));
    }
    for id in self.removed.iter() {
      let file = get_file(id);
      let storage = self.storage.clone();
      tasks.push(spawn(async move {
        // ignoring errors for now
        storage.lock().await.remove(&file).await
      }));
    }
    let mut itasks = tasks.iter_mut();
    loop {
      let a = itasks.next();
      if a.is_none() { break }
      let b = itasks.next();
      if b.is_none() {
        a.unwrap().await?;
        break;
      }
      let c = itasks.next();
      if c.is_none() {
        a.unwrap().try_join(b.unwrap()).await?;
        break;
      }
      let d = itasks.next();
      if d.is_none() {
        a.unwrap().try_join(b.unwrap()).try_join(c.unwrap()).await?;
        break;
      }
      a.unwrap()
        .try_join(b.unwrap())
        .try_join(c.unwrap().try_join(d.unwrap()))
        .await?;
    }
    self.updated.clear();
    self.removed.clear();
    Ok(())
  }
}

fn get_file(id: &TreeId) -> String {
  format![
    "t/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}",
    (id >> (8*7)) % 0xff,
    (id >> (8*6)) % 0xff,
    (id >> (8*5)) % 0xff,
    (id >> (8*4)) % 0xff,
    (id >> (8*3)) % 0xff,
    (id >> (8*2)) % 0xff,
    (id >> (8*1)) % 0xff,
    (id >> (8*0)) % 0xff,
  ]
}
