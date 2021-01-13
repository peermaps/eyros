use lru::{LruCache as LRU};
use crate::{Tree,TreeId,Error,Point,Value,Storage};
use std::collections::{HashMap,HashSet};
use random_access_storage::RandomAccess;
use async_std::sync::{Arc,Mutex};

pub struct TreeFile<S,T,P,V>
where T: Tree<P,V>, P: Point, V: Value, S: RandomAccess<Error=Error>+Unpin+Send+Sync {
  cache: LRU<TreeId,Arc<Mutex<T>>>,
  storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  updated: HashMap<TreeId,Arc<Mutex<T>>>,
  removed: HashSet<TreeId>,
  _marker: std::marker::PhantomData<(P,V)>,
}

impl<S,T,P,V> TreeFile<S,T,P,V>
where T: Tree<P,V>, P: Point, V: Value, S: RandomAccess<Error=Error>+Unpin+Send+Sync {
  pub fn new(n: usize, storage: Arc<Mutex<Box<dyn Storage<S>>>>) -> Self {
    Self {
      cache: LRU::new(n),
      storage,
      updated: HashMap::new(),
      removed: HashSet::new(),
      _marker: std::marker::PhantomData,
    }
  }
  pub async fn get(&mut self, id: &TreeId) -> Result<Arc<Mutex<T>>,Error> {
    match &self.cache.get(id) {
      Some(t) => Ok(Arc::clone(t)),
      None => {
        let file = get_file(id);
        let mut s = self.storage.lock().await.open(&file).await?;
        let bytes = s.read(0, s.len().await?).await?;
        Ok(Arc::new(Mutex::new(T::from_bytes(&bytes)?.1)))
      }
    }
  }
  pub fn put(&mut self, id: &TreeId, t: Arc<Mutex<T>>) -> () {
    self.cache.put(*id, Arc::clone(&t));
    self.updated.insert(*id, Arc::clone(&t));
  }
  pub fn remove(&mut self, id: &TreeId) -> () {
    self.cache.pop(id);
    self.updated.remove(id);
    self.removed.insert(*id);
  }
  pub async fn flush(&mut self) -> Result<(),Error> {
    for (id,t) in self.updated.iter() {
      let file = get_file(id);
      let mut s = self.storage.lock().await.open(&file).await?;
      s.write(0, &t.lock().await.to_bytes()?).await?;
    }
    self.updated.clear();
    for id in self.removed.iter() {
      let file = get_file(id);
      self.storage.lock().await.remove(&file).await?;
    }
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
