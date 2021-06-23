use lru::{LruCache as LRU};
use crate::{Tree,TreeId,Error,Point,Value,Storage,RA,SetupFields,EyrosErrorKind};
use std::collections::{HashMap,HashSet};
use async_std::{sync::{Arc,Mutex}};
#[path="./join.rs"] mod join;
use join::Join;

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
      self.fields.log(&format![
        "get tree id={} file={}: updated", id, get_file(id)
      ]).await?;
      return Ok(Arc::clone(t));
    }
    if self.removed.contains(id) {
      self.fields.log(&format![
        "get tree id={} file={}: removed (error)", id, get_file(id)
      ]).await?;
      return EyrosErrorKind::TreeRemoved { id: *id }.raise();
    }
    match &self.cache.get(id) {
      Some(t) => {
        self.fields.log(&format![
          "get tree id={} file={}: cached", id, get_file(id)
        ]).await?;
        Ok(Arc::clone(t))
      },
      None => {
        let file = get_file(id);
        self.fields.log(&format![
          "get tree id={} file={}: not cached", id, &file
        ]).await?;
        let mut s = self.storage.lock().await.open(&file).await?;
        let len = s.len().await?;
        if len == 0 {
          return EyrosErrorKind::TreeEmpty { id: *id, file }.raise();
        }
        let bytes = s.read(0, len).await?;
        self.fields.log(&format!["read {} bytes from tree id={}", len, id]).await?;
        let t = Arc::new(Mutex::new(T::from_bytes(&bytes)?.1));
        self.cache.put(*id, Arc::clone(&t));
        Ok(t)
      }
    }
  }
  pub async fn put(&mut self, id: &TreeId, t: Arc<Mutex<T>>) -> Result<(),Error> {
    self.fields.log(&format!["put tree id={}", id]).await?;
    self.cache.put(*id, Arc::clone(&t));
    self.updated.insert(*id, Arc::clone(&t));
    self.removed.remove(id);
    Ok(())
  }
  pub async fn remove(&mut self, id: &TreeId) -> Result<(),Error> {
    self.fields.log(&format!["remove tree id={}", id]).await?;
    self.cache.pop(id);
    self.updated.remove(id);
    self.removed.insert(*id);
    Ok(())
  }
  pub async fn sync(&mut self) -> Result<(),Error> {
    self.fields.log("sync begin").await?;
    let mut join = Join::new();
    for (id,t) in self.updated.iter() {
      let file = get_file(id);
      let tree = Arc::clone(t);
      let storage = Arc::clone(&self.storage);
      self.fields.log(&format!["sync tree (updated) id={} file={}", id, &file]).await?;
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
      self.fields.log(&format!["sync tree (remove) id={} file={}", id, &file]).await?;
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
    self.fields.log("sync complete").await?;
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
    (id >> 8) % 0x100,
    id % 0x100,
  ]
}
