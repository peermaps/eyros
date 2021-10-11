use lru::{LruCache as LRU};
use crate::{Tree,TreeId,tree,Error,Point,Value,Storage,RA,SetupFields,EyrosErrorKind};
use std::collections::{HashMap,HashSet};
use async_std::{sync::{Arc,Mutex,RwLock}};
#[cfg(not(feature="wasm"))] use async_std::task::spawn;
#[cfg(feature="wasm")] use async_std::task::{spawn_local as spawn};
use futures::future::join_all;

pub struct TreeFile<S,T,P,V> where T: Tree<P,V>, P: Point, V: Value, S: RA {
  fields: Arc<SetupFields>,
  cache: Arc<Mutex<LRU<TreeId,Arc<Mutex<T>>>>>,
  storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  updated: Arc<RwLock<HashMap<TreeId,Arc<Mutex<T>>>>>,
  removed: Arc<RwLock<HashSet<TreeId>>>,
  _marker: std::marker::PhantomData<(P,V)>,
}

impl<S,T,P,V> Clone for TreeFile<S,T,P,V> where T: Tree<P,V>, P: Point, V: Value, S: RA {
  fn clone(&self) -> Self {
    Self {
      fields: self.fields.clone(),
      cache: self.cache.clone(),
      storage: self.storage.clone(),
      updated: self.updated.clone(),
      removed: self.removed.clone(),
      _marker: std::marker::PhantomData,
    }
  }
}

impl<S,T,P,V> TreeFile<S,T,P,V> where T: Tree<P,V>, P: Point, V: Value, S: RA {
  pub fn new(fields: Arc<SetupFields>, storage: Arc<Mutex<Box<dyn Storage<S>>>>) -> Self {
    let cache = Arc::new(Mutex::new(LRU::new(fields.tree_cache_size)));
    Self {
      fields,
      cache,
      storage,
      updated: Arc::new(RwLock::new(HashMap::new())),
      removed: Arc::new(RwLock::new(HashSet::new())),
      _marker: std::marker::PhantomData,
    }
  }
  pub async fn get(&self, id: &TreeId) -> Result<Arc<Mutex<T>>,Error> {
    if let Some(t) = self.updated.read().await.get(id) {
      self.fields.log(&format![
        "get tree id={} file={}: updated", id, tree::get_file_from_id(id)
      ]).await?;
      return Ok(Arc::clone(t));
    }
    if self.removed.read().await.contains(id) {
      self.fields.log(&format![
        "get tree id={} file={}: removed (error)", id, tree::get_file_from_id(id)
      ]).await?;
      return EyrosErrorKind::TreeRemoved { id: *id }.raise();
    }
    {
      let mut cache = self.cache.lock().await;
      if let Some(t) = cache.get(id) {
        self.fields.log(&format![
          "get tree id={} file={}: cached", id, tree::get_file_from_id(id)
        ]).await?;
        return Ok(Arc::clone(t));
      }
    }
    {
      let file = tree::get_file_from_id(id);
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
      self.cache.lock().await.put(*id, Arc::clone(&t));
      Ok(t)
    }
  }
  pub async fn put(&self, id: &TreeId, t: Arc<Mutex<T>>) -> Result<(),Error> {
    self.fields.log(&format!["put tree id={}", id]).await?;
    let mut cache = self.cache.lock().await;
    let mut updated = self.updated.write().await;
    let mut removed = self.removed.write().await;
    cache.put(*id, Arc::clone(&t));
    updated.insert(*id, Arc::clone(&t));
    removed.remove(id);
    Ok(())
  }
  pub async fn remove(&self, id: &TreeId) -> Result<(),Error> {
    self.fields.log(&format!["remove tree id={}", id]).await?;
    let mut cache = self.cache.lock().await;
    let mut updated = self.updated.write().await;
    let mut removed = self.removed.write().await;
    cache.pop(id);
    updated.remove(id);
    removed.insert(*id);
    Ok(())
  }
  pub async fn sync(&self) -> Result<(),Error> {
    self.fields.log("sync begin").await?;
    let mut updated = self.updated.write().await;
    let mut removed = self.removed.write().await;
    let mut work = vec![];
    for (id,t) in updated.iter() {
      let file = tree::get_file_from_id(id);
      let tree = Arc::clone(t);
      let storage = Arc::clone(&self.storage);
      self.fields.log(&format!["sync tree (updated) id={} file={}", id, &file]).await?;
      work.push(spawn(async move {
        let bytes = tree.lock().await.to_bytes()?;
        let mut s = storage.lock().await.open(&file).await?;
        s.write(0, &bytes).await?;
        s.sync_all().await?;
        let res: Result<(),Error> = Ok(());
        res
      }));
    }
    for id in removed.iter() {
      let file = tree::get_file_from_id(id);
      let storage = self.storage.clone();
      self.fields.log(&format!["sync tree (remove) id={} file={}", id, &file]).await?;
      work.push(spawn(async move {
        // ignore errors
        match storage.lock().await.remove(&file).await {
          Ok(()) => {},
          Err(_err) => {}
        }
        let r: Result<(),Error> = Ok(());
        r
      }));
    }
    for r in join_all(work).await { r?; }
    updated.clear();
    removed.clear();
    self.fields.log("sync complete").await?;
    Ok(())
  }
}
