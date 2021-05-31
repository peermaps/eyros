use crate::{DB,Tree,Storage,Point,Value,Error,RA,Debugger};
use async_std::{sync::{Arc,Mutex},channel::{unbounded,Sender}};

#[cfg(not(feature="wasm"))] use async_std::task::spawn;
#[cfg(feature="wasm")] use async_std::task::{spawn_local as spawn};

/// Struct for reading database properties.
#[derive(Clone)]
pub struct SetupFields {
  pub branch_factor: usize,
  pub max_depth: usize,
  pub max_records: usize,
  pub inline: usize,
  pub tree_cache_size: usize,
  pub rebuild_depth: usize,
  pub debug: Option<Sender<String>>,
}

impl std::fmt::Debug for SetupFields {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SetupFields")
      .field("branch_factor", &self.branch_factor)
      .field("max_depth", &self.max_depth)
      .field("max_records", &self.max_records)
      .field("inline", &self.inline)
      .field("tree_cache_size", &self.tree_cache_size)
      .field("rebuild_depth", &self.rebuild_depth)
      .field("debug", &format_args!["{}", match &self.debug {
        Some(_) => "[enabled]",
        None => "[not enabled]",
      }])
      .finish()
  }
}

impl SetupFields {
  pub fn default() -> Self {
    Self {
      branch_factor: 6,
      max_depth: 8,
      max_records: 20_000,
      inline: 500,
      tree_cache_size: 1000,
      rebuild_depth: 2,
      debug: None,
    }
  }
  pub async fn log(&self, msg: &str) -> Result<(),Error> {
    if let Some(d) = &self.debug {
      d.send(msg.into()).await?;
    }
    Ok(())
  }
}

/// Builder to configure and instantiate an eyros database.
///
/// The `Setup` builder lets you create a database with a more custom
/// configuration:
///
/// ```rust,no_run
/// use eyros::{DB,Coord,Tree2,Setup};
/// use random_access_disk::RandomAccessDisk;
/// use std::path::PathBuf;
///
/// type T = Tree2<f32,f32,V>;
/// type P = (Coord<f32>,Coord<f32>);
/// type V = u32;
///
/// # #[async_std::main]
/// # async fn main () -> Result<(),Box<dyn std::error::Error+Sync+Send>> {
/// let mut db: DB<_,T,P,V> = Setup::from_path(&PathBuf::from("/tmp/eyros-db/"))
///   .branch_factor(6)
///   .max_depth(8)
///   .max_records(20_000)
///   .inline(500)
///   .tree_cache_size(1000)
///   .rebuild_depth(2)
///   .build()
///   .await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Setup<S> where S: RA {
  pub storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  pub fields: SetupFields
}

impl<S> Setup<S> where S: RA {
  /// Create a new `Setup` builder from a storage function.
  pub fn from_storage(storage: Box<dyn Storage<S>>) -> Self {
    Self {
      storage: Arc::new(Mutex::new(storage)),
      fields: SetupFields::default()
    }
  }
  pub fn branch_factor(mut self, bf: usize) -> Self {
    self.fields.branch_factor = bf;
    self
  }
  pub fn max_depth(mut self, md: usize) -> Self {
    self.fields.max_depth = md;
    self
  }
  pub fn max_records(mut self, mr: usize) -> Self {
    self.fields.max_records = mr;
    self
  }
  pub fn inline(mut self, n: usize) -> Self {
    self.fields.inline = n;
    self
  }
  pub fn tree_cache_size(mut self, n: usize) -> Self {
    self.fields.tree_cache_size = n;
    self
  }
  pub fn rebuild_depth(mut self, n: usize) -> Self {
    self.fields.rebuild_depth = n;
    self
  }
  pub fn debug(mut self, d: impl Debugger+Send+Sync+'static) -> Self {
    let debug = Arc::new(Mutex::new(d));
    let (sender,receiver) = unbounded();
    spawn(async move {
      while let Ok(r) = receiver.recv().await {
        let s: String = r;
        debug.lock().await.send(&s);
      }
    });
    // todo read sender into f
    // Arc::new(Mutex::new(f)));
    self.fields.debug = Some(sender);
    self
  }
  pub async fn build<T,P,V> (self) -> Result<DB<S,T,P,V>,Error>
  where P: Point, V: Value, T: Tree<P,V> {
    DB::open_from_setup(self).await
  }
}
