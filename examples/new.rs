#[path="../src/new/tree.rs"] mod tree;
use tree::{Tree,Bucket,Branch,Node};
use rand::random;

type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let buckets: Vec<Bucket<f32,f32>> = (0..10_000).map(|i| {
    let xmin = (random::<f32>()*2.0-1.0)*180.0;
    let xmax = xmin + random::<f32>().powf(32.0)*(180.0-xmin);
    let ymin = (random::<f32>()*2.0-1.0)*90.0;
    let ymax = ymin + random::<f32>().powf(32.0)*(90.0-xmin);
    Bucket {
      bounds: (xmin,ymin,xmax,ymax),
      offset: i as u64
    }
  }).collect();
  let tree = Tree::build(9, &buckets);
  println!["{:?}", tree];
  Ok(())
}
