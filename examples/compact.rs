// analyze the compactness of data blocks
// a smaller area is more compact and better

use eyros::{DB,Row,Mix,Mix2};
use rand::random;
use std::path::PathBuf;
use async_std::prelude::*;
use std::collections::HashSet;

type P = Mix2<f32,f32>;
type V = u32;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,P,V> = DB::open_from_path(
    &PathBuf::from("/tmp/eyros-compact.db")
  ).await?;
  let polygons: Vec<Row<P,V>> = (0..500_000).map(|_| {
    let x = Mix::Scalar((random::<f32>()*2.0-1.0)*180.0);
    let y = Mix::Scalar((random::<f32>()*2.0-1.0)*90.0);
    let value: u32 = random();
    Row::Insert(Mix2::new(x,y), value)
  }).collect();
  db.batch(&polygons).await?;

  let mut locations = HashSet::new();
  let bbox = ((-180.0,-90.0),(180.0,90.0));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    let offset = (result?.2).0;
    if offset > 0 {
      locations.insert(offset-1);
    }
  }

  let mut sum = 0_f32;
  let mut max = 0_f32;
  for offset in locations.iter() {
    let rows = db.data_store.lock().await.list(*offset).await?;
    let mut extents = ((180.0,90.0),(-180.0,-90.0));
    for (p,_,_) in rows.iter() {
      match p {
        Mix2 { v0: Mix::Scalar(x), v1: Mix::Scalar(y) } => {
          (extents.0).0 = x.min((extents.0).0);
          (extents.0).1 = y.min((extents.0).1);
          (extents.1).0 = x.max((extents.1).0);
          (extents.1).1 = y.max((extents.1).1);
        },
        _ => {}
      }
    }
    let span = ((extents.1).0-(extents.0).0, (extents.1).1-(extents.0).1);
    let area = span.0 * span.1;
    sum += area;
    max = max.max(area);
  }
  println!["mean {}", sum / (locations.len() as f32)];
  println!["max  {}", max];

  Ok(())
}
