use eyros::{Row,Coord};
use rand::random;
use async_std::prelude::*;
use std::time;

type P = (Coord<f32>,Coord<f32>);
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db = eyros::open_from_path2(
    &std::path::PathBuf::from("/tmp/eyros.db")
  ).await?;
  let batch: Vec<Row<P,V>> = (0..1000).map(|i| {
    let xmin = (random::<f32>()*2.0-1.0)*180.0;
    let xmax = xmin + random::<f32>().powf(16.0)*(180.0-xmin);
    let ymin = (random::<f32>()*2.0-1.0)*90.0;
    let ymax = ymin + random::<f32>().powf(16.0)*(90.0-ymin);
    let point = (Coord::Interval(xmin,xmax), Coord::Interval(ymin,ymax));
    Row::Insert(point, i)
  }).collect();
  db.batch(&batch).await?;

  let bbox = ((-120.0,20.0),(-100.0,35.0));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!("{:?}", result?);
  }
  Ok(())
}
