use eyros::{DB,Row};
use rand::random;
use std::path::PathBuf;
use async_std::prelude::*;

type P = ((f32,f32),(f32,f32),f32);
type V = u32;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,P,V> = DB::open_from_path(
    &PathBuf::from("/tmp/eyros-polygons.db")
  ).await?;
  let polygons: Vec<Row<P,V>> = (0..800).map(|_| {
    let xmin: f32 = random::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + random::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = random::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + random::<f32>().powf(64.0)*(1.0-ymin);
    let time: f32 = random::<f32>()*1000.0;
    let value: u32 = random();
    let point = ((xmin,xmax),(ymin,ymax),time);
    Row::Insert(point, value)
  }).collect();
  db.batch(&polygons).await?;

  let bbox = ((-0.5,-0.8,0.0),(0.3,-0.5,100.0));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!("{:?}", result?);
  }
  Ok(())
}
