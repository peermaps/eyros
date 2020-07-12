use eyros::{DB,Row,Mix,Mix2};
use rand::random;
use std::path::PathBuf;
use async_std::prelude::*;

type P = Mix2<f32,f32>;
type V = u32;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,P,V> = DB::open_from_path(
    &PathBuf::from("/tmp/eyros-mix.db")
  ).await?;
  let batch: Vec<Row<P,V>> = (0..1_000).map(|_| {
    let value = random::<u32>();
    if random::<f32>() > 0.5 {
      let xmin: f32 = random::<f32>()*2.0-1.0;
      let xmax: f32 = xmin + random::<f32>().powf(64.0)*(1.0-xmin);
      let ymin: f32 = random::<f32>()*2.0-1.0;
      let ymax: f32 = ymin + random::<f32>().powf(64.0)*(1.0-ymin);
      Row::Insert(Mix2::new(
        Mix::Interval(xmin,xmax),
        Mix::Interval(ymin,ymax)
      ), value)
    } else {
      let x: f32 = random::<f32>()*2.0-1.0;
      let y: f32 = random::<f32>()*2.0-1.0;
      Row::Insert(Mix2::new(
        Mix::Scalar(x),
        Mix::Scalar(y)
      ), value)
    }
  }).collect();
  db.batch(&batch).await?;

  let bbox = ((-0.5,-0.8),(0.3,-0.5));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!("{:?}", result?);
  }
  Ok(())
}
