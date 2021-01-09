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
  let nbatch = 10;
  let batch_size = 100_000;
  let batches: Vec<Vec<Row<P,V>>> = (0..nbatch).map(|_| {
    (0..batch_size).map(|i| {
      let xmin = (random::<f32>()*2.0-1.0)*180.0;
      let xmax = xmin + random::<f32>().powf(16.0)*(180.0-xmin);
      let ymin = (random::<f32>()*2.0-1.0)*90.0;
      let ymax = ymin + random::<f32>().powf(16.0)*(90.0-ymin);
      let point = (Coord::Interval(xmin,xmax), Coord::Interval(ymin,ymax));
      Row::Insert(point, i)
    }).collect()
  }).collect();
  let start = time::Instant::now();
  for batch in batches.iter() {
    db.batch(batch).await?;
  }
  let elapsed = start.elapsed().as_secs_f64();
  eprintln!["{} rows x{} in {}s ({}/s)",
    batch_size,
    nbatch,
    elapsed,
    ((batch_size*nbatch) as f64)/elapsed
  ];

  /*
  let bbox = ((-180.0,-90.0),(180.0,90.0));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!("{:?}", result?);
  }
  */
  Ok(())
}
