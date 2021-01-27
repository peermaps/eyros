use eyros::{Row,Coord};
use rand::random;
//use async_std::prelude::*;
use std::time;

type P = (Coord<f32>,Coord<f32>);
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db = eyros::open_from_path2(
    &std::path::PathBuf::from("/tmp/eyros.db")
  ).await?;
  let nbatch = 100;
  let batch_size = 100_000;
  let batches: Vec<Vec<Row<P,V>>> = (0..nbatch).map(|_| {
    (0..batch_size).map(|i| {
      let xmin = (random::<f32>()*2.0-1.0)*180.0;
      let xmax = xmin + random::<f32>().powf(32.0)*(180.0-xmin);
      let ymin = (random::<f32>()*2.0-1.0)*90.0;
      let ymax = ymin + random::<f32>().powf(32.0)*(90.0-ymin);
      let point = (Coord::Interval(xmin,xmax), Coord::Interval(ymin,ymax));
      Row::Insert(point, i)
    }).collect()
  }).collect();
  let start = time::Instant::now();
  for batch in batches.iter() {
    db.batch(batch).await?;
  }
  let elapsed0 = start.elapsed().as_secs_f64();
  db.sync().await?;
  let elapsed1 = start.elapsed().as_secs_f64();
  eprintln!["MEMORY {}*{}={} rows in {}s ({}/s)",
    batch_size,
    nbatch,
    batch_size*nbatch,
    elapsed0,
    ((batch_size*nbatch) as f64)/elapsed0
  ];
  eprintln!["DISK {}*{}={} rows in {}s ({}/s)",
    batch_size,
    nbatch,
    batch_size*nbatch,
    elapsed1,
    ((batch_size*nbatch) as f64)/elapsed1
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
