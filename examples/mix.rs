use eyros::{DB,Row,Mix,Mix2};
use rand::random;
use failure::Error;
use random_access_disk::RandomAccessDisk;
use std::path::PathBuf;

type P = Mix2<f32,f32>;
type V = u32;

fn main() -> Result<(),Error> {
  let mut db: DB<_,_,P,V> = DB::open(storage)?;
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
  db.batch(&batch)?;

  let bbox = ((-0.5,-0.8),(0.3,-0.5));
  for result in db.query(&bbox)? {
    println!("{:?}", result?);
  }
  Ok(())
}

fn storage(name:&str) -> Result<RandomAccessDisk,Error> {
  let mut p = PathBuf::from("/tmp/eyros-mix-db/");
  p.push(name);
  Ok(RandomAccessDisk::builder(p)
    .auto_sync(false)
    .build()?)
}
