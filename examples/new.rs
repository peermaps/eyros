use eyros::d2::{DB,Row,Coord};
use rand::random;

type X = f32;
type Y = f32;
type V = u64;
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let mut db: DB<_,X,Y,V> = DB::open_from_path(
    &std::path::PathBuf::from("/tmp/eyros.db")
  ).await?;
  let rows: Vec<Row<X,Y,V>> = (0..100).map(|i| {
    let xmin = (random::<f32>()*2.0-1.0)*180.0;
    let xmax = xmin + random::<f32>().powf(4.0)*(180.0-xmin);
    let ymin = (random::<f32>()*2.0-1.0)*90.0;
    let ymax = ymin + random::<f32>().powf(4.0)*(90.0-xmin);
    Row::Insert(
      Coord::Interval(xmin,xmax),
      Coord::Interval(ymin,ymax),
      random::<u64>()
    )
  }).collect();
  db.batch(&rows).await?;

  /*
  let bbox = ((-0.5,-0.8,0.0),(0.3,-0.5,100.0));
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    println!("{:?}", result?);
  }
  */

  Ok(())
}
