use eyros::{TreeRef,TreeId,Point,Coord,Row,QTrace,Error};
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;
use async_std::{prelude::*,task,sync::{Arc,Mutex}};

type P = (Coord<f32>,Coord<f32>);
type V = u32;

#[async_std::test]
async fn trace() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let size = 10;
  let mut r = rand().seed([13,12]);
  let batch: Vec<Row<P,V>> = (0..size).map(|_| {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = r.read::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + r.read::<f32>().powf(64.0)*(1.0-ymin);
    let value: u32 = r.read();
    let point = (
      Coord::Interval(xmin,xmax),
      Coord::Interval(ymin,ymax),
    );
    Row::Insert(point, value)
  }).collect();
  let mut db = eyros::open_from_path2(dir.path()).await?;
  db.batch(&batch).await?;
  db.sync().await?;

  let bbox = ((-1.0,-1.0),(1.0,1.0));
  //let mut refs: Arc<Mutex<Vec<TreeRef<P>>>> = Arc::new(Mutex::new(vec![]));
  let refs = Arc::new(Mutex::new(vec![]));
  let trace = Box::new(Trace { refs: refs.clone() });
  let mut stream = db.query_trace(&bbox, trace).await?;
  let mut count = 0;
  while let Some(result) = stream.next().await {
    result?;
    count += 1;
  }
  assert_eq![count, size];
  assert_eq![
    refs.lock().await.iter().map(|r| r.id).collect::<Vec<TreeId>>(),
    vec![0]
  ];
  Ok(())
}

struct Trace<P: Point> {
  pub refs: Arc<Mutex<Vec<TreeRef<P>>>>,
}
impl<P> QTrace<P> for Trace<P> where P: Point {
  fn trace(&mut self, tr: TreeRef<P>) {
    let refs_r = self.refs.clone();
    task::block_on(async move {
      refs_r.lock().await.push(tr);
    });
  }
}
