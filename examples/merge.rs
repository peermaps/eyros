use eyros::{DB,Row};
use random_access_disk::RandomAccessDisk;
use std::path::PathBuf;

type P = ((f32,f32),(f32,f32));
type V = (u32,u64);
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let args: Vec<String> = std::env::args().collect();
  let base = PathBuf::from(args[1].clone());
  let mut db: DB<_,P,V> = DB::open_from_path(&base).await?;
  //let mut b_offset = 0;
  for (b_index,bdir) in args[2..].iter().enumerate() {
    let mut bfile = PathBuf::from(bdir);
    bfile.push("range");
    let mut ranges = eyros::DataRange::new(
      RandomAccessDisk::builder(bfile)
        .auto_sync(false)
        .build()
        .await?,
      0
    );
    // TODO: incorporate len field and pre-set data offsets into Row enum
    let batch: Vec<Row<P,V>> = ranges.list().await?.iter().map(|(offset,range,_len)| {
      //Row::Insert(*range,(b_index as u32,b_offset+*offset))
      Row::Insert(*range,(b_index as u32,*offset))
    }).collect();
    db.batch(&batch).await?;
    //b_offset += ranges.store.len()? as u64;
  }
  Ok(())
}
