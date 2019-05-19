use eyros::{DB,Row};
use failure::Error;
use std::path::PathBuf;
use std::rc::Rc;
use random_access_disk::RandomAccessDisk;

type R = ((f32,f32),(f32,f32));
type V = (u32,u64);

fn main() -> Result<(),Error> {
  let args: Vec<String> = std::env::args().collect();
  let base = PathBuf::from(args[1].clone());
  let mut db: DB<_,_,R,V> = DB::open(|name| {
    let mut p = base.clone();
    p.push(name);
    Ok(RandomAccessDisk::open(p)?)
  })?;
  //let mut b_offset = 0;
  for (b_index,bdir) in args[2..].iter().enumerate() {
    let mut bfile = PathBuf::from(bdir);
    bfile.push("range");
    let mut ranges = eyros::DataRange::new(
      RandomAccessDisk::open(bfile)?,
      0,
      Rc::clone(&db.bincode)
    );
    // TODO: incorporate len field and pre-set data offsets into Row enum
    db.batch(&ranges.list()?.iter().map(|(offset,range,_len)| {
      //Row::Insert(*range,(b_index as u32,b_offset+*offset))
      Row::Insert(*range,(b_index as u32,*offset))
    }).collect())?;
    //b_offset += ranges.store.len()? as u64;
  }
  Ok(())
}
