use async_std::task;
use criterion::black_box;
use ipld_collections::{create_store, create_list, Result};
use libipld::Ipld;

async fn run() -> Result<()> {
    let store = create_store(16).await?;
    let mut list = create_list(store, 256).await?;
    // push: 1024xi128; n: 4; width: 256; size: 4096
    for i in 0..1024 {
        list.push(Ipld::Integer(i as i128)).await?;
    }
    black_box(list);
    Ok(())
}

fn main() -> Result<()> {
    task::block_on(run())
}
