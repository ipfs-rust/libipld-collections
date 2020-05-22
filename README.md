# Rust IPLD collections library
Basic rust ipld collections library implementing a multiblock vector and hash map.

## Getting started
```rust
use ipfs_embed::{Config, Store};
use ipld_collections::List;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_path("/tmp/db")?;
    let store = Store::new(config)?;
    let mut list = List::new(store, 64, 256).await?;
    list.push(0 as i64).await?;
    Ok(())
}
```

## License
Dual licensed under MIT or Apache License (Version 2.0).
