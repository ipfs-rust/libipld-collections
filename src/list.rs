use libipld::cache::Cache;
use libipld::cache::IpldCache;
use libipld::cbor::DagCbor;
use libipld::cbor::DagCborCodec;
use libipld::cid::Cid;
use libipld::error::Result;
use libipld::ipld::Ipld;
use libipld::prelude::{Decode, Encode, References};
use libipld::store::Store;
use libipld::store::StoreParams;
use libipld::DagCbor;

pub struct ListConfig<S>
where
    S: Store,
    <S::Params as StoreParams>::Codecs: Into<DagCborCodec>,
    DagCborCodec: Into<<S::Params as StoreParams>::Codecs>,
    Ipld: References<<S::Params as StoreParams>::Codecs>,
{
    store: S,
    cache_size: usize,
    hash: <S::Params as StoreParams>::Hashes,
    width: Option<usize>,
}

impl<S> ListConfig<S>
where
    S: Store,
    <S::Params as StoreParams>::Codecs: Into<DagCborCodec>,
    DagCborCodec: Into<<S::Params as StoreParams>::Codecs>,
    Ipld: References<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: S, hash: <S::Params as StoreParams>::Hashes) -> Self {
        Self {
            store,
            cache_size: 64,
            hash,
            width: None,
        }
    }

    pub fn set_cache_size(&mut self, cache_size: usize) {
        self.cache_size = cache_size;
    }

    pub fn set_width(&mut self, width: usize) {
        self.width = Some(width);
    }

    fn width<T>(&self) -> usize {
        if let Some(width) = self.width {
            width
        } else {
            let elem_size = usize::max(std::mem::size_of::<T>(), std::mem::size_of::<Cid>());
            <S::Params as StoreParams>::MAX_BLOCK_SIZE / elem_size
        }
    }

    fn cache<T>(self) -> IpldCache<S, DagCborCodec, Node<T>>
    where
        T: DagCbor + Clone + Send + Sync,
    {
        IpldCache::new(self.store, DagCborCodec, self.hash, self.cache_size)
    }
}

pub struct List<S: Store, T: DagCbor> {
    cache: IpldCache<S, DagCborCodec, Node<T>>,
    root: Cid,
    tmp: S::TempPin,
}

impl<S, T> List<S, T>
where
    S: Store,
    <S::Params as StoreParams>::Codecs: Into<DagCborCodec>,
    DagCborCodec: Into<<S::Params as StoreParams>::Codecs>,
    Ipld: References<<S::Params as StoreParams>::Codecs>,
    T: DagCbor + Clone + Send + Sync,
{
    pub async fn new(config: ListConfig<S>) -> Result<Self> {
        let width = config.width::<T>();
        let cache = config.cache();
        let tmp = cache.temp_pin().await?;
        let root = cache
            .insert(Node::new(width as _, 0, vec![]), Some(&tmp))
            .await?;
        Ok(Self { cache, root, tmp })
    }

    pub async fn open(config: ListConfig<S>, root: Cid) -> Result<Self> {
        let cache = config.cache();
        let tmp = cache.temp_pin().await?;
        // warm up the cache and make sure it's available
        cache.get(&root, Some(&tmp)).await?;
        Ok(Self { cache, root, tmp })
    }

    pub fn root(&self) -> &Cid {
        &self.root
    }

    pub async fn from(config: ListConfig<S>, items: impl Iterator<Item = T>) -> Result<Self> {
        let width = config.width::<T>();
        let cache = config.cache();
        let tmp = cache.temp_pin().await?;

        let mut items: Vec<Data<T>> = items.map(Data::Value).collect();
        let mut height = 0;
        let mut cid = cache
            .insert(Node::new(width as _, height, vec![]), Some(&tmp))
            .await?;

        loop {
            let n_items = items.len() / width + 1;
            let mut items_next = Vec::with_capacity(n_items);
            for chunk in items.chunks(width) {
                let node = Node::new(width as u32, height, chunk.to_vec());
                cid = cache.insert(node, Some(&tmp)).await?;
                items_next.push(Data::Link(cid));
            }
            if items_next.len() == 1 {
                return Ok(Self {
                    cache,
                    root: cid,
                    tmp,
                });
            }
            items = items_next;
            height += 1;
        }
    }

    pub async fn push(&mut self, value: T) -> Result<()> {
        let mut value = Data::Value(value);
        let root = self.cache.get(&self.root, Some(&self.tmp)).await?;
        let height = root.height();
        let width = root.width();

        let chain = {
            let mut height = root.height();
            let mut chain = Vec::with_capacity(height as usize + 1);
            chain.push(root);
            while height > 0 {
                let cid = chain
                    .last()
                    .expect("at least one block")
                    .data()
                    .last()
                    .expect("at least one link")
                    .cid()
                    .expect("height > 0, payload must be a cid");
                let node = self.cache.get(cid, Some(&self.tmp)).await?;
                height = node.height();
                chain.push(node);
            }
            chain
        };

        let mut mutated = false;
        let mut last = self
            .cache
            .insert(Node::new(width as u32, height, vec![]), Some(&self.tmp))
            .await?;
        for mut node in chain.into_iter().rev() {
            if mutated {
                let data = node.data_mut();
                data.pop();
                data.push(value);
                last = self.cache.insert(node, Some(&self.tmp)).await?;
                value = Data::Link(last);
            } else {
                let data = node.data_mut();
                if data.len() < width {
                    data.push(value);
                    last = self.cache.insert(node, Some(&self.tmp)).await?;
                    value = Data::Link(last);
                    mutated = true;
                } else {
                    let node = Node::new(width as u32, node.height(), vec![value]);
                    last = self.cache.insert(node, Some(&self.tmp)).await?;
                    value = Data::Link(last);
                    mutated = false;
                }
            }
        }

        if !mutated {
            let children = vec![Data::Link(*self.root()), value];
            let node = Node::new(width as u32, height + 1, children);
            last = self.cache.insert(node, Some(&self.tmp)).await?;
        }

        self.root = last;

        Ok(())
    }

    pub async fn pop(&mut self) -> Result<Option<T>> {
        // TODO
        Ok(None)
    }

    pub async fn get(&mut self, mut index: usize) -> Result<Option<T>> {
        let node = self.cache.get(&self.root, Some(&self.tmp)).await?;
        let mut node_ref = &node;
        let width = node.width();
        let mut height = node.height();
        let mut node;

        if index > width.pow(height + 1) {
            return Ok(None);
        }

        loop {
            let data_index = index / width.pow(height);
            if let Some(data) = node_ref.data().get(data_index) {
                if height == 0 {
                    return Ok(Some(data.value().unwrap().clone()));
                }
                let cid = data.cid().unwrap();
                node = self.cache.get(cid, Some(&self.tmp)).await?;
                node_ref = &node;
                index %= width.pow(height);
                height = node.height();
            } else {
                return Ok(None);
            }
        }
    }

    pub async fn set(&mut self, _index: usize, _value: T) -> Result<()> {
        // TODO
        Ok(())
    }

    pub async fn len(&mut self) -> Result<usize> {
        let root = self.cache.get(&self.root, Some(&self.tmp)).await?;
        let width = root.width();
        let mut height = root.height();
        let mut size = width.pow(height + 1);
        let mut node = root;
        loop {
            let data = node.data();
            size -= width.pow(height) * (width - data.len());
            if height == 0 {
                return Ok(size);
            }
            let cid = data.last().unwrap().cid().unwrap();
            node = self.cache.get(cid, Some(&self.tmp)).await?;
            height = node.height();
        }
    }

    pub async fn is_empty(&mut self) -> Result<bool> {
        let root = self.cache.get(&self.root, Some(&self.tmp)).await?;
        Ok(root.data().is_empty())
    }

    pub fn iter(&mut self) -> ListIter<'_, S, T> {
        ListIter {
            list: self,
            index: 0,
        }
    }

    pub async fn flush<A: AsRef<[u8]> + Send + Sync>(&mut self, alias: A) -> Result<()> {
        self.cache.alias(alias, Some(self.root())).await?;
        self.tmp = self.cache.temp_pin().await?;
        self.cache.flush().await?;
        Ok(())
    }
}

pub struct ListIter<'a, S: Store, T: DagCbor> {
    list: &'a mut List<S, T>,
    index: usize,
}

impl<'a, S, T: DagCbor> ListIter<'a, S, T>
where
    S: Store,
    <S::Params as StoreParams>::Codecs: Into<DagCborCodec>,
    DagCborCodec: Into<<S::Params as StoreParams>::Codecs>,
    Ipld: References<<S::Params as StoreParams>::Codecs>,
    T: Decode<DagCborCodec> + Encode<DagCborCodec> + Clone + Send + Sync,
{
    #[allow(clippy::should_implement_trait)]
    pub async fn next(&mut self) -> Result<Option<T>> {
        let elem = self.list.get(self.index).await?;
        self.index += 1;
        Ok(elem)
    }
}

#[derive(Clone, Debug, DagCbor)]
struct Node<T: DagCbor> {
    width: u32,
    height: u32,
    data: Vec<Data<T>>,
}

impl<T: DagCbor> Node<T> {
    fn new(width: u32, height: u32, data: Vec<Data<T>>) -> Self {
        Node {
            width,
            height,
            data,
        }
    }

    fn width(&self) -> usize {
        self.width as usize
    }

    fn height(&self) -> u32 {
        self.height
    }

    fn data(&self) -> &[Data<T>] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut Vec<Data<T>> {
        &mut self.data
    }
}

#[derive(Clone, Debug, DagCbor)]
enum Data<T: DagCbor> {
    Value(T),
    Link(Cid),
}

impl<T: DagCbor> Data<T> {
    fn value(&self) -> Option<&T> {
        if let Self::Value(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn cid(&self) -> Option<&Cid> {
        if let Self::Link(cid) = self {
            Some(cid)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libipld::mem::MemStore;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use model::*;

    #[async_std::test]
    async fn test_list() -> Result<()> {
        let store = MemStore::<DefaultParams>::default();
        let mut config = ListConfig::new(store, Code::Blake2b256);
        config.set_width(3);
        let mut list = List::new(config).await?;
        for i in 0..13 {
            assert_eq!(list.get(i).await?, None);
            assert_eq!(list.len().await?, i);
            list.push(i as i64).await?;
            for j in 0..i {
                assert_eq!(list.get(j).await?, Some(j as i64));
            }
        }
        /*for i in 0..13 {
            list.set(i, (i as i128 + 1).into())?;
            assert_eq!(list.get(i)?, int(i + 1));
        }*/
        /*for i in (0..13).rev() {
            assert_eq!(vec.len()?, i + 1);
            assert_eq!(vec.pop()?, int(i));
        }*/
        Ok(())
    }

    #[async_std::test]
    async fn test_list_from() -> Result<()> {
        let store = MemStore::<DefaultParams>::default();
        let mut config = ListConfig::new(store, Code::Blake2b256);
        config.set_width(3);
        let data: Vec<_> = (0..13).map(|i| i as i64).collect();
        let mut list = List::from(config, data.clone().into_iter()).await?;
        let mut data2 = vec![];
        let mut iter = list.iter();
        while let Some(elem) = iter.next().await? {
            data2.push(elem)
        }
        assert_eq!(data, data2);
        Ok(())
    }

    #[test]
    fn list_vec_eqv() {
        const LEN: usize = 25;
        model! {
            Model => let mut vec = Vec::new(),
            Implementation => let mut list = {
                let store = MemStore::<DefaultParams>::default();
                let mut config = ListConfig::new(store, Code::Blake2b256);
                config.set_width(3);
                let fut = List::new(config);
                task::block_on(fut).unwrap()
            },
            Push(usize)(i in 0..LEN) => {
                vec.push(i as i64);
                task::block_on(list.push(i as i64)).unwrap();
            },
            Get(usize)(i in 0..LEN) => {
                let r1 = vec.get(i).cloned();
                let r2 = task::block_on(list.get(i)).unwrap();
                assert_eq!(r1, r2);
            },
            Len(usize)(_ in 0..LEN) => {
                let r1 = vec.len();
                let r2 = task::block_on(list.len()).unwrap();
                assert_eq!(r1, r2);
            },
            IsEmpty(usize)(_ in 0..LEN) => {
                let r1 = vec.is_empty();
                let r2 = task::block_on(list.is_empty()).unwrap();
                assert_eq!(r1, r2);
            }
        }
    }

    #[async_std::test]
    async fn test_width() -> Result<()> {
        let store = MemStore::<DefaultParams>::default();
        let config = ListConfig::new(store, Code::Blake2b256);
        let n = DefaultParams::MAX_BLOCK_SIZE / 8 * 10;
        let _list = List::from(config, (0..n).map(|n| n as u64)).await?;
        Ok(())
    }
}
