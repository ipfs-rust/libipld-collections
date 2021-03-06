use Bit::{One, Zero};

use libipld::cache::{Cache, IpldCache};
use libipld::cbor::{DagCbor, DagCborCodec};
use libipld::prelude::{References, Store, StoreParams};
use libipld::DagCbor;
use libipld::{Cid, Ipld, Result};
use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::iter::once;

// TODO use const generics
const MAP_LEN: usize = 32;

// For testing need a hash with easy collisions
fn hash(bytes: &[u8]) -> Vec<u8> {
    use libipld::multihash::{Hasher, Identity256, Sha2_256};
    if cfg!(test) {
        Identity256::digest(bytes).as_ref().to_vec()
    } else {
        Sha2_256::digest(bytes).as_ref().to_vec()
    }
}

macro_rules! validate {
    ($block:expr) => {
        if $block.data.len() != 0 && $block.data.len() != popcount_all(&$block.map) as usize {
            todo!("Return error: Malformed block");
        }
    };
}

macro_rules! validate_or_empty {
    ($block:expr) => {
        if $block.data.len() == 0 && *$block.map != [0; 32]
            || $block.data.len() != 0 && $block.data.len() != popcount_all(&$block.map) as usize
        {
            todo!("Return error: Malformed block");
        }
    };
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PathNode<T: DagCbor> {
    idx: usize,
    block: Node<T>,
}

impl<T: DagCbor> PathNode<T> {
    fn new(block: Node<T>, idx: usize) -> Self {
        Self { block, idx }
    }
}

// all the information needed to retrace the path down the tree, to "bubble up" changes
// elements should alternate: block -> idx -> block -> idx -> ... -> block
#[derive(Clone, Debug, PartialEq, Eq)]
struct Path<T: DagCbor>(Vec<PathNode<T>>);

impl<T: DagCbor> IntoIterator for Path<T> {
    type Item = PathNode<T>;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: DagCbor> Path<T> {
    fn new() -> Self {
        Path(vec![])
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn record(&mut self, block: Node<T>, idx: usize) {
        self.0.push(PathNode::new(block, idx));
    }
    fn record_last(self, last: Node<T>) -> FullPath<T> {
        FullPath::new(last, self)
    }
    fn pop(&mut self) -> Option<PathNode<T>> {
        self.0.pop()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct FullPath<T: DagCbor> {
    path: Path<T>,
    last: Node<T>,
}

impl<T: DagCbor> FullPath<T> {
    fn new(last: Node<T>, path: Path<T>) -> Self {
        Self { last, path }
    }
    // collapses last node in the path into the previous one if possible
    fn reduce(&mut self, bucket_size: usize) {
        let last = &self.last;
        if !last.has_children() && !last.more_entries_than(bucket_size) && self.len() != 0 {
            let next = self.path.pop().unwrap();
            let PathNode { block, idx } = next;
            let entries = self.last.extract();
            let _ = std::mem::replace(&mut self.last, block);
            self.last.data[idx] = Element::Bucket(entries);
        }
    }
    fn len(&self) -> usize {
        self.path.len()
    }
    // collapses all nodes that are possible into previous ones
    fn full_reduce(&mut self, bucket_size: usize) {
        let mut old = self.len();
        let mut new = 0;
        while old != new {
            old = new;
            self.reduce(bucket_size);
            new = self.len();
        }
        self.last.unset_empty();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Bit {
    Zero,
    One,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum InsertError<T: DagCbor> {
    Id(Entry<T>, Cid, usize),
    Overflow(Vec<Entry<T>>, usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RemoveError {
    Id(Cid, usize),
}

#[cfg(test)]
impl<T: DagCbor> InsertError<T> {
    fn is_id(&self) -> bool {
        if let InsertError::Id(_, _, _) = self {
            true
        } else {
            false
        }
    }
    fn is_overflow(&self) -> bool {
        if let InsertError::Overflow(_, _) = self {
            true
        } else {
            false
        }
    }
}

// number of bits from the left up to and excluding bit itself equal 1
fn popcount(map: &[u8], bit: u8) -> u8 {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifted = if bit % 8 == 0 {
        0
    } else {
        let shifts = (7 - bit % 8) % 8 + 1;
        map[in_byte] >> shifts
    };
    let mut count_ones = 0x00;
    for &byte in map[0..in_byte].iter().chain(once(&shifted)) {
        let mut shifted = byte;
        for _bit in 0..=7 {
            count_ones += 0x01 & shifted;
            shifted >>= 1;
        }
    }
    count_ones
}

fn popcount_all(map: &[u8]) -> u8 {
    // if not true, can overflow
    debug_assert!(map.len() * 8 <= 256);
    let mut count_ones = 0x00;
    for &byte in map.iter() {
        let mut shifted = byte;
        for _bit in 0..=7 {
            count_ones += 0x01 & shifted;
            shifted >>= 1;
        }
    }
    count_ones
}

fn get_bit(map: &[u8], bit: u8) -> Bit {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifts = (7 - bit % 8) % 8;
    let byte = map[in_byte];
    let bit = byte >> shifts & 0x01;
    if bit == 0x01 {
        One
    } else {
        Zero
    }
}

fn set_bit(map: &mut [u8], bit: u8, val: Bit) {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifts = (7 - bit % 8) % 8;
    let bit = 0x01 << shifts;
    match val {
        Bit::One => {
            map[in_byte] |= bit;
        }
        Bit::Zero => {
            map[in_byte] &= !bit;
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
struct Node<T: DagCbor> {
    // map has 2.pow(bit_width) bits, here 256
    map: Box<[u8]>,
    data: Vec<Element<T>>,
}

impl<T: DagCbor> Node<T> {
    fn new() -> Self {
        Self {
            map: Box::new([0; MAP_LEN]),
            data: vec![],
        }
    }
    fn has_children(&self) -> bool {
        self.data.iter().any(|elt| elt.is_hash_node())
    }
    fn more_entries_than(&self, bucket_size: usize) -> bool {
        let mut acc = 0_usize;
        for elt in self.data.iter() {
            if let Element::Bucket(bucket) = elt {
                acc += bucket.len();
                if acc > bucket_size {
                    return true;
                }
            }
        }
        false
    }
    fn extract(&mut self) -> Vec<Entry<T>> {
        let mut entries = Vec::with_capacity(3);
        for elt in self.data.iter_mut() {
            match elt {
                Element::Bucket(bucket) => {
                    for elt in bucket.drain(0..) {
                        entries.push(elt);
                    }
                }
                _ => unreachable!(),
            }
        }
        entries
    }
    fn get(&self, bit: u8) -> Option<&Element<T>> {
        let idx = popcount(&self.map, bit);
        match get_bit(&self.map, bit) {
            Zero => None,
            One => self.data.get(idx as usize),
        }
    }
    fn unset_empty(&mut self) {
        for bit in 0..=255 {
            match self.get(bit) {
                Some(Element::Bucket(bucket)) if bucket.is_empty() => {
                    self.unset(bit);
                }
                _ => {}
            }
        }
    }
    #[cfg(test)]
    fn set(&mut self, index: u8, element: Element<T>) {
        let idx = popcount(&self.map, index);
        match get_bit(&self.map, index) {
            Zero => {
                set_bit(&mut self.map, index, One);
                self.data.insert(idx as usize, element);
            }
            One => {
                self.data[idx as usize] = element;
            }
        }
    }
    fn unset(&mut self, index: u8) {
        let idx = popcount(&self.map, index);
        match get_bit(&self.map, index) {
            Zero => {}
            One => {
                self.data.remove(idx as usize);
                set_bit(&mut self.map, index, Zero);
            }
        }
    }
    // return Overflow with the removed elements if inserting element would overflow bucket
    fn insert(
        &mut self,
        level: usize,
        entry_with_hash: EntryWithHash<T>,
        bucket_size: usize,
    ) -> Result<(), InsertError<T>> {
        use InsertError::{Id, Overflow};
        let hash = entry_with_hash.hash;
        let map_index = hash[level];
        let bit = get_bit(&self.map, map_index);
        let data_index = popcount(&self.map, map_index) as usize;
        let EntryWithHash { entry, .. } = entry_with_hash;
        match bit {
            Zero => {
                self.data.insert(data_index, Element::Bucket(vec![entry]));
                set_bit(&mut self.map, map_index, One);
                Ok(())
            }
            One => {
                match &mut self.data[data_index] {
                    Element::HashNode(cid) => Err(Id(entry, *cid, data_index)),
                    Element::Bucket(ref mut bucket) => {
                        let found = bucket
                            .iter_mut()
                            .find(|elt_mut_ref| elt_mut_ref.key == entry.key);
                        match found {
                            Some(elt) => elt.value = entry.value,
                            None => {
                                if bucket.len() < bucket_size {
                                    bucket.push(entry)
                                } else {
                                    let mut overflow = vec![entry];
                                    overflow.append(bucket);
                                    return Err(Overflow(overflow, data_index));
                                }
                            }
                        }
                        // todo!("Inserting place has to be sorted.");
                        Ok(())
                    }
                }
            }
        }
    }
    fn insert_all(
        &mut self,
        level: usize,
        queue: &mut Queue<T>,
        bucket_size: usize,
    ) -> Result<(), InsertError<T>> {
        while let Some(entry_with_hash) = queue.take() {
            self.insert(level, entry_with_hash, bucket_size)?
        }
        Ok(())
    }
    fn remove(&mut self, level: usize, key: &[u8], hash: &[u8]) -> Result<(), RemoveError> {
        use RemoveError::Id;
        let map_index = hash[level];
        let bit = get_bit(&self.map, map_index);
        let data_index = popcount(&self.map, map_index) as usize;
        match bit {
            Zero => Ok(()),
            One => {
                let elt = &mut self.data[data_index];
                match elt {
                    Element::HashNode(cid) => Err(Id(*cid, data_index)),
                    Element::Bucket(bucket) if bucket.len() != 1 => {
                        for i in 0..bucket.len() {
                            if &*bucket[i].key == key {
                                bucket.remove(i);
                                return Ok(());
                            }
                        }
                        // todo!("Inserting place has to be sorted.");
                        Ok(())
                    }
                    Element::Bucket(_) => {
                        self.data.remove(data_index);
                        set_bit(&mut self.map, map_index, Bit::Zero);
                        Ok(())
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
enum Element<T: DagCbor> {
    HashNode(Cid),
    Bucket(Vec<Entry<T>>),
}

impl<T: DagCbor> Default for Element<T> {
    fn default() -> Self {
        Element::Bucket(vec![])
    }
}

impl<T: DagCbor> Element<T> {
    fn is_hash_node(&self) -> bool {
        match self {
            Element::HashNode(_) => true,
            Element::Bucket(_) => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
struct Entry<T: DagCbor> {
    key: Box<[u8]>,
    value: T,
}

impl<T: DagCbor> Entry<T> {
    pub fn new<I: Into<Box<[u8]>>>(key: I, value: T) -> Self {
        Entry {
            key: key.into(),
            value,
        }
    }
    fn with_hash(self) -> EntryWithHash<T> {
        let hash = hash(&self.key);
        EntryWithHash { entry: self, hash }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Queue<T: DagCbor> {
    entries: Vec<EntryWithHash<T>>,
}

impl<T: DagCbor> Queue<T> {
    fn new() -> Self {
        Self { entries: vec![] }
    }
    fn take(&mut self) -> Option<EntryWithHash<T>> {
        self.entries.pop()
    }
    fn add(&mut self, entry: Entry<T>) {
        self.entries.insert(0, entry.with_hash());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EntryWithHash<T: DagCbor> {
    entry: Entry<T>,
    hash: Vec<u8>,
}

pub struct HamtConfig<S>
where
    S: Store,
    <S::Params as StoreParams>::Codecs: Into<DagCborCodec>,
    DagCborCodec: Into<<S::Params as StoreParams>::Codecs>,
    Ipld: References<<S::Params as StoreParams>::Codecs>,
{
    store: S,
    cache_size: usize,
    hash: <S::Params as StoreParams>::Hashes,
    bucket_size: usize,
}

impl<S> HamtConfig<S>
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
            bucket_size: 3,
        }
    }

    pub fn set_cache_size(&mut self, cache_size: usize) {
        self.cache_size = cache_size;
    }

    pub fn set_bucket_size(&mut self, bucket_size: usize) {
        self.bucket_size = bucket_size;
    }

    fn bucket_size(&self) -> usize {
        self.bucket_size
    }

    fn cache<T>(self) -> IpldCache<S, DagCborCodec, Node<T>>
    where
        T: DagCbor + Clone + Send + Sync,
    {
        IpldCache::new(self.store, DagCborCodec, self.hash, self.cache_size)
    }
}

pub struct Hamt<S: Store, T: DagCbor> {
    cache: IpldCache<S, DagCborCodec, Node<T>>,
    root: Cid,
    tmp: S::TempPin,
    bucket_size: usize,
}

impl<S, T> Hamt<S, T>
where
    S: Store,
    <S::Params as StoreParams>::Codecs: Into<DagCborCodec>,
    DagCborCodec: Into<<S::Params as StoreParams>::Codecs>,
    Ipld: References<<S::Params as StoreParams>::Codecs>,
    T: DagCbor + Clone + Send + Sync,
{
    pub async fn new(config: HamtConfig<S>) -> Result<Self> {
        let bucket_size = config.bucket_size();
        let cache = config.cache();
        let tmp = cache.temp_pin().await?;
        let root = cache.insert(Node::new(), Some(&tmp)).await?;
        Ok(Self {
            cache,
            root,
            tmp,
            bucket_size,
        })
    }

    pub async fn open(config: HamtConfig<S>, root: Cid) -> Result<Self> {
        let bucket_size = config.bucket_size();
        let cache = config.cache();
        let tmp = cache.temp_pin().await?;
        // warm up the cache and make sure it's available
        cache.get(&root, Some(&tmp)).await?;
        Ok(Self {
            cache,
            root,
            tmp,
            bucket_size,
        })
    }

    pub fn root(&self) -> &Cid {
        &self.root
    }

    pub async fn from<I: Into<Box<[u8]>>>(
        config: HamtConfig<S>,
        btree: BTreeMap<I, T>,
    ) -> Result<Self> {
        let mut hamt = Hamt::new(config).await?;
        for (key, value) in btree {
            hamt.insert(key.into(), value).await?;
        }
        Ok(hamt)
    }

    // retrace the path traveled backwards, "bubbling up" the changes
    async fn bubble_up(&mut self, full_path: FullPath<T>) -> Result<Cid> {
        let FullPath {
            last: mut block,
            path,
        } = full_path;
        let path = path.into_iter().rev();
        let mut cid = self.cache.insert(block, Some(&self.tmp)).await?;
        for elt in path {
            let PathNode { idx, block: node } = elt;
            block = node;
            block.data[idx] = Element::HashNode(cid);
            cid = self.cache.insert(block, Some(&self.tmp)).await?;
        }
        Ok(cid)
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<T>> {
        // TODO calculate correct hash
        let hash = hash(&key);

        let mut current = self.cache.get(&self.root, Some(&self.tmp)).await?;
        validate_or_empty!(current);
        for index in hash.iter() {
            let bit = get_bit(&current.map, *index);
            if let Bit::Zero = bit {
                return Ok(None);
            }
            let data_index = popcount(&current.map, *index) as usize;
            let Node { mut data, .. } = current;
            current = match data.remove(data_index) {
                Element::HashNode(cid) => self.cache.get(&cid, Some(&self.tmp)).await?,
                Element::Bucket(bucket) => {
                    for elt in bucket {
                        if &*elt.key == key {
                            let Entry { value, .. } = elt;
                            return Ok(Some(value));
                        }
                    }
                    return Ok(None);
                }
            };
            validate!(current);
        }
        Ok(None)
    }

    pub async fn insert(&mut self, key: Box<[u8]>, value: T) -> Result<()> {
        let mut queue = Queue::new();
        let hash_len = hash(&key).len();
        queue.add(Entry::new(key, value));
        let mut path = Path::new();
        // start from root going down
        let mut current = self.cache.get(&self.root, Some(&self.tmp)).await?;
        for lvl in 0..hash_len {
            // validate_or_empty!(current);
            use InsertError::{Id, Overflow};
            match current.insert_all(lvl, &mut queue, self.bucket_size) {
                Ok(_) => {
                    let full_path = path.record_last(current);
                    // recalculate cids recursively
                    self.root = self.bubble_up(full_path).await?;
                    return Ok(());
                }
                Err(Id(entry, cid, data_index)) => {
                    path.record(current, data_index);
                    queue.add(entry);
                    current = self.cache.get(&cid, Some(&self.tmp)).await?;
                    validate!(current);
                }
                Err(Overflow(overflow, data_index)) => {
                    for elt in overflow {
                        queue.add(elt);
                    }
                    path.record(current, data_index);
                    current = Node::new();
                }
            }
        }
        todo!("Output error due to maximum collision depth reached");
    }

    pub async fn remove(&mut self, key: &[u8]) -> Result<()> {
        use RemoveError::Id;
        let hash = hash(key);
        let hash_len = hash.len();
        let mut path = Path::new();
        // start from root going down
        let mut current = self.cache.get(&self.root, Some(&self.tmp)).await?;
        // validate_or_empty!(current);
        for lvl in 0..hash_len {
            match current.remove(lvl, key, &hash) {
                Ok(_) => {
                    let mut full_path = path.record_last(current);
                    full_path.full_reduce(self.bucket_size);
                    // recalculate cids recursively
                    self.root = self.bubble_up(full_path).await?;
                    return Ok(());
                }
                Err(Id(cid, data_index)) => {
                    path.record(current, data_index);
                    current = self.cache.get(&cid, Some(&self.tmp)).await?;
                    validate!(current);
                }
            }
        }
        todo!("Output error due to maximum collision depth reached");
    }

    pub async fn flush<A: AsRef<[u8]> + Send + Sync>(&mut self, alias: A) -> Result<()> {
        self.cache.alias(alias, Some(self.root())).await?;
        self.tmp = self.cache.temp_pin().await?;
        self.cache.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libipld::mem::MemStore;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use proptest::prelude::*;

    #[test]
    fn test_popcount() {
        assert_eq!(popcount(&[0b0000_0001], 0), 0);
        assert_eq!(popcount(&[0b0000_0001], 7), 0);
        assert_eq!(popcount(&[0b0000_0010], 7), 1);
        assert_eq!(popcount(&[0b0000_0011], 7), 1);
        assert_eq!(popcount(&[0b0000_0100], 7), 1);
        assert_eq!(popcount(&[0b0000_0101], 7), 1);
        assert_eq!(popcount(&[0b0000_0110], 7), 2);
        assert_eq!(popcount(&[0b0000_0111], 7), 2);
        assert_eq!(popcount(&[0b0000_1000], 7), 1);
    }

    fn strat_bit_value() -> impl Strategy<Value = Bit> {
        prop_oneof![Just(Bit::Zero), Just(Bit::One),]
    }

    fn strat_vec_and_bit() -> impl Strategy<Value = (Vec<u8>, u8)> {
        prop::collection::vec(0..255u8, 2..32).prop_flat_map(|vec| {
            let len = vec.len();
            (Just(vec), 8..(len * 8) as u8)
        })
    }

    proptest! {
        #[test]
        fn test_popcount_invariant((vec, bit) in strat_vec_and_bit()) {
            let mut shift = vec.clone();

            shift.pop();
            shift.insert(0, 0);
            assert_eq!(popcount(&shift, bit), popcount(&vec, bit - 8));
        }

        #[test]
        fn test_popcount_shift((vec, bit) in strat_vec_and_bit()) {
            let mut set_one_zero = vec.clone();
            set_bit(&mut set_one_zero, bit - 1, Bit::Zero);
            assert_eq!(popcount(&set_one_zero, bit), popcount(&vec, bit - 1));
        }

        #[test]
        fn test_set_and_get((mut vec, bit) in strat_vec_and_bit(),val in strat_bit_value()) {
            set_bit(&mut vec, bit, val.clone());
            assert_eq!(get_bit(&vec, bit), val);
        }
    }

    #[test]
    fn test_get_bit() {
        assert_eq!(get_bit(&[0b0000_0001], 7), Bit::One);
        assert_eq!(get_bit(&[0b0000_0010], 6), Bit::One);
        assert_eq!(get_bit(&[0b0000_0100], 5), Bit::One);
        assert_eq!(get_bit(&[0b0000_1000], 4), Bit::One);
        assert_eq!(get_bit(&[0b0001_0000], 3), Bit::One);
        assert_eq!(get_bit(&[0b0010_0000], 2), Bit::One);
        assert_eq!(get_bit(&[0b0100_0000], 1), Bit::One);
        assert_eq!(get_bit(&[0b1000_0000], 0), Bit::One);
    }

    fn dummy_node() -> Node<u8> {
        Node {
            map: Box::new([0_u8]),
            data: vec![],
        }
    }

    async fn dummy_hamt() -> Hamt<MemStore<DefaultParams>, u8> {
        let store = MemStore::default();
        let mut config = HamtConfig::new(store, Code::Blake2b256);
        config.set_bucket_size(1);
        Hamt::new(config).await.unwrap()
    }

    #[async_std::test]
    async fn test_dummy_hamt() {
        let hamt = dummy_hamt().await;
        assert_eq!(
            &[
                17, 5, 205, 113, 186, 135, 108, 41, 45, 228, 103, 3, 117, 148, 111, 12, 194, 34,
                144, 30, 201, 157, 222, 81, 41, 154, 114, 30, 207, 222, 150, 53
            ],
            hamt.root.hash().digest()
        );
    }

    #[test]
    fn test_node_insert() {
        let mut node = dummy_node();
        assert_eq!(
            node.insert(0, Entry::new([0, 0, 0], 0).with_hash(), 3),
            Ok(())
        );
        assert_eq!(
            node.insert(0, Entry::new([0, 0, 1], 0).with_hash(), 3),
            Ok(())
        );
        assert_eq!(
            node.insert(0, Entry::new([0, 0, 2], 1).with_hash(), 3),
            Ok(())
        );
        assert!(node
            .insert(0, Entry::new([0, 0, 3], 3).with_hash(), 3)
            .unwrap_err()
            .is_overflow());
    }
    #[test]
    fn test_node_remove() {
        let mut node = dummy_node();
        assert_eq!(node.insert(1, Entry::new([0, 0], 0).with_hash(), 1), Ok(()));
        assert_eq!(node.insert(1, Entry::new([0, 1], 0).with_hash(), 1), Ok(()));
        assert_eq!(node.remove(1, &[0, 0], &[0, 0]), Ok(()));
        assert_eq!(node.remove(1, &[0, 1], &[0, 1]), Ok(()));
        assert_eq!(node, dummy_node());
    }

    #[test]
    fn test_node_methods() {
        let mut node = dummy_node();
        let entries = vec![
            Entry::new([0, 0, 0], 0),
            Entry::new([0, 0, 1], 0),
            Entry::new([0, 0, 2], 0),
            Entry::new([1, 0, 2], 0),
        ];
        for elt in entries.iter().take(3) {
            let _ = node.insert(0, elt.clone().with_hash(), 3);
        }
        assert!(!node.has_children());
        assert!(!node.more_entries_than(3));
        let _ = node.insert(0, entries[3].clone().with_hash(), 3);
        assert!(node.more_entries_than(3));
        assert_eq!(node.extract(), entries);

        let mut node: Node<u8> = Node::new();
        node.set(3, Element::default());
        node.unset(3);
        assert_eq!(node, Node::new());
        for i in 0..=255 {
            node.set(i, Element::default());
            node.set(i, Element::default());
        }
        for i in 0..=255 {
            node.unset(i);
            node.unset(i);
        }
        assert_eq!(node, Node::new());
    }

    #[async_std::test]
    async fn test_bubble_up() {
        let mut hamt = dummy_hamt().await;
        let mut node1 = dummy_node();
        let mut node2 = dummy_node();
        node1.set(0, Element::Bucket(vec![]));
        let cid = hamt.cache.insert(node1, None).await.unwrap();
        node2.set(0, Element::HashNode(cid));
        let cid = hamt.cache.insert(node2, None).await.unwrap();
        hamt.root = cid;

        let mut hamt_clone = dummy_hamt().await;
        let mut node1_clone = dummy_node();
        node1_clone.set(0, Element::Bucket(vec![]));
        let mut node2_clone = dummy_node();
        let mut path = Path::new();
        node2_clone.set(0, Element::Bucket(vec![]));
        path.record(node2_clone, 0);
        let full_path = path.record_last(node1_clone);
        let cid = hamt_clone.bubble_up(full_path).await.unwrap();
        hamt_clone.root = cid;

        assert_eq!(hamt.root, hamt_clone.root);
        let block = hamt.cache.get(&hamt.root, None).await.unwrap();
        let block_compare = hamt_clone.cache.get(&hamt_clone.root, None).await.unwrap();
        assert_eq!(block, block_compare);
    }

    #[async_std::test]
    async fn test_reduce() {
        let size = 2;
        let entries = vec![Entry::new([0, 0], 0), Entry::new([0, 1], 0)];
        let mut node = Node::new();
        node.set(0, Element::HashNode(Cid::default()));
        let mut path = Path::new();
        path.record(node, 0);
        let mut next = Node::new();
        next.set(0, Element::Bucket(vec![entries[0].clone()]));
        next.set(1, Element::Bucket(vec![entries[1].clone()]));
        let mut full_path = path.record_last(next);
        let pre_len = full_path.len();

        full_path.reduce(size);
        let post_len = full_path.len();
        assert!(pre_len != post_len);
    }

    #[async_std::test]
    async fn test_hamt_insert() {
        // insert single element
        let mut hamt = dummy_hamt().await;
        let entry = Entry::new([0, 0, 0], 0);
        hamt.insert(entry.key, entry.value).await.unwrap();
        let mut node = Node::new();
        let _ = node.insert(0, Entry::new([0, 0, 0], 0).with_hash(), 3);
        assert_eq!(node, hamt.cache.get(&hamt.root, None).await.unwrap());
        let mut hamt = dummy_hamt().await;
        let entry1 = Entry::new([0, 0, 0], 0);
        let entry2 = Entry::new([0, 0, 1], 0);
        let entry3 = Entry::new([0, 0, 2], 0);
        let entry4 = Entry::new([0, 0, 3], 0);
        let entries = vec![entry1, entry2, entry3, entry4];
        let copy = entries.clone();
        for entry in entries {
            hamt.insert(entry.key, entry.value).await.unwrap();
        }
        let mut node = hamt.cache.get(&hamt.root, None).await.unwrap();
        assert_eq!(
            &hamt.root.hash().digest(),
            &[
                10, 133, 110, 7, 1, 116, 103, 149, 130, 193, 198, 132, 161, 142, 33, 76, 89, 142,
                81, 181, 60, 135, 167, 116, 140, 112, 168, 13, 40, 172, 223, 90
            ]
        );
        assert!(node
            .insert(0, copy[0].clone().with_hash(), 3)
            .unwrap_err()
            .is_id());
        for entry in copy {
            assert_eq!(Some(entry.value), hamt.get(&entry.key).await.unwrap());
        }
    }
    proptest! {
        #[test]
        fn test_hamt_set_and_get(batch in prop::collection::vec((prop::collection::vec(0..=255u8, 6), 0..1u8), 20)) {
            let _ = task::block_on(batch_set_and_get(batch)).unwrap();
        }
        #[test]
        fn test_hamt_remove_and_get(batch in prop::collection::vec((prop::collection::vec(0..=255u8, 6), 0..1u8), 20)) {
            let _ = task::block_on(batch_remove_and_get(batch)).unwrap();
        }
    }

    async fn batch_set_and_get(batch: Vec<(Vec<u8>, u8)>) -> Result<()> {
        let mut hamt = dummy_hamt().await;
        for elt in batch.into_iter() {
            let (key, val) = elt;
            hamt.insert(key.clone().into(), val).await?;
            let elt = hamt.get(&key).await?;
            assert_eq!(elt, Some(val));
        }
        Ok(())
    }
    async fn batch_remove_and_get(mut batch: Vec<(Vec<u8>, u8)>) -> Result<()> {
        let mut other = dummy_hamt().await;
        let mut hamt = dummy_hamt().await;
        let size = batch.len();

        // make sure there are no repeated keys
        batch.sort();
        batch.dedup_by(|a, b| a.0 == b.0);

        let insert_batch = batch.clone();
        let mut remove_batch = vec![];
        let mut get_batch = vec![];
        for (counter, elt) in batch.into_iter().enumerate() {
            if counter <= size / 2 {
                get_batch.push(elt);
            } else {
                remove_batch.push(elt);
            }
        }
        for elt in insert_batch.into_iter() {
            let (key, val) = elt;
            hamt.insert(key.clone().into(), val).await?;
        }
        for elt in remove_batch.into_iter() {
            let (key, _) = elt;
            hamt.remove(&key).await?;
        }

        // inserting n elements into a hamt other should give equal root cid as
        // inserting additional n elements and deleting them
        let insert_other_batch = get_batch.clone();
        for elt in insert_other_batch.into_iter() {
            let (key, val) = elt;
            other.insert(key.clone().into(), val).await?;
        }

        // the non-removed elements should be retrievable
        for elt in get_batch.into_iter() {
            let (key, _) = elt;
            assert!(hamt.get(&key).await?.is_some());
        }

        assert_eq!(hamt.root, other.root);
        Ok(())
    }
    #[async_std::test]
    async fn test_remove() {
        // first deletion test
        let mut other = dummy_hamt().await;
        let mut hamt = dummy_hamt().await;
        let entries = vec![
            Entry::new([0, 0], 0),
            Entry::new([0, 1], 0),
            // Entry::new(vec![0, 2], 0),
            // Entry::new(vec![0, 3], 0),
        ];
        let mut entries_clone = entries.clone();
        for entry in entries {
            hamt.insert(entry.key, entry.value).await.unwrap();
        }
        let entry = entries_clone.pop().unwrap();
        other.insert(entry.key, entry.value).await.unwrap();
        for entry in entries_clone {
            hamt.remove(&entry.key).await.unwrap();
        }
        assert_eq!(hamt.root, other.root);
    }
}
