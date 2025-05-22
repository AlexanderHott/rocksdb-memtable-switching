#![allow(clippy::needless_return)]
#![allow(dead_code)]

use anyhow::{bail, Context, Result};
use rand::distr::Alphanumeric;
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256Plus;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Workload specification.
pub mod spec {
    use schemars::JsonSchema;

    /// Specification for inserts in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone, Debug)]
    pub struct Inserts {
        /// Number of inserts
        pub(crate) amount: usize,
        /// Key length
        pub(crate) key_len: usize,
        /// Value length
        pub(crate) val_len: usize,
    }

    /// Specification for updates in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone, Debug)]
    pub struct Updates {
        /// Number of updates
        pub(crate) amount: usize,
        /// Value length
        pub(crate) val_len: usize,
    }

    /// Specification for point deletes in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone, Debug)]
    pub struct Deletes {
        /// Number of deletes
        pub(crate) amount: usize,
    }

    /// Specification for point queries in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone, Debug)]
    pub struct PointQueries {
        /// Number of point queries
        pub(crate) amount: usize,
    }

    /// Specification for empty point queries in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone, Debug)]
    pub struct EmptyPointQueries {
        /// Number of point queries
        pub(crate) amount: usize,
        /// Key length
        pub(crate) key_len: usize,
    }

    /// Specification for range queries in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone, Debug)]
    pub struct RangeQueries {
        /// Number of range queries
        pub(crate) amount: usize,
        /// Selectivity of range queries. Based off of the range of valid keys, not the full
        /// key-space.
        pub(crate) selectivity: f32,
    }

    #[derive(serde::Deserialize, JsonSchema, Copy, Clone, Debug)]
    pub(crate) struct WorkloadSpecGroup {
        pub(crate) inserts: Option<Inserts>,
        pub(crate) updates: Option<Updates>,
        pub(crate) deletes: Option<Deletes>,
        pub(crate) point_queries: Option<PointQueries>,
        pub(crate) empty_point_queries: Option<EmptyPointQueries>,
        pub(crate) range_queries: Option<RangeQueries>,
    }

    impl WorkloadSpecGroup {
        pub fn operation_count(&self) -> usize {
            let operation_count = self.inserts.map_or(0, |s| s.amount)
                + self.updates.map_or(0, |us| us.amount)
                + self.point_queries.map_or(0, |is| is.amount)
                + self.empty_point_queries.map_or(0, |is| is.amount)
                + self.range_queries.map_or(0, |is| is.amount)
                + self.deletes.map_or(0, |is| is.amount);
            return operation_count;
        }

        pub fn bytes_count(&self, insert_key_len: usize) -> usize {
            let bytes_insert = self.inserts.map_or(0, |is| {
                (b"I ".len() + is.key_len + b" ".len() + is.val_len + b"\n".len()) * is.amount
            });
            let bytes_update = self.updates.map_or(0, |us| {
                (b"U ".len() + insert_key_len + b" ".len() + us.val_len + b"\n".len()) * us.amount
            });
            let bytes_delete = self.deletes.map_or(0, |ds| {
                (b"D ".len() + insert_key_len + b"\n".len()) * ds.amount
            });
            let bytes_point_queries = self.point_queries.map_or(0, |pq| {
                (b"P ".len() + insert_key_len + b"\n".len()) * pq.amount
            });
            let bytes_empty_point_queries = self.empty_point_queries.map_or(0, |epq| {
                (b"P ".len() + epq.key_len + b"\n".len()) * epq.amount
            });
            let bytes_range_queries = self.range_queries.map_or(0, |rq| {
                (b"R ".len() + insert_key_len + b" ".len() + insert_key_len + b"\n".len())
                    * rq.amount
            });
            return bytes_insert
                + bytes_update
                + bytes_delete
                + bytes_point_queries
                + bytes_empty_point_queries
                + bytes_range_queries;
        }

        // pub fn needs_static_sorted_keys(&self) -> bool {
        //     return self.range_queries.is_some();
        // }
        //
        // pub fn needs_dynamic_sorted_keys(&self) -> bool {
        //     return (self.inserts.is_some() || self.deletes.is_some())
        //         && self.range_queries.is_some();
        // }
    }

    #[derive(serde::Deserialize, JsonSchema, Default, Clone, Debug)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum KeySpace {
        #[default]
        Alphanumeric,
    }
    #[derive(serde::Deserialize, JsonSchema, Default, Clone, Debug)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum KeyDistribution {
        #[default]
        Uniform,
    }

    #[derive(serde::Deserialize, JsonSchema, Clone, Debug)]
    pub(crate) struct WorkloadSpecSection {
        /// A list of operation groups that share keys between operations.
        ///
        /// E.g., non-empty point queries will use a key from an insert in this group.
        pub(crate) groups: Vec<WorkloadSpecGroup>,
        /// The domain from which the keys will be created from.
        #[serde(default = "KeySpace::default")]
        pub(crate) key_space: KeySpace,
        /// The domain from which the keys will be created from.
        #[serde(default = "KeyDistribution::default")]
        pub(crate) key_distribution: KeyDistribution,
    }

    impl WorkloadSpecSection {
        pub fn operation_count(&self) -> usize {
            return self.groups.iter().map(|g| g.operation_count()).sum();
        }

        pub fn bytes_count(&self) -> usize {
            let insert_key_len = self
                .groups
                .iter()
                .map(|g| g.inserts.map_or(0, |is| is.key_len))
                .max()
                .expect("No groups in workload spec");
            return self
                .groups
                .iter()
                .map(|g| g.bytes_count(insert_key_len))
                .sum();
        }

        pub fn insert_count(&self) -> usize {
            return self
                .groups
                .iter()
                .map(|g| g.inserts.map_or(0, |is| is.amount))
                .sum();
        }

        pub fn has_inserts(&self) -> bool {
            return self.groups.iter().any(|g| g.inserts.is_some());
        }

        pub fn has_updates(&self) -> bool {
            return self.groups.iter().any(|g| g.updates.is_some());
        }
        pub fn has_deletes(&self) -> bool {
            return self.groups.iter().any(|g| g.deletes.is_some());
        }
        pub fn has_point_queries(&self) -> bool {
            return self.groups.iter().any(|g| g.point_queries.is_some());
        }
        pub fn has_empty_point_queries(&self) -> bool {
            return self.groups.iter().any(|g| g.empty_point_queries.is_some());
        }

        pub fn has_range_queries(&self) -> bool {
            return self.groups.iter().any(|g| g.range_queries.is_some());
        }
    }

    #[derive(serde::Deserialize, JsonSchema, Debug, Clone)]
    pub struct WorkloadSpec {
        /// Sections of a workload where a key from one will (probably) not appear in another.
        pub(crate) sections: Vec<WorkloadSpecSection>,
    }

    impl WorkloadSpec {
        pub fn operation_count(&self) -> usize {
            return self.sections.iter().map(|s| s.operation_count()).sum();
        }

        pub fn bytes_count(&self) -> usize {
            return self.sections.iter().map(|s| s.bytes_count()).sum();
        }
    }
}

/// Json schema generation
mod schema {
    use crate::spec::WorkloadSpec;
    use schemars::schema_for;

    pub fn generate_workload_spec_schema() -> serde_json::Result<String> {
        let schema = schema_for!(WorkloadSpec);
        return serde_json::to_string_pretty(&schema);
    }
}

mod keyset {
    use crate::Key;
    use bloom::{BloomFilter, ASMS};
    use rand::Rng;
    use rand_xoshiro::Xoshiro256Plus;
    use std::collections::{HashMap, HashSet};

    pub trait KeySet {
        fn new(capacity: usize) -> Self;

        fn len(&self) -> usize;

        fn is_empty(&self) -> bool;

        fn push(&mut self, key: Key);

        fn remove(&mut self, idx: usize) -> Key;

        fn get(&self, idx: usize) -> Option<&Key>;

        fn get_random(&self, rng: &mut Xoshiro256Plus) -> &Key;

        fn contains(&self, key: &Key) -> bool;

        fn sort(&mut self);
    }

    pub struct VecKeySet {
        keys: Vec<Key>,
        sorted: bool,
    }

    impl KeySet for VecKeySet {
        fn new(capacity: usize) -> Self {
            return Self {
                keys: Vec::with_capacity(capacity),
                sorted: true,
            };
        }

        fn len(&self) -> usize {
            return self.keys.len();
        }

        fn is_empty(&self) -> bool {
            return self.keys.is_empty();
        }

        fn push(&mut self, key: Key) {
            if self.sorted && self.keys.last().is_some_and(|last_key| last_key > &key) {
                self.sorted = false;
            }
            self.keys.push(key);
        }

        fn remove(&mut self, idx: usize) -> Key {
            let key = self.keys.remove(idx);
            return key;
        }

        fn get(&self, idx: usize) -> Option<&Key> {
            return self.keys.get(idx);
        }

        fn get_random(&self, rng: &mut Xoshiro256Plus) -> &Key {
            return self
                .keys
                .get(rng.random_range(0..self.keys.len()))
                .expect("KeySet to not be empty");
        }

        fn contains(&self, key: &Key) -> bool {
            return self.keys.contains(key);
        }

        fn sort(&mut self) {
            if !self.sorted {
                self.keys.sort();
                self.sorted = true;
            }
        }
    }
    pub struct VecHashSetKeySet {
        keys: Vec<Key>,
        key_set: HashSet<Key>,
        sorted: bool,
    }

    impl KeySet for VecHashSetKeySet {
        fn new(capacity: usize) -> Self {
            return Self {
                keys: Vec::with_capacity(capacity),
                key_set: HashSet::with_capacity(capacity),
                sorted: true,
            };
        }

        fn len(&self) -> usize {
            return self.keys.len();
        }

        fn is_empty(&self) -> bool {
            return self.keys.is_empty();
        }

        fn push(&mut self, key: Key) {
            if self.sorted && self.keys.last().is_some_and(|last_key| last_key > &key) {
                self.sorted = false;
            }
            self.keys.push(key.clone());
            self.key_set.insert(key);
        }

        fn remove(&mut self, idx: usize) -> Key {
            let key = self.keys.remove(idx);
            self.key_set.remove(&key);
            return key;
        }

        fn get(&self, idx: usize) -> Option<&Key> {
            return self.keys.get(idx);
        }

        fn get_random(&self, rng: &mut Xoshiro256Plus) -> &Key {
            return self
                .keys
                .get(rng.random_range(0..self.keys.len()))
                .expect("KeySet to not be empty");
        }

        fn contains(&self, key: &Key) -> bool {
            return self.key_set.contains(key);
        }

        fn sort(&mut self) {
            if !self.sorted {
                self.keys.sort();
                self.sorted = true;
            }
        }
    }
    pub struct VecBloomFilterKeySet {
        keys: Vec<Key>,
        bf: BloomFilter,
        sorted: bool,
    }

    impl KeySet for VecBloomFilterKeySet {
        fn new(capacity: usize) -> Self {
            return Self {
                keys: Vec::with_capacity(capacity),
                bf: BloomFilter::with_rate(0.01, capacity as u32),
                sorted: true,
            };
        }

        fn len(&self) -> usize {
            return self.keys.len();
        }

        fn is_empty(&self) -> bool {
            return self.keys.is_empty();
        }

        fn push(&mut self, key: Key) {
            if self.sorted && self.keys.last().is_some_and(|last_key| last_key > &key) {
                self.sorted = false;
            }
            self.bf.insert(&key);
            self.keys.push(key);
        }

        fn remove(&mut self, idx: usize) -> Key {
            let key = self.keys.remove(idx);
            self.bf.clear();
            for k in &self.keys {
                self.bf.insert(k);
            }
            return key;
        }

        fn get(&self, idx: usize) -> Option<&Key> {
            return self.keys.get(idx);
        }

        fn get_random(&self, rng: &mut Xoshiro256Plus) -> &Key {
            return self
                .keys
                .get(rng.random_range(0..self.keys.len()))
                .expect("KeySet to not be empty");
        }

        fn contains(&self, key: &Key) -> bool {
            return self.bf.contains(key);
        }

        fn sort(&mut self) {
            if !self.sorted {
                self.keys.sort();
                self.sorted = true;
            }
        }
    }

    pub struct VecHashMapIndexKeySet {
        keys: Vec<Key>,
        key_to_index: HashMap<Key, usize>,
        sorted: bool,
    }

    impl KeySet for VecHashMapIndexKeySet {
        fn new(capacity: usize) -> Self {
            return Self {
                keys: Vec::with_capacity(capacity),
                key_to_index: HashMap::with_capacity(capacity),
                sorted: true,
            };
        }

        fn len(&self) -> usize {
            return self.keys.len();
        }

        fn is_empty(&self) -> bool {
            return self.keys.is_empty();
        }

        fn push(&mut self, key: Key) {
            if !self.key_to_index.contains_key(&key) {
                self.key_to_index.insert(key.clone(), self.keys.len());
                self.keys.push(key);
            }
        }

        fn remove(&mut self, idx: usize) -> Key {
            assert!(idx < self.keys.len());

            // Swap with last, pop, and update hashmap
            self.keys.swap(idx, self.keys.len() - 1);
            let removed = self.keys.pop().unwrap();
            self.key_to_index.remove(&removed);

            // Update index of swapped element if necessary
            if idx < self.keys.len() {
                let swapped_key = &self.keys[idx];
                self.key_to_index.insert(swapped_key.clone(), idx);
            }

            return removed;
        }

        fn get(&self, idx: usize) -> Option<&Key> {
            return self.keys.get(idx);
        }

        fn get_random(&self, rng: &mut Xoshiro256Plus) -> &Key {
            let idx = rng.random_range(0..self.keys.len());
            return &self.keys[idx];
        }

        fn contains(&self, key: &Key) -> bool {
            return self.key_to_index.contains_key(key);
        }

        fn sort(&mut self) {
            self.keys.sort();
            self.key_to_index.clear();
            for (i, key) in self.keys.iter().enumerate() {
                self.key_to_index.insert(key.clone(), i);
            }
        }
    }
}

pub use crate::schema::generate_workload_spec_schema;
use crate::spec::WorkloadSpec;
use crate::keyset::{KeySet, VecHashSetKeySet, VecKeySet};

type Key = Box<[u8]>;

struct AsciiWriter;
impl AsciiWriter {
    fn write_insert(w: &mut impl Write, key: &Key, val: &Key) -> Result<()> {
        w.write_all("I ".as_bytes())?;
        w.write_all(key)?;
        w.write_all(" ".as_bytes())?;
        w.write_all(val)?;
        w.write_all("\n".as_bytes())?;

        return Ok(());
    }
    fn write_update(w: &mut impl Write, key: &Key, val: &Key) -> Result<()> {
        w.write_all("U ".as_bytes())?;
        w.write_all(key)?;
        w.write_all(" ".as_bytes())?;
        w.write_all(val)?;
        w.write_all("\n".as_bytes())?;

        return Ok(());
    }
    fn write_delete(w: &mut impl Write, key: &Key) -> Result<()> {
        w.write_all("D ".as_bytes())?;
        w.write_all(key)?;
        w.write_all("\n".as_bytes())?;

        return Ok(());
    }
    fn write_point_query(w: &mut impl Write, key: &Key) -> Result<()> {
        w.write_all("P ".as_bytes())?;
        w.write_all(key)?;
        w.write_all("\n".as_bytes())?;

        return Ok(());
    }
    fn write_range_query(w: &mut impl Write, key1: &Key, key2: &Key) -> Result<()> {
        w.write_all("R ".as_bytes())?;
        w.write_all(key1)?;
        w.write_all(" ".as_bytes())?;
        w.write_all(key2)?;
        w.write_all("\n".as_bytes())?;

        return Ok(());
    }
}

#[derive(Debug, Copy, Clone, Eq, Ord, PartialOrd, PartialEq)]
enum OpMarker {
    Insert,
    Update,
    Delete,
    PointQuery,
    EmptyPointQuery,
    RangeQuery,
}

#[inline]
fn gen_string(rng: &mut Xoshiro256Plus, len: usize) -> Key {
    return rng.sample_iter(Alphanumeric).take(len).collect();
}

pub fn write_operations(mut writer: &mut impl Write, workload: &WorkloadSpec) -> Result<()> {
    let mut rng = Xoshiro256Plus::from_os_rng();

    for section in &workload.sections {
        let mut keys_valid = keyset::VecBloomFilterKeySet::new(section.insert_count());

        for group in &section.groups {
            let rng_ref = &mut rng;
            let mut markers: Vec<OpMarker> = Vec::with_capacity(group.operation_count());

            if let Some(ds) = group.deletes {
                if ds.amount > keys_valid.len() {
                    bail!("Cannot have more deletes than existing valid keys.");
                }
            }

            // A group must have at least 1 valid key before any other operation can occur.
            // TODO: handle empty point queries
            if (group.inserts.is_some()
                || group.updates.is_some()
                || group.deletes.is_some()
                || group.point_queries.is_some()
                || group.range_queries.is_some())
                && keys_valid.is_empty()
            {
                if let Some(is) = group.inserts {
                    markers.append(&mut vec![OpMarker::Insert; is.amount - 1]);

                    let key = gen_string(rng_ref, is.key_len);
                    let val = gen_string(rng_ref, is.val_len);
                    AsciiWriter::write_insert(&mut writer, &key, &val)?;
                    keys_valid.push(key);
                } else {
                    eprintln!("{workload:#?}");
                    bail!("Invalid workload spec. Group must have existing valid keys or have insert operations.");
                }
            } else if let Some(is) = group.inserts {
                markers.append(&mut vec![OpMarker::Insert; is.amount]);
            }

            if let Some(us) = group.updates {
                markers.append(&mut vec![OpMarker::Update; us.amount]);
            }
            if let Some(ds) = group.deletes {
                markers.append(&mut vec![OpMarker::Delete; ds.amount]);
            }
            if let Some(pqs) = group.point_queries {
                markers.append(&mut vec![OpMarker::PointQuery; pqs.amount]);
            }
            if let Some(epqs) = group.empty_point_queries {
                markers.append(&mut vec![OpMarker::EmptyPointQuery; epqs.amount]);
            }
            if let Some(rqs) = group.range_queries {
                markers.append(&mut vec![OpMarker::RangeQuery; rqs.amount]);
            }

            for marker in markers.iter() {
                match marker {
                    OpMarker::Insert => {
                        let is = group
                            .inserts
                            .context("Insert marker can only appear when inserts is not None")?;
                        let key = gen_string(rng_ref, is.key_len);
                        let val = gen_string(rng_ref, is.val_len);
                        AsciiWriter::write_insert(writer, &key, &val)?;
                        keys_valid.push(key);
                    }
                    OpMarker::Update => {
                        let us = group
                            .updates
                            .context("Update marker can only appear when updates is not None")?;
                        let key = keys_valid.get_random(rng_ref);
                        let val = gen_string(rng_ref, us.val_len);

                        AsciiWriter::write_update(writer, key, &val)?;
                    }
                    OpMarker::Delete => {
                        let idx = rng_ref.random_range(0..keys_valid.len());
                        let key = keys_valid.remove(idx);

                        AsciiWriter::write_delete(writer, &key)?;
                    }
                    OpMarker::PointQuery => {
                        let key = keys_valid
                            .get(rng_ref.random_range(0..keys_valid.len()))
                            .unwrap();
                        AsciiWriter::write_point_query(writer, key)?
                    }
                    OpMarker::EmptyPointQuery => {
                        let epq = group.empty_point_queries.context(
                            "EmptyPointQuery marker can only appear when point_queries is not None",
                        )?;
                        let key = loop {
                            let key = gen_string(rng_ref, epq.key_len);
                            if !keys_valid.contains(&key) {
                                break key;
                            }
                        };

                        AsciiWriter::write_point_query(writer, &key)?
                    }
                    OpMarker::RangeQuery => {
                        let rs = group.range_queries.context(
                            "RangeQuery marker can only appear when range_queries is not None",
                        )?;

                        keys_valid.sort();
                        // It would be better to use `from` and `try_from` instead of `as` here.
                        // Maybe the `num_traits` crate could help.
                        // https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-as-int
                        let num_items =
                            (rs.selectivity * (keys_valid.len() as f32).floor()) as usize;
                        let start_range = 0..keys_valid.len() - num_items;

                        let start_idx = rng_ref.random_range(start_range);
                        let key1 = &keys_valid.get(start_idx).expect("index to be in range");
                        let key2 = &keys_valid
                            .get(start_idx + num_items)
                            .expect("index to be in range");

                        AsciiWriter::write_range_query(writer, key1, key2)?
                    }
                }
            }
        }
    }

    return Ok(());
}

/// Takes in a JSON representation of a workload specification and writes the workload to a file.
pub fn generate_workload(workload_spec_string: &str, output_file: PathBuf) -> Result<()> {
    let workload_spec: WorkloadSpec =
        serde_json::from_str(workload_spec_string).context("parsing json file")?;
    let mut buf_writer = BufWriter::with_capacity(1024 * 1024, File::create(output_file)?);
    write_operations(&mut buf_writer, &workload_spec)?;
    buf_writer.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufRead;

    #[test]
    fn workload_1m_i() {
        let spec_str = include_str!("../test_specs/1m_i.json");
        let spec = serde_json::from_str::<WorkloadSpec>(&spec_str).unwrap();
        let bytes_count = spec.bytes_count();
        let mut buf = Vec::with_capacity(bytes_count);
        write_operations(&mut buf, &spec).unwrap();
        assert_eq!(buf.lines().count(), 1_000_000);
        assert_eq!(buf.len(), bytes_count);
    }

    #[test]
    fn workload_1m_i_1m_rq() {
        let spec_str = include_str!("../test_specs/1m_i-1m_rq.json");
        let spec = serde_json::from_str::<WorkloadSpec>(&spec_str).unwrap();
        let bytes_count = spec.bytes_count();
        let mut buf = Vec::with_capacity(bytes_count);
        write_operations(&mut buf, &spec).unwrap();

        assert_eq!(buf.lines().count(), 2_000_000);
        assert_eq!(buf.len(), bytes_count);
    }

    #[test]
    fn deletes() {
        let spec_str = include_str!("../test_specs/deletes.json");
        let spec = serde_json::from_str::<WorkloadSpec>(&spec_str).unwrap();
        let bytes_count = spec.bytes_count();
        let mut buf = Vec::with_capacity(bytes_count);
        write_operations(&mut buf, &spec).unwrap();
        assert_eq!(buf.lines().count(), 1_100_000);
        assert_eq!(buf.len(), bytes_count);
    }

    #[test]
    fn empty_point_queries() {
        let spec_str = include_str!("../test_specs/empty_point_queries.json");
        let spec = serde_json::from_str::<WorkloadSpec>(&spec_str).unwrap();
        let bytes_count = spec.bytes_count();
        let mut buf = Vec::with_capacity(bytes_count);
        write_operations(&mut buf, &spec).unwrap();
        assert_eq!(buf.lines().count(), 101_000);
        assert_eq!(buf.len(), bytes_count);
    }
}
