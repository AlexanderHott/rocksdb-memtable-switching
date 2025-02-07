#![allow(clippy::needless_return)]
#![allow(dead_code)]

use anyhow::{bail, Context, Result};
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256Plus;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Workload specification.
pub mod spec {
    use schemars::JsonSchema;

    /// Specification for inserts in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct Inserts {
        /// Number of inserts
        pub(crate) amount: usize,
        /// Key length
        pub(crate) key_len: usize,
        /// Value length
        pub(crate) val_len: usize,
    }

    /// Specification for updates in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct Updates {
        /// Number of updates
        pub(crate) amount: usize,
        /// Value length
        pub(crate) val_len: usize,
    }

    /// Specification for point deletes in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct Deletes {
        /// Number of deletes
        pub(crate) amount: usize,
    }

    /// Specification for point queries in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct PointQueries {
        /// Number of point queries
        pub(crate) amount: usize,
    }

    /// Specification for range queries in a workload group.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct RangeQueries {
        /// Number of range queries
        pub(crate) amount: usize,
        /// Selectivity of range queries. Based off of the range of valid keys, not the full
        /// key-space.
        pub(crate) selectivity: f32,
    }

    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub(crate) struct WorkloadSpecGroup {
        pub(crate) inserts: Option<Inserts>,
        pub(crate) updates: Option<Updates>,
        pub(crate) deletes: Option<Deletes>,
        pub(crate) point_queries: Option<PointQueries>,
        pub(crate) range_queries: Option<RangeQueries>,
    }

    impl WorkloadSpecGroup {
        pub fn operation_count(&self) -> usize {
            let operation_count = self.inserts.map_or(0, |s| s.amount)
                + self.updates.map_or(0, |us| us.amount)
                + self.point_queries.map_or(0, |is| is.amount)
                + self.range_queries.map_or(0, |is| is.amount)
                + self.deletes.map_or(0, |is| is.amount);
            return operation_count;
        }
    }

    #[derive(serde::Deserialize, JsonSchema, Default, Clone)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum KeySpace {
        #[default]
        Alphanumeric,
    }
    #[derive(serde::Deserialize, JsonSchema, Default, Clone)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum KeyDistribution {
        #[default]
        Uniform,
    }

    #[derive(serde::Deserialize, JsonSchema, Clone)]
    pub(crate) struct WorkloadSpecSection {
        /// A list of operation groups that share keys between operations.
        ///
        /// E.g. non-empty point queries will use a key from an insert in this group.
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

        pub fn insert_count(&self) -> usize {
            return self
                .groups
                .iter()
                .map(|g| g.inserts.map_or(0, |is| is.amount))
                .sum();
        }

        pub fn has_inserts(&self) -> bool {
            return self.groups.iter().map(|g| g.inserts.is_some()).any(|x| x);
        }

        pub fn has_updates(&self) -> bool {
            return self.groups.iter().map(|g| g.updates.is_some()).any(|x| x);
        }
        pub fn has_deletes(&self) -> bool {
            return self.groups.iter().map(|g| g.deletes.is_some()).any(|x| x);
        }
        pub fn has_point_queries(&self) -> bool {
            return self
                .groups
                .iter()
                .map(|g| g.point_queries.is_some())
                .any(|x| x);
        }

        pub fn has_range_queries(&self) -> bool {
            return self
                .groups
                .iter()
                .map(|g| g.range_queries.is_some())
                .any(|x| x);
        }

        pub fn needs_dynamic_sorted_keys(&self) -> bool {
            return self
                .groups
                .iter()
                .map(|g| (g.inserts.is_some() || g.deletes.is_some()) && g.range_queries.is_some())
                .any(|x| x);
        }
    }

    #[derive(serde::Deserialize, JsonSchema, Clone)]
    pub struct WorkloadSpec {
        /// Sections of a workload where a key from one will (probably) not appear in another.
        pub(crate) sections: Vec<WorkloadSpecSection>,
    }

    impl WorkloadSpec {
        pub fn operation_count(&self) -> usize {
            return self.sections.iter().map(|s| s.operation_count()).sum();
        }
    }

    pub enum Operation {
        Insert(String, String),
        Update(String, String),
        Delete(String),
        PointQuery(String),
        RangeQuery(String, String),
    }

    impl Operation {
        pub fn to_string(&self) -> String {
            match self {
                Operation::Insert(k, v) => format!("I {k} {v}"),
                Operation::Update(k, v) => format!("U {k} {v}"),
                Operation::Delete(k) => format!("D {k}"),
                Operation::PointQuery(k) => format!("P {k}"),
                Operation::RangeQuery(k1, k2) => format!("R {k1} {k2}"),
            }
        }
    }
}

mod schema {
    use crate::spec::WorkloadSpec;
    use schemars::schema_for;

    pub fn generate_workload_spec_schema() -> serde_json::Result<String> {
        let schema = schema_for!(WorkloadSpec);
        return serde_json::to_string_pretty(&schema);
    }
}

pub use schema::generate_workload_spec_schema;
use spec::*;

#[derive(Debug, Copy, Clone, Eq, Ord, PartialOrd, PartialEq)]
enum OpMarker {
    Insert,
    Update,
    Delete,
    PointQuery,
    RangeQuery,
}

enum KeysSorted {
    Dynamic(BTreeSet<String>),
    Static(Vec<String>),
}

#[inline(always)]
fn gen_string(rng: &mut Xoshiro256Plus, len: usize) -> String {
    let bytes: Vec<u8> = rng.sample_iter(Alphanumeric).take(len).collect();
    let s = String::from_utf8(bytes)
        .context("Generated an invalid utf-8 string")
        .unwrap();
    return s;
}

pub fn generate_operations2(workload: WorkloadSpec) -> Result<Vec<Operation>> {
    let mut all_operations: Vec<Operation> = Vec::with_capacity(workload.operation_count());
    let mut rng = Xoshiro256Plus::from_entropy();

    for section in workload.sections {
        let mut keys_valid: Vec<String> = Vec::with_capacity(section.insert_count());
        let has_rqs_and_is = section.needs_dynamic_sorted_keys();

        for group in section.groups {
            let mut keys_sorted = if has_rqs_and_is {
                println!("[Warning] `inserts` and `range_queries` defined in the same group. This will be slower because the valid keys need to be sorted after insert.");
                let mut btreeset = BTreeSet::new();
                btreeset.extend(keys_valid.clone());
                KeysSorted::Dynamic(btreeset)
            } else {
                let mut v = keys_valid.clone();
                v.sort();
                KeysSorted::Static(v)
            };
            // if section.has_deletes() && (section.has_point_queries() || section.has_range_queries() || section.has_inserts() || section.has_updates()) {
            //     println!("[Warning] `deletes` and [`inserts` or `updates` or `point_queries` or `range_queries`] defined in the same group. This will be slower because the valid keys need to be sorted after insert.}}");
            // }
            let rng_ref = &mut rng;
            let mut markers: Vec<OpMarker> = Vec::with_capacity(group.operation_count());
            let mut operations: Vec<Operation> = Vec::with_capacity(group.operation_count());

            if let Some(ds) = group.deletes {
                if ds.amount > keys_valid.len() {
                    bail!("Cannot have more deletes than existing valid keys.");
                }
            }

            // A group must have at least 1 valid key before any other operation can occur.
            // TODO: handle empty point queries
            if keys_valid.len() == 0 {
                if let Some(is) = group.inserts {
                    markers.append(&mut vec![OpMarker::Insert; is.amount - 1]);

                    let key = gen_string(rng_ref, is.key_len);
                    let val = gen_string(rng_ref, is.val_len);
                    operations.push(Operation::Insert(key.clone(), val));
                    match keys_sorted {
                        KeysSorted::Dynamic(ref mut keys) => {
                            keys.insert(key.clone());
                        }
                        KeysSorted::Static(_) => {
                            // no need to insert because the vec will be recreated in the next group
                        }
                    }
                    keys_valid.push(key);
                } else {
                    bail!("Invalid workload spec. Group must have existing valid keys or have insert operations.");
                }
            } else {
                if let Some(is) = group.inserts {
                    markers.append(&mut vec![OpMarker::Insert; is.amount]);
                }
            }

            if let Some(us) = group.updates {
                markers.append(&mut vec![OpMarker::Update; us.amount]);
            }
            if let Some(pqs) = group.point_queries {
                markers.append(&mut vec![OpMarker::PointQuery; pqs.amount]);
            }
            if let Some(rqs) = group.range_queries {
                markers.append(&mut vec![OpMarker::RangeQuery; rqs.amount]);
            }

            markers.shuffle(rng_ref);

            for marker in markers.iter() {
                match marker {
                    OpMarker::Insert => {
                        let is = group
                            .inserts
                            .context("Insert marker can only appear when inserts is not None")?;
                        let key = gen_string(rng_ref, is.key_len);
                        let val = gen_string(rng_ref, is.val_len);
                        operations.push(Operation::Insert(key.clone(), val));
                        match keys_sorted {
                            KeysSorted::Dynamic(ref mut keys) => {
                                keys.insert(key.clone());
                            }
                            KeysSorted::Static(_) => {
                                // no need to insert because the vec will be recreated in the next group
                            }
                        }
                        keys_valid.push(key);
                    }
                    OpMarker::Update => {
                        let us = group
                            .updates
                            .context("Update marker can only appear when updates is not None")?;
                        let key = keys_valid[rng_ref.gen_range(0..keys_valid.len())].clone();
                        let val = gen_string(rng_ref, us.val_len);

                        operations.push(Operation::Update(key.clone(), val));
                    }
                    OpMarker::Delete => {
                        let idx = rng_ref.gen_range(0..keys_valid.len());
                        let key = keys_valid.remove(idx);
                        match keys_sorted {
                            KeysSorted::Dynamic(ref mut keys) => {
                                keys.remove(&key);
                            }
                            KeysSorted::Static(_) => {
                                // No need to remove key because keys_sorted will be recalculated in the next group
                            }
                        }

                        operations.push(Operation::Delete(key));
                    }
                    OpMarker::PointQuery => {
                        let key = keys_valid
                            .iter()
                            .nth(rng_ref.gen_range(0..keys_valid.len()))
                            .unwrap();
                        operations.push(Operation::PointQuery(key.clone()));
                    }
                    OpMarker::RangeQuery => {
                        let rs = group.range_queries.context(
                            "RangeQuery marker can only appear when range_queries is not None",
                        )?;

                        match keys_sorted {
                            KeysSorted::Dynamic(ref mut keys) => {
                                assert_eq!(keys.len(), keys_valid.len());

                                let num_items =
                                    (rs.selectivity * keys.len() as f32).floor() as usize;
                                let start_range = 0..keys.len() - num_items;

                                let start_idx = rng_ref.gen_range(start_range);
                                let key1 = keys
                                    .iter()
                                    .nth(start_idx)
                                    .context("Invalid range query start")?
                                    .clone();

                                let key2 = keys
                                    .iter()
                                    .nth(start_idx + num_items)
                                    .context("Invalid range query end")?
                                    .clone();

                                operations.push(Operation::RangeQuery(key1, key2));
                            }
                            KeysSorted::Static(ref mut keys) => {
                                assert_eq!(keys.len(), keys_valid.len());

                                let num_items =
                                    (rs.selectivity * keys.len() as f32).floor() as usize;
                                let start_range = 0..keys.len() - num_items;

                                let start_idx = rng_ref.gen_range(start_range);
                                let key1 = keys[start_idx].clone();
                                let key2 = keys[start_idx + num_items].clone();

                                operations.push(Operation::RangeQuery(key1, key2));
                            }
                        }
                    }
                }
            }

            all_operations.append(&mut operations);
        }
    }

    return Ok(all_operations);
}

/// Takes in a json representation of a workload specification and produces a workload string.
pub fn generate_workload(workload_spec_string: String, output_file: PathBuf) -> Result<()> {
    let workload_spec: WorkloadSpec =
        serde_json::from_str(&workload_spec_string).context("parsing json file")?;
    let operations = generate_operations2(workload_spec)?;

    let mut buf_writer = BufWriter::new(File::create(output_file)?);
    operations.iter().for_each(|op| {
        writeln!(buf_writer, "{}", op.to_string()).unwrap();
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_1m_i() {
        let spec_str = include_str!("../test_specs/1m_i.json");
        let spec = serde_json::from_str::<WorkloadSpec>(spec_str).unwrap();
        let operations = generate_operations2(spec).unwrap();
        assert_eq!(operations.len(), 1_000_000);
    }

    #[test]
    fn test_1m_i_1m_rq() -> Result<()> {
        let spec_str = include_str!("../test_specs/1m_i-1m_rq.json");
        let spec = serde_json::from_str::<WorkloadSpec>(spec_str)?;
        let operations = generate_operations2(spec)?;
        assert_eq!(operations.len(), 2_000_000);

        return Ok(());
    }

    #[test]
    fn test_deletes() -> Result<()> {
        let spec_str = include_str!("../test_specs/deletes.json");
        let spec = serde_json::from_str::<WorkloadSpec>(spec_str)?;
        let operations = generate_operations2(spec)?;
        assert_eq!(operations.len(), 2_000_000);
        return Ok(());
    }
}
