#![allow(clippy::needless_return)]
use anyhow::{Context, Result};
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::thread::spawn;
use rand_xoshiro::Xoshiro256PlusPlus;

/// Workload specification.
mod spec {
    use std::collections::HashSet;

    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand_distr::Alphanumeric;
    use rayon::prelude::*;
    use schemars::JsonSchema;

    /// Specification for inserts in a workload section.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct Inserts {
        /// Number of inserts in the section
        pub(crate) amount: usize,
        /// Key length
        pub(crate) key_len: usize,
        /// Value length
        pub(crate) val_len: usize,
    }

    impl Inserts {
        pub(crate) fn generate_operations(&self, rng: &mut ThreadRng) -> Vec<Operation> {
            let mut key = String::with_capacity(self.key_len);
            let mut val = String::with_capacity(self.val_len);
            (0..self.amount)
                .map(|i| {
                    if i % 1000 == 0 {
                        println!("Generating insert {}", i);
                    }
                    key.clear();
                    key.extend(
                        rng.sample_iter(&Alphanumeric)
                            .take(self.key_len)
                            .map(char::from),
                    );
                    val.clear();
                    val.extend(
                        rng.sample_iter(&Alphanumeric)
                            .take(self.val_len)
                            .map(char::from),
                    );

                    return Operation::Insert(key.clone(), val.clone());
                })
                .collect()
        }
    }

    /// Specification for updates in a workload section.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct Updates {
        /// Number of updates
        pub(crate) amount: usize,
        /// Value length
        val_len: usize,
    }

    impl Updates {
        pub(crate) fn generate_operations(&self, valid_keys: &HashSet<String>) -> Vec<Operation> {
            (0..self.amount)
                .map(|_| {
                    let random_idx = rand::thread_rng().gen_range(0..valid_keys.len());
                    let key = valid_keys
                        .iter()
                        .nth(random_idx)
                        .expect("index to be in range");

                    let val: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(self.val_len)
                        .map(char::from)
                        .collect();

                    return Operation::Update(key.clone(), val);
                })
                .collect()
        }
    }

    /// Specification for point deletes in a workload section.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct Deletes {
        /// Number of deletes
        pub(crate) amount: usize,
    }

    impl Deletes {
        pub(crate) fn generate_operations(&self, valid_keys: &HashSet<String>) -> Vec<Operation> {
            (0..self.amount)
                .map(|_| {
                    let random_idx = rand::thread_rng().gen_range(0..valid_keys.len());
                    let key = valid_keys
                        .iter()
                        .nth(random_idx)
                        .expect("index to be in range");

                    return Operation::Delete(key.clone());
                })
                .collect()
        }
    }

    /// Specification for point queries in a workload section.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct PointQueries {
        /// Number of point queries
        pub(crate) amount: usize,
    }

    impl PointQueries {
        pub(crate) fn generate_operations(
            &self,
            valid_keys: &HashSet<String>,
            rng: &mut ThreadRng,
        ) -> Vec<Operation> {
            (0..self.amount)
                .map(|i| {
                    if i % 1000 == 0 {
                        println!("Generating point query {}", i);
                    }
                    let random_idx = rng.gen_range(0..valid_keys.len());
                    let key = valid_keys
                        .iter()
                        .nth(random_idx)
                        .expect("index to be in range");

                    return Operation::PointQuery(key.clone());
                })
                .collect()
        }
    }

    /// Specification for range queries in a workload section.
    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub struct RangeQueries {
        /// Number of range queries
        pub(crate) amount: usize,
        /// Selectivity of range queries. Based off of the range of valid keys, not the full
        /// key-space.
        selectivity: f32,
    }

    impl RangeQueries {
        pub(crate) fn generate_operations(&self, valid_keys: &HashSet<String>) -> Vec<Operation> {
            assert!(0. <= self.selectivity && self.selectivity <= 1.);
            let mut sorted_keys: Vec<_> = Vec::from_iter(valid_keys);
            sorted_keys.sort();

            (0..self.amount)
                .map(|_| {
                    let range_in_values =
                        (sorted_keys.len() as f32 * self.selectivity).floor() as usize;
                    let max_start_idx = sorted_keys.len() - range_in_values;
                    let random_idx = rand::thread_rng().gen_range(0..max_start_idx);
                    let key_start = sorted_keys[random_idx];
                    let key_end = sorted_keys[random_idx + range_in_values];

                    return Operation::RangeQuery(key_start.clone(), key_end.clone());
                })
                .collect()
        }
    }

    #[derive(serde::Deserialize, JsonSchema, Copy, Clone)]
    pub(crate) struct WorkloadSpecSection {
        pub(crate) inserts: Inserts,
        pub(crate) updates: Option<Updates>,
        pub(crate) deletes: Option<Deletes>,
        pub(crate) point_queries: Option<PointQueries>,
        pub(crate) range_queries: Option<RangeQueries>,
    }

    impl WorkloadSpecSection {
        pub fn operation_count(&self) -> usize {
            let operation_count = self.inserts.amount
                + self.updates.map_or(0, |us| us.amount)
                + self.point_queries.map_or(0, |is| is.amount)
                + self.range_queries.map_or(0, |is| is.amount)
                + self.deletes.map_or(0, |is| is.amount);
            return operation_count;
        }
    }

    #[derive(serde::Deserialize, JsonSchema, Clone)]
    pub(crate) struct WorkloadSpec {
        pub(crate) sections: Vec<WorkloadSpecSection>,
    }

    impl WorkloadSpec {
        pub fn operation_count(&self) -> usize {
            return self.sections.iter().map(|s| s.operation_count()).sum();
        }
    }

    pub(crate) enum Operation {
        Insert(String, String),
        Update(String, String),
        Delete(String),
        PointQuery(String),
        RangeQuery(String, String),
    }

    impl Operation {
        pub fn to_str(&self) -> String {
            match self {
                Operation::Insert(k, v) => format!("I {k} {v}"),
                Operation::Update(k, v) => format!("U {k} {v}"),
                Operation::Delete(k) => format!("D {k}"),
                // Operation::RangeDelete(ks, ke) => format!("X {ks} {ke}"),
                Operation::PointQuery(k) => format!("P {k}"),
                Operation::RangeQuery(ks, ke) => format!("R {ks} {ke}"),
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

fn generate_operations2(workload: WorkloadSpec) -> Vec<Operation> {
    let mut all_operations: Vec<Operation> = Vec::with_capacity(workload.operation_count());
    // let mut rng = rand::thread_rng();
    let mut rng = Xoshiro256PlusPlus::from_entropy();

    for workload_section in workload.sections {
        let mut markers: Vec<OpMarker> = Vec::with_capacity(workload_section.operation_count());
        let mut operations: Vec<Operation> = Vec::with_capacity(workload_section.operation_count());
        let is = workload_section.inserts;
        let mut valid_keys: Vec<String> = Vec::with_capacity(is.amount);

        markers.append(&mut vec![OpMarker::Insert; is.amount - 1]);
        if let Some(pqs) = workload_section.point_queries {
            markers.append(&mut vec![OpMarker::PointQuery; pqs.amount]);
        }
        let rng_ref = &mut rng;
        markers.shuffle(rng_ref);

        // push the first insert
        {
            let mut key = String::with_capacity(is.key_len);
            key.extend(
                rng_ref
                    .sample_iter(&Alphanumeric)
                    .take(is.key_len)
                    .map(char::from),
            );
            let mut val = String::with_capacity(is.val_len);
            val.extend(
                rng_ref
                    .sample_iter(&Alphanumeric)
                    .take(is.key_len)
                    .map(char::from),
            );
            operations.push(Operation::Insert(key.clone(), val));
            valid_keys.push(key);
        }
        for (i, marker) in markers.iter().enumerate() {
            if i % 5000 == 0 {
                println!("Generating operation {}", i);
            }
            match marker {
                OpMarker::Insert => {
                    let mut key = String::with_capacity(is.key_len);
                    key.extend(
                        rng_ref
                            .sample_iter(&Alphanumeric)
                            .take(is.key_len)
                            .map(char::from),
                    );
                    let mut val = String::with_capacity(is.val_len);
                    val.extend(
                        rng_ref
                            .sample_iter(&Alphanumeric)
                            .take(is.key_len)
                            .map(char::from),
                    );
                    operations.push(Operation::Insert(key.clone(), val));
                    valid_keys.push(key);
                }
                OpMarker::PointQuery => {
                    let key = valid_keys
                        .iter()
                        .nth(rng_ref.gen_range(0..valid_keys.len()))
                        .unwrap();
                    operations.push(Operation::PointQuery(key.clone()));
                }
                _ => {}
            }
        }

        all_operations.append(&mut operations);
    }

    return all_operations;
}

fn generate_operations(workload: WorkloadSpec) -> Vec<Operation> {
    let mut all_operations: Vec<Operation> = Vec::with_capacity(workload.operation_count());
    let mut rng = rand::thread_rng();

    for workload_section in workload.sections {
        let mut operations: Vec<Operation> = Vec::with_capacity(workload_section.operation_count());
        let mut valid_keys: HashSet<String> = HashSet::new();

        // inserts
        {
            let is = workload_section.inserts;
            let insert_operations = is.generate_operations(&mut rng);
            let keys = insert_operations
                .iter()
                .map(|op| match op {
                    Operation::Insert(k, _) => k.clone(),
                    _ => unreachable!(),
                })
                .collect::<Vec<String>>();
            valid_keys.extend(keys);

            operations.extend(insert_operations.into_iter());
        }

        // updates
        if let Some(us) = workload_section.updates {
            let update_operations = us.generate_operations(&valid_keys);
            operations.extend(update_operations.into_iter());
        }

        // deletes
        if let Some(ds) = workload_section.deletes {
            let update_operations = ds.generate_operations(&valid_keys);
            operations.extend(update_operations.into_iter());
        }

        // point queries
        if let Some(pqs) = workload_section.point_queries {
            let point_query_operations = pqs.generate_operations(&valid_keys, &mut rng);
            operations.extend(point_query_operations.into_iter());
        }

        // range queries
        if let Some(rqs) = workload_section.range_queries {
            let update_operations = rqs.generate_operations(&valid_keys);
            operations.extend(update_operations.into_iter());
        }

        operations.shuffle(&mut rng);
        all_operations.extend(operations);
    }

    return all_operations;
}

/// Takes in a json representation of a workload specification and produces a workload string.
///
/// ```rust
/// use workload_gen::generate_workload;
/// let workload = generate_workload(
///     r#" {"sections":[{"inserts":{"amount":1,"key_len":1,"val_len":1}}]} "#.into(),
///    std::path::PathBuf::from("output.txt")
/// );
/// assert!(workload.is_ok());
/// ```
pub fn generate_workload(workload_spec_string: String, output_file: PathBuf) -> Result<()> {
    let workload_spec: WorkloadSpec =
        serde_json::from_str(&workload_spec_string).context("parsing json file")?;
    let operations = generate_operations2(workload_spec);

    let mut buf_writer = BufWriter::new(File::create(output_file)?);
    operations.iter().for_each(|op| {
        writeln!(buf_writer, "{}", op.to_str()).unwrap();
    });

    Ok(())
}
