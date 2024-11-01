#![allow(clippy::needless_return)]
use anyhow::{Context, Result};
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::thread::spawn;

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
        key_len: usize,
        /// Value length
        val_len: usize,
    }

    impl Inserts {
        pub(crate) fn generate_operations(&self, rng: &mut ThreadRng) -> Vec<Operation> {
            let mut key = String::with_capacity(self.key_len);
            let mut val = String::with_capacity(self.val_len);
            (0..self.amount)
                .map(|_| {
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
                .map(|_| {
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
        pub(crate) inserts: Option<Inserts>,
        pub(crate) updates: Option<Updates>,
        pub(crate) deletes: Option<Deletes>,
        pub(crate) point_queries: Option<PointQueries>,
        pub(crate) range_queries: Option<RangeQueries>,
    }

    impl WorkloadSpecSection {
        pub fn operation_count(&self) -> usize {
            let operation_count = self.inserts.map_or(0, |is| is.amount)
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
            return self
                .sections
                .iter()
                .fold(0, |acc, sec| acc + sec.operation_count());
        }
    }

    pub(crate) enum Operation {
        Insert(String, String),
        Update(String, String),
        Delete(String),
        RangeDelete(String, String),
        PointQuery(String),
        RangeQuery(String, String),
    }

    impl Operation {
        pub fn to_str(&self) -> String {
            match self {
                Operation::Insert(k, v) => format!("I {k} {v}"),
                Operation::Update(k, v) => format!("U {k} {v}"),
                Operation::Delete(k) => format!("D {k}"),
                Operation::RangeDelete(ks, ke) => format!("X {ks} {ke}"),
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
fn generate_operations(workload: WorkloadSpec) -> Vec<Operation> {
    let mut all_operations: Vec<Operation> = Vec::with_capacity(workload.operation_count());
    let mut rng = rand::thread_rng();

    for workload_section in workload.sections {
        let mut operations: Vec<Operation> = Vec::with_capacity(workload_section.operation_count());
        let mut valid_keys: HashSet<String> = HashSet::new();

        // inserts
        if let Some(is) = workload_section.inserts {
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
    let operations = generate_operations(workload_spec);

    let mut buf_writer = std::io::BufWriter::new(std::fs::File::create(output_file)?);
    operations.iter().for_each(|op| {
        writeln!(buf_writer, "{}", op.to_str()).unwrap();
    });

    Ok(())
}

mod empty {
    use crate::spec::WorkloadSpec;
    use anyhow::Result;
    use rand::prelude::SliceRandom;
    use rand::Rng;
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    #[derive(Copy, Clone)]
    pub enum Op {
        Insert,
        Update,
        Delete,
        PointQuery,
        RangeQuery,
    }

    pub(crate) fn generate_empty_operations(workload_spec: WorkloadSpec) -> Result<Vec<Op>> {
        let mut operations = Vec::with_capacity(workload_spec.operation_count());
        let max_operations = workload_spec
            .sections
            .iter()
            .map(|s| s.operation_count())
            .max()
            .unwrap_or(0);
        let mut section_operations = Vec::with_capacity(max_operations);
        for section in workload_spec.sections {
            section_operations.clear();
            if let Some(i) = section.inserts {
                let operation = vec![Op::Insert; i.amount];
                section_operations.extend(operation);
            }

            section_operations.shuffle(&mut rand::thread_rng());
            operations.extend(&section_operations);
        }

        Ok(operations)
    }

    pub(crate) fn generate_batch<'a>(ops: &'a [Op]) -> [String; BATCH_SIZE] {
        let mut rng = rand::thread_rng();

        let mut batch: [MaybeUninit<String>; BATCH_SIZE] =
            unsafe { MaybeUninit::uninit().assume_init() };

        for i in 0..BATCH_SIZE {
            batch[i] = MaybeUninit::new(format!("Random line: {}\n", rng.gen::<u64>()));
        }

        // SAFETY: All slice positions were initialized, so we can safely cast
        unsafe { std::mem::transmute::<_, [String; BATCH_SIZE]>(batch) }
    }

    pub const BATCH_SIZE: usize = 1000;
}

use empty::{generate_batch, generate_empty_operations, BATCH_SIZE};
pub fn generate_workload2(workload_spec_string: String) -> Result<()> {
    let workload_spec: WorkloadSpec =
        serde_json::from_str(&workload_spec_string).context("parsing json file")?;

    let empty_operations = generate_empty_operations(workload_spec)?;
    let empty_operations = Arc::new(empty_operations);

    let NUM_LINES: usize = empty_operations.len();
    const NUM_WORKERS: usize = 4;
    assert_eq!(NUM_LINES % BATCH_SIZE, 0);

    let (tx, rx) = mpsc::channel::<Arc<[String; BATCH_SIZE]>>();
    let tx = Arc::new(tx);

    let writer_handle = spawn(move || {
        let file = File::create("output.txt").expect("Unable to create file");
        let mut writer = BufWriter::new(file);
        for batch in rx {
            for line in batch.iter() {
                writer
                    .write_all(line.as_bytes())
                    .expect("Unable to write data");
            }
        }
    });

    let chunk_size = NUM_LINES / NUM_WORKERS;
    let mut handles = Vec::new();

    // Split the operations into chunks manually
    for i in 0..NUM_WORKERS {
        let tx = Arc::clone(&tx);
        let operations_chunk = Arc::clone(&empty_operations);

        let start_index = i * chunk_size;
        let end_index = if i == NUM_WORKERS - 1 {
            NUM_LINES
        } else {
            (i + 1) * chunk_size
        };

        let handle = spawn(move || {
            let chunk = &operations_chunk[start_index..end_index];
            for chunk in chunk.chunks_exact(BATCH_SIZE) {
                let batch = Arc::new(generate_batch(chunk));
                tx.send(batch).unwrap()
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().expect("Worker thread panicked");
    }

    drop(tx); // Close the channel

    // Wait for the writer thread to finish
    writer_handle.join().expect("Writer thread panicked");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // #[test]
    // fn test_from_string() {
    //     let workload = generate_workload(
    //         r#"{"sections":[{"inserts":{"amount":1,"key_len":1,"val_len":1}}]}"#.into(),
    //     );
    //     assert!(workload.is_ok());
    // }
    //
    // #[test]
    // fn test_empty_file() {
    //     let schema = "";
    //     let workload = generate_workload(schema.into());
    //     assert!(!workload.is_ok());
    // }
    //
    // #[test]
    // fn test_empty_schema() {
    //     let schema = include_str!("../test_specs/empty.json");
    //     let workload = generate_workload(schema.into());
    //     assert!(workload.is_ok());
    // }
    //
    // #[test]
    // fn test_simple_schema() {
    //     let schema = include_str!("../test_specs/simple.json");
    //     let workload = generate_workload(schema.into());
    //     assert!(workload.is_ok());
    // }
    //
    // #[test]
    // fn test_complex_schema() {
    //     let schema = include_str!("../test_specs/complex.json");
    //     let workload = generate_workload(schema.into());
    //     assert!(workload.is_ok());
    // }
    //
    // #[test]
    // fn test_large_schema() {
    //     let schema = include_str!("../test_specs/large.json");
    //     let workload = generate_workload(schema.into());
    //     assert!(workload.is_ok());
    // }
    //
    // #[test]
    // fn test_missing_properties() {
    //     let schema = include_str!("../test_specs/missing_properties.json");
    //     let workload = generate_workload(schema.into());
    //     assert!(!workload.is_ok());
    // }
    //
    // #[test]
    // fn test_wrong_types() {
    //     let schema = include_str!("../test_specs/wrong_types.json");
    //     let workload = generate_workload(schema.into());
    //     assert!(!workload.is_ok());
    // }
    //
    // #[test]
    // fn test_invalid_values() {
    //     let schema = include_str!("../test_specs/invalid_values.json");
    //     let workload = generate_workload(schema.into());
    //     assert!(!workload.is_ok());
    // }

    #[test]
    fn schema_generation_works() {
        let workload = generate_workload_spec_schema();
        assert!(workload.is_ok());
    }
    // #[test]
    // fn test_1m_i() {
    //     let schema = include_str!("../test_specs/1m_i.json");
    //     let workload = generate_workload(schema.into());
    //     assert!(workload.is_ok());
    // }
}
