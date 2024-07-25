#![allow(clippy::needless_return)]
use anyhow::{Context, Result};
use std::collections::HashSet;

use rand::seq::SliceRandom;

/// Workload specification.
mod spec {
    use std::collections::HashSet;

    use rand::Rng;
    use rand_distr::Alphanumeric;
    use schemars::JsonSchema;

    /// Specification for inserts in a workload section.
    #[derive(serde::Deserialize, JsonSchema)]
    pub struct Inserts {
        /// Number of inserts in the section
        amount: usize,
        /// Key length
        key_len: usize,
        /// Value length
        val_len: usize,
    }

    impl Inserts {
        pub(crate) fn generate_operations(&self) -> Vec<Operation> {
            (0..self.amount)
                .map(|_| {
                    let key: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(self.key_len)
                        .map(char::from)
                        .collect();
                    let val: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(self.val_len)
                        .map(char::from)
                        .collect();

                    return Operation::Insert(key, val);
                })
                .collect()
        }
    }

    /// Specification for updates in a workload section.
    #[derive(serde::Deserialize, JsonSchema)]
    pub struct Updates {
        /// Number of updates
        amount: usize,
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
    #[derive(serde::Deserialize, JsonSchema)]
    pub struct Deletes {
        /// Number of deletes
        amount: usize,
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
    #[derive(serde::Deserialize, JsonSchema)]
    pub struct PointQueries {
        /// Number of point queries
        amount: usize,
    }

    impl PointQueries {
        pub(crate) fn generate_operations(&self, valid_keys: &HashSet<String>) -> Vec<Operation> {
            (0..self.amount)
                .map(|_| {
                    let random_idx = rand::thread_rng().gen_range(0..valid_keys.len());
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
    #[derive(serde::Deserialize, JsonSchema)]
    pub struct RangeQueries {
        /// Number of range queries
        amount: usize,
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

    #[derive(serde::Deserialize, JsonSchema)]
    pub(crate) struct WorkloadSpecSection {
        pub(crate) inserts: Option<Inserts>,
        pub(crate) updates: Option<Updates>,
        pub(crate) deletes: Option<Deletes>,
        pub(crate) point_queries: Option<PointQueries>,
        pub(crate) range_queries: Option<RangeQueries>,
    }

    #[derive(serde::Deserialize, JsonSchema)]
    pub(crate) struct WorkloadSpec {
        pub(crate) sections: Vec<WorkloadSpecSection>,
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
                Operation::RangeDelete(ks, ke) => format!("RD {ks} {ke}"),
                Operation::PointQuery(k) => format!("PQ {k}"),
                Operation::RangeQuery(ks, ke) => format!("RQ {ks} {ke}"),
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
    let mut all_operations: Vec<Operation> = Vec::new();
    let mut rng = rand::thread_rng();

    let mut valid_keys: HashSet<String> = HashSet::new();

    for workload_section in workload.sections {
        let mut operations: Vec<Operation> = Vec::new();

        // inserts
        if let Some(is) = workload_section.inserts {
            let insert_operations = is.generate_operations();
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
            let point_query_operations = pqs.generate_operations(&valid_keys);
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
/// );
/// assert!(workload.is_ok());
/// ```
pub fn generate_workload(workload_spec_string: String) -> Result<String> {
    let workload_spec: WorkloadSpec =
        serde_json::from_str(&workload_spec_string).context("parsing json file")?;
    let operations = generate_operations(workload_spec);

    let workload_str = operations
        .iter()
        .map(|op| op.to_str())
        .collect::<Vec<_>>()
        .join("\n");

    return Ok(workload_str);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_from_string() {
        let workload = generate_workload(
            r#"{"sections":[{"inserts":{"amount":1,"key_len":1,"val_len":1}}]}"#.into(),
        );
        assert!(workload.is_ok());
    }

    #[test]
    fn test_empty_file() {
        let schema = "";
        let workload = generate_workload(schema.into());
        assert!(!workload.is_ok());
    }

    #[test]
    fn test_empty_schema() {
        let schema = include_str!("../test_specs/empty.json");
        let workload = generate_workload(schema.into());
        assert!(workload.is_ok());
    }

    #[test]
    fn test_simple_schema() {
        let schema = include_str!("../test_specs/simple.json");
        let workload = generate_workload(schema.into());
        assert!(workload.is_ok());
    }

    #[test]
    fn test_complex_schema() {
        let schema = include_str!("../test_specs/complex.json");
        let workload = generate_workload(schema.into());
        assert!(workload.is_ok());
    }

    #[test]
    fn test_missing_properties() {
        let schema = include_str!("../test_specs/missing_properties.json");
        let workload = generate_workload(schema.into());
        assert!(!workload.is_ok());
    }

    #[test]
    fn test_wrong_types() {
        let schema = include_str!("../test_specs/wrong_types.json");
        let workload = generate_workload(schema.into());
        assert!(!workload.is_ok());
    }

    #[test]
    fn test_invalid_values() {
        let schema = include_str!("../test_specs/invalid_values.json");
        let workload = generate_workload(schema.into());
        assert!(!workload.is_ok());
    }

    #[test]
    fn schema_generation_works() {
        let workload = generate_workload_spec_schema();
        assert!(workload.is_ok());
    }
}
