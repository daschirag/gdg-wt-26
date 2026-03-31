#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::ingestion::Connection;
use crate::types::{AggregateValue, ConfidenceFlag, Value};

/// Python-facing wrapper around [`Connection`].
#[pyclass(name = "Connection")]
struct PyConnection {
    inner: Connection,
}

#[pymethods]
impl PyConnection {
    /// Insert a row. Pass values as a Python list matching schema column order.
    ///
    /// ```python
    /// conn.insert([1001, 42, 3.14, "hello"])
    /// ```
    fn insert(&self, py: Python<'_>, values: Vec<PyObject>) -> PyResult<()> {
        let mut typed: Vec<Value> = Vec::with_capacity(values.len());
        for obj in &values {
            if let Ok(i) = obj.extract::<i64>(py) {
                typed.push(Value::Int(i));
            } else if let Ok(f) = obj.extract::<f64>(py) {
                typed.push(Value::Float(f));
            } else if let Ok(s) = obj.extract::<String>(py) {
                typed.push(Value::String(s));
            } else {
                return Err(PyRuntimeError::new_err(format!(
                    "Unsupported value type: {:?}",
                    obj
                )));
            }
        }
        self.inner
            .insert(typed)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Execute a SQL query and return a dict with the result.
    ///
    /// ```python
    /// result = conn.query("SELECT COUNT(*) FROM data")
    /// # {"type": "scalar", "value": 42.0, "confidence": "Exact", "rows_scanned": 42, "warnings": []}
    /// ```
    fn query(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        let result = self
            .inner
            .query(sql)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let d = PyDict::new_bound(py);
        match result.value {
            AggregateValue::Scalar(v) => {
                d.set_item("type", "scalar")?;
                d.set_item("value", v)?;
            }
            AggregateValue::Groups(groups) => {
                d.set_item("type", "groups")?;
                let list = PyList::empty_bound(py);
                for (k, v, c) in groups {
                    let gd = PyDict::new_bound(py);
                    gd.set_item("key", k)?;
                    gd.set_item("value", v)?;
                    gd.set_item("confidence", confidence_str(c))?;
                    list.append(gd)?;
                }
                d.set_item("groups", list)?;
            }
            AggregateValue::Empty => {
                d.set_item("type", "empty")?;
            }
        }
        d.set_item("confidence", confidence_str(result.confidence))?;
        d.set_item("rows_scanned", result.rows_scanned)?;
        d.set_item("sampling_rate", result.sampling_rate)?;
        d.set_item("storage_path", format!("{:?}", result.storage_path))?;
        d.set_item("warnings", result.warnings)?;
        d.set_item("next_offset", result.next_offset)?;
        Ok(d.into())
    }

    /// Return up to `n` rows from the live reservoir sample as a list of dicts.
    ///
    /// ```python
    /// rows = conn.reservoir_sample(50)
    /// # [{"user_id": 1001, "amount": 42, ...}, ...]
    /// ```
    fn reservoir_sample(&self, py: Python<'_>, n: usize) -> PyResult<PyObject> {
        let rows = self.inner.reservoir_sample(n);
        let schema = &self.inner.config.schema;
        let list = PyList::empty_bound(py);
        for row in rows {
            let d = PyDict::new_bound(py);
            for (i, col) in schema.columns.iter().enumerate() {
                if let Some(val) = row.values.get(i) {
                    match val {
                        Value::Int(v) => d.set_item(&col.name, v)?,
                        Value::Float(v) => d.set_item(&col.name, v)?,
                        Value::String(v) => d.set_item(&col.name, v)?,
                    }
                }
            }
            list.append(d)?;
        }
        Ok(list.into())
    }

    /// Close the connection (explicit API parity with sqlite3).
    fn close(&self) {}
}

fn confidence_str(c: ConfidenceFlag) -> &'static str {
    match c {
        ConfidenceFlag::Exact => "Exact",
        ConfidenceFlag::High => "High",
        ConfidenceFlag::Low => "Low",
    }
}

/// Open a connection to a storage directory.
///
/// ```python
/// import lsm
/// conn = lsm.connect("data/")
/// ```
#[pyfunction]
fn connect(data_dir: &str) -> PyResult<PyConnection> {
    let inner = Connection::open(data_dir)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyConnection { inner })
}

/// The `lsm` Python extension module.
#[pymodule]
pub fn lsm(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<PyConnection>()?;
    Ok(())
}
