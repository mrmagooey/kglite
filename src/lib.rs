// src/lib.rs

// ─── Tracking allocator (FFI builds only) ────────────────────────────────────
//
// When compiled with --features ffi (no Python), replace the global allocator
// with a thin wrapper that counts live bytes, peak bytes, and total allocation
// count using atomics.  The Python extension uses its own memory model (CPython
// owns the interpreter heap), so we only instrument the pure-Rust/C build.
#[cfg(all(feature = "ffi", not(feature = "python")))]
mod tracking_alloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

    pub static CURRENT_BYTES: AtomicI64 = AtomicI64::new(0);
    pub static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
    pub static TOTAL_ALLOCS: AtomicU64 = AtomicU64::new(0);

    pub struct TrackingAllocator;

    unsafe impl GlobalAlloc for TrackingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = System.alloc(layout);
            if !ptr.is_null() {
                let prev = CURRENT_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
                let new_val = (prev + layout.size() as i64) as u64;
                let mut peak = PEAK_BYTES.load(Ordering::Relaxed);
                while new_val > peak {
                    match PEAK_BYTES.compare_exchange_weak(
                        peak,
                        new_val,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }
                TOTAL_ALLOCS.fetch_add(1, Ordering::Relaxed);
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout);
            CURRENT_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = System.alloc_zeroed(layout);
            if !ptr.is_null() {
                let prev = CURRENT_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
                let new_val = (prev + layout.size() as i64) as u64;
                let mut peak = PEAK_BYTES.load(Ordering::Relaxed);
                while new_val > peak {
                    match PEAK_BYTES.compare_exchange_weak(
                        peak,
                        new_val,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }
                TOTAL_ALLOCS.fetch_add(1, Ordering::Relaxed);
            }
            ptr
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let new_ptr = System.realloc(ptr, layout, new_size);
            if !new_ptr.is_null() {
                let delta = new_size as i64 - layout.size() as i64;
                let prev = CURRENT_BYTES.fetch_add(delta, Ordering::Relaxed);
                if delta > 0 {
                    let new_val = (prev + delta) as u64;
                    let mut peak = PEAK_BYTES.load(Ordering::Relaxed);
                    while new_val > peak {
                        match PEAK_BYTES.compare_exchange_weak(
                            peak,
                            new_val,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(p) => peak = p,
                        }
                    }
                }
            }
            new_ptr
        }
    }
}

#[cfg(all(feature = "ffi", not(feature = "python")))]
#[global_allocator]
static GLOBAL: tracking_alloc::TrackingAllocator = tracking_alloc::TrackingAllocator;

// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "python")]
use pyo3::prelude::*;
pub(crate) mod datatypes;
pub(crate) mod graph;
#[cfg(feature = "python")]
use graph::cypher::{ResultIter, ResultView};
#[cfg(feature = "python")]
use graph::io_operations::load_file;
#[cfg(feature = "python")]
use graph::{KnowledgeGraph, Transaction};

#[cfg(feature = "python")]
#[pyfunction]
fn load(py: Python<'_>, path: String) -> PyResult<KnowledgeGraph> {
    py.detach(|| load_file(&path))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
}

#[cfg(feature = "python")]
#[pymodule]
fn kglite(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(load, m)?)?;
    m.add_class::<KnowledgeGraph>()?;
    m.add_class::<Transaction>()?;
    m.add_class::<ResultView>()?;
    m.add_class::<ResultIter>()?;
    Ok(())
}

#[cfg(feature = "ffi")]
pub mod ffi;
