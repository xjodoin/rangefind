//! C ABI over [`crate::host::Session`]. The same symbols are exported by
//! the native `cdylib`/`staticlib` (iOS, Android NDK, desktop, Dart FFI)
//! and by the `wasm32-unknown-unknown` module, so every platform drives
//! the engine through one protocol.
//!
//! Strings cross as UTF-8 `(ptr, len)` pairs. Results come back through
//! `rf_poll`, which returns a pointer to a NUL-free UTF-8 JSON buffer whose
//! length is read with `rf_result_len`; the buffer stays valid until the
//! next `rf_poll` on that session. Memory for inputs is allocated by the
//! host with `rf_alloc` and released with `rf_free`.
//!
//! Protocol:
//! 1. `s = rf_session_new()`
//! 2. `op = rf_open(s, url, opts)` / `op = rf_call(s, engine, method, params)`
//! 3. loop: `rf_poll(s)` → `{requests, done, pending}`; perform each
//!    request and answer with `rf_complete` / `rf_complete_missing` /
//!    `rf_complete_error`; stop when your op appears in `done`.

#![allow(clippy::missing_safety_doc)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use crate::host::Session;

struct Slot {
    session: Session,
    result: Vec<u8>,
}

thread_local! {
    static SESSIONS: RefCell<HashMap<u32, Slot>> = RefCell::new(HashMap::new());
    static NEXT: Cell<u32> = const { Cell::new(0) };
}

unsafe fn input<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(ptr, len)
    }
}

unsafe fn input_str(ptr: *const u8, len: usize) -> String {
    String::from_utf8_lossy(input(ptr, len)).into_owned()
}

unsafe fn input_json(ptr: *const u8, len: usize) -> serde_json::Value {
    let bytes = input(ptr, len);
    if bytes.is_empty() {
        return serde_json::Value::Null;
    }
    serde_json::from_slice(bytes).unwrap_or(serde_json::Value::Null)
}

/// Run `f` on session `id` with **no** `RefCell` borrow held: the slot is
/// taken out of the table for the call and put back afterwards. Engine code
/// runs inside `f`, and in wasm (`panic = "abort"`) a trap there unwinds
/// nothing; had the table stayed mutably borrowed, its borrow flag would
/// stay set and every later `rf_*` call on the instance would trap too.
/// Now a trap only loses the session it happened in.
fn with_session<T>(id: u32, fallback: T, f: impl FnOnce(&mut Slot) -> T) -> T {
    let Some(mut slot) = SESSIONS.with(|sessions| sessions.borrow_mut().remove(&id)) else { return fallback };
    let out = f(&mut slot);
    // The result buffer lives on the heap, so pointers into it stay valid
    // across the move back into the table.
    SESSIONS.with(|sessions| sessions.borrow_mut().insert(id, slot));
    out
}

/// Split `bytes` into the per-range buffers `lengths` describes. `None`
/// when the lengths do not add up to exactly `bytes.len()` (or overflow).
fn split_parts(bytes: &[u8], lengths: &[u32]) -> Option<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(lengths.len());
    let mut at = 0usize;
    for length in lengths {
        let end = at.checked_add(usize::try_from(*length).ok()?)?;
        out.push(bytes.get(at..end)?.to_vec());
        at = end;
    }
    (at == bytes.len()).then_some(out)
}

#[no_mangle]
pub extern "C" fn rf_alloc(len: usize) -> *mut u8 {
    let mut buffer = Vec::<u8>::with_capacity(len.max(1));
    let ptr = buffer.as_mut_ptr();
    std::mem::forget(buffer);
    ptr
}

#[no_mangle]
pub unsafe extern "C" fn rf_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        drop(Vec::from_raw_parts(ptr, 0, len.max(1)));
    }
}

#[no_mangle]
pub extern "C" fn rf_session_new() -> u32 {
    let id = NEXT.with(|next| {
        let id = next.get().wrapping_add(1).max(1);
        next.set(id);
        id
    });
    SESSIONS.with(|sessions| sessions.borrow_mut().insert(id, Slot { session: Session::new(), result: Vec::new() }));
    id
}

#[no_mangle]
pub extern "C" fn rf_session_free(session: u32) {
    // Dropped outside the table borrow: tearing a session down runs engine
    // code (cancelled loads reject and evict their cache entries).
    let slot = SESSIONS.with(|sessions| sessions.borrow_mut().remove(&session));
    drop(slot);
}

#[no_mangle]
pub unsafe extern "C" fn rf_open(session: u32, url: *const u8, url_len: usize, options: *const u8, options_len: usize) -> u32 {
    let url = input_str(url, url_len);
    let options = input_json(options, options_len);
    with_session(session, 0, |slot| slot.session.open(&url, options))
}

#[no_mangle]
pub unsafe extern "C" fn rf_call(
    session: u32,
    engine: u32,
    method: *const u8,
    method_len: usize,
    params: *const u8,
    params_len: usize,
) -> u32 {
    let method = input_str(method, method_len);
    let params = input_json(params, params_len);
    with_session(session, 0, |slot| slot.session.call(engine, &method, params))
}

#[no_mangle]
pub extern "C" fn rf_close(session: u32, engine: u32) -> u32 {
    with_session(session, 0, |slot| slot.session.close(engine) as u32)
}

/// Deliver bytes for request `request`. `data` holds the concatenated
/// per-range buffers; `lengths` (u32 × `count`) splits it and must sum to
/// exactly `data_len` — otherwise the request completes with an error
/// rather than with misaligned buffers. Pass `count = 0` for a whole-object
/// response.
#[no_mangle]
pub unsafe extern "C" fn rf_complete(
    session: u32,
    request: u32,
    data: *const u8,
    data_len: usize,
    lengths: *const u32,
    count: usize,
) {
    let bytes = input(data, data_len);
    let parts = if count == 0 {
        Some(vec![bytes.to_vec()])
    } else if lengths.is_null() {
        None
    } else {
        split_parts(bytes, std::slice::from_raw_parts(lengths, count))
    };
    with_session(session, (), |slot| match parts {
        Some(parts) => slot.session.complete(request, parts),
        None => slot.session.complete_error(
            request,
            &format!("Rangefind host returned inconsistent range lengths ({count} ranges over {data_len} bytes)."),
        ),
    });
}

#[no_mangle]
pub extern "C" fn rf_complete_missing(session: u32, request: u32) {
    with_session(session, (), |slot| slot.session.complete_missing(request));
}

#[no_mangle]
pub unsafe extern "C" fn rf_complete_error(session: u32, request: u32, message: *const u8, message_len: usize) {
    let message = input_str(message, message_len);
    with_session(session, (), |slot| slot.session.complete_error(request, &message));
}

/// Poll the session; returns a pointer to the JSON event buffer.
#[no_mangle]
pub extern "C" fn rf_poll(session: u32) -> *const u8 {
    with_session(session, std::ptr::null(), |slot| {
        let value = slot.session.poll();
        slot.result = serde_json::to_vec(&value).unwrap_or_default();
        slot.result.as_ptr()
    })
}

#[no_mangle]
pub extern "C" fn rf_result_len(session: u32) -> usize {
    with_session(session, 0, |slot| slot.result.len())
}

#[no_mangle]
pub extern "C" fn rf_version() -> u32 {
    // major << 16 | minor << 8 | patch
    let parse = |value: &str| value.parse::<u32>().unwrap_or(0);
    (parse(env!("CARGO_PKG_VERSION_MAJOR")) << 16) | (parse(env!("CARGO_PKG_VERSION_MINOR")) << 8) | parse(env!("CARGO_PKG_VERSION_PATCH"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_parts_requires_exact_lengths() {
        let data = b"abcdef";
        assert_eq!(split_parts(data, &[2, 4]), Some(vec![b"ab".to_vec(), b"cdef".to_vec()]));
        assert_eq!(split_parts(data, &[0, 6, 0]), Some(vec![vec![], data.to_vec(), vec![]]));
        assert_eq!(split_parts(data, &[2, 5]), None, "lengths past the data");
        assert_eq!(split_parts(data, &[2, 3]), None, "lengths short of the data");
        assert_eq!(split_parts(data, &[u32::MAX, u32::MAX, 2]), None, "lengths that overflow");
    }

    fn poll_json(session: u32) -> serde_json::Value {
        let ptr = rf_poll(session);
        let len = rf_result_len(session);
        serde_json::from_slice(unsafe { std::slice::from_raw_parts(ptr, len) }).unwrap()
    }

    #[test]
    fn inconsistent_lengths_complete_with_an_error() {
        let session = rf_session_new();
        let url = b"mem://idx/";
        let op = unsafe { rf_open(session, url.as_ptr(), url.len(), std::ptr::null(), 0) };
        let events = poll_json(session);
        let request = events["requests"][0]["id"].as_u64().unwrap() as u32;
        let data = b"{}";
        let lengths = [1u32, 5];
        unsafe { rf_complete(session, request, data.as_ptr(), data.len(), lengths.as_ptr(), lengths.len()) };
        let events = poll_json(session);
        let done = events["done"].as_array().unwrap().iter().find(|done| done["op"] == serde_json::json!(op)).cloned().unwrap();
        assert_eq!(done["ok"], serde_json::json!(false));
        assert!(done["error"].as_str().unwrap().contains("inconsistent range lengths"), "{done}");
        rf_session_free(session);
        assert!(rf_poll(session).is_null());
    }
}
