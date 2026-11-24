//! Byte transport abstraction.
//!
//! The engine only ever needs two things from its host: "give me this whole
//! object" and "give me these byte ranges of this object". Everything about
//! *how* — `fetch` with Range headers, multipart byteranges, `pread` on a
//! local file, an asset bundle on a phone — lives behind [`Transport`], so
//! the query engine itself is identical on every platform.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use crate::error::{Error, Result};
use crate::rt::Promise;
use crate::util::bytes::Bytes;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchRequest {
    pub url: String,
    /// Empty means the whole object.
    pub ranges: Vec<ByteRange>,
}

#[derive(Clone, Debug)]
pub enum FetchResponse {
    /// The object does not exist (HTTP 404 and friends). Only meaningful for
    /// whole-object requests; a missing ranged object is an error.
    Missing,
    /// One buffer per requested range, in request order; a whole-object
    /// request yields exactly one buffer.
    Ok(Vec<Bytes>),
}

pub trait Transport {
    fn fetch(&self, request: FetchRequest) -> Promise<FetchResponse>;
}

/// Resolve `path` against `base` like `new URL(path, base).href` for the
/// forms index manifests use: relative paths, `./`, `../`, root-relative
/// and protocol-relative paths, query-only and fragment-only references,
/// and absolute URLs. Non-URL bases are filesystem paths (POSIX, relative,
/// or Windows `C:\dir\` / UNC `\\host\share\`) and resolve with path
/// semantics instead: no query or fragment parsing, `..` never climbs above
/// the root (or drive), and a backslash base keeps backslash separators.
pub fn resolve_url(base: &str, path: &str) -> String {
    if has_scheme(path) {
        return path.to_string();
    }
    if has_scheme(base) {
        resolve_against_url(base, path)
    } else {
        resolve_against_path(base, path)
    }
}

fn has_scheme(value: &str) -> bool {
    match value.find("://") {
        Some(index) => {
            index > 0
                && value.as_bytes()[0].is_ascii_alphabetic()
                && value[..index].bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
        }
        None => false,
    }
}

/// WHATWG "special" schemes: their paths treat `\` as `/` and an empty
/// path reads as `/`.
fn is_special(scheme: &str) -> bool {
    matches!(scheme.to_ascii_lowercase().as_str(), "http" | "https" | "ws" | "wss" | "ftp" | "file")
}

struct UrlParts<'a> {
    scheme: &'a str,
    authority: &'a str,
    path: &'a str,
    /// Including the leading `?`, or "".
    query: &'a str,
}

/// Split a `scheme://authority/path?query#fragment` URL (fragment dropped).
fn parse_url(url: &str) -> UrlParts<'_> {
    let scheme_end = url.find("://").unwrap_or(0);
    let scheme = &url[..scheme_end];
    let rest = &url[scheme_end + 3..];
    let rest = rest.split('#').next().unwrap_or(rest);
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let mut authority = &rest[..authority_end];
    if scheme.eq_ignore_ascii_case("file") && authority.eq_ignore_ascii_case("localhost") {
        authority = "";
    }
    let after = &rest[authority_end..];
    let (path, query) = match after.find('?') {
        Some(index) => (&after[..index], &after[index..]),
        None => (after, ""),
    };
    UrlParts { scheme, authority, path, query }
}

/// Split a relative reference into path, `?query` and `#fragment`.
fn split_reference(reference: &str) -> (&str, &str, &str) {
    let (before_fragment, fragment) = match reference.find('#') {
        Some(index) => (&reference[..index], &reference[index..]),
        None => (reference, ""),
    };
    let (path, query) = match before_fragment.find('?') {
        Some(index) => (&before_fragment[..index], &before_fragment[index..]),
        None => (before_fragment, ""),
    };
    (path, query, fragment)
}

fn resolve_against_url(base: &str, reference: &str) -> String {
    let parts = parse_url(base);
    let special = is_special(parts.scheme);
    let file = parts.scheme.eq_ignore_ascii_case("file");
    let normalized;
    let reference = if special && reference.contains('\\') {
        let (path, query, fragment) = split_reference(reference);
        normalized = format!("{}{query}{fragment}", path.replace('\\', "/"));
        normalized.as_str()
    } else {
        reference
    };
    let base_path = if parts.path.is_empty() && special { "/" } else { parts.path };
    let prefix = format!("{}://{}", parts.scheme, parts.authority);
    if reference.is_empty() {
        return format!("{prefix}{base_path}{}", parts.query);
    }
    if let Some(rest) = reference.strip_prefix("//") {
        // Protocol-relative: a new authority under the base's scheme.
        let (path, query, fragment) = split_reference(rest);
        let authority_end = path.find('/').unwrap_or(path.len());
        let (authority, path) = path.split_at(authority_end);
        let path = if path.is_empty() && special { "/".to_string() } else { remove_dot_segments(path, file) };
        return format!("{}://{authority}{path}{query}{fragment}", parts.scheme);
    }
    let (ref_path, ref_query, ref_fragment) = split_reference(reference);
    if ref_path.is_empty() {
        // `?q` replaces the base query; `#f` keeps it.
        let query = if ref_query.is_empty() { parts.query } else { ref_query };
        return format!("{prefix}{base_path}{query}{ref_fragment}");
    }
    let merged = if ref_path.starts_with('/') {
        ref_path.to_string()
    } else {
        let dir_end = base_path.rfind('/').map(|index| index + 1).unwrap_or(0);
        let dir = &base_path[..dir_end];
        if dir.is_empty() {
            format!("/{ref_path}")
        } else {
            format!("{dir}{ref_path}")
        }
    };
    format!("{prefix}{}{ref_query}{ref_fragment}", remove_dot_segments(&merged, file))
}

/// RFC 3986 / WHATWG dot-segment removal on an absolute path. A final `.`
/// or `..` leaves a trailing slash (`/a/b/..` → `/a/`); empty segments are
/// kept, as `new URL` keeps them. In `file:` URLs a leading Windows drive
/// (`/C:/`) is never popped, as in WHATWG's "shorten a path".
fn remove_dot_segments(path: &str, file: bool) -> String {
    let body = path.strip_prefix('/').unwrap_or(path);
    let parts: Vec<&str> = body.split('/').collect();
    let mut out: Vec<&str> = Vec::with_capacity(parts.len());
    let last = parts.len().saturating_sub(1);
    for (index, part) in parts.iter().enumerate() {
        match *part {
            "." => {
                if index == last {
                    out.push("");
                }
            }
            ".." => {
                let drive = file && out.len() == 1 && is_drive_letter(out[0]);
                if !drive {
                    out.pop();
                }
                if index == last {
                    out.push("");
                }
            }
            other => out.push(other),
        }
    }
    format!("/{}", out.join("/"))
}

fn is_drive_letter(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

/// Filesystem-path resolution for non-URL bases.
fn resolve_against_path(base: &str, path: &str) -> String {
    if path.is_empty() {
        return base.to_string();
    }
    let windows = is_windows_path(base);
    let is_sep = |c: char| c == '/' || (windows && c == '\\');
    if windows && is_windows_absolute(path) {
        return path.to_string();
    }
    let sep = if base.contains('\\') { '\\' } else { '/' };
    let (root, rest) = split_path_root(base, windows);
    let mut segments: Vec<&str> = Vec::new();
    if !path.starts_with(is_sep) {
        // Directory of the base: everything up to the last separator.
        let dir_end = rest.rfind(is_sep).map(|index| index + 1).unwrap_or(0);
        segments.extend(rest[..dir_end].split(is_sep).filter(|part| !part.is_empty()));
    }
    let parts: Vec<&str> = path.split(is_sep).collect();
    for part in &parts {
        match *part {
            "" | "." => {}
            ".." => match segments.last() {
                Some(&"..") | None if root.is_empty() => segments.push(".."),
                Some(_) => {
                    segments.pop();
                }
                None => {}
            },
            other => segments.push(other),
        }
    }
    let mut out = String::with_capacity(base.len() + path.len());
    out.push_str(root);
    out.push_str(&segments.join(&sep.to_string()));
    let directory = path.ends_with(is_sep) || matches!(parts.last(), Some(&".") | Some(&".."));
    if directory && !segments.is_empty() {
        out.push(sep);
    }
    out
}

/// `C:\…`, `C:/…`, `\\host\share\…`, or any base written with backslashes.
fn is_windows_path(value: &str) -> bool {
    is_windows_absolute(value) || value.contains('\\')
}

fn is_windows_absolute(value: &str) -> bool {
    let bytes = value.as_bytes();
    (bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && matches!(bytes[2], b'/' | b'\\'))
        || value.starts_with("\\\\")
}

/// The part of a filesystem path `..` may never remove: `/`, `C:\`, or
/// `\\host\share\`; "" for relative paths.
fn split_path_root(base: &str, windows: bool) -> (&str, &str) {
    let bytes = base.as_bytes();
    if windows && bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && matches!(bytes[2], b'/' | b'\\') {
        return base.split_at(3);
    }
    if windows && base.starts_with("\\\\") {
        // `\\host\share\` is the root of a UNC path.
        let mut seen = 0;
        for (index, c) in base.char_indices().skip(2) {
            if c == '\\' || c == '/' {
                seen += 1;
                if seen == 2 {
                    return base.split_at(index + 1);
                }
            }
        }
        return (base, "");
    }
    if base.starts_with('/') || (windows && base.starts_with('\\')) {
        return base.split_at(1);
    }
    ("", base)
}

/// Split `scheme://authority` from the path. `file:///x` has an empty
/// authority, so its origin is `file://`.
fn split_origin(base: &str) -> (&str, &str) {
    if !has_scheme(base) {
        return ("", base);
    }
    let scheme_end = base.find("://").unwrap_or(0) + 3;
    match base[scheme_end..].find(['/', '?', '#']) {
        Some(index) => (&base[..scheme_end + index], &base[scheme_end + index..]),
        None => (base, "/"),
    }
}

/// `scheme://authority` of a URL, or "" for non-URLs.
pub fn url_origin(url: &str) -> &str {
    split_origin(url).0
}

/// In-memory object store. Used by tests and by hosts that bundle a small
/// index into the binary.
#[derive(Default)]
pub struct MemoryTransport {
    objects: RefCell<HashMap<String, Bytes>>,
    log: RefCell<Vec<FetchRequest>>,
}

impl MemoryTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, url: impl Into<String>, data: Vec<u8>) {
        self.objects.borrow_mut().insert(url.into(), Bytes::new(data));
    }

    pub fn requests(&self) -> Vec<FetchRequest> {
        self.log.borrow().clone()
    }
}

fn slice_ranges(url: &str, data: &Bytes, ranges: &[ByteRange]) -> Result<FetchResponse> {
    if ranges.is_empty() {
        return Ok(FetchResponse::Ok(vec![data.clone()]));
    }
    let mut out = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.offset > data.len() as u64 {
            return Err(Error::new(format!("Range request failed for {url}")));
        }
        // offset <= len, so both conversions are lossless.
        let start = range.offset as usize;
        let end = range.offset.saturating_add(range.length).min(data.len() as u64) as usize;
        out.push(data.slice(start, end));
    }
    Ok(FetchResponse::Ok(out))
}

impl Transport for MemoryTransport {
    fn fetch(&self, request: FetchRequest) -> Promise<FetchResponse> {
        self.log.borrow_mut().push(request.clone());
        match self.objects.borrow().get(&request.url) {
            None => {
                if request.ranges.is_empty() {
                    Promise::resolved(FetchResponse::Missing)
                } else {
                    Promise::rejected(Error::new(format!("Range request failed for {}", request.url)))
                }
            }
            Some(data) => match slice_ranges(&request.url, data, &request.ranges) {
                Ok(response) => Promise::resolved(response),
                Err(error) => Promise::rejected(error),
            },
        }
    }
}

/// Local filesystem transport. Accepts `file://` URLs and plain paths, and
/// answers ranges with positioned reads so a multi-gigabyte pack is never
/// loaded whole.
///
/// Open handles are kept in a small LRU (64 by default) so a sharded index
/// with thousands of pack files never exhausts the process's descriptor
/// limit; a missing file is not remembered, so it is found once it appears.
#[cfg(not(target_arch = "wasm32"))]
pub struct FileTransport {
    /// Most recently used last.
    files: RefCell<Vec<(String, Rc<std::fs::File>)>>,
    max_open: usize,
    stats: RefCell<TransportStats>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportStats {
    pub requests: u64,
    pub ranges: u64,
    pub bytes: u64,
}

#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_MAX_OPEN_FILES: usize = 64;

#[cfg(not(target_arch = "wasm32"))]
impl Default for FileTransport {
    fn default() -> Self {
        Self::with_max_open_files(DEFAULT_MAX_OPEN_FILES)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl FileTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep at most `max_open` file handles open (at least one).
    pub fn with_max_open_files(max_open: usize) -> Self {
        FileTransport { files: RefCell::new(Vec::new()), max_open: max_open.max(1), stats: RefCell::new(TransportStats::default()) }
    }

    /// Number of file handles currently held open.
    pub fn open_files(&self) -> usize {
        self.files.borrow().len()
    }

    pub fn stats(&self) -> TransportStats {
        *self.stats.borrow()
    }

    pub fn reset_stats(&self) {
        *self.stats.borrow_mut() = TransportStats::default();
    }

    fn path_for(url: &str) -> Result<String> {
        file_url_to_path(url, cfg!(windows))
    }

    fn open(&self, path: &str) -> Result<Option<Rc<std::fs::File>>> {
        {
            let mut files = self.files.borrow_mut();
            if let Some(index) = files.iter().position(|(cached, _)| cached == path) {
                let entry = files.remove(index);
                let file = entry.1.clone();
                files.push(entry);
                return Ok(Some(file));
            }
        }
        let file = match std::fs::File::open(path) {
            Ok(file) => Rc::new(file),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::new(format!("Unable to fetch {path}: {error}"))),
        };
        let mut files = self.files.borrow_mut();
        if files.len() >= self.max_open {
            let excess = files.len() + 1 - self.max_open;
            files.drain(..excess);
        }
        files.push((path.to_string(), file.clone()));
        Ok(Some(file))
    }

    fn read(&self, request: &FetchRequest) -> Result<FetchResponse> {
        use std::io::Read;
        let path = Self::path_for(&request.url)?;
        let Some(file) = self.open(&path)? else {
            if request.ranges.is_empty() {
                return Ok(FetchResponse::Missing);
            }
            return Err(Error::new(format!("Range request failed for {}", request.url)));
        };
        let mut stats = self.stats.borrow_mut();
        stats.requests += 1;
        if request.ranges.is_empty() {
            let mut data = Vec::new();
            let mut handle: &std::fs::File = &file;
            std::io::Seek::seek(&mut handle, std::io::SeekFrom::Start(0))?;
            handle.read_to_end(&mut data)?;
            stats.bytes += data.len() as u64;
            return Ok(FetchResponse::Ok(vec![Bytes::new(data)]));
        }
        let file_len = file.metadata()?.len();
        let mut out = Vec::with_capacity(request.ranges.len());
        for range in &request.ranges {
            // Like an HTTP 416: a range starting past the end fails; one
            // running past it is cut at the end. The buffer is sized from
            // what the file can actually supply, never from the (possibly
            // corrupt) requested length.
            if range.offset > file_len {
                return Err(Error::new(format!("Range request failed for {}", request.url)));
            }
            let available = range.length.min(file_len - range.offset);
            let size = usize::try_from(available)
                .map_err(|_| Error::new(format!("Range request failed for {}: {available} bytes do not fit in memory", request.url)))?;
            let mut data = vec![0u8; size];
            let read = read_at(&file, &mut data, range.offset)?;
            data.truncate(read);
            stats.ranges += 1;
            stats.bytes += read as u64;
            out.push(Bytes::new(data));
        }
        Ok(FetchResponse::Ok(out))
    }
}

/// The filesystem path a [`FileTransport`] URL names. Only `file://` URLs
/// are URL-decoded (query and fragment dropped, `%xx` decoded, `localhost`
/// host accepted, `/C:/x` → `C:/x` and other hosts → UNC on Windows);
/// anything else is a plain path and is used verbatim, so a file name
/// containing `?`, `#` or `%` still opens.
#[cfg(not(target_arch = "wasm32"))]
fn file_url_to_path(url: &str, windows: bool) -> Result<String> {
    let Some(rest) = url.get(..7).filter(|prefix| prefix.eq_ignore_ascii_case("file://")).map(|_| &url[7..]) else {
        return Ok(url.to_string());
    };
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    let host_end = rest.find('/').unwrap_or(rest.len());
    let (host, path) = rest.split_at(host_end);
    let path = percent_decode(path);
    if !host.is_empty() && !host.eq_ignore_ascii_case("localhost") {
        if windows {
            return Ok(format!("\\\\{host}{}", path.replace('/', "\\")));
        }
        return Err(Error::new(format!("File URL host must be \"localhost\" or empty: {url}")));
    }
    if windows {
        let bytes = path.as_bytes();
        if bytes.len() >= 3 && bytes[0] == b'/' && bytes[1].is_ascii_alphabetic() && bytes[2] == b':' {
            return Ok(path[1..].to_string());
        }
    }
    Ok(path)
}

#[cfg(all(not(target_arch = "wasm32"), unix))]
fn read_at(file: &std::fs::File, buffer: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::unix::fs::FileExt;
    let mut filled = 0;
    while filled < buffer.len() {
        let read = file.read_at(&mut buffer[filled..], offset + filled as u64)?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

#[cfg(all(not(target_arch = "wasm32"), windows))]
fn read_at(file: &std::fs::File, buffer: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::windows::fs::FileExt;
    let mut filled = 0;
    while filled < buffer.len() {
        let read = file.seek_read(&mut buffer[filled..], offset + filled as u64)?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

#[cfg(all(not(target_arch = "wasm32"), not(unix), not(windows)))]
fn read_at(file: &std::fs::File, buffer: &mut [u8], offset: u64) -> Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    let mut handle: &std::fs::File = file;
    handle.seek(SeekFrom::Start(offset))?;
    let mut filled = 0;
    while filled < buffer.len() {
        let read = handle.read(&mut buffer[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

#[cfg(not(target_arch = "wasm32"))]
impl Transport for FileTransport {
    fn fetch(&self, request: FetchRequest) -> Promise<FetchResponse> {
        match self.read(&request) {
            Ok(response) => Promise::resolved(response),
            Err(error) => Promise::rejected(error),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn percent_decode(value: &str) -> String {
    if !value.contains('%') {
        return value.to_string();
    }
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(byte) = hex {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Host-driven transport: requests queue up for the embedder (JavaScript
/// glue around a wasm module, Swift/Kotlin through the C ABI, ...), which
/// performs them however the platform prefers and hands the bytes back.
#[derive(Default)]
pub struct HostTransport {
    next_id: RefCell<u32>,
    queue: RefCell<VecDeque<(u32, FetchRequest)>>,
    waiting: RefCell<HashMap<u32, Promise<FetchResponse>>>,
}

impl HostTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drain the requests issued since the last call.
    pub fn take_requests(&self) -> Vec<(u32, FetchRequest)> {
        self.queue.borrow_mut().drain(..).collect()
    }

    pub fn has_requests(&self) -> bool {
        !self.queue.borrow().is_empty()
    }

    pub fn in_flight(&self) -> usize {
        self.waiting.borrow().len()
    }

    /// Forget every queued and in-flight request, rejecting the promises
    /// still waiting on the host.
    pub fn cancel_all(&self) {
        self.queue.borrow_mut().clear();
        let waiting = std::mem::take(&mut *self.waiting.borrow_mut());
        for (_, promise) in waiting {
            promise.reject(Error::new(crate::rt::CANCELLED_MESSAGE));
        }
    }

    pub fn complete(&self, id: u32, result: Result<FetchResponse>) -> bool {
        let promise = self.waiting.borrow_mut().remove(&id);
        match promise {
            Some(promise) => {
                promise.settle(result);
                true
            }
            None => false,
        }
    }
}

impl Transport for HostTransport {
    fn fetch(&self, request: FetchRequest) -> Promise<FetchResponse> {
        let id = {
            let mut next = self.next_id.borrow_mut();
            *next = next.wrapping_add(1).max(1);
            *next
        };
        let promise = Promise::pending();
        self.waiting.borrow_mut().insert(id, promise.clone());
        self.queue.borrow_mut().push_back((id, request));
        promise
    }
}

impl<T: Transport + ?Sized> Transport for Rc<T> {
    fn fetch(&self, request: FetchRequest) -> Promise<FetchResponse> {
        (**self).fetch(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_like_url() {
        assert_eq!(resolve_url("https://x.dev/rangefind/", "terms/packs/0.bin"), "https://x.dev/rangefind/terms/packs/0.bin");
        assert_eq!(resolve_url("https://x.dev/rangefind/manifest.json", "a.bin"), "https://x.dev/rangefind/a.bin");
        assert_eq!(resolve_url("https://x.dev/a/b/", "../c/d"), "https://x.dev/a/c/d");
        assert_eq!(resolve_url("https://x.dev/a/b/", "/root.bin"), "https://x.dev/root.bin");
        assert_eq!(resolve_url("https://x.dev/a/", "https://y.dev/z"), "https://y.dev/z");
        assert_eq!(resolve_url("https://x.dev", "a"), "https://x.dev/a");
        assert_eq!(resolve_url("/tmp/idx/", "docs/packs/0.bin"), "/tmp/idx/docs/packs/0.bin");
        assert_eq!(resolve_url("file:///tmp/idx/", "./shards/0001/"), "file:///tmp/idx/shards/0001/");
        assert_eq!(resolve_url("rel/idx/", "x/y"), "rel/idx/x/y");
        assert_eq!(url_origin("https://x.dev/a/b"), "https://x.dev");
    }

    /// `new URL(path, base).href` from Node, row by row.
    #[test]
    fn resolves_like_whatwg_url() {
        let rows = [
            ("https://x.dev/a/m.json?v=a/b", "c.bin", "https://x.dev/a/c.bin"),
            ("https://x.dev?x=1", "a", "https://x.dev/a"),
            ("https://x.dev?x=1", "?y=2", "https://x.dev/?y=2"),
            ("https://x.dev?x=1", "#f", "https://x.dev/?x=1#f"),
            ("file://localhost/tmp/idx/", "a", "file:///tmp/idx/a"),
            ("https://x.dev/a/", "//cdn.dev/x", "https://cdn.dev/x"),
            ("https://x.dev/a/", "//cdn.dev", "https://cdn.dev/"),
            ("https://x.dev/a/b", "?q=1", "https://x.dev/a/b?q=1"),
            ("https://x.dev/a/b?z=1", "?q=1", "https://x.dev/a/b?q=1"),
            ("https://x.dev/a/b?z=1", "#f", "https://x.dev/a/b?z=1#f"),
            ("https://x.dev/a/b#old", "#f", "https://x.dev/a/b#f"),
            ("https://x.dev/a/b?z=1#old", "", "https://x.dev/a/b?z=1"),
            ("https://x.dev/a/", ".", "https://x.dev/a/"),
            ("https://x.dev/a/", "..", "https://x.dev/"),
            ("https://x.dev/a/", "b/..", "https://x.dev/a/"),
            ("https://x.dev/a/", "b/.", "https://x.dev/a/b/"),
            ("https://x.dev/a/", "../../../x", "https://x.dev/x"),
            ("https://x.dev/a/", "x?v=1#h", "https://x.dev/a/x?v=1#h"),
            ("https://x.dev/a/", "x\\y", "https://x.dev/a/x/y"),
            ("https://x.dev/a//b/", "c", "https://x.dev/a//b/c"),
            ("https://x.dev/a/b/", "./", "https://x.dev/a/b/"),
            ("https://x.dev:8080/a/", "b", "https://x.dev:8080/a/b"),
            ("file:///C:/idx/", "a.bin", "file:///C:/idx/a.bin"),
            ("file:///C:/idx/", "../../a.bin", "file:///C:/a.bin"),
            ("https://x.dev/a/#frag", "b", "https://x.dev/a/b"),
            ("https://x.dev/a/", "/x/../y", "https://x.dev/y"),
        ];
        for (base, path, expected) in rows {
            assert_eq!(resolve_url(base, path), expected, "new URL({path:?}, {base:?})");
        }
    }

    #[test]
    fn resolves_filesystem_paths() {
        let rows = [
            ("/tmp/idx/", "a/../b.bin", "/tmp/idx/b.bin"),
            ("/tmp/idx/", "../../../x", "/x"),
            ("/tmp/idx/", "/root.bin", "/root.bin"),
            ("/tmp/idx/", "b/..", "/tmp/idx/"),
            ("/tmp/idx/", "pack?v=1#x", "/tmp/idx/pack?v=1#x"),
            ("./rangefind/", "manifest.json", "./rangefind/manifest.json"),
            ("rel/idx/", "../../../x", "../x"),
            ("C:\\idx\\", "a.bin", "C:\\idx\\a.bin"),
            ("C:\\idx\\", "shards/0001/", "C:\\idx\\shards\\0001\\"),
            ("C:\\idx\\", "..\\..\\..\\a.bin", "C:\\a.bin"),
            ("C:\\idx\\", "/root.bin", "C:\\root.bin"),
            ("C:\\idx\\", "D:\\other\\x", "D:\\other\\x"),
            ("C:/idx/", "../../a.bin", "C:/a.bin"),
            ("\\\\host\\share\\idx\\", "../../x", "\\\\host\\share\\x"),
        ];
        for (base, path, expected) in rows {
            assert_eq!(resolve_url(base, path), expected, "resolve {path:?} against {base:?}");
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn file_urls_map_to_paths() {
        let ok = |url: &str, windows: bool| file_url_to_path(url, windows).unwrap();
        assert_eq!(ok("file:///tmp/idx/a.bin", false), "/tmp/idx/a.bin");
        assert_eq!(ok("file://localhost/tmp/x", false), "/tmp/x");
        assert_eq!(ok("FILE:///tmp/a%20b.bin?v=1#f", false), "/tmp/a b.bin");
        assert!(file_url_to_path("file://server/tmp/x", false).is_err());
        assert_eq!(ok("file:///C:/idx/a.bin", true), "C:/idx/a.bin");
        assert_eq!(ok("file:///C:/idx/a.bin", false), "/C:/idx/a.bin");
        assert_eq!(ok("file://server/share/x", true), "\\\\server\\share\\x");
        // Plain paths are verbatim: no query/fragment cut, no decoding.
        assert_eq!(ok("/tmp/a?b#c%20d", false), "/tmp/a?b#c%20d");
        assert_eq!(ok("C:\\idx\\a%41.bin", true), "C:\\idx\\a%41.bin");
    }

    #[test]
    fn memory_transport_slices() {
        let transport = MemoryTransport::new();
        transport.insert("a", vec![1, 2, 3, 4, 5]);
        let response = transport
            .fetch(FetchRequest { url: "a".into(), ranges: vec![ByteRange { offset: 1, length: 2 }, ByteRange { offset: 4, length: 9 }] })
            .peek()
            .unwrap()
            .unwrap();
        match response {
            FetchResponse::Ok(parts) => {
                assert_eq!(&*parts[0], &[2, 3]);
                assert_eq!(&*parts[1], &[5]);
            }
            FetchResponse::Missing => panic!("expected bytes"),
        }
        assert!(matches!(
            transport.fetch(FetchRequest { url: "nope".into(), ranges: vec![] }).peek(),
            Some(Ok(FetchResponse::Missing))
        ));
    }

    #[test]
    fn host_transport_round_trip() {
        let transport = HostTransport::new();
        let promise = transport.fetch(FetchRequest { url: "u".into(), ranges: vec![] });
        let requests = transport.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(promise.peek().is_none());
        assert!(transport.complete(requests[0].0, Ok(FetchResponse::Ok(vec![Bytes::new(vec![9])]))));
        assert!(promise.peek().unwrap().is_ok());
        assert!(!transport.complete(requests[0].0, Ok(FetchResponse::Missing)));
    }
}
