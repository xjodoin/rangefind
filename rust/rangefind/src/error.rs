//! Engine error type. Errors are cheap to clone because a single failed
//! fetch may reject many waiters that share one in-flight request.

use std::fmt;
use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    message: Rc<str>,
    code: Option<Rc<str>>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(message: impl AsRef<str>) -> Self {
        Error { message: Rc::from(message.as_ref()), code: None }
    }

    /// An error carrying a stable machine-readable code (mirrors the JS
    /// runtime's `error.code`, e.g. `RANGEFIND_GEO_TEXT_SORT_BUDGET`).
    pub fn with_code(message: impl AsRef<str>, code: impl AsRef<str>) -> Self {
        Error { message: Rc::from(message.as_ref()), code: Some(Rc::from(code.as_ref())) }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::new(format!("Rangefind JSON error: {error}"))
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::new(format!("Rangefind I/O error: {error}"))
    }
}

#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::error::Error::new(format!($($arg)*)))
    };
}
