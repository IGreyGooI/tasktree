//! Error types for robot behavior tree operations.
//!
//! [`RobotBTError`] is the top-level error returned by all public APIs.
//! When the `xml` feature is enabled, [`XmlError`] provides typed sub-variants
//! for every class of XML parse failure, and [`SourcePos`] carries the exact
//! byte offset, line number, and column number of the failure location.

use std::io;
use thiserror::Error;

// ── Top-level error ───────────────────────────────────────────────────────────

/// Errors that can occur in robot behavior tree operations.
#[derive(Debug, Error)]
pub enum RobotBTError {
    /// I/O error during file operations.
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),

    /// YAML parsing or generation error.
    #[error("YAML error: {0}")]
    YamlError(String),

    /// Structured XML parse error (xml feature).
    #[cfg(feature = "xml")]
    #[error("XML error: {0}")]
    XmlError(#[from] XmlError),

    /// An action name in a tree definition was not found in the registry.
    #[error("Unknown action '{name}' — register it with register_action!")]
    UnknownAction { name: String },

    /// A condition name in a tree definition was not found in the registry.
    #[error("Unknown condition '{name}' — register it with register_condition!")]
    UnknownCondition { name: String },

    /// Lua scripting error.
    #[cfg(feature = "lua")]
    #[error("Lua error: {0}")]
    LuaError(String),
}

impl RobotBTError {
    /// Create a YAML parsing error.
    pub fn yaml_error<S: Into<String>>(msg: S) -> Self {
        RobotBTError::YamlError(msg.into())
    }
}

#[cfg(feature = "lua")]
impl From<mlua::Error> for RobotBTError {
    fn from(e: mlua::Error) -> Self {
        RobotBTError::LuaError(e.to_string())
    }
}

// ── SourcePos — position memo ─────────────────────────────────────────────────

/// A source-position memo: byte offset, 1-based line number, and 1-based column
/// number within an XML source string.
///
/// Constructed once per error from the `quick-xml` reader's current position.
/// Stored inside each [`XmlError`] variant so callers can display or compare
/// exact locations without re-parsing.
///
/// ## Construction
///
/// Use [`SourcePos::from_reader`] inside the XML parser to capture the current
/// position from a `quick_xml::Reader<&[u8]>` and the original source string.
#[cfg(feature = "xml")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePos {
    /// Byte offset into the source string (0-based).
    pub offset: u64,
    /// 1-based line number (counts `\n` bytes before `offset`).
    pub line: usize,
    /// 1-based column number (bytes since the last `\n`).
    pub col: usize,
}

#[cfg(feature = "xml")]
impl SourcePos {
    /// Compute a `SourcePos` from a `quick_xml::Reader` and the original source string.
    ///
    /// `src` must be the same string that was passed to `Reader::from_str(src)`.
    /// `reader.buffer_position()` is used for the byte offset, then line/col are
    /// derived by counting `\n` bytes in the prefix (O(offset), negligible for
    /// config files).
    pub fn from_reader(reader: &quick_xml::Reader<&[u8]>, src: &str) -> Self {
        let offset = reader.buffer_position();
        let prefix = &src[..offset.min(src.len() as u64) as usize];
        let line = prefix.bytes().filter(|&b| b == b'\n').count() + 1;
        let col = prefix
            .rfind('\n')
            .map(|p| offset as usize - p - 1)
            .unwrap_or(offset as usize)
            + 1;
        Self { offset, line, col }
    }

    /// Construct a `SourcePos` directly from known values (for tests).
    pub fn new(offset: u64, line: usize, col: usize) -> Self {
        Self { offset, line, col }
    }
}

#[cfg(feature = "xml")]
impl std::fmt::Display for SourcePos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{} (byte {})", self.line, self.col, self.offset)
    }
}

// ── XmlError sub-enum ─────────────────────────────────────────────────────────

/// Typed XML parse error — one variant per class of failure.
///
/// Returned as `RobotBTError::XmlError(XmlError::...)` from `NodeDef::from_xml()`
/// and `BehaviorTreeRuntime::from_xml()`.
///
/// Every streaming-position variant carries `pos: SourcePos` so the caller
/// can display or test against `pos.line`, `pos.col`, and `pos.offset`.
#[cfg(feature = "xml")]
#[derive(Debug, Error, PartialEq, Eq)]
pub enum XmlError {
    /// A low-level `quick-xml` reader error (malformed byte stream).
    #[error("XML reader error at {pos}: {message}")]
    ReaderError { pos: SourcePos, message: String },

    /// An XML element name was not one of the recognised BT node types.
    ///
    /// Example: `<Robot name="r"/>` → `UnknownElement { name: "Robot", pos: ... }`
    #[error(
        "at {pos}: unknown BT node element <{name}>; \
         expected Action, Sequence, Selector, Parallel, or Condition"
    )]
    UnknownElement { name: String, pos: SourcePos },

    /// A required XML attribute was absent from an element.
    ///
    /// Example: `<Action/>` → `MissingAttribute { element: "Action", attr: "name", pos: ... }`
    #[error("at {pos}: element <{element}> is missing required attribute '{attr}'")]
    MissingAttribute {
        element: String,
        attr: String,
        pos: SourcePos,
    },

    /// A self-closing element was used where only `Action` is permitted.
    #[error(
        "at {pos}: self-closing element <{name}/> is only valid for Action; \
         container nodes (Sequence, Selector, Parallel, Condition) must have child content"
    )]
    InvalidSelfClosing { name: String, pos: SourcePos },

    /// A closing tag did not match the expected opening tag.
    #[error(
        "at {pos}: unexpected closing tag </{got}> \
         while parsing children of <{expected}>"
    )]
    UnexpectedClosingTag {
        expected: String,
        got: String,
        pos: SourcePos,
    },

    /// Unexpected element found directly inside a `<Condition>` body.
    #[error(
        "at {pos}: unexpected element <{found}> inside Condition '{condition}'; \
         expected <true_branch> or <false_branch>"
    )]
    UnexpectedConditionChild {
        condition: String,
        found: String,
        pos: SourcePos,
    },

    /// A `<Condition>` did not provide a `<true_branch>` element.
    #[error(
        "at {pos}: Condition '{name}' requires a <true_branch>; \
         for false-only conditions wrap the false action in a <false_branch> \
         and add a dummy <true_branch> with <Action name=\"noop\"/>"
    )]
    MissingTrueBranch { name: String, pos: SourcePos },

    /// A branch wrapper element contained no child nodes.
    #[error("at {pos}: branch wrapper element must contain at least one child node")]
    EmptyBranchWrapper { pos: SourcePos },

    /// An unrecognised string was given for a `ParallelPolicy` attribute.
    #[error(
        "at {pos}: unknown ParallelPolicy '{value}'; \
         expected AllSucceed, FirstSucceed, or AnySucceed"
    )]
    UnknownPolicy { value: String, pos: SourcePos },

    /// Unexpected end-of-file before the tree was fully parsed.
    #[error("at {pos}: unexpected EOF while parsing <{context}>")]
    UnexpectedEof { context: String, pos: SourcePos },

    /// Invalid UTF-8 bytes in a tag name or attribute value.
    #[error("at {pos}: invalid UTF-8 in XML: {detail}")]
    InvalidUtf8 { detail: String, pos: SourcePos },

    /// A self-closing element appeared inside a `<Condition>` body.
    #[error(
        "at {pos}: self-closing <{name}/> is not valid inside Condition '{condition}'; \
         use <{name}><Action name=\"...\"/></{name}>"
    )]
    SelfClosingInsideCondition {
        condition: String,
        name: String,
        pos: SourcePos,
    },
}
