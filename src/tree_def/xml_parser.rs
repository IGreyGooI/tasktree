//! XML parser for `NodeDef` — hand-written event-API parser using `quick-xml`.
//!
//! Element name = node type. Attributes carry scalar fields.
//! `<true_branch>` / `<false_branch>` are wrapper elements for `Condition` branches.
//! Multiple children inside a branch wrapper are implicitly wrapped in a `Sequence`.
//!
//! ## Format
//!
//! ```xml
//! <Selector name="npc_root">
//!   <Condition name="has_pending_dialogue" condition_name="player_dialogue_pending">
//!     <true_branch>
//!       <Action name="respond_to_player"/>
//!     </true_branch>
//!   </Condition>
//! </Selector>
//! ```
//!
//! ## Position tracking
//!
//! All parser functions receive a [`ParseCtx`] that carries both the `Reader`
//! and the original source string. [`SourcePos::from_reader`] is called at
//! each error site to record byte offset + 1-based line + column.

use super::NodeDef;
use crate::error::{RobotBTError, SourcePos, XmlError};
use crate::types::ParallelPolicy;
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};

// ── ParseCtx ─────────────────────────────────────────────────────────────────

/// Parser context: bundles the `quick-xml` reader with the original source
/// string so every error site can compute a [`SourcePos`] without extra
/// parameter threading.
pub(super) struct ParseCtx<'src> {
    pub reader: Reader<&'src [u8]>,
    pub src: &'src str,
}

impl<'src> ParseCtx<'src> {
    /// Snapshot the current reader position as a [`SourcePos`].
    pub fn pos(&self) -> SourcePos {
        SourcePos::from_reader(&self.reader, self.src)
    }
}

// ── public entry points called from NodeDef::from_xml ────────────────────────

/// Parse a node that opened with a `Start` event (multi-child element).
pub(super) fn parse_node_start(
    ctx: &mut ParseCtx<'_>,
    e: &BytesStart<'_>,
) -> Result<NodeDef, RobotBTError> {
    let pos = ctx.pos();
    let tag = std::str::from_utf8(e.local_name().into_inner())
        .map_err(|err| XmlError::InvalidUtf8 {
            detail: format!("tag name: {err}"),
            pos: pos.clone(),
        })?
        .to_string();

    match tag.as_str() {
        "Action" => {
            // <Action> with child elements — drain to closing tag (attributes only)
            let name = attr_required(e, "name", &pos)?;
            drain_to_end(ctx, &tag)?;
            Ok(NodeDef::Action { name })
        }
        "Sequence" | "Selector" | "Parallel" => {
            let name = attr_required(e, "name", &pos)?;
            let policy = if tag == "Parallel" {
                attr_optional(e, "policy", &pos)?
                    .as_deref()
                    .map(|s| parse_policy(s, &pos))
                    .transpose()?
                    .unwrap_or_default()
            } else {
                ParallelPolicy::default()
            };
            let children = parse_children(ctx, &tag)?;
            match tag.as_str() {
                "Sequence" => Ok(NodeDef::Sequence { name, children }),
                "Selector" => Ok(NodeDef::Selector { name, children }),
                "Parallel" => Ok(NodeDef::Parallel {
                    name,
                    policy,
                    children,
                }),
                _ => unreachable!(),
            }
        }
        "Condition" => {
            let name = attr_required(e, "name", &pos)?;
            let condition_name = attr_required(e, "condition_name", &pos)?;
            let (true_branch, false_branch) = parse_condition_branches(ctx, &name)?;
            let end_pos = ctx.pos();
            let true_branch = true_branch.ok_or_else(|| XmlError::MissingTrueBranch {
                name: name.clone(),
                pos: end_pos,
            })?;
            Ok(NodeDef::Condition {
                name,
                condition_name,
                true_branch: Box::new(true_branch),
                false_branch: false_branch.map(Box::new),
            })
        }
        other => Err(XmlError::UnknownElement {
            name: other.to_string(),
            pos,
        }
        .into()),
    }
}

/// Parse a self-closing node (`<Action name="..."/>`, `Event::Empty`).
pub(super) fn parse_node_empty(
    ctx: &ParseCtx<'_>,
    e: &BytesStart<'_>,
) -> Result<NodeDef, RobotBTError> {
    let pos = ctx.pos();
    let tag = std::str::from_utf8(e.local_name().into_inner())
        .map_err(|err| XmlError::InvalidUtf8 {
            detail: format!("tag name: {err}"),
            pos: pos.clone(),
        })?
        .to_string();

    match tag.as_str() {
        "Action" => Ok(NodeDef::Action {
            name: attr_required(e, "name", &pos)?,
        }),
        other => Err(XmlError::InvalidSelfClosing {
            name: other.to_string(),
            pos,
        }
        .into()),
    }
}

// ── internal helpers ──────────────────────────────────────────────────────────

/// Read all direct child nodes until the closing tag for `parent_tag`.
fn parse_children(ctx: &mut ParseCtx<'_>, parent_tag: &str) -> Result<Vec<NodeDef>, RobotBTError> {
    let mut children = Vec::new();
    loop {
        let pos = ctx.pos();
        match ctx.reader.read_event().map_err(|e| XmlError::ReaderError {
            pos: pos.clone(),
            message: e.to_string(),
        })? {
            Event::Start(ref e) => {
                children.push(parse_node_start(ctx, e)?);
            }
            Event::Empty(ref e) => {
                children.push(parse_node_empty(ctx, e)?);
            }
            Event::End(ref e) => {
                let end_tag = std::str::from_utf8(e.local_name().into_inner()).map_err(|err| {
                    XmlError::InvalidUtf8 {
                        detail: format!("closing tag: {err}"),
                        pos: pos.clone(),
                    }
                })?;
                if end_tag == parent_tag {
                    break;
                }
                return Err(XmlError::UnexpectedClosingTag {
                    expected: parent_tag.to_string(),
                    got: end_tag.to_string(),
                    pos,
                }
                .into());
            }
            Event::Text(_) | Event::Comment(_) | Event::CData(_) => {
                // Whitespace / comments between child elements — skip.
            }
            Event::Eof => {
                return Err(XmlError::UnexpectedEof {
                    context: parent_tag.to_string(),
                    pos,
                }
                .into());
            }
            _ => {}
        }
    }
    Ok(children)
}

/// Read `<true_branch>` and `<false_branch>` wrapper elements inside a `<Condition>`.
///
/// Multiple children inside a branch wrapper are implicitly wrapped in a `NodeDef::Sequence`.
/// Either branch may be absent. Returns `(true_branch, false_branch)`.
fn parse_condition_branches(
    ctx: &mut ParseCtx<'_>,
    condition_name: &str,
) -> Result<(Option<NodeDef>, Option<NodeDef>), RobotBTError> {
    let mut true_branch: Option<NodeDef> = None;
    let mut false_branch: Option<NodeDef> = None;

    loop {
        let pos = ctx.pos();
        match ctx.reader.read_event().map_err(|e| XmlError::ReaderError {
            pos: pos.clone(),
            message: e.to_string(),
        })? {
            Event::Start(ref e) => {
                let tag = std::str::from_utf8(e.local_name().into_inner())
                    .map_err(|err| XmlError::InvalidUtf8 {
                        detail: format!("branch tag: {err}"),
                        pos: pos.clone(),
                    })?
                    .to_string();
                match tag.as_str() {
                    "true_branch" => {
                        let children = parse_children(ctx, "true_branch")?;
                        true_branch = Some(children_to_node(children, "true_branch_seq", &pos)?);
                    }
                    "false_branch" => {
                        let children = parse_children(ctx, "false_branch")?;
                        false_branch = Some(children_to_node(children, "false_branch_seq", &pos)?);
                    }
                    other => {
                        return Err(XmlError::UnexpectedConditionChild {
                            condition: condition_name.to_string(),
                            found: other.to_string(),
                            pos,
                        }
                        .into());
                    }
                }
            }
            Event::Empty(ref e) => {
                let tag = std::str::from_utf8(e.local_name().into_inner())
                    .map_err(|err| XmlError::InvalidUtf8 {
                        detail: format!("branch tag: {err}"),
                        pos: pos.clone(),
                    })?
                    .to_string();
                return Err(XmlError::SelfClosingInsideCondition {
                    condition: condition_name.to_string(),
                    name: tag,
                    pos,
                }
                .into());
            }
            Event::End(ref e) => {
                let end_tag = std::str::from_utf8(e.local_name().into_inner()).map_err(|err| {
                    XmlError::InvalidUtf8 {
                        detail: format!("closing tag: {err}"),
                        pos: pos.clone(),
                    }
                })?;
                if end_tag == "Condition" {
                    break;
                }
                return Err(XmlError::UnexpectedClosingTag {
                    expected: "Condition".to_string(),
                    got: end_tag.to_string(),
                    pos,
                }
                .into());
            }
            Event::Text(_) | Event::Comment(_) => {}
            Event::Eof => {
                return Err(XmlError::UnexpectedEof {
                    context: format!("Condition '{condition_name}'"),
                    pos,
                }
                .into());
            }
            _ => {}
        }
    }

    Ok((true_branch, false_branch))
}

/// Convert a list of child nodes into a single `NodeDef`:
/// - 0 children → error (branch wrappers must not be empty)
/// - 1 child → the child directly (no wrapping)
/// - 2+ children → implicit `NodeDef::Sequence` (sequential by default)
fn children_to_node(
    children: Vec<NodeDef>,
    seq_name: &str,
    pos: &SourcePos,
) -> Result<NodeDef, RobotBTError> {
    match children.len() {
        0 => Err(XmlError::EmptyBranchWrapper { pos: pos.clone() }.into()),
        1 => Ok(children.into_iter().next().unwrap()),
        _ => Ok(NodeDef::Sequence {
            name: seq_name.to_string(),
            children,
        }),
    }
}

/// Drain events until the closing tag for `tag` is found.
fn drain_to_end(ctx: &mut ParseCtx<'_>, tag: &str) -> Result<(), RobotBTError> {
    loop {
        let pos = ctx.pos();
        match ctx.reader.read_event().map_err(|e| XmlError::ReaderError {
            pos: pos.clone(),
            message: e.to_string(),
        })? {
            Event::End(ref e) => {
                let end_tag = std::str::from_utf8(e.local_name().into_inner()).map_err(|err| {
                    XmlError::InvalidUtf8 {
                        detail: format!("closing tag: {err}"),
                        pos: pos.clone(),
                    }
                })?;
                if end_tag == tag {
                    return Ok(());
                }
            }
            Event::Eof => {
                return Err(XmlError::UnexpectedEof {
                    context: tag.to_string(),
                    pos: SourcePos::from_reader(&ctx.reader, ctx.src),
                }
                .into());
            }
            _ => {}
        }
    }
}

/// Read a required XML attribute by name. Returns `MissingAttribute` if absent.
pub(super) fn attr_required(
    e: &BytesStart<'_>,
    attr_name: &str,
    pos: &SourcePos,
) -> Result<String, RobotBTError> {
    let element = std::str::from_utf8(e.local_name().into_inner())
        .unwrap_or("?")
        .to_string();
    for attr in e.attributes() {
        let attr = attr.map_err(|err| XmlError::ReaderError {
            pos: pos.clone(),
            message: err.to_string(),
        })?;
        if attr.key.local_name().into_inner() == attr_name.as_bytes() {
            return Ok(std::str::from_utf8(&attr.value)
                .map_err(|err| XmlError::InvalidUtf8 {
                    detail: format!("attribute '{attr_name}': {err}"),
                    pos: pos.clone(),
                })?
                .to_string());
        }
    }
    Err(XmlError::MissingAttribute {
        element,
        attr: attr_name.to_string(),
        pos: pos.clone(),
    }
    .into())
}

/// Read an optional XML attribute by name. Returns `None` if missing.
fn attr_optional(
    e: &BytesStart<'_>,
    attr_name: &str,
    pos: &SourcePos,
) -> Result<Option<String>, RobotBTError> {
    for attr in e.attributes() {
        let attr = attr.map_err(|err| XmlError::ReaderError {
            pos: pos.clone(),
            message: err.to_string(),
        })?;
        if attr.key.local_name().into_inner() == attr_name.as_bytes() {
            return Ok(Some(
                std::str::from_utf8(&attr.value)
                    .map_err(|err| XmlError::InvalidUtf8 {
                        detail: format!("attribute '{attr_name}': {err}"),
                        pos: pos.clone(),
                    })?
                    .to_string(),
            ));
        }
    }
    Ok(None)
}

/// Parse a `ParallelPolicy` from a string attribute value.
fn parse_policy(s: &str, pos: &SourcePos) -> Result<ParallelPolicy, RobotBTError> {
    match s {
        "AllSucceed" => Ok(ParallelPolicy::AllSucceed),
        "FirstSucceed" => Ok(ParallelPolicy::FirstSucceed),
        "AnySucceed" => Ok(ParallelPolicy::AnySucceed),
        other => Err(XmlError::UnknownPolicy {
            value: other.to_string(),
            pos: pos.clone(),
        }
        .into()),
    }
}

// ── Public factory used by NodeDef::from_xml ──────────────────────────────────

/// Build a [`ParseCtx`] from a source string and parse the root node.
///
/// Called by `NodeDef::from_xml`. Returns `Err(RobotBTError::XmlError(...))` on
/// any parse failure, with `SourcePos` pointing at the offending byte.
pub(super) fn parse_root(src: &str) -> Result<NodeDef, RobotBTError> {
    let mut reader = Reader::from_str(src);
    reader.config_mut().trim_text(true);

    // Find the first real element.
    loop {
        let pos_snapshot = SourcePos::from_reader(&reader, src);
        match reader.read_event().map_err(|e| XmlError::ReaderError {
            pos: pos_snapshot.clone(),
            message: e.to_string(),
        })? {
            Event::Start(ref e) => {
                let mut ctx = ParseCtx { reader, src };
                return parse_node_start(&mut ctx, e);
            }
            Event::Empty(ref e) => {
                let ctx = ParseCtx { reader, src };
                return parse_node_empty(&ctx, e);
            }
            Event::Text(_) | Event::Comment(_) | Event::Decl(_) | Event::DocType(_) => {
                // Prolog / whitespace — skip.
            }
            Event::Eof => {
                return Err(XmlError::UnexpectedEof {
                    context: "(document root)".to_string(),
                    pos: SourcePos::from_reader(&reader, src),
                }
                .into());
            }
            _ => {}
        }
    }
}
