//! Error types for robot behavior tree operations.
//! 
//! This module provides a centralized error type for all operations in the behavior tree system,
//! including serialization, deserialization, and I/O operations.

use std::io;
use thiserror::Error;

/// Errors that can occur in robot behavior tree operations.
/// 
/// This enum implements the standard `Error` trait and wraps various error types
/// that may occur during behavior tree operations, providing a uniform error handling approach.
#[derive(Debug, Error)]
pub enum RobotBTError {
    /// I/O error during file operations
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),

    /// YAML parsing or generation error
    #[error("YAML error: {0}")]
    YamlError(String),

    /// An action name in a tree definition was not found in the registry
    #[error("Unknown action '{name}' — register it with register_action!")]
    UnknownAction { name: String },

    /// A condition name in a tree definition was not found in the registry
    #[error("Unknown condition '{name}' — register it with register_condition!")]
    UnknownCondition { name: String },

    /// Lua scripting error
    #[cfg(feature = "lua")]
    #[error("Lua error: {0}")]
    LuaError(String),
}

#[cfg(feature = "lua")]
impl From<mlua::Error> for RobotBTError {
    fn from(e: mlua::Error) -> Self {
        RobotBTError::LuaError(e.to_string())
    }
}

/// Utility functions for working with errors
impl RobotBTError {
    /// Create a new YAML parsing error with a custom message
    pub fn yaml_error<S: Into<String>>(msg: S) -> Self {
        RobotBTError::YamlError(msg.into())
    }
}
