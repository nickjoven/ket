//! Code Document Object Model (CDOM).
//!
//! tree-sitter AST parsing -> symbol extraction -> content-addressed snapshots.
//! Supports Rust and Python grammars.

use ket_cas::{Cid, Store as CasStore};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum CdomError {
    #[error("CAS error: {0}")]
    Cas(#[from] ket_cas::CasError),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Unsupported language: {0}")]
    UnsupportedLanguage(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// A symbol extracted from source code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Symbol {
    pub name: String,
    pub kind: SymbolKind,
    pub start_line: usize,
    pub end_line: usize,
    pub parent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SymbolKind {
    Function,
    Method,
    Class,
    Struct,
    Enum,
    Trait,
    Impl,
    Module,
    Import,
    Constant,
    Variable,
}

impl std::fmt::Display for SymbolKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SymbolKind::Function => write!(f, "function"),
            SymbolKind::Method => write!(f, "method"),
            SymbolKind::Class => write!(f, "class"),
            SymbolKind::Struct => write!(f, "struct"),
            SymbolKind::Enum => write!(f, "enum"),
            SymbolKind::Trait => write!(f, "trait"),
            SymbolKind::Impl => write!(f, "impl"),
            SymbolKind::Module => write!(f, "module"),
            SymbolKind::Import => write!(f, "import"),
            SymbolKind::Constant => write!(f, "constant"),
            SymbolKind::Variable => write!(f, "variable"),
        }
    }
}

/// A content-addressed snapshot of a file's symbols.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdomSnapshot {
    pub file_path: String,
    pub language: String,
    pub content_cid: Cid,
    pub symbols: Vec<Symbol>,
}

/// Detect language from file extension.
pub fn detect_language(path: &Path) -> Option<&'static str> {
    match path.extension()?.to_str()? {
        "rs" => Some("rust"),
        "py" => Some("python"),
        _ => None,
    }
}

/// Parse a source file and extract symbols.
pub fn parse_file(path: &Path) -> Result<Vec<Symbol>, CdomError> {
    let lang = detect_language(path)
        .ok_or_else(|| CdomError::UnsupportedLanguage(path.display().to_string()))?;
    let source = std::fs::read_to_string(path)?;
    parse_source(&source, lang)
}

/// Parse source code string and extract symbols.
pub fn parse_source(source: &str, language: &str) -> Result<Vec<Symbol>, CdomError> {
    let mut parser = tree_sitter::Parser::new();

    let ts_language = match language {
        "rust" => tree_sitter_rust::LANGUAGE,
        "python" => tree_sitter_python::LANGUAGE,
        _ => return Err(CdomError::UnsupportedLanguage(language.to_string())),
    };

    parser
        .set_language(&ts_language.into())
        .map_err(|e| CdomError::Parse(e.to_string()))?;

    let tree = parser
        .parse(source, None)
        .ok_or_else(|| CdomError::Parse("Failed to parse".to_string()))?;

    let mut symbols = Vec::new();
    extract_symbols(tree.root_node(), source, language, None, &mut symbols);
    Ok(symbols)
}

fn extract_symbols(
    node: tree_sitter::Node,
    source: &str,
    language: &str,
    parent: Option<&str>,
    symbols: &mut Vec<Symbol>,
) {
    let kind = node.kind();
    let symbol = match language {
        "rust" => extract_rust_symbol(kind, node, source, parent),
        "python" => extract_python_symbol(kind, node, source, parent),
        _ => None,
    };

    let current_name = symbol.as_ref().map(|s| s.name.clone());
    if let Some(sym) = symbol {
        symbols.push(sym);
    }

    let parent_name = current_name.as_deref().or(parent);
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        extract_symbols(child, source, language, parent_name, symbols);
    }
}

fn extract_rust_symbol(
    kind: &str,
    node: tree_sitter::Node,
    source: &str,
    parent: Option<&str>,
) -> Option<Symbol> {
    let (sym_kind, name_field) = match kind {
        "function_item" => (SymbolKind::Function, "name"),
        "struct_item" => (SymbolKind::Struct, "name"),
        "enum_item" => (SymbolKind::Enum, "name"),
        "trait_item" => (SymbolKind::Trait, "name"),
        "impl_item" => {
            // For impl blocks, get the type name
            if let Some(type_node) = node.child_by_field_name("type") {
                let name = node_text(type_node, source);
                return Some(Symbol {
                    name,
                    kind: SymbolKind::Impl,
                    start_line: node.start_position().row + 1,
                    end_line: node.end_position().row + 1,
                    parent: parent.map(String::from),
                });
            }
            return None;
        }
        "mod_item" => (SymbolKind::Module, "name"),
        "const_item" => (SymbolKind::Constant, "name"),
        "static_item" => (SymbolKind::Constant, "name"),
        _ => return None,
    };

    let name_node = node.child_by_field_name(name_field)?;
    let name = node_text(name_node, source);

    Some(Symbol {
        name,
        kind: sym_kind,
        start_line: node.start_position().row + 1,
        end_line: node.end_position().row + 1,
        parent: parent.map(String::from),
    })
}

fn extract_python_symbol(
    kind: &str,
    node: tree_sitter::Node,
    source: &str,
    parent: Option<&str>,
) -> Option<Symbol> {
    let (sym_kind, name_field) = match kind {
        "function_definition" => {
            let sk = if parent.is_some() {
                SymbolKind::Method
            } else {
                SymbolKind::Function
            };
            (sk, "name")
        }
        "class_definition" => (SymbolKind::Class, "name"),
        "import_statement" | "import_from_statement" => {
            let text = node_text(node, source);
            return Some(Symbol {
                name: text,
                kind: SymbolKind::Import,
                start_line: node.start_position().row + 1,
                end_line: node.end_position().row + 1,
                parent: parent.map(String::from),
            });
        }
        _ => return None,
    };

    let name_node = node.child_by_field_name(name_field)?;
    let name = node_text(name_node, source);

    Some(Symbol {
        name,
        kind: sym_kind,
        start_line: node.start_position().row + 1,
        end_line: node.end_position().row + 1,
        parent: parent.map(String::from),
    })
}

fn node_text(node: tree_sitter::Node, source: &str) -> String {
    source[node.byte_range()].to_string()
}

/// Scan a file, extract symbols, store snapshot in CAS.
pub fn scan_file(path: &Path, cas: &CasStore) -> Result<CdomSnapshot, CdomError> {
    let lang = detect_language(path)
        .ok_or_else(|| CdomError::UnsupportedLanguage(path.display().to_string()))?;
    let source = std::fs::read_to_string(path)?;

    let content_cid = cas.put(source.as_bytes())?;
    let symbols = parse_source(&source, lang)?;

    let snapshot = CdomSnapshot {
        file_path: path.display().to_string(),
        language: lang.to_string(),
        content_cid,
        symbols,
    };

    // Store the snapshot itself in CAS
    let snapshot_bytes = serde_json::to_vec(&snapshot)?;
    cas.put(&snapshot_bytes)?;

    Ok(snapshot)
}

/// Query symbols by name pattern (simple substring match).
pub fn query_symbols<'a>(symbols: &'a [Symbol], query: &str) -> Vec<&'a Symbol> {
    let query_lower = query.to_lowercase();
    symbols
        .iter()
        .filter(|s| s.name.to_lowercase().contains(&query_lower))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rust_source() {
        let source = r#"
fn hello() {}

struct Foo {
    bar: i32,
}

impl Foo {
    fn method(&self) {}
}

enum Color {
    Red,
    Green,
}
"#;
        let symbols = parse_source(source, "rust").unwrap();
        let names: Vec<&str> = symbols.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"hello"));
        assert!(names.contains(&"Foo"));
        assert!(names.contains(&"Color"));
    }

    #[test]
    fn parse_python_source() {
        let source = r#"
import os
from pathlib import Path

class MyClass:
    def __init__(self):
        pass

    def method(self):
        pass

def standalone():
    pass
"#;
        let symbols = parse_source(source, "python").unwrap();
        let names: Vec<&str> = symbols.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"MyClass"));
        assert!(names.contains(&"standalone"));
    }

    #[test]
    fn query_symbols_filters() {
        let symbols = vec![
            Symbol {
                name: "hello_world".to_string(),
                kind: SymbolKind::Function,
                start_line: 1,
                end_line: 3,
                parent: None,
            },
            Symbol {
                name: "goodbye".to_string(),
                kind: SymbolKind::Function,
                start_line: 5,
                end_line: 7,
                parent: None,
            },
        ];

        let results = query_symbols(&symbols, "hello");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "hello_world");
    }
}
