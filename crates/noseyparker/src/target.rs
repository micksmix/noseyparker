use bstr::BString;
use bstring_serde::BStringLossyUtf8;
use input_enumerator::git_commit_metadata::CommitMetadata;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use polodb_core::bson::{Bson, Document};
use serde_json::Value;

fn commit_metadata_to_bson(commit_metadata: CommitMetadata) -> Bson {
    let json_value = serde_json::to_value(commit_metadata).expect("Serialization failed");
    serde_json_to_bson(json_value)
}

fn serde_json_to_bson(value: Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                Bson::Null
            }
        }
        Value::String(s) => Bson::String(s),
        Value::Array(arr) => Bson::Array(arr.into_iter().map(serde_json_to_bson).collect()),
        Value::Object(obj) => {
            let mut doc = Document::new();
            for (k, v) in obj {
                doc.insert(k, serde_json_to_bson(v));
            }
            Bson::Document(doc)
        }
    }
}
fn bson_to_serde_json(bson: Bson) -> serde_json::Value {
    match bson {
        Bson::Null => serde_json::Value::Null,
        Bson::Boolean(b) => serde_json::Value::Bool(b),
        Bson::Int32(i) => serde_json::Value::Number(i.into()),
        Bson::Int64(i) => serde_json::Value::Number(i.into()),
        Bson::Double(f) => serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap()),
        Bson::String(s) => serde_json::Value::String(s),
        Bson::Array(arr) => serde_json::Value::Array(arr.into_iter().map(bson_to_serde_json).collect()),
        Bson::Document(doc) => {
            let mut map = serde_json::Map::new();
            for (k, v) in doc {
                map.insert(k, bson_to_serde_json(v));
            }
            serde_json::Value::Object(map)
        }
        _ => panic!("Unexpected BSON type"),
    }
}



// -------------------------------------------------------------------------------------------------
// Target
// -------------------------------------------------------------------------------------------------
/// `Target` indicates where a particular blob or match was found when scanning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[allow(clippy::large_enum_variant)]
pub enum Target {
    File(FileTarget),
    GitRepo(GitRepoTarget),
    Extended(ExtendedTarget),
}

impl Target {
    /// Create a `Target` entry for a plain file.
    pub fn from_file(path: PathBuf) -> Self {
        Target::File(FileTarget { path })
    }

    /// Create a `Target` entry for a blob found within a Git repo's history, without any extra
    /// commit target.
    ///
    /// See also `from_git_repo_with_first_commit`.
    pub fn from_git_repo(repo_path: PathBuf) -> Self {
        Target::GitRepo(GitRepoTarget {
            repo_path,
            first_commit: None,
        })
    }

    /// Create a `Target` entry for a blob found within a Git repo's history, with commit
    /// target.
    ///
    /// See also `from_git_repo`.
    pub fn from_git_repo_with_first_commit(
        repo_path: PathBuf,
        commit_metadata: CommitMetadata,
        blob_path: BString,
    ) -> Self {
        let first_commit = Some(CommitTarget {
            commit_metadata,
            blob_path,
        });
        Target::GitRepo(GitRepoTarget {
            repo_path,
            first_commit,
        })
    }

    /// Create a `Target` entry from an arbitrary JSON value.
    pub fn from_extended(value: serde_json::Value) -> Self {
        Target::Extended(ExtendedTarget(value))
    }

    /// Get the path for the blob from this `Target` entry, if one is specified.
    pub fn blob_path(&self) -> Option<&Path> {
        use bstr::ByteSlice;
        match self {
            Self::File(e) => Some(&e.path),
            Self::GitRepo(e) => e
                .first_commit
                .as_ref()
                .and_then(|c| c.blob_path.to_path().ok()),
            Self::Extended(e) => e.path(),
        }
    }
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::File(e) => write!(f, "file {}", e.path.display()),
            Target::GitRepo(e) => match &e.first_commit {
                Some(md) => write!(
                    f,
                    "git repo {}: first seen in commit {} as {}",
                    e.repo_path.display(),
                    md.commit_metadata.commit_id,
                    md.blob_path,
                ),
                None => write!(f, "git repo {}", e.repo_path.display()),
            },
            Target::Extended(e) => {
                write!(f, "extended {}", e)
            }
        }
    }
}

impl FromStr for Target {
    type Err = String; // You can define a more specific error type if needed.

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Define your parsing logic here. Adjust as necessary to fit your enum variants.
        let parts: Vec<&str> = s.split(':').collect();
        match parts[0] {
            "file" => Ok(Target::from_file(PathBuf::from(parts.get(1).ok_or("Missing path")?))),
            "git_repo" => Ok(Target::from_git_repo(PathBuf::from(parts.get(1).ok_or("Missing repo path")?))),
            // Add parsing for `ExtendedTarget` if needed.
            _ => Err(format!("Invalid target: {}", s)),
        }
    }
}

// -------------------------------------------------------------------------------------------------
// FileTarget
// -------------------------------------------------------------------------------------------------
/// Indicates that a blob was seen at a particular file path
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FileTarget {
    pub path: PathBuf,
}

// -------------------------------------------------------------------------------------------------
// GitRepoTarget
// -------------------------------------------------------------------------------------------------
/// Indicates that a blob was seen in a Git repo, optionally with particular commit target info
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GitRepoTarget {
    pub repo_path: PathBuf,
    pub first_commit: Option<CommitTarget>,
}

// -------------------------------------------------------------------------------------------------
// CommitTarget
// -------------------------------------------------------------------------------------------------
/// How was a particular Git commit encountered?
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CommitTarget {
    pub commit_metadata: CommitMetadata,

    #[serde(with = "BStringLossyUtf8")]
    pub blob_path: BString,
}

// -------------------------------------------------------------------------------------------------
// ExtendedTarget
// -------------------------------------------------------------------------------------------------
/// An extended target entry.
///
/// This is an arbitrary JSON value.
/// If the value is an object containing certain fields, they will be interpreted specially by
/// Nosey Parker:
///
/// - A `path` field containing a string
//
// - XXX A `url` string field that is a syntactically-valid URL
// - XXX A `time` string field
// - XXX A `display` string field
//
// - XXX A `parent_blob` string field with a hex-encoded blob ID that the associated blob was derived from
// - XXX A `parent_transform` string field identifying the transform method used to derive the associated blob
// - XXX A `parent_start_byte` integer field
// - XXX A `parent_end_byte` integer field
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExtendedTarget(pub serde_json::Value);

impl std::fmt::Display for ExtendedTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl ExtendedTarget {
    pub fn path(&self) -> Option<&Path> {
        let p = self.0.get("path")?.as_str()?;
        Some(Path::new(p))
    }
}

// -------------------------------------------------------------------------------------------------
// bson
// -------------------------------------------------------------------------------------------------
mod bson {
    use super::*;
    use polodb_core::bson::{Bson, Document};

    impl From<Target> for Bson {
        fn from(target: Target) -> Self {
            Bson::Document(match target {
                Target::File(ft) => {
                    let mut doc = Document::new();
                    doc.insert("kind", Bson::String("file".to_string()));
                    doc.insert("path", Bson::String(ft.path.display().to_string()));
                    doc
                }
                Target::GitRepo(grt) => {
                    let mut doc = Document::new();
                    doc.insert("kind", Bson::String("git_repo".to_string()));
                    doc.insert("repo_path", Bson::String(grt.repo_path.display().to_string()));
                    if let Some(fc) = grt.first_commit {
                        let mut commit_doc = Document::new();
                        commit_doc.insert("commit_metadata", commit_metadata_to_bson(fc.commit_metadata));
                        commit_doc.insert("blob_path", Bson::String(fc.blob_path.to_string()));
                        doc.insert("first_commit", Bson::Document(commit_doc));
                    }
                    doc
                }
                Target::Extended(et) => {
                    let mut doc = Document::new();
                    doc.insert("kind", Bson::String("extended".to_string()));
                    doc.insert("value", serde_json_to_bson(et.0).as_document().cloned().unwrap());
                    doc
                }
            })
        }
    }
    
    impl From<Bson> for Target {
        fn from(bson: Bson) -> Self {
            let doc = bson.as_document().expect("Expected document");
            let kind = doc.get_str("kind").expect("Expected kind field");
            match kind {
                "file" => Target::File(FileTarget {
                    path: PathBuf::from(doc.get_str("path").expect("Expected path field")),
                }),
                "git_repo" => Target::GitRepo(GitRepoTarget {
                    repo_path: PathBuf::from(doc.get_str("repo_path").expect("Expected repo_path field")),
                    first_commit: doc.get_document("first_commit").ok().map(|commit_doc| {
                        let commit_metadata_bson = commit_doc.get("commit_metadata").unwrap().clone();
                        let commit_metadata_json = bson_to_serde_json(commit_metadata_bson);
                        CommitTarget {
                            commit_metadata: serde_json::from_value(commit_metadata_json).expect("Deserialization failed"),
                            blob_path: BString::from(commit_doc.get_str("blob_path").expect("Expected blob_path field").as_bytes()),
                        }
                    }),
                }),
                "extended" => Target::Extended(ExtendedTarget(Bson::Document(doc.clone()).into())),
                _ => panic!("Unknown target kind"),
            }
        }
    }
    

    impl From<Target> for Document {
        fn from(target: Target) -> Self {
            Bson::from(target).as_document().cloned().unwrap()
        }
    }

    impl From<Document> for Target {
        fn from(doc: Document) -> Self {
            Target::from(Bson::Document(doc))
        }
    }
}
