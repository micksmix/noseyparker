use bstr::BString;
use bstring_serde::BStringLossyUtf8;
use input_enumerator::git_commit_metadata::CommitMetadata;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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
// sql
// -------------------------------------------------------------------------------------------------
mod sql {
    use super::*;

    use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
    use rusqlite::Error::ToSqlConversionFailure;

    impl ToSql for Target {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            match serde_json::to_string(self) {
                Err(e) => Err(ToSqlConversionFailure(e.into())),
                Ok(s) => Ok(s.into()),
            }
        }
    }

    impl FromSql for Target {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            let s = value.as_str()?;
            serde_json::from_str(s).map_err(|e| FromSqlError::Other(e.into()))
        }
    }
}
