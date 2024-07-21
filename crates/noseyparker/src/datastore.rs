use anyhow::{bail, Context, Result};
use bstr::BString;
use polodb_core::bson::{doc, Bson, Document};
use polodb_core::{Collection, Database};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::{debug, debug_span, info};

use crate::blob_id::BlobId;
use crate::blob_metadata::BlobMetadata;
use crate::git_url::GitUrl;
use crate::location::{Location, OffsetSpan, SourcePoint, SourceSpan};
use crate::match_type::{Groups, Match};
use crate::target::Target;
use crate::target_set::TargetSet;
use crate::snippet::Snippet;
use crate::datastore::status::{Status, Statuses};

pub mod annotation;
pub mod finding_data;
pub mod finding_metadata;
pub mod finding_summary;
pub mod status;

pub use annotation::{Annotations, FindingAnnotation, MatchAnnotation};
pub use finding_data::{FindingData, FindingDataEntry};
pub use finding_metadata::FindingMetadata;
pub use finding_summary::{FindingSummary, FindingSummaryEntry};

pub struct Datastore {
    root_dir: PathBuf,
    db: Database,
}

impl Datastore {
    pub fn create_or_open(root_dir: &Path, cache_size: i64) -> Result<Self> {
        debug!("Attempting to create or open an existing datastore at {}", root_dir.display());

        Self::create(root_dir, cache_size).or_else(|e| {
            debug!(
                "Failed to create datastore: {e:#}: will try to open existing datastore instead"
            );
            Self::open(root_dir, cache_size)
        })
    }

    pub fn create(root_dir: &Path, _cache_size: i64) -> Result<Self> {
        debug!("Attempting to create new datastore at {}", root_dir.display());

        std::fs::create_dir_all(root_dir).with_context(|| {
            format!("Failed to create datastore root directory at {}", root_dir.display())
        })?;

        std::fs::write(root_dir.join(".gitignore"), "*\n").with_context(|| {
            format!("Failed to write .gitignore to datastore at {}", root_dir.display())
        })?;

        let db_path = root_dir.join("datastore.pdb");
        let db = Database::open_file(&db_path)?;

        Ok(Datastore { root_dir: root_dir.to_path_buf(), db })
    }

    pub fn open(root_dir: &Path, _cache_size: i64) -> Result<Self> {
        debug!("Attempting to open existing datastore at {}", root_dir.display());

        let db_path = root_dir.join("datastore.pdb");
        let db = Database::open_file(&db_path)?;

        Ok(Datastore { root_dir: root_dir.to_path_buf(), db })
    }

    pub fn scratch_dir(&self) -> PathBuf {
        self.root_dir.join("scratch")
    }

    pub fn clones_dir(&self) -> PathBuf {
        self.root_dir.join("clones")
    }

    pub fn blobs_dir(&self) -> PathBuf {
        self.root_dir.join("blobs")
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn clone_destination(&self, repo: &GitUrl) -> Result<PathBuf> {
        clone_destination(&self.clones_dir(), repo)
    }

    pub fn analyze(&self) -> Result<()> {
        let _span = debug_span!("Datastore::analyze", "{}", self.root_dir.display()).entered();
        Ok(())
    }

    pub fn begin(&self) -> Result<()> {
        let _span = debug_span!("Datastore::begin", "{}", self.root_dir.display()).entered();
        Ok(())
    }

    pub fn get_num_matches(&self) -> Result<u64> {
        let collection: Collection<Document> = self.db.collection("matches");
        let count = collection.count_documents()?;
        Ok(count)
    }

    pub fn get_num_findings(&self) -> Result<u64> {
        let collection: Collection<Document> = self.db.collection("findings");
        let count = collection.count_documents()?;
        Ok(count)
    }

    pub fn get_summary(&self) -> Result<FindingSummary> {
        let _span = debug_span!("Datastore::get_summary", "{}", self.root_dir.display()).entered();
        let collection: Collection<Document> = self.db.collection("findings");

        let cursor = collection.find(doc! {})?;
        let mut entries = vec![];

        for result in cursor {
            let doc = result?;
            let entry = FindingSummaryEntry {
                rule_name: doc.get_str("rule_name")?.to_string(),
                distinct_count: doc.get_i64("distinct_count")? as usize,
                total_count: doc.get_i64("total_count")? as usize,
                accept_count: doc.get_i64("accept_count")? as usize,
                reject_count: doc.get_i64("reject_count")? as usize,
                mixed_count: doc.get_i64("mixed_count")? as usize,
                unlabeled_count: doc.get_i64("unlabeled_count")? as usize,
            };
            entries.push(entry);
        }

        Ok(FindingSummary(entries))
    }

    pub fn get_annotations(&self) -> Result<Annotations> {
        let _span = debug_span!("Datastore::get_annotations", "{}", self.root_dir.display()).entered();

        let collection: Collection<Document> = self.db.collection("annotations");
        let cursor = collection.find(doc! {})?;
        let mut match_annotations = vec![];
        let mut finding_annotations = vec![];

        for result in cursor {
            let doc = result?;
            if let Some(comment) = doc.get_str("comment").ok() {
                if doc.contains_key("structural_id") {
                    match_annotations.push(MatchAnnotation {
                        finding_id: doc.get_str("finding_id")?.to_string(),
                        rule_name: doc.get_str("rule_name")?.to_string(),
                        rule_text_id: doc.get_str("rule_text_id")?.to_string(),
                        rule_structural_id: doc.get_str("rule_structural_id")?.to_string(),
                        match_id: doc.get_str("structural_id")?.to_string(),
                        blob_id: BlobId::from_hex(doc.get_str("blob_id")?).expect("Invalid BlobId hex string"),
                        start_byte: doc.get_i64("start_byte")? as usize,
                        end_byte: doc.get_i64("end_byte")? as usize,
                        groups: Groups::from(doc.get_array("groups")?.clone()),
                        status: match doc.get_str("status") {
                            Ok(s) => Some(Status::from_str(s).expect("Invalid status")),
                            Err(_) => None,
                        },
                        comment: Some(comment.to_string()),
                    });
                } else {
                    finding_annotations.push(FindingAnnotation {
                        finding_id: doc.get_str("finding_id")?.to_string(),
                        rule_name: doc.get_str("rule_name")?.to_string(),
                        rule_text_id: doc.get_str("rule_text_id")?.to_string(),
                        rule_structural_id: doc.get_str("rule_structural_id")?.to_string(),
                        groups: Groups::from(doc.get_array("groups")?.clone()),
                        comment: comment.to_string(),
                    });
                }
            }
        }

        Ok(Annotations {
            match_annotations,
            finding_annotations,
        })
    }
    pub fn import_annotations(&mut self, annotations: &Annotations) -> Result<()> {
        #[derive(Default, Debug)]
        struct Stats {
            n_imported: usize,
            n_conflicting: usize,
            n_existing: usize,
            n_missing: usize,
        }
    
        impl std::fmt::Display for Stats {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{} existing; {} missing; {} conflicting; {} imported",
                    self.n_existing, self.n_missing, self.n_conflicting, self.n_imported
                )
            }
        }
    
        let collection: Collection<Document> = self.db.collection("annotations");
        let mut finding_comment_stats = Stats::default();
        let mut match_comment_stats = Stats::default();
        let mut match_status_stats = Stats::default();
    
        for fa in annotations.finding_annotations.iter() {
            let existing = collection.find_one(doc! { "finding_id": fa.finding_id.clone() })?;
            match existing {
                Some(doc) => {
                    if doc.get_str("comment")? == fa.comment.as_str(){//}.unwrap_or("") {
                        finding_comment_stats.n_existing += 1;
                    } else {
                        finding_comment_stats.n_conflicting += 1;
                    }
                }
                None => {
                    collection.insert_one(doc! {
                        "finding_id": fa.finding_id.clone(),
                        "rule_name": &fa.rule_name,
                        "rule_text_id": &fa.rule_text_id,
                        "rule_structural_id": fa.rule_structural_id.clone(),
                        "groups": Bson::from(fa.groups.clone()),
                        "comment": fa.comment.as_str()//.unwrap_or("")
                    })?;
                    finding_comment_stats.n_imported += 1;
                }
            }
        }
    
        for ma in annotations.match_annotations.iter() {
            let existing = collection.find_one(doc! { "match_id": ma.match_id.clone() })?;
            match existing {
                Some(doc) => {
                    if doc.get_str("comment")? == ma.comment.as_deref().unwrap_or("") {
                        match_comment_stats.n_existing += 1;
                    } else {
                        match_comment_stats.n_conflicting += 1;
                    }
                }
                None => {
                    collection.insert_one(doc! {
                        "finding_id": ma.finding_id.clone(),
                        "rule_name": &ma.rule_name,
                        "rule_text_id": &ma.rule_text_id,
                        "rule_structural_id": ma.rule_structural_id.clone(),
                        "match_id": ma.match_id.clone(),
                        "blob_id": Bson::from(ma.blob_id.clone()),
                        "start_byte": ma.start_byte as i64,
                        "end_byte": ma.end_byte as i64,
                        "groups": Bson::from(ma.groups.clone()),
                        "status": ma.status.as_ref().map(|s| s.to_string()),
                        "comment": ma.comment.as_deref().unwrap_or("")
                    })?;
                    match_comment_stats.n_imported += 1;
                }
            }
        }
    
        info!(
            "{} findings and {} matches in datastore at {}",
            self.get_num_findings()?,
            self.get_num_matches()?,
            self.root_dir.display()
        );
        info!("Finding comment annotations: {}", finding_comment_stats);
        info!("Match comment annotations: {}", match_comment_stats);
    
        Ok(())
    }
    

    pub fn get_finding_metadata(&self) -> Result<Vec<FindingMetadata>> {
        let _span = debug_span!("Datastore::get_finding_metadata", "{}", self.root_dir.display()).entered();

        let collection: Collection<Document> = self.db.collection("findings");
        let cursor = collection.find(doc! {})?;
        let mut entries = vec![];

        for result in cursor {
            let doc = result?;
            let entry = FindingMetadata {
                finding_id: doc.get_str("finding_id")?.to_string(),
                groups: Groups::from(doc.get_array("groups")?.clone()),
                rule_structural_id: doc.get_str("rule_structural_id")?.to_string(),
                rule_text_id: doc.get_str("rule_text_id")?.to_string(),
                rule_name: doc.get_str("rule_name")?.to_string(),
                num_matches: doc.get_i64("num_matches")? as usize,
                comment: doc.get_str("comment").map(|s| s.to_string()).ok(),
                statuses: Statuses::from(doc.get_array("statuses")?.clone()),
                mean_score: doc.get_f64("mean_score").ok(),
            };
            entries.push(entry);
        }

        Ok(entries)
    }

    pub fn get_finding_data(
        &self,
        metadata: &FindingMetadata,
        limit: Option<usize>,
    ) -> Result<FindingData> {
        let _span = debug_span!("Datastore::get_finding_data", "{}", self.root_dir.display()).entered();

        let collection: Collection<Document> = self.db.collection("matches");
        let cursor = collection.find(doc! {
            "groups": Bson::from(metadata.groups.clone()),
            "rule_structural_id": metadata.rule_structural_id
        })?;

        let mut entries = vec![];
        for result in cursor {
            let doc = result?;
            let m = Match {
                blob_id: BlobId::from_hex(doc.get_str("blob_id")?).expect("Invalid BlobId hex string"),
                location: Location {
                    offset_span: OffsetSpan {
                        start: doc.get_i64("start_byte")? as usize,
                        end: doc.get_i64("end_byte")? as usize,
                    },
                    source_span: SourceSpan {
                        start: SourcePoint {
                            line: doc.get_i64("start_line")? as usize,
                            column: doc.get_i64("start_column")? as usize,
                        },
                        end: SourcePoint {
                            line: doc.get_i64("end_line")? as usize,
                            column: doc.get_i64("end_column")? as usize,
                        },
                    },
                },
                snippet: Snippet {
                    before: BString::new(doc.get_binary_generic("snippet_before")?.to_vec()),
                    matching: BString::new(doc.get_binary_generic("snippet_matching")?.to_vec()),
                    after: BString::new(doc.get_binary_generic("snippet_after")?.to_vec()),
                },
                groups: Groups::from(doc.get_array("groups")?.clone()),
                rule_structural_id: doc.get_str("rule_structural_id")?.to_string(),
                rule_name: metadata.rule_name.clone(),
                rule_text_id: metadata.rule_text_id.clone(),
                structural_id: doc.get_str("structural_id")?.to_string(),
            };

            let blob_metadata = BlobMetadata {
                id: BlobId::from_hex(doc.get_str("blob_id")?).expect("Invalid BlobId hex string"),
                num_bytes: doc.get_i64("num_bytes")? as usize,
                mime_essence: doc.get_str("mime_essence").map(|s| s.to_string()).ok(),
                charset: doc.get_str("charset").map(|s| s.to_string()).ok(),
            };

            let entry = FindingDataEntry {
                target: self.get_target_set(&blob_metadata)?,
                blob_metadata,
                match_id: doc.get_i64("match_id")?,
                match_val: m,
                match_comment: doc.get_str("comment").map(|s| s.to_string()).ok(),
                match_score: doc.get_f64("score").ok(),
                // match_status: doc.get_str("status").map(|s| Status::from_str(s)).transpose()?,
                match_status: match doc.get_str("status") {
                    Ok(s) => Some(Status::from_str(s).expect("Invalid status")),
                    Err(_) => None,
                },
                
            };

            entries.push(entry);
        }

        Ok(entries)
    }

    fn get_target_set(&self, metadata: &BlobMetadata) -> Result<TargetSet> {
        let collection: Collection<Document> = self.db.collection("blob_targets");
        let cursor = collection.find(doc! { "blob_id": Bson::from(metadata.id) })?;
        let mut targets = vec![];
    
        for result in cursor {
            let doc = result?;
            let target_str = doc.get_str("target")?;
            let target = Target::from_str(target_str).expect("Invalid Target");
            targets.push(target);
        }
    
        match TargetSet::try_from_iter(targets) {
            Some(ts) => Ok(ts),
            None => bail!("should have at least 1 target entry"),
        }
    }
    
    
    // fn get_target_set(&self, metadata: &BlobMetadata) -> Result<TargetSet> {
    //     let collection: Collection<Document> = self.db.collection("blob_targets");
    //     let cursor = collection.find(doc! { "blob_id": Bson::from(metadata.id) })?;
    //     let mut targets = vec![];

    //     for result in cursor {
    //         let doc = result?;
    //         targets.push(Target::from_str(doc.get_str("target")?).expect("Invalid Target"));
    //     }

    //     match TargetSet::try_from_iter(targets) {
    //         Some(ts) => Ok(ts),
    //         None => bail!("should have at least 1 target entry"),
    //     }
    // }
}

fn clone_destination(root: &std::path::Path, repo: &GitUrl) -> Result<PathBuf> {
    Ok(root.join(repo.to_path_buf()))
}

#[cfg(test)]
mod test {
    macro_rules! clone_destination_success_tests {
        ($($case_name:ident: ($root:expr, $repo:expr) => $expected:expr,)*) => {
            mod clone_destination {
                use crate::git_url::GitUrl;
                use pretty_assertions::assert_eq;
                use std::path::{PathBuf, Path};
                use std::str::FromStr;
                use super::super::clone_destination;

                $(
                    #[test]
                    fn $case_name() {
                        let expected: Option<PathBuf> = Some(Path::new($expected).to_owned());

                        let root = Path::new($root);
                        let repo = GitUrl::from_str($repo).expect("repo should be a URL");
                        assert_eq!(clone_destination(root, &repo).ok(), expected);
                    }
                )*
            }
        }
    }

    clone_destination_success_tests! {
        https_01: ("rel_root", "https://example.com/testrepo.git") => "rel_root/https/example.com/testrepo.git",
        https_02: ("/abs_root", "https://example.com/testrepo.git") => "/abs_root/https/example.com/testrepo.git",
    }
}
