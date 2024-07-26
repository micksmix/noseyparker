use anyhow::{bail, Context, Result};
use bstr::BString;
use polodb_core::bson::{doc, Bson, Document};
use polodb_core::{Collection, Database};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, debug_span, info};

use crate::blob_id::BlobId; 
use crate::blob_metadata::BlobMetadata;
use crate::git_url::GitUrl;
use crate::location::{Location, OffsetSpan, SourcePoint, SourceSpan};
use crate::match_type::{Groups, Match};
use crate::target::Target;
use crate::target_set::TargetSet;
use crate::snippet::Snippet;

pub mod annotation;
pub mod finding_data;
pub mod finding_metadata;
pub mod finding_summary;
pub mod status;

pub use annotation::{Annotations, FindingAnnotation, MatchAnnotation};
pub use finding_data::{FindingData, FindingDataEntry};
pub use finding_metadata::FindingMetadata;
pub use finding_summary::{FindingSummary, FindingSummaryEntry};
pub use status::{Status, Statuses};

// const CURRENT_SCHEMA_VERSION: u64 = 60;

pub struct Datastore {
    pub db: Database,
    // pub root_dir: PathBuf,
}

impl Datastore {
    pub fn new_in_memory() -> Result<Self> {
        let db = Database::open_memory().context("Failed to open in-memory database")?;
        Ok(Datastore { db })
    }
    
    // pub fn create_or_open(root_dir: &Path) -> Result<Self> {
    //     debug!("Attempting to create or open an existing datastore at {}", root_dir.display());

    //     Self::create(root_dir).or_else(|e| {
    //         debug!("Failed to create datastore: {e:#}: will try to open existing datastore instead");
    //         Self::open(root_dir)
    //     })
    // }

    // pub fn open(root_dir: &Path) -> Result<Self> {
    //     debug!("Attempting to open existing datastore at {}", root_dir.display());
        
    //     let db = Database::open_file(root_dir.join("datastore.polo").to_str().unwrap()).context("Failed to open database")?;
    //     let ds = Self { db, root_dir: root_dir.to_path_buf() };

    //     let scratch_dir = ds.scratch_dir();
    //     std::fs::create_dir_all(&scratch_dir).with_context(|| {
    //         format!("Failed to create scratch directory {}", scratch_dir.display())
    //     })?;

    //     let clones_dir = ds.clones_dir();
    //     std::fs::create_dir_all(&clones_dir).with_context(|| {
    //         format!("Failed to create clones directory {}", clones_dir.display())
    //     })?;

    //     let blobs_dir = ds.blobs_dir();
    //     std::fs::create_dir_all(&blobs_dir).with_context(|| {
    //         format!("Failed to create blobs directory {}", blobs_dir.display())
    //     })?;

    //     Ok(ds)
    // }

    // pub fn create(root_dir: &Path) -> Result<Self> {
    //     debug!("Attempting to create new datastore at {}", root_dir.display());

    //     std::fs::create_dir(root_dir).with_context(|| {
    //         format!("Failed to create datastore root directory at {}", root_dir.display())
    //     })?;

    //     std::fs::write(root_dir.join(".gitignore"), "*\n").with_context(|| {
    //         format!("Failed to write .gitignore to datastore at {}", root_dir.display())
    //     })?;

    //     let db = Database::open_file(root_dir.join("datastore.polo").to_str().unwrap()).context("Failed to open database")?;
    //     let ds = Self { db, root_dir: root_dir.to_path_buf() };

    //     Ok(ds)
    // }

    // pub fn scratch_dir(&self) -> PathBuf {
    //     self.root_dir.join("scratch")
    // }

    // pub fn clones_dir(&self) -> PathBuf {
    //     self.root_dir.join("clones")
    // }

    // pub fn blobs_dir(&self) -> PathBuf {
    //     self.root_dir.join("blobs")
    // }

    // pub fn root_dir(&self) -> &Path {
    //     &self.root_dir
    // }

    // pub fn clone_destination(&self, repo: &GitUrl) -> Result<PathBuf> {
    //     clone_destination(&self.clones_dir(), repo)
    // }

    pub fn analyze(&self) -> Result<()> {
        let _span = debug_span!("Datastore::analyze");
        
        let matches_collection: Collection<Document> = self.db.collection("matches");
        let findings_collection: Collection<Document> = self.db.collection("findings");
        
        // Clear existing findings
        findings_collection.delete_many(doc! {})?;
    
        // Retrieve all matches
        let matches_cursor = matches_collection.find(doc! {})?;
    
        // Use a HashMap to group matches by rule and finding
        let mut findings_map: HashMap<(String, String, String), Vec<Document>> = HashMap::new();
    
        for match_doc in matches_cursor {
            let doc = match_doc?;
            let match_data = doc.get_document("match").context("'match' field not present")?;
            let rule_name = match_data.get_str("rule_name").unwrap_or("Unknown Rule");
            let rule_text_id = match_data.get_str("rule_text_id").unwrap_or("");
            let rule_structural_id = match_data.get_str("rule_structural_id").unwrap_or("");
            let finding_id = match_data.get_str("finding_id").unwrap_or(""); //todo: this is always empty
    
            let key = (rule_name.to_string(), rule_structural_id.to_string(), finding_id.to_string());
            findings_map.entry(key).or_insert_with(Vec::new).push(doc);
        }
    
        // Create findings from grouped matches
        for ((rule_name, rule_structural_id, finding_id), matches) in findings_map {
            let count = matches.len() as i64;
            let finding_doc = doc! {
                "rule_name": rule_name,
                "rule_structural_id": rule_structural_id,
                "finding_id": finding_id,
                "count": count,
                "matches": matches,
                "accept_count": 0i64,
                "reject_count": 0i64,
                "mixed_count": 0i64,
                "unlabeled_count": count
            };
    
            findings_collection.insert_one(finding_doc)?;
        }
    
        Ok(())
    }

    pub fn begin(&self) -> Result<()> {
        let _span = debug_span!("Datastore::begin").entered();
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
        let _span = debug_span!("Datastore::get_summary").entered();
        let collection: Collection<Document> = self.db.collection("findings");
    
        let cursor = collection.find(doc! {})?;
        let mut summary_map: HashMap<String, FindingSummaryEntry> = HashMap::new();
    
        for result in cursor {
            let doc = result?;
            let rule_name = doc.get_str("rule_name")?.to_string();
            let count = doc.get_i64("count")? as usize;
            let accept_count = doc.get_i64("accept_count")? as usize;
            let reject_count = doc.get_i64("reject_count")? as usize;
            let mixed_count = doc.get_i64("mixed_count")? as usize;
            let unlabeled_count = doc.get_i64("unlabeled_count")? as usize;
    
            summary_map.entry(rule_name.clone()).and_modify(|e| {
                e.distinct_count += 1;
                e.total_count += count;
                e.accept_count += accept_count;
                e.reject_count += reject_count;
                e.mixed_count += mixed_count;
                e.unlabeled_count += unlabeled_count;
            }).or_insert(FindingSummaryEntry {
                rule_name,
                distinct_count: 1,
                total_count: count,
                accept_count,
                reject_count,
                mixed_count,
                unlabeled_count,
            });
        }
    
        Ok(FindingSummary(summary_map.into_values().collect()))
    }

    pub fn get_annotations(&self) -> Result<Annotations> {
        let _span = debug_span!("Datastore::get_annotations").entered();

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
                    if doc.get_str("comment")? == fa.comment.as_str() {
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
                        "comment": fa.comment.as_str()
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
                        "blob_id": Bson::try_from(ma.blob_id.clone()).expect("Failed to convert BlobId to Bson"),
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
            "{} findings and {} matches in datastore",
            self.get_num_findings()?,
            self.get_num_matches()?
        );
        info!("Finding comment annotations: {}", finding_comment_stats);
        info!("Match comment annotations: {}", match_comment_stats);

        Ok(())
    }

    pub fn get_finding_metadata(&self) -> Result<Vec<FindingMetadata>> {
        let _span = debug_span!("Datastore::get_finding_metadata").entered();
    
        let collection: Collection<Document> = self.db.collection("findings");
    
        let count = collection.count_documents()?;
        debug!("Number of documents in 'findings' collection: {}", count);
    
        let matches_collection: Collection<Document> = self.db.collection("matches");
        let matches_count = matches_collection.count_documents()?;
        debug!("Number of documents in 'matches' collection: {}", matches_count);
    
        let cursor = collection.find(doc! {})?;
        let mut entries = vec![];
    
        for result in cursor {
            let doc = result?;
            let entry = FindingMetadata {
                finding_id: doc.get_str("finding_id")?.to_string(),
                groups: doc.get_array("groups")
                    .map(|arr| Groups::from(arr.clone()))
                    .unwrap_or_else(|_| Groups::default()),
                rule_structural_id: doc.get_str("rule_structural_id").unwrap_or("").to_string(),
                rule_text_id: doc.get_str("rule_text_id").unwrap_or("").to_string(),
                rule_name: doc.get_str("rule_name").unwrap_or("").to_string(),
                num_matches: doc.get_i64("num_matches").unwrap_or(0) as usize,
                comment: doc.get_str("comment").map(|s| s.to_string()).ok(),
                statuses: doc.get_array("statuses")
                    .map(|arr| Statuses::from(arr.clone()))
                    .unwrap_or_else(|_| Statuses::default()),
                mean_score: doc.get_f64("mean_score").ok(),
            };
            entries.push(entry);
        }
    
        Ok(entries)
    }

    // pub fn get_finding_data(
    //     &self,
    //     metadata: &FindingMetadata,
    // ) -> Result<FindingData> {
    //     let _span = debug_span!("Datastore::get_finding_data").entered();
    
    //     let collection: Collection<Document> = self.db.collection("matches");
    
    //     let groups_query = if metadata.groups.0.is_empty() {
    //         doc! {
    //             "rule_structural_id": &metadata.rule_structural_id,
    //         }
    //     } else {
    //         doc! {
    //             "rule_structural_id": &metadata.rule_structural_id,
    //             "groups": {
    //                 "$in": metadata.groups.0.iter().map(|g| Bson::String(String::from_utf8_lossy(&g.0).into_owned())).collect::<Vec<Bson>>()
    //             }
    //         }
    //     };
    
    //     let cursor = collection.find(groups_query)?;
    
    //     let mut entries = vec![];
    //     for result in cursor {
    //         let doc = result?;
    //         let m = Match {
    //             blob_id: BlobId::from_hex(doc.get_str("blob_id")?).expect("Invalid BlobId hex string"),
    //             location: Location {
    //                 offset_span: OffsetSpan {
    //                     start: doc.get_i64("start_byte")? as usize,
    //                     end: doc.get_i64("end_byte")? as usize,
    //                 },
    //                 source_span: SourceSpan {
    //                     start: SourcePoint {
    //                         line: doc.get_i64("start_line")? as usize,
    //                         column: doc.get_i64("start_column")? as usize,
    //                     },
    //                     end: SourcePoint {
    //                         line: doc.get_i64("end_line")? as usize,
    //                         column: doc.get_i64("end_column")? as usize,
    //                     },
    //                 },
    //             },
    //             snippet: Snippet {
    //                 before: BString::new(doc.get_binary_generic("snippet_before")?.to_vec()),
    //                 matching: BString::new(doc.get_binary_generic("snippet_matching")?.to_vec()),
    //                 after: BString::new(doc.get_binary_generic("snippet_after")?.to_vec()),
    //             },
    //             groups: Groups::from(doc.get_array("groups")?.clone()),
    //             rule_structural_id: doc.get_str("rule_structural_id")?.to_string(),
    //             rule_name: metadata.rule_name.clone(),
    //             rule_text_id: metadata.rule_text_id.clone(),
    //             structural_id: doc.get_str("structural_id")?.to_string(),
    //         };
    
    //         let blob_metadata = BlobMetadata {
    //             id: BlobId::from_hex(doc.get_str("blob_id")?).expect("Invalid BlobId hex string"),
    //             num_bytes: doc.get_i64("num_bytes")? as usize,
    //             mime_essence: doc.get_str("mime_essence").map(|s| s.to_string()).ok(),
    //             charset: doc.get_str("charset").map(|s| s.to_string()).ok(),
    //         };
    
    //         let entry = FindingDataEntry {
    //             target: self.get_target_set(&blob_metadata)?,
    //             blob_metadata,
    //             match_id: doc.get_str("match_id")?.to_string(),
    //             match_val: m,
    //             match_comment: doc.get_str("comment").map(|s| s.to_string()).ok(),
    //             match_score: doc.get_f64("score").ok(),
    //             match_status: match doc.get_str("status") {
    //                 Ok(s) => Some(Status::from_str(s).expect("Invalid status")),
    //                 Err(_) => None,
    //             },
    //         };
    
    //         entries.push(entry);
    //     }
    
    //     Ok(entries)
    // }
    
    // fn get_target_set(&self, metadata: &BlobMetadata) -> Result<TargetSet> {
    //     let collection: Collection<Document> = self.db.collection("blob_targets");
    //     let cursor = collection.find(doc! { "blob_id": Bson::try_from(metadata.id).unwrap_or_else(|e| panic!("Failed to convert BlobId to Bson: {}", e)) })?;
    //     let mut targets = vec![];

    //     for result in cursor {
    //         let doc = result?;
    //         let target_str = doc.get_str("target")?;
    //         let target = Target::from_str(target_str).expect("Invalid Target");
    //         targets.push(target);
    //     }

    //     match TargetSet::try_from_iter(targets) {
    //         Some(ts) => Ok(ts),
    //         None => bail!("should have at least 1 target entry"),
    //     }
    // }

    pub fn get_finding_data(
        &self,
        metadata: &FindingMetadata,
        limit: Option<usize>,
    ) -> Result<FindingData> {
        let _span = debug_span!("Datastore::get_finding_data").entered();
    
        let collection: Collection<Document> = self.db.collection("matches");
    
        let groups_query = if metadata.groups.0.is_empty() {
            doc! {
                "rule_structural_id": &metadata.rule_structural_id,
            }
        } else {
            doc! {
                "rule_structural_id": &metadata.rule_structural_id,
                "groups": {
                    "$in": metadata.groups.0.iter().map(|g| Bson::String(String::from_utf8_lossy(&g.0).into_owned())).collect::<Vec<Bson>>()
                }
            }
        };
    
        let cursor = collection.find(groups_query)?;
    
        let match_limit = limit.unwrap_or(usize::MAX);
    
        let mut entries = vec![];
        for result in cursor.take(match_limit) {
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
                match_id: doc.get_str("match_id")?.to_string(),
                match_val: m,
                match_comment: doc.get_str("comment").map(|s| s.to_string()).ok(),
                match_score: doc.get_f64("score").ok(),
                match_status: match doc.get_str("status") {
                    Ok(s) => Some(Status::from_str(s).expect("Invalid status")),
                    Err(_) => None,
                },
            };
    
            entries.push(entry);
        }
    
        Ok(entries)  // Return the Vec<FindingDataEntry> directly
    }
    
    fn get_target_set(&self, metadata: &BlobMetadata) -> Result<TargetSet> {
        let collection: Collection<Document> = self.db.collection("blob_targets");
        let cursor = collection.find(doc! { "blob_id": Bson::try_from(metadata.id).unwrap_or_else(|e| panic!("Failed to convert BlobId to Bson: {}", e)) })?;
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
}    
// fn clone_destination(root: &std::path::Path, repo: &GitUrl) -> Result<std::path::PathBuf> {
//     Ok(root.join(repo.to_path_buf()))
// }
