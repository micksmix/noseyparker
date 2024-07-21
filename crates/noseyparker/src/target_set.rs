use schemars::JsonSchema;
use serde::ser::SerializeSeq;
use std::collections::HashSet;
use std::path::PathBuf;

use crate::target::Target;

// XXX this could be reworked to use https://docs.rs/nonempty instead of handrolling that

/// A non-empty set of `Target` entries.
#[derive(Debug)]
pub struct TargetSet {
    target: Target,
    more_target: Vec<Target>,
}

/// Serialize `TargetSet` as a flat sequence
impl serde::Serialize for TargetSet {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.len()))?;
        for p in self.iter() {
            seq.serialize_element(p)?;
        }
        seq.end()
    }
}

impl JsonSchema for TargetSet {
    fn schema_name() -> String {
        "TargetSet".into()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let s = <Vec<Target>>::json_schema(gen);
        let mut o = s.into_object();
        o.array().min_items = Some(1);
        let md = o.metadata();
        md.description = Some("A non-empty set of `Target` entries".into());
        schemars::schema::Schema::Object(o)
    }
}

impl TargetSet {
    /// Create a new `TargetSet` from the given items, filtering out redundant less-specific
    /// `Target` records.
    pub fn new(target: Target, more_target: Vec<Target>) -> Self {
        let mut git_repos_with_detailed: HashSet<PathBuf> = HashSet::new();

        for p in std::iter::once(&target).chain(&more_target) {
            if let Target::GitRepo(e) = p {
                if e.first_commit.is_some() {
                    git_repos_with_detailed.insert(e.repo_path.clone());
                }
            }
        }

        let mut it = std::iter::once(target)
            .chain(more_target)
            .filter(|p| match p {
                Target::GitRepo(e) => {
                    e.first_commit.is_some() || !git_repos_with_detailed.contains(&e.repo_path)
                }
                Target::File(_) => true,
                Target::Extended(_) => true,
            });

        Self {
            target: it.next().unwrap(),
            more_target: it.collect(),
        }
    }

    #[inline]
    pub fn try_from_iter<I>(it: I) -> Option<Self>
    where
        I: IntoIterator<Item = Target>,
    {
        let mut it = it.into_iter();
        let target = it.next()?;
        let more_target = it.collect();
        Some(Self::new(target, more_target))
    }

    #[inline]
    pub fn first(&self) -> &Target {
        &self.target
    }

    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        1 + self.more_target.len()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Target> {
        std::iter::once(&self.target).chain(&self.more_target)
    }
}

impl IntoIterator for TargetSet {
    type Item = Target;
    type IntoIter =
        std::iter::Chain<std::iter::Once<Target>, <Vec<Target> as IntoIterator>::IntoIter>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self.target).chain(self.more_target)
    }
}
