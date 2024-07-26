--------------------------------------------------------------------------------
-- blobs
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS blob
-- This table records basic metadata about blobs.
(
    -- An arbitrary integer identifier for the blob
    id INTEGER PRIMARY KEY,

    -- The blob hash, computed a la Git, i.e., a hex digest of a fancy SHA-1 hash
    blob_id TEXT UNIQUE NOT NULL,

    -- Size of the blob in bytes
    size INTEGER NOT NULL,

    CONSTRAINT valid_id CHECK(
        length(blob_id) == 40 AND NOT glob('*[^abcdefABCDEF1234567890]*', blob_id)
    ),

    CONSTRAINT valid_size CHECK(0 <= size)
) STRICT;

CREATE TABLE IF NOT EXISTS blob_mime_essence
-- This table records mime type metadata about blobs.
(
    -- The integer identifier of the blob
    blob_id INTEGER PRIMARY KEY REFERENCES blob(id),

    -- Guessed mime type of the blob
    mime_essence TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS blob_charset
-- This table records charset metadata about blobs.
(
    -- The integer identifier of the blob
    blob_id INTEGER PRIMARY KEY REFERENCES blob(id),

    -- Guessed charset encoding of the blob
    charset TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS blob_source_span
-- This table represents source span-based location information for ranges within blobs.
-- This allows you to look up line and column information given a (start byte, end byte) range.
(
    blob_id INTEGER NOT NULL REFERENCES blob(id),
    start_byte INTEGER NOT NULL,
    end_byte INTEGER NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL,

    UNIQUE(blob_id, start_byte, end_byte),

    CONSTRAINT valid_offsets CHECK(0 <= start_byte AND start_byte <= end_byte),

    CONSTRAINT valid_span CHECK(0 <= start_line
        AND start_line <= end_line
        AND 0 <= start_column
        AND 0 <= end_column
    )
) STRICT;

CREATE TABLE IF NOT EXISTS blob_provenance
-- This table records the various ways in which blobs were encountered.
-- A blob can be encountered multiple ways when scanning; this table records all of them.
(
    -- The integer identifier of the blob
    blob_id INTEGER NOT NULL REFERENCES blob(id),

    -- The minified JSON-formatted provenance information
    -- XXX: deduplicate these values via another table?
    -- XXX: allow recursive representation of provenance values? I.e., structural decomposition and sharing, like `git repo` -> `commit` -> `blob path`?
    -- XXX: define special JSON object fields that will be handled specially by NP? E.g., `path`, `repo_path`, ...?
    provenance TEXT NOT NULL,

    UNIQUE(blob_id, provenance),

    CONSTRAINT payload_valid CHECK(json_type(provenance) = 'object')
) STRICT;

--------------------------------------------------------------------------------
-- rules
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rule
-- This table records rules used for detection.
(
    -- An arbitrary integer identifier for the rule
    id INTEGER PRIMARY KEY,

    -- The human-readable name of the rule
    name TEXT NOT NULL,

    -- The textual identifier defined in the rule
    text_id TEXT NOT NULL,

    -- A content-based identifier, defined as the hex-encoded sha1 hash of the pattern.
    structural_id TEXT UNIQUE NOT NULL,

    -- The minified JSON serialization of the rule
    syntax TEXT NOT NULL,

    CONSTRAINT json_syntax_valid CHECK(json_type(syntax) = 'object')
) STRICT;

--------------------------------------------------------------------------------
-- snippets
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS snippet
-- This table represents contextual snippets in a deduplicated way.
--
-- Deduplication of snippets reduces the size of large datastores 20-100x or more.
-- Keeping them in a separate table also makes it possible to update _just_ the
-- snippets of matches when scanning using a larger context window.
(
    -- An arbitrary integer identifier for the snippet
    id INTEGER PRIMARY KEY,

    -- The snippet content
    snippet BLOB UNIQUE NOT NULL
) STRICT;

--------------------------------------------------------------------------------
-- findings
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS finding
-- This table represents findings.
--
-- A finding is defined as a group of matches that have the same rule and groups.
-- Each finding is assigned a content-based identifier that is computed from
-- its rule and groups:
--
-- sha1_hex(rule structural identifier + '\0' + minified JSON array of base64-encoded groups)
(
    -- An arbitrary integer identifier for the match
    id INTEGER PRIMARY KEY,

    finding_id TEXT UNIQUE NOT NULL,

    -- The rule that produced this finding
    rule_id INTEGER NOT NULL REFERENCES rule(id),

    -- The capture groups, encoded as a minified JSON array of base64-encoded bytestrings
    groups TEXT NOT NULL,

    CONSTRAINT valid_id CHECK(
        length(finding_id) == 40 AND NOT glob('*[^abcdefABCDEF1234567890]*', finding_id)
    ),

    CONSTRAINT valid_groups CHECK(json_type(groups) = 'array'),

    UNIQUE(rule_id, groups)
) STRICT;

--------------------------------------------------------------------------------
-- matches
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match
-- This table represents the matches found from scanning.
--
-- See the `noseyparker::match_type::Match` type in noseyparker for correspondence.
(
    -- An arbitrary integer identifier for the match
    id INTEGER PRIMARY KEY,

    -- The content-based unique identifier of the match
    -- sha1_hex(rule structural identifier + '\0' + hex blob id + '\0' + decimal start byte + '\0' + decimal end byte)
    structural_id TEXT UNIQUE NOT NULL,

    -- The identifier of the finding this match belongs to
    finding_id INTEGER NOT NULL REFERENCES finding(id),

    -- The blob in which this match occurs
    blob_id INTEGER NOT NULL REFERENCES blob(id),

    -- The byte offset within the blob for the start of the match
    start_byte INTEGER NOT NULL,

    -- The byte offset within the blob for the end of the match
    end_byte INTEGER NOT NULL,

    -- the contextual snippet preceding the matching input
    before_snippet_id INTEGER NOT NULL REFERENCES snippet(id),

    -- the entire matching input
    matching_snippet_id INTEGER NOT NULL REFERENCES snippet(id),

    -- the contextual snippet trailing the matching input
    after_snippet_id INTEGER NOT NULL REFERENCES snippet(id),

    UNIQUE (
        blob_id,
        start_byte,
        end_byte,
        finding_id
    ),

    FOREIGN KEY (blob_id, start_byte, end_byte) REFERENCES blob_source_span(blob_id, start_byte, end_byte)
) STRICT;

CREATE INDEX IF NOT EXISTS match_finding_id_index ON match(finding_id);

--------------------------------------------------------------------------------
-- Statuses
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_status
-- This table records the accepted/rejected status of matches.
(
    -- The integer identifier of the match
    match_id INTEGER PRIMARY KEY REFERENCES match(id),

    -- The assigned status, either `accept` or `reject`
    status TEXT NOT NULL,

    CONSTRAINT status_valid CHECK (status IN ('accept', 'reject'))
) STRICT;

--------------------------------------------------------------------------------
-- Comments
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS finding_comment
-- This table records ad-hoc comments assigned to findings.
(
    -- The integer identifier of the finding
    finding_id INTEGER PRIMARY KEY REFERENCES finding(id),

    -- The assigned comment, a non-empty string
    comment TEXT NOT NULL,

    CONSTRAINT comment_valid CHECK (comment != '')
) STRICT;

CREATE TABLE IF NOT EXISTS match_comment
-- This table records ad-hoc comments assigned to matches.
(
    -- The integer identifier of the match
    match_id INTEGER PRIMARY KEY REFERENCES match(id),

    -- The assigned comment, a non-empty string
    comment TEXT NOT NULL,

    CONSTRAINT comment_valid CHECK (comment != '')
) STRICT;

--------------------------------------------------------------------------------
-- Scores
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_score
-- This table records a numeric score for matches.
(
    -- The integer identifier of the match
    match_id INTEGER PRIMARY KEY REFERENCES match(id),

    -- The numeric score in [0, 1]
    score REAL NOT NULL,

    CONSTRAINT score_valid CHECK (0.0 <= score AND score <= 1.0)
) STRICT;

--------------------------------------------------------------------------------
-- Convenience Views
--------------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS match_denorm AS
SELECT
    m.id,
    m.structural_id,
    f.finding_id,

    b.blob_id,

    m.start_byte,
    m.end_byte,

    bss.start_line,
    bss.start_column,
    bss.end_line,
    bss.end_column,

    r.name,
    r.text_id,
    r.structural_id,

    f.groups,

    before_snippet.snippet,
    matching_snippet.snippet,
    after_snippet.snippet,

    match_status.status,
    match_comment.comment,
    match_score.score
FROM
    match m
    INNER JOIN finding f ON (m.finding_id = f.id)
    INNER JOIN blob_source_span bss ON (m.blob_id = bss.blob_id AND m.start_byte = bss.start_byte AND m.end_byte = bss.end_byte)
    INNER JOIN blob b ON (m.blob_id = b.id)
    INNER JOIN rule r ON (f.rule_id = r.id)
    INNER JOIN snippet before_snippet ON (m.before_snippet_id = before_snippet.id)
    INNER JOIN snippet matching_snippet ON (m.matching_snippet_id = matching_snippet.id)
    INNER JOIN snippet after_snippet ON (m.after_snippet_id = after_snippet.id)
    LEFT OUTER JOIN match_status ON (m.id = match_status.match_id)
    LEFT OUTER JOIN match_comment ON (m.id = match_comment.match_id)
    LEFT OUTER JOIN match_score ON (m.id = match_score.match_id);

CREATE VIEW IF NOT EXISTS blob_denorm AS
SELECT
    b.id,
    b.blob_id,
    b.size,
    bm.mime_essence,
    bc.charset
FROM
    blob b
    LEFT OUTER JOIN blob_mime_essence bm ON (b.id = bm.blob_id)
    LEFT OUTER JOIN blob_charset bc ON (b.id = bc.blob_id);

CREATE VIEW IF NOT EXISTS blob_provenance_denorm AS
SELECT
    b.blob_id,
    bp.provenance
FROM
    blob b
    INNER JOIN blob_provenance bp ON (b.id = bp.blob_id);

CREATE VIEW IF NOT EXISTS finding_denorm AS
SELECT
    f.finding_id,
    r.name,
    r.text_id,
    r.structural_id,
    r.syntax,
    f.groups,
    COUNT(*),
    AVG(ms.score),
    fc.comment,
    json_group_array(DISTINCT match_status.status)
        FILTER (WHERE match_status.status IS NOT NULL) AS match_statuses
FROM
    finding f
    INNER JOIN match m ON (m.finding_id = f.id)
    INNER JOIN rule r ON (f.rule_id = r.id)
    LEFT OUTER JOIN match_score ms ON (m.id = ms.match_id)
    LEFT OUTER JOIN match_status ON (m.id = match_status.match_id)
    LEFT OUTER JOIN finding_comment fc ON (f.id = fc.finding_id)
GROUP BY f.id;

CREATE VIEW IF NOT EXISTS finding_summary AS
SELECT
    r.name AS rule_name,
    r.structural_id AS rule_structural_id,
    COUNT(DISTINCT f.finding_id) AS total_findings,
    COUNT(*) AS total_matches
FROM
    finding f
    INNER JOIN match m ON (m.finding_id = f.id)
    INNER JOIN rule r ON (f.rule_id = r.id)
GROUP BY rule_name, rule_structural_id;
