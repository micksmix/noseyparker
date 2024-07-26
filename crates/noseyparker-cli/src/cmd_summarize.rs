use anyhow::{Context, Result};
use indicatif::HumanCount;
use polodb_core::Database;

use noseyparker::datastore::{Datastore, FindingSummary};

use crate::args::{GlobalArgs, SummarizeArgs, SummarizeOutputFormat};
use crate::reportable::Reportable;

struct FindingSummaryReporter(FindingSummary);

impl Reportable for FindingSummaryReporter {
    type Format = SummarizeOutputFormat;

    fn report<W: std::io::Write>(&self, format: Self::Format, writer: W) -> Result<()> {
        match format {
            SummarizeOutputFormat::Human => self.human_format(writer),
            SummarizeOutputFormat::Json => self.json_format(writer),
            SummarizeOutputFormat::Jsonl => self.jsonl_format(writer),
        }
    }
}

impl FindingSummaryReporter {
    fn human_format<W: std::io::Write>(&self, mut writer: W) -> Result<()> {
        let summary = &self.0;
        writeln!(writer)?;
        let table = summary_table(summary);
        // FIXME: this doesn't preserve ANSI styling on the table
        table.print(&mut writer)?;
        Ok(())
    }

    fn json_format<W: std::io::Write>(&self, writer: W) -> Result<()> {
        let summary = &self.0;
        serde_json::to_writer_pretty(writer, &summary)?;
        Ok(())
    }

    fn jsonl_format<W: std::io::Write>(&self, mut writer: W) -> Result<()> {
        let summary = &self.0;
        for entry in summary.0.iter() {
            serde_json::to_writer(&mut writer, entry)?;
            writeln!(&mut writer)?;
        }
        Ok(())
    }
}

pub fn run(global_args: &GlobalArgs, args: &SummarizeArgs, datastore: &Datastore) -> Result<()> {
    // Initialize in-memory datastore
    // let db = Database::open_memory().context("Failed to open in-memory database")?;
    // let datastore = Datastore::new_in_memory()?;

    let output = args
        .output_args
        .get_writer()
        .context("Failed to get output writer")?;
    let summary = datastore
        .get_summary()
        .context("Failed to get finding summary")
        .unwrap();
    FindingSummaryReporter(summary).report(args.output_args.format, output)
}

pub fn summary_table(summary: &FindingSummary) -> prettytable::Table {
    use prettytable::format::{FormatBuilder, LinePosition, LineSeparator};
    use prettytable::row;

    let f = FormatBuilder::new()
        .column_separator(' ')
        .separators(&[LinePosition::Title], LineSeparator::new('─', '─', '─', '─'))
        .padding(1, 1)
        .build();

    let mut table: prettytable::Table = summary
        .0
        .iter()
        .map(|e| {
            row![
                 l -> &e.rule_name,
                 r -> HumanCount(e.distinct_count.try_into().unwrap()),
                 r -> HumanCount(e.total_count.try_into().unwrap()),
                 r -> HumanCount(e.accept_count.try_into().unwrap()),
                 r -> HumanCount(e.reject_count.try_into().unwrap()),
                 r -> HumanCount(e.mixed_count.try_into().unwrap()),
                 r -> HumanCount(e.unlabeled_count.try_into().unwrap()),
            ]
        })
        .collect();
    table.set_format(f);
    table.set_titles(row![
        lb -> "Rule",
        cb -> "Findings",
        cb -> "Matches",
        cb -> "Accepted",
        cb -> "Rejected",
        cb -> "Mixed",
        cb -> "Unlabeled",
    ]);
    table
}
