mod shapes;

pub use shapes::FixtureOptions;

pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub table_root: String,
    pub schema_bytes: usize,
    pub width: usize,
    pub initial_files: usize,
    pub tail_commits: u64,
    pub schema_fields: usize,
    pub stats_json_bytes: usize,
    pub add_actions: usize,
    pub remove_actions: usize,
    pub checkpoints: usize,
}

impl Fixture {
    pub fn description(&self) -> serde_json::Value {
        serde_json::json!({
            "path": self.directory.path(), "table_root": self.table_root,
            "width": self.width, "schema_fields": self.schema_fields,
            "schema_json_bytes": self.schema_bytes, "initial_files": self.initial_files,
            "tail_commits": self.tail_commits, "checkpoints": self.checkpoints,
            "stats_json_bytes": self.stats_json_bytes, "add_actions": self.add_actions,
            "remove_actions": self.remove_actions,
            "expected_live_files": self.add_actions - self.remove_actions,
        })
    }

    pub fn create(
        width: usize,
        initial_files: usize,
        tail_commits: u64,
        schema_bytes: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::create_with_action_size(width, initial_files, tail_commits, schema_bytes, 0)
    }

    pub fn create_with_action_size(
        width: usize,
        initial_files: usize,
        tail_commits: u64,
        schema_bytes: Option<usize>,
        stats_bytes: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::create_with_options(
            width,
            initial_files,
            tail_commits,
            schema_bytes,
            stats_bytes,
            0,
            false,
        )
    }

    pub fn create_with_options(
        width: usize,
        initial_files: usize,
        tail_commits: u64,
        schema_bytes: Option<usize>,
        stats_bytes: usize,
        stats_columns: i64,
        parsed_stats: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::from_options(&FixtureOptions {
            width,
            initial_files,
            tail_commits,
            schema_bytes,
            stats_bytes,
            stats_columns,
            parsed_stats,
            ..FixtureOptions::default()
        })
    }
}
