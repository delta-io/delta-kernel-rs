//! `cargo xtask update-delta-error-catalog`: vendors Delta's error catalog and generates
//! `DeltaErrorCondition` from it.
//!
//! Leaf conditions follow Spark's `ErrorClassesJsonReader`: message lines join with `"\n"`, a
//! subclass template is the parent template + `" "` + the subclass template, a subclass inherits
//! its parent's SQLSTATE unless it sets its own, and only subclasses of a family are conditions.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{bail, ensure, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::codegen::{add_preamble, ensure_file_contents};

const COMMAND: &str = "update-delta-error-catalog";
const REPOSITORY: &str = "https://github.com/delta-io/delta";
const BRANCH: &str = "refs/heads/master";
const UPSTREAM_PATH: &str = "spark/src/main/resources/error/delta-error-classes.json";
const CATALOG: &str = "kernel/error_catalog/delta-error-classes.json";
const SOURCE: &str = "kernel/error_catalog/source.json";
const DESTINATION: &str = "kernel/src/error/delta_error_condition/generated.rs";

#[derive(clap::Args)]
pub(crate) struct UpdateArgs {
    /// Regenerate from the vendored catalog instead of fetching delta-io/delta master.
    #[arg(long)]
    offline: bool,
    /// Fail if the vendored catalog or generated code is stale instead of writing them.
    /// Implies `--offline`.
    #[arg(long)]
    check: bool,
}

/// Runs the command against the workspace rooted at `root`.
pub(crate) fn update(root: &Path, args: UpdateArgs) -> anyhow::Result<()> {
    let (catalog, source) = if args.offline || args.check {
        load_vendored(root)?
    } else {
        fetch_master()?
    };
    write_outputs(root, &catalog, &source, args.check)
}

/// Where the vendored catalog came from, recorded in `source.json`.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Source {
    repository: String,
    commit: String,
    path: String,
    sha256: String,
}

impl Source {
    fn new(commit: String, catalog: &[u8]) -> Self {
        Self {
            repository: REPOSITORY.to_string(),
            commit,
            path: UPSTREAM_PATH.to_string(),
            sha256: sha256_hex(catalog),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ErrorClass {
    message: Vec<String>,
    sql_state: Option<String>,
    sub_class: Option<BTreeMap<String, ErrorSubClass>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ErrorSubClass {
    message: Vec<String>,
    sql_state: Option<String>,
}

/// A leaf condition, ready to render.
#[derive(Debug, PartialEq)]
struct Condition {
    name: String,
    variant: String,
    sql_state: Option<String>,
    parameter_names: Vec<String>,
    template: String,
}

fn load_vendored(root: &Path) -> anyhow::Result<(Vec<u8>, Source)> {
    let catalog_path = root.join(CATALOG);
    let catalog =
        fs::read(&catalog_path).with_context(|| format!("reading {}", catalog_path.display()))?;
    let source_path = root.join(SOURCE);
    let source = fs::read_to_string(&source_path)
        .with_context(|| format!("reading {}", source_path.display()))?;
    let source: Source = serde_json::from_str(&source)
        .with_context(|| format!("parsing {}", source_path.display()))?;
    ensure!(
        sha256_hex(&catalog) == source.sha256,
        "{} does not match the sha256 in {}; rerun `cargo xtask {COMMAND}` without `--offline` \
         to re-vendor it",
        catalog_path.display(),
        source_path.display(),
    );
    Ok((catalog, source))
}

fn fetch_master() -> anyhow::Result<(Vec<u8>, Source)> {
    let ls_remote = run(
        "git",
        &[
            "ls-remote",
            "--exit-code",
            &format!("{REPOSITORY}.git"),
            BRANCH,
        ],
    )?;
    let commit = parse_ls_remote(&String::from_utf8(ls_remote)?)?;
    let url = format!("https://raw.githubusercontent.com/delta-io/delta/{commit}/{UPSTREAM_PATH}");
    let catalog = run(
        "curl",
        &["--fail", "--silent", "--show-error", "--location", &url],
    )?;
    let source = Source::new(commit, &catalog);
    Ok((catalog, source))
}

/// Validates `catalog` and then writes the vendored catalog, `source.json`, and generated code.
fn write_outputs(root: &Path, catalog: &[u8], source: &Source, check: bool) -> anyhow::Result<()> {
    let conditions = parse_catalog(catalog)?;
    let generated = add_preamble(COMMAND, &render(source, &conditions)?);
    let catalog = std::str::from_utf8(catalog).context("the catalog is not UTF-8")?;
    let mut source_json = serde_json::to_string_pretty(source)?;
    source_json.push('\n');
    ensure_file_contents(COMMAND, &root.join(CATALOG), catalog, check)?;
    ensure_file_contents(COMMAND, &root.join(SOURCE), &source_json, check)?;
    ensure_file_contents(COMMAND, &root.join(DESTINATION), &generated, check)?;
    eprintln!(
        "{} Delta error conditions from delta-io/delta@{}",
        conditions.len(),
        source.commit
    );
    Ok(())
}

/// Returns the commit of [`BRANCH`] from `git ls-remote` output.
fn parse_ls_remote(output: &str) -> anyhow::Result<String> {
    let Some(commit) = output.lines().find_map(|line| {
        let (commit, reference) = line.split_once('\t')?;
        (reference == BRANCH).then_some(commit)
    }) else {
        bail!("`git ls-remote` did not report {BRANCH}: {output:?}");
    };
    ensure!(
        commit.len() == 40
            && commit
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
        "`git ls-remote` reported an invalid commit for {BRANCH}: {commit:?}"
    );
    Ok(commit.to_string())
}

/// Flattens and validates the catalog into leaf conditions, ordered by name.
fn parse_catalog(catalog: &[u8]) -> anyhow::Result<Vec<Condition>> {
    let classes: BTreeMap<String, ErrorClass> =
        serde_json::from_slice(catalog).context("parsing the Delta error catalog")?;
    let mut conditions = Vec::new();
    for (class_name, class) in classes {
        validate_name_part(&class_name)?;
        let class_template = class.message.join("\n");
        let Some(sub_classes) = class.sub_class else {
            conditions.push(condition(class_name, class.sql_state, class_template)?);
            continue;
        };
        ensure!(
            !sub_classes.is_empty(),
            "{class_name} has an empty subClass"
        );
        for (sub_name, sub) in sub_classes {
            validate_name_part(&sub_name)?;
            let name = format!("{class_name}.{sub_name}");
            let sql_state = sub.sql_state.or_else(|| class.sql_state.clone());
            let template = format!("{class_template} {}", sub.message.join("\n"));
            conditions.push(condition(name, sql_state, template)?);
        }
    }
    conditions.sort_by(|a, b| a.name.cmp(&b.name));

    let mut variants = HashMap::new();
    for condition in &conditions {
        if let Some(other) = variants.insert(condition.variant.as_str(), condition.name.as_str()) {
            bail!(
                "{other} and {} both map to the Rust variant {}",
                condition.name,
                condition.variant
            );
        }
    }
    Ok(conditions)
}

fn condition(
    name: String,
    sql_state: Option<String>,
    template: String,
) -> anyhow::Result<Condition> {
    if let Some(sql_state) = &sql_state {
        ensure!(
            sql_state.len() == 5
                && sql_state
                    .bytes()
                    .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase()),
            "{name} has invalid SQLSTATE {sql_state:?}"
        );
    }
    Ok(Condition {
        variant: variant_name(&name),
        parameter_names: parameter_names(&template),
        name,
        sql_state,
        template,
    })
}

/// A class or subclass name must match `[A-Z][A-Z0-9_]*`.
fn validate_name_part(part: &str) -> anyhow::Result<()> {
    let mut bytes = part.bytes();
    ensure!(
        bytes.next().is_some_and(|b| b.is_ascii_uppercase())
            && bytes.all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
        "invalid error class name {part:?}"
    );
    Ok(())
}

/// Converts `PARENT_NAME.SUB_NAME` into `ParentNameSubName`.
fn variant_name(name: &str) -> String {
    let mut variant = String::with_capacity(name.len());
    for word in name.split(['_', '.']) {
        let mut chars = word.chars();
        if let Some(first) = chars.next() {
            variant.push(first.to_ascii_uppercase());
            variant.extend(chars.map(|c| c.to_ascii_lowercase()));
        }
    }
    variant
}

/// Returns the unique `<([a-zA-Z0-9_-]+)>` placeholders in `template`, in first-appearance order,
/// matching Spark's `TEMPLATE_REGEX`. Any other `<` or `>` is literal text.
fn parameter_names(template: &str) -> Vec<String> {
    let is_name_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'-';
    let bytes = template.as_bytes();
    let mut names: Vec<String> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'<' {
            i += 1;
            continue;
        }
        let start = i + 1;
        let end = start
            + bytes[start..]
                .iter()
                .take_while(|&&b| is_name_byte(b))
                .count();
        if end > start && bytes.get(end) == Some(&b'>') {
            let name = &template[start..end];
            if !names.iter().any(|n| n == name) {
                names.push(name.to_string());
            }
            i = end + 1;
        } else {
            i = start;
        }
    }
    names
}

fn render(source: &Source, conditions: &[Condition]) -> Result<String, std::fmt::Error> {
    let mut out = String::new();
    writeln!(out, "//!")?;
    writeln!(
        out,
        "//! Source: <{}> at `{}`,",
        source.repository, source.commit
    )?;
    writeln!(out, "//! `{}`.", source.path)?;
    writeln!(out, "#![cfg_attr(rustfmt, rustfmt::skip)]")?;
    writeln!(out)?;
    writeln!(out, "use super::ConditionMetadata;")?;
    writeln!(out)?;
    writeln!(
        out,
        "/// A stable error condition from the Delta error catalog."
    )?;
    writeln!(out, "///")?;
    writeln!(
        out,
        "/// Persist or transmit [`Self::name`]; variant discriminants are not stable."
    )?;
    writeln!(out, "#[non_exhaustive]")?;
    writeln!(out, "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]")?;
    writeln!(out, "pub enum DeltaErrorCondition {{")?;
    for c in conditions {
        let sql_state = match &c.sql_state {
            Some(sql_state) => format!(" (SQLSTATE `{sql_state}`)"),
            None => String::new(),
        };
        writeln!(out, "    /// `{}`{sql_state}.", c.name)?;
        writeln!(out, "    {},", c.variant)?;
    }
    writeln!(out, "}}")?;
    writeln!(out)?;
    writeln!(out, "impl DeltaErrorCondition {{")?;
    writeln!(
        out,
        "    // Lets invariant tests check metadata for every condition in the catalog."
    )?;
    writeln!(out, "    #[cfg(test)]")?;
    writeln!(out, "    pub(super) const ALL: &'static [Self] = &[")?;
    for c in conditions {
        writeln!(out, "        Self::{},", c.variant)?;
    }
    writeln!(out, "    ];")?;
    writeln!(out)?;
    writeln!(
        out,
        "    pub(super) const fn metadata(self) -> ConditionMetadata {{"
    )?;
    writeln!(out, "        match self {{")?;
    for c in conditions {
        let sql_state = match &c.sql_state {
            Some(sql_state) => format!("Some({sql_state:?})"),
            None => "None".to_string(),
        };
        let parameter_names = c
            .parameter_names
            .iter()
            .map(|name| format!("{name:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            out,
            "            Self::{} => ConditionMetadata {{",
            c.variant
        )?;
        writeln!(out, "                name: {:?},", c.name)?;
        writeln!(out, "                sql_state: {sql_state},")?;
        writeln!(
            out,
            "                parameter_names: &[{parameter_names}],"
        )?;
        writeln!(out, "                message_template: {:?},", c.template)?;
        writeln!(out, "            }},")?;
    }
    writeln!(out, "        }}")?;
    writeln!(out, "    }}")?;
    writeln!(out, "}}")?;
    Ok(out)
}

/// Runs `program` and returns its stdout, failing on a non-zero exit.
fn run(program: &str, args: &[&str]) -> anyhow::Result<Vec<u8>> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("running `{program}`; it must be on PATH"))?;
    ensure!(
        output.status.success(),
        "`{program} {}` failed ({}): {}",
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output.stdout)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    const FIXTURE: &str = r#"{
  "TOP_LEVEL": {
    "message": ["Value <a> and <b>."],
    "sqlState": "22000"
  },
  "FAMILY": {
    "message": ["Parent <p>."],
    "subClass": {
      "INHERITS": { "message": ["Sub <s>."] }
    },
    "sqlState": "42000"
  }
}
"#;

    fn conditions(json: &str) -> Vec<Condition> {
        parse_catalog(json.as_bytes()).unwrap()
    }

    fn condition_named(json: &str, name: &str) -> Condition {
        conditions(json)
            .into_iter()
            .find(|c| c.name == name)
            .unwrap()
    }

    #[rstest]
    #[case::top_level(
        r#"{"A_B": {"message": ["x <y>"], "sqlState": "22000"}}"#,
        "A_B",
        Some("22000"),
        "x <y>"
    )]
    #[case::subclass_inherits_sql_state(
        r#"{"P": {"message": ["p"], "sqlState": "22000", "subClass": {"S": {"message": ["s"]}}}}"#,
        "P.S",
        Some("22000"),
        "p s"
    )]
    #[case::subclass_overrides_sql_state(
        r#"{"P": {"message": ["p"], "sqlState": "22000",
                  "subClass": {"S": {"message": ["s"], "sqlState": "42K01"}}}}"#,
        "P.S",
        Some("42K01"),
        "p s"
    )]
    #[case::multi_line_messages(
        r#"{"P": {"message": ["p1", "p2"], "subClass": {"S": {"message": ["s1", "s2"]}}}}"#,
        "P.S",
        None,
        "p1\np2 s1\ns2"
    )]
    fn leaf_composition(
        #[case] json: &str,
        #[case] name: &str,
        #[case] sql_state: Option<&str>,
        #[case] template: &str,
    ) {
        let condition = condition_named(json, name);
        assert_eq!(condition.sql_state.as_deref(), sql_state);
        assert_eq!(condition.template, template);
    }

    #[test]
    fn family_emits_only_leaves_ordered_by_name() {
        let json = r#"{
            "P": {"message": ["p"], "subClass": {"B": {"message": ["b"]}, "A": {"message": ["a"]}}},
            "P_Z": {"message": ["z"]}
        }"#;
        let names: Vec<_> = conditions(json).into_iter().map(|c| c.name).collect();
        assert_eq!(names, ["P.A", "P.B", "P_Z"]);
    }

    #[rstest]
    #[case::dedup_in_first_appearance_order("<b> <a> <b>", &["b", "a"])]
    #[case::underscore_dash_and_digits("<a_b> <c-d> <e1>", &["a_b", "c-d", "e1"])]
    #[case::stray_angle_brackets("a < b > c <> d", &[])]
    #[case::dotted_name_is_literal("<a.b>", &[])]
    #[case::nested_open_bracket("<<a>>", &["a"])]
    #[case::unterminated("<a", &[])]
    fn placeholders(#[case] template: &str, #[case] expected: &[&str]) {
        assert_eq!(parameter_names(template), expected);
    }

    #[rstest]
    #[case("DELTA_VERSIONS_NOT_CONTIGUOUS", "DeltaVersionsNotContiguous")]
    #[case(
        "DELTA_CANNOT_WRITE_EMPTY_SCHEMA.STRUCT_NO_FIELDS",
        "DeltaCannotWriteEmptySchemaStructNoFields"
    )]
    #[case("DELTAV2_X", "Deltav2X")]
    fn variant_naming(#[case] name: &str, #[case] variant: &str) {
        assert_eq!(variant_name(name), variant);
    }

    #[rstest]
    #[case::variant_collision(
        r#"{"A_B": {"message": ["x"]}, "A": {"message": ["x"], "subClass": {"B": {"message": ["y"]}}}}"#,
        "A.B and A_B both map to the Rust variant AB"
    )]
    #[case::unknown_class_field(r#"{"A": {"message": ["x"], "extra": 1}}"#, "parsing")]
    #[case::unknown_subclass_field(
        r#"{"A": {"message": ["x"], "subClass": {"B": {"message": ["y"], "extra": 1}}}}"#,
        "parsing"
    )]
    #[case::invalid_sql_state(r#"{"A": {"message": ["x"], "sqlState": "4200"}}"#, "SQLSTATE")]
    #[case::lowercase_sql_state(r#"{"A": {"message": ["x"], "sqlState": "4200a"}}"#, "SQLSTATE")]
    #[case::invalid_name(r#"{"a": {"message": ["x"]}}"#, "invalid error class name")]
    #[case::invalid_sub_name(
        r#"{"A": {"message": ["x"], "subClass": {"B.C": {"message": ["y"]}}}}"#,
        "invalid error class name"
    )]
    #[case::empty_subclass(r#"{"A": {"message": ["x"], "subClass": {}}}"#, "empty subClass")]
    fn rejects_invalid_catalog(#[case] json: &str, #[case] needle: &str) {
        let err = parse_catalog(json.as_bytes()).unwrap_err();
        assert!(format!("{err:#}").contains(needle), "{err:#}");
    }

    #[rstest]
    #[case::master(&format!("{COMMIT}\trefs/heads/master\n"), Some(COMMIT))]
    #[case::empty("", None)]
    #[case::other_ref(&format!("{COMMIT}\trefs/heads/main\n"), None)]
    #[case::short_sha("0123abc\trefs/heads/master\n", None)]
    #[case::uppercase_sha(&format!("{}\trefs/heads/master\n", COMMIT.to_uppercase()), None)]
    fn ls_remote_parsing(#[case] output: &str, #[case] expected: Option<&str>) {
        assert_eq!(parse_ls_remote(output).ok().as_deref(), expected);
    }

    #[test]
    fn render_emits_metadata() {
        let source = Source::new(COMMIT.to_string(), FIXTURE.as_bytes());
        let rendered = render(&source, &conditions(FIXTURE)).unwrap();
        assert!(rendered.contains(&format!("at `{COMMIT}`")));
        assert!(
            rendered.contains("    /// `FAMILY.INHERITS` (SQLSTATE `42000`).\n    FamilyInherits,")
        );
        assert!(rendered.contains(
            "            Self::TopLevel => ConditionMetadata {\n\
             \x20               name: \"TOP_LEVEL\",\n\
             \x20               sql_state: Some(\"22000\"),\n\
             \x20               parameter_names: &[\"a\", \"b\"],\n\
             \x20               message_template: \"Value <a> and <b>.\",\n"
        ));
    }

    // === End to end on a tempdir workspace root ===

    fn write_fixture(root: &Path) {
        let source = Source::new(COMMIT.to_string(), FIXTURE.as_bytes());
        write_outputs(root, FIXTURE.as_bytes(), &source, false).unwrap();
    }

    fn update_offline(root: &Path, check: bool) -> anyhow::Result<()> {
        update(
            root,
            UpdateArgs {
                offline: true,
                check,
            },
        )
    }

    fn read_all(root: &Path) -> Vec<String> {
        [CATALOG, SOURCE, DESTINATION]
            .map(|path| fs::read_to_string(root.join(path)).unwrap())
            .into()
    }

    #[test]
    fn write_mode_creates_all_files_and_check_passes() {
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path());
        let [catalog, source, generated]: [String; 3] = read_all(dir.path()).try_into().unwrap();
        assert_eq!(catalog, FIXTURE);
        let source: Source = serde_json::from_str(&source).unwrap();
        assert_eq!(source.commit, COMMIT);
        assert_eq!(source.sha256, sha256_hex(FIXTURE.as_bytes()));
        assert!(generated.starts_with("//! Generated by `cargo xtask update-delta-error-catalog`"));
        update(
            dir.path(),
            UpdateArgs {
                offline: false,
                check: true,
            },
        )
        .unwrap();
    }

    #[test]
    fn offline_rewrites_nothing() {
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path());
        let before = read_all(dir.path());
        update_offline(dir.path(), false).unwrap();
        assert_eq!(read_all(dir.path()), before);
    }

    #[test]
    fn check_fails_when_catalog_edited_by_hand() {
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path());
        fs::write(
            dir.path().join(CATALOG),
            FIXTURE.replace("Parent", "Changed"),
        )
        .unwrap();
        let err = update_offline(dir.path(), true).unwrap_err();
        assert!(
            err.to_string().contains("does not match the sha256"),
            "{err:#}"
        );
    }

    #[test]
    fn check_fails_when_generated_code_edited_by_hand() {
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path());
        let generated = dir.path().join(DESTINATION);
        let edited = fs::read_to_string(&generated)
            .unwrap()
            .replace("TopLevel", "Edited");
        fs::write(&generated, edited).unwrap();
        let err = update_offline(dir.path(), true).unwrap_err();
        assert!(err.to_string().contains("is not up to date"), "{err:#}");
    }
}
