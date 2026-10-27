//! Generates `DeltaErrorCondition` for the allowlisted leaves of the pinned Delta error catalog.
//! Catalog updates are explicit changes to the checked-in inputs; generation never fetches data.
//!
//! Leaf conditions follow Spark's `ErrorClassesJsonReader`: message lines join with `"\n"`, a
//! subclass template is the parent template + `" "` + the subclass template, a subclass inherits
//! its parent's SQLSTATE unless it sets its own, and only subclasses of a family are conditions.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{bail, ensure, Context};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::codegen::{add_preamble, ensure_file_contents};

const COMMAND: &str = "update-delta-error-catalog";
const CATALOG: &str = "kernel/error_catalog/delta-error-classes.json";
const SOURCE: &str = "kernel/error_catalog/source.json";
const SELECTION: &str = "kernel/error_catalog/selected-conditions.json";
const DESTINATION: &str = "kernel/src/error/delta_error_condition/generated.rs";

#[derive(clap::Args)]
pub(crate) struct UpdateArgs {
    /// Use the checked-in inputs (also the default; accepted for explicit offline workflows).
    #[arg(long = "offline")]
    _offline: bool,
    /// Fail if generated code differs from the checked-in inputs, without writing files.
    #[arg(long)]
    check: bool,
}

/// Generates selected conditions in the workspace rooted at `root`.
/// Returns an error for invalid inputs, failed file operations, or stale output in check mode.
pub(crate) fn update(root: &Path, args: UpdateArgs) -> anyhow::Result<()> {
    let (catalog, source) = load_vendored(root)?;
    let selection_path = root.join(SELECTION);
    let selection = fs::read(&selection_path)
        .with_context(|| format!("reading {}", selection_path.display()))?;
    let conditions = select_conditions(parse_catalog(&catalog)?, &selection)?;
    let generated = add_preamble(COMMAND, &render(&source, &conditions)?);
    ensure_file_contents(COMMAND, &root.join(DESTINATION), &generated, args.check)?;
    eprintln!(
        "{} selected Delta error conditions from delta-io/delta@{}",
        conditions.len(),
        source.commit
    );
    Ok(())
}

/// Where the vendored catalog came from, recorded in `source.json`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Source {
    repository: String,
    commit: String,
    path: String,
    sha256: String,
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
        source.repository == "https://github.com/delta-io/delta"
            && source.path == "spark/src/main/resources/error/delta-error-classes.json"
            && source.commit.len() == 40
            && source
                .commit
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "{} must identify the Delta error catalog at a full lowercase commit SHA",
        source_path.display()
    );
    ensure!(
        sha256_hex(&catalog) == source.sha256,
        "{} does not match the sha256 in {}; update the catalog and its source metadata together",
        catalog_path.display(),
        source_path.display(),
    );
    Ok((catalog, source))
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

    Ok(conditions)
}

/// Selects exact leaf names, rejecting duplicate or unknown entries and Rust variant collisions.
/// The output retains catalog order so reordering the allowlist does not change generated code.
fn select_conditions(
    conditions: Vec<Condition>,
    selection: &[u8],
) -> anyhow::Result<Vec<Condition>> {
    let names: Vec<String> =
        serde_json::from_slice(selection).context("parsing the Delta error condition selection")?;
    let mut selected = BTreeSet::new();
    for name in names {
        ensure!(
            selected.insert(name.clone()),
            "duplicate selected condition {name:?}"
        );
    }
    let conditions: Vec<_> = conditions
        .into_iter()
        .filter(|condition| selected.remove(&condition.name))
        .collect();
    ensure!(
        selected.is_empty(),
        "selected conditions are not catalog leaves: {selected:?}"
    );
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
        "/// A stable error condition selected for Kernel from the Delta error catalog."
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
        "    // Lets invariant tests check metadata for every selected condition."
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
    #[case::top_level(r#"["TOP_LEVEL"]"#, &["TOP_LEVEL"])]
    #[case::subclass(r#"["FAMILY.INHERITS"]"#, &["FAMILY.INHERITS"])]
    #[case::unsorted(r#"["TOP_LEVEL", "FAMILY.INHERITS"]"#, &["FAMILY.INHERITS", "TOP_LEVEL"])]
    #[case::sorted(r#"["FAMILY.INHERITS", "TOP_LEVEL"]"#, &["FAMILY.INHERITS", "TOP_LEVEL"])]
    #[case::empty("[]", &[])]
    fn selection_preserves_metadata_in_catalog_order(
        #[case] selection: &str,
        #[case] expected: &[&str],
    ) {
        let selected = select_conditions(conditions(FIXTURE), selection.as_bytes()).unwrap();
        let expected: Vec<_> = expected
            .iter()
            .map(|name| condition_named(FIXTURE, name))
            .collect();
        assert_eq!(selected, expected);
    }

    #[rstest]
    #[case::unknown(r#"["MISSING"]"#, "not catalog leaves")]
    #[case::parent(r#"["FAMILY"]"#, "not catalog leaves")]
    #[case::duplicate(r#"["TOP_LEVEL", "TOP_LEVEL"]"#, "duplicate selected condition")]
    #[case::malformed("{}", "parsing")]
    fn rejects_invalid_selection(#[case] selection: &str, #[case] needle: &str) {
        let err = select_conditions(conditions(FIXTURE), selection.as_bytes()).unwrap_err();
        assert!(format!("{err:#}").contains(needle), "{err:#}");
    }

    #[rstest]
    #[case::selected_collision(r#"["A.B", "A_B"]"#, true)]
    #[case::unselected_collision(r#"["A.B"]"#, false)]
    fn only_selected_variant_collisions_are_rejected(#[case] selection: &str, #[case] fails: bool) {
        let catalog = r#"{
            "A_B": {"message": ["x"]},
            "A": {"message": ["p"], "subClass": {"B": {"message": ["s"]}}}
        }"#;
        let result = select_conditions(conditions(catalog), selection.as_bytes());
        if fails {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("both map to the Rust variant AB"));
        } else {
            assert_eq!(result.unwrap(), [condition_named(catalog, "A.B")]);
        }
    }

    #[test]
    fn render_emits_metadata() {
        let source = fixture_source();
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

    fn fixture_source() -> Source {
        Source {
            repository: "https://github.com/delta-io/delta".to_string(),
            commit: COMMIT.to_string(),
            path: "spark/src/main/resources/error/delta-error-classes.json".to_string(),
            sha256: sha256_hex(FIXTURE.as_bytes()),
        }
    }

    fn write_fixture(root: &Path) {
        fs::create_dir_all(root.join(CATALOG).parent().unwrap()).unwrap();
        fs::write(root.join(CATALOG), FIXTURE).unwrap();
        let source = fixture_source();
        fs::write(
            root.join(SOURCE),
            serde_json::json!({
                "repository": source.repository,
                "commit": source.commit,
                "path": source.path,
                "sha256": source.sha256,
            })
            .to_string(),
        )
        .unwrap();
        fs::write(root.join(SELECTION), r#"["TOP_LEVEL", "FAMILY.INHERITS"]"#).unwrap();
        update_offline(root, false).unwrap();
    }

    fn update_offline(root: &Path, check: bool) -> anyhow::Result<()> {
        update(
            root,
            UpdateArgs {
                _offline: false,
                check,
            },
        )
    }

    fn read_all(root: &Path) -> Vec<String> {
        [CATALOG, SOURCE, SELECTION, DESTINATION]
            .map(|path| fs::read_to_string(root.join(path)).unwrap())
            .into()
    }

    #[test]
    fn generates_from_local_inputs_and_check_passes() {
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path());
        let [catalog, source, _, generated]: [String; 4] = read_all(dir.path()).try_into().unwrap();
        assert_eq!(catalog, FIXTURE);
        let source: Source = serde_json::from_str(&source).unwrap();
        assert_eq!(source.commit, COMMIT);
        assert_eq!(source.sha256, sha256_hex(FIXTURE.as_bytes()));
        assert!(generated.starts_with("//! Generated by `cargo xtask update-delta-error-catalog`"));
        update(
            dir.path(),
            UpdateArgs {
                _offline: false,
                check: true,
            },
        )
        .unwrap();
    }

    #[rstest]
    fn regeneration_does_not_change_inputs(#[values(false, true)] offline: bool) {
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path());
        let before = read_all(dir.path());
        update(
            dir.path(),
            UpdateArgs {
                _offline: offline,
                check: false,
            },
        )
        .unwrap();
        assert_eq!(read_all(dir.path()), before);
    }

    #[test]
    fn selection_change_requires_regeneration_and_check_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        write_fixture(dir.path());
        fs::write(dir.path().join(SELECTION), r#"["FAMILY.INHERITS"]"#).unwrap();
        let before = read_all(dir.path());
        let err = update_offline(dir.path(), true).unwrap_err();
        assert!(err.to_string().contains("is not up to date"), "{err:#}");
        assert_eq!(read_all(dir.path()), before);
        update_offline(dir.path(), false).unwrap();
        let after = read_all(dir.path());
        assert_eq!(after[..3], before[..3]);
        assert!(after[3].contains("FamilyInherits"));
        assert!(!after[3].contains("TopLevel"));
        update_offline(dir.path(), true).unwrap();
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
