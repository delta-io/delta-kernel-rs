use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

const CATALOG_RELATIVE_PATH: &str = "kernel/error_catalog/delta-error-classes.json";
const SOURCE_METADATA_RELATIVE_PATH: &str = "kernel/error_catalog/source.json";
const GENERATED_RELATIVE_PATH: &str = "kernel/src/error/delta_error_conditions.rs";
const OSS_REPOSITORY: &str = "https://github.com/delta-io/delta";
const OSS_CATALOG_PATH: &str = "spark/src/main/resources/error/delta-error-classes.json";

#[derive(Debug)]
struct Paths {
    catalog: PathBuf,
    source_metadata: PathBuf,
    generated: PathBuf,
}

impl Paths {
    fn new(workspace_root: &Path) -> Self {
        Self {
            catalog: workspace_root.join(CATALOG_RELATIVE_PATH),
            source_metadata: workspace_root.join(SOURCE_METADATA_RELATIVE_PATH),
            generated: workspace_root.join(GENERATED_RELATIVE_PATH),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceMetadata {
    repository: String,
    commit: String,
    path: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct Catalog(
    #[serde(deserialize_with = "deserialize_unique_map")] BTreeMap<String, CatalogEntry>,
);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CatalogEntry {
    message: Vec<String>,
    #[serde(default)]
    sql_state: Option<String>,
    #[serde(
        default,
        rename = "subClass",
        deserialize_with = "deserialize_unique_map"
    )]
    subclasses: BTreeMap<String, CatalogEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Condition {
    name: String,
    variant: String,
    sql_state: Option<String>,
    parameters: Vec<String>,
    template: String,
}

/// Generates or checks the checked-in Delta condition metadata.
pub fn generate(workspace_root: &Path, check: bool) -> Result<()> {
    let paths = Paths::new(workspace_root);
    let source = load_source(&paths.source_metadata)?;
    let catalog = fs::read(&paths.catalog)
        .with_context(|| format!("failed to read {}", paths.catalog.display()))?;
    verify_catalog_checksum(&source, &catalog)?;
    let conditions = parse_catalog(&catalog)?;
    let generated = render_rust(&conditions, &source)?;

    if check {
        check_artifact(&paths.generated, &generated)?;
        println!(
            "verified {} generated Delta error conditions",
            conditions.len()
        );
    } else {
        write_if_changed(&paths.generated, generated.as_bytes())?;
        println!("generated {} Delta error conditions", conditions.len());
    }
    Ok(())
}

/// Imports the catalog from a pinned Delta repository revision and regenerates its projection.
pub fn update(workspace_root: &Path, delta_repo: &Path, revision: &str) -> Result<()> {
    let paths = Paths::new(workspace_root);
    let commit = resolve_commit(delta_repo, revision)?;
    let catalog = read_catalog_at_commit(delta_repo, &commit)?;
    let conditions = parse_catalog(&catalog)?;
    let source = SourceMetadata {
        repository: OSS_REPOSITORY.to_string(),
        commit,
        path: OSS_CATALOG_PATH.to_string(),
        sha256: sha256(&catalog),
    };
    let generated = render_rust(&conditions, &source)?;
    let source_json = render_source(&source)?;

    write_if_changed(&paths.catalog, &catalog)?;
    write_if_changed(&paths.source_metadata, &source_json)?;
    write_if_changed(&paths.generated, generated.as_bytes())?;
    println!(
        "updated {} Delta error conditions from {}",
        conditions.len(),
        source.commit
    );
    Ok(())
}

fn deserialize_unique_map<'de, D, T>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct UniqueMapVisitor<T>(PhantomData<T>);

    impl<'de, T> Visitor<'de> for UniqueMapVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = BTreeMap<String, T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an object with unique keys")
        }

        fn visit_map<A>(self, mut access: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut values = BTreeMap::new();
            while let Some((key, value)) = access.next_entry::<String, T>()? {
                if values.contains_key(&key) {
                    return Err(de::Error::custom(format!(
                        "duplicate JSON object key {key:?}"
                    )));
                }
                values.insert(key, value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_map(UniqueMapVisitor(PhantomData))
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn load_source(path: &Path) -> Result<SourceMetadata> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let source: SourceMetadata = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    if source.repository != OSS_REPOSITORY {
        bail!("source metadata repository must be {OSS_REPOSITORY}");
    }
    if source.path != OSS_CATALOG_PATH {
        bail!("source metadata path must be {OSS_CATALOG_PATH}");
    }
    validate_lower_hex("commit", &source.commit, 40)?;
    validate_lower_hex("sha256", &source.sha256, 64)?;
    Ok(source)
}

fn validate_lower_hex(field: &str, value: &str, length: usize) -> Result<()> {
    if value.len() != length
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        bail!("source metadata field {field} must be {length} lowercase hexadecimal digits");
    }
    Ok(())
}

fn verify_catalog_checksum(source: &SourceMetadata, catalog: &[u8]) -> Result<()> {
    let actual = sha256(catalog);
    if source.sha256 != actual {
        bail!(
            "catalog checksum mismatch: source.json has {}, but the catalog is {actual}",
            source.sha256
        );
    }
    Ok(())
}

fn render_source(source: &SourceMetadata) -> Result<Vec<u8>> {
    let mut json =
        serde_json::to_vec_pretty(source).context("failed to serialize source metadata")?;
    json.push(b'\n');
    Ok(json)
}

fn parse_catalog(bytes: &[u8]) -> Result<Vec<Condition>> {
    let Catalog(entries): Catalog =
        serde_json::from_slice(bytes).context("failed to parse Delta error catalog")?;
    let mut conditions = Vec::new();
    for (name, entry) in entries {
        visit_catalog_entry(name, entry, None, None, &mut conditions)?;
    }
    validate_variants(&conditions)?;
    Ok(conditions)
}

fn visit_catalog_entry(
    name: String,
    entry: CatalogEntry,
    inherited_sql_state: Option<String>,
    parent_template: Option<String>,
    conditions: &mut Vec<Condition>,
) -> Result<()> {
    validate_condition_name(&name)?;
    let sql_state = entry.sql_state.or(inherited_sql_state);
    if let Some(sql_state) = &sql_state {
        validate_sql_state(&name, sql_state)?;
    }
    let own_template = entry.message.join("\n");
    let template = match parent_template {
        Some(parent) => format!("{parent}\n{own_template}"),
        None => own_template,
    };
    let variant = variant_name(&name);
    validate_rust_variant(&name, &variant)?;
    let parameters = ordered_parameters(&name, &template)?;
    conditions.push(Condition {
        name: name.clone(),
        variant,
        sql_state: sql_state.clone(),
        parameters,
        template: template.clone(),
    });

    for (subclass_name, subclass) in entry.subclasses {
        visit_catalog_entry(
            format!("{name}.{subclass_name}"),
            subclass,
            sql_state.clone(),
            Some(template.clone()),
            conditions,
        )?;
    }
    Ok(())
}

fn validate_condition_name(name: &str) -> Result<()> {
    let valid = name.split('.').all(|part| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    });
    if !valid {
        bail!("{name}: invalid condition identifier");
    }
    Ok(())
}

fn validate_sql_state(owner: &str, sql_state: &str) -> Result<()> {
    let valid = sql_state.len() == 5
        && sql_state
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit());
    if !valid {
        bail!("{owner}: SQLSTATE must be five uppercase letters or digits");
    }
    Ok(())
}

fn validate_parameter_name(owner: &str, parameter: &str) -> Result<()> {
    if parameter.is_empty()
        || !parameter
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("{owner}: invalid message parameter {parameter:?}");
    }
    Ok(())
}

fn variant_name(condition_name: &str) -> String {
    let mut variant = String::new();
    for word in condition_name
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
    {
        let mut characters = word.chars();
        if let Some(first) = characters.next() {
            variant.push(first.to_ascii_uppercase());
        }
        variant.extend(characters.map(|character| character.to_ascii_lowercase()));
    }
    variant
}

fn validate_rust_variant(owner: &str, variant: &str) -> Result<()> {
    let mut bytes = variant.bytes();
    let starts_with_letter = bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic());
    if variant == "Self" || !starts_with_letter || !bytes.all(|byte| byte.is_ascii_alphanumeric()) {
        bail!("{owner}: generated invalid Rust variant {variant:?}");
    }
    Ok(())
}

fn ordered_parameters(owner: &str, template: &str) -> Result<Vec<String>> {
    let mut parameters = Vec::new();
    let mut seen = HashSet::new();
    let mut remaining = template;
    while let Some(open) = remaining.find('<') {
        if remaining[..open].contains('>') {
            bail!("{owner}: unmatched '>' in message template");
        }
        let after_open = &remaining[open + 1..];
        let close = after_open
            .find('>')
            .with_context(|| format!("{owner}: unmatched '<' in message template"))?;
        let parameter = &after_open[..close];
        validate_parameter_name(owner, parameter)?;
        if seen.insert(parameter.to_string()) {
            parameters.push(parameter.to_string());
        }
        remaining = &after_open[close + 1..];
    }
    if remaining.contains('>') {
        bail!("{owner}: unmatched '>' in message template");
    }
    Ok(parameters)
}

fn validate_variants(conditions: &[Condition]) -> Result<()> {
    let mut variants = BTreeMap::new();
    for condition in conditions {
        if let Some(previous) = variants.insert(&condition.variant, &condition.name) {
            bail!(
                "Rust variant collision: {previous} and {} both generate {}",
                condition.name,
                condition.variant
            );
        }
    }
    Ok(())
}

fn render_rust(conditions: &[Condition], source: &SourceMetadata) -> Result<String> {
    let mut output = String::new();
    output.push_str(
        "// Copyright 2025 The Delta Kernel Authors\n\
         //\n\
         // Licensed under the Apache License, Version 2.0 (the \"License\");\n\
         // you may not use this file except in compliance with the License.\n\
         // You may obtain a copy of the License at\n\
         //\n\
         //     http://www.apache.org/licenses/LICENSE-2.0\n\
         //\n\
         // Unless required by applicable law or agreed to in writing, software\n\
         // distributed under the License is distributed on an \"AS IS\" BASIS,\n\
         // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
         // See the License for the specific language governing permissions and\n\
         // limitations under the License.\n\n",
    );
    output.push_str("// @generated by `cargo xtask generate-delta-error-conditions`.\n");
    writeln!(output, "// Source repository: {}", source.repository)?;
    writeln!(output, "// Source commit: {}", source.commit)?;
    writeln!(output, "// Source path: {}", source.path)?;
    writeln!(output, "// Catalog SHA-256: {}", source.sha256)?;
    output.push_str(
        "// Do not edit by hand. Update the pinned catalog and rerun the generator.\n\n\
         /// Stable, string-identified Delta error conditions.\n\
         ///\n\
         /// Enum layout and discriminant values are deliberately unspecified. Persist or transmit\n\
         /// [`Self::name`] rather than casting this enum to an integer.\n\
         #[rustfmt::skip]\n\
         #[non_exhaustive]\n\
         #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]\n\
         pub enum DeltaErrorCondition {\n",
    );
    for condition in conditions {
        writeln!(output, "    // Catalog condition: `{}`", condition.name)?;
        writeln!(
            output,
            "    /// Delta error condition `{}`.",
            condition.name
        )?;
        writeln!(output, "    {},", condition.variant)?;
    }
    output.push_str("}\n\n#[rustfmt::skip]\nimpl DeltaErrorCondition {\n");
    output.push_str("    /// Every condition in the vendored Delta error catalog.\n");
    output.push_str("    pub const ALL: &'static [Self] = &[\n");
    for condition in conditions {
        writeln!(output, "        Self::{},", condition.variant)?;
    }
    output.push_str("    ];\n\n");

    output.push_str("    /// Looks up a condition by its stable catalog identity.\n");
    output.push_str("    pub fn from_name(name: &str) -> Option<Self> {\n        match name {\n");
    for condition in conditions {
        writeln!(
            output,
            "            {} => Some(Self::{}),",
            rust_string(&condition.name),
            condition.variant
        )?;
    }
    output.push_str("            _ => None,\n        }\n    }\n\n");

    output.push_str("    /// Returns the stable string identity of this condition.\n");
    output.push_str(
        "    pub const fn name(self) -> &'static str {\n        self.metadata().name\n    }\n\n",
    );

    output.push_str("    /// Returns the SQLSTATE associated with this condition, when defined.\n");
    output.push_str(
        "    pub const fn sql_state(self) -> Option<&'static str> {\n        self.metadata().sql_state\n    }\n\n",
    );

    output.push_str(
        "    /// Returns the ordered, deduplicated names of this condition's message parameters.\n",
    );
    output.push_str(
        "    pub const fn parameter_names(self) -> &'static [&'static str] {\n        self.metadata().parameter_names\n    }\n\n",
    );

    output.push_str("    /// Returns the diagnostic message template for this condition.\n");
    output.push_str(
        "    pub const fn message_template(self) -> &'static str {\n        self.metadata().message_template\n    }\n\n",
    );

    output.push_str("    const fn metadata(self) -> ConditionMetadata {\n        match self {\n");
    for condition in conditions {
        let sql_state = condition
            .sql_state
            .as_deref()
            .map(|value| format!("Some({})", rust_string(value)))
            .unwrap_or_else(|| "None".to_string());
        let parameters = condition
            .parameters
            .iter()
            .map(|parameter| rust_string(parameter))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            output,
            "            Self::{} => ConditionMetadata {{",
            condition.variant
        )?;
        writeln!(
            output,
            "                name: {},",
            rust_string(&condition.name)
        )?;
        writeln!(output, "                sql_state: {sql_state},")?;
        writeln!(output, "                parameter_names: &[{parameters}],")?;
        writeln!(
            output,
            "                message_template: {},",
            rust_string(&condition.template)
        )?;
        output.push_str("            },\n");
    }
    output.push_str(concat!(
        "        }\n",
        "    }\n",
        "}\n\n",
        "struct ConditionMetadata {\n",
        "    name: &'static str,\n",
        "    sql_state: Option<&'static str>,\n",
        "    parameter_names: &'static [&'static str],\n",
        "    message_template: &'static str,\n",
        "}\n\n",
        "#[cfg(test)]\n",
        "mod tests {\n",
        "    use std::collections::HashSet;\n\n",
        "    use super::DeltaErrorCondition;\n\n",
        "    #[test]\n",
        "    fn condition_metadata_is_unique_and_round_trips() {\n",
        "        let mut names = HashSet::new();\n",
        "        for condition in DeltaErrorCondition::ALL {\n",
        "            assert!(names.insert(condition.name()));\n",
        "            assert_eq!(\n",
        "                DeltaErrorCondition::from_name(condition.name()),\n",
        "                Some(*condition)\n",
        "            );\n",
        "            let mut parameters = HashSet::new();\n",
        "            assert!(condition\n",
        "                .parameter_names()\n",
        "                .iter()\n",
        "                .all(|parameter| parameters.insert(*parameter)));\n",
        "        }\n",
        "    }\n",
        "}\n",
    ));
    Ok(output)
}

fn rust_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => escaped.extend(character.escape_unicode()),
            character => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}

fn check_artifact(path: &Path, expected: &str) -> Result<()> {
    match fs::read_to_string(path) {
        Ok(actual) if actual == expected => Ok(()),
        Ok(_) | Err(_) => {
            bail!(
                "generated Delta error conditions are stale: {}",
                path.display()
            )
        }
    }
}

fn write_if_changed(path: &Path, contents: &[u8]) -> Result<()> {
    if fs::read(path).is_ok_and(|current| current == contents) {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))
}

fn resolve_commit(delta_repo: &Path, revision: &str) -> Result<String> {
    let revision = format!("{revision}^{{commit}}");
    let output = Command::new("git")
        .arg("-C")
        .arg(delta_repo)
        .args(["rev-parse", "--verify"])
        .arg(&revision)
        .output()
        .with_context(|| format!("failed to run git in {}", delta_repo.display()))?;
    if !output.status.success() {
        bail!(
            "git could not resolve {revision:?}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let commit = String::from_utf8(output.stdout).context("Git commit must be valid UTF-8")?;
    let commit = commit.trim().to_string();
    if commit.is_empty() || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("git returned invalid commit {commit:?}");
    }
    Ok(commit)
}

fn read_catalog_at_commit(delta_repo: &Path, commit: &str) -> Result<Vec<u8>> {
    let object = format!("{commit}:{OSS_CATALOG_PATH}");
    let output = Command::new("git")
        .arg("-C")
        .arg(delta_repo)
        .arg("show")
        .arg(&object)
        .output()
        .with_context(|| format!("failed to run git in {}", delta_repo.display()))?;
    if !output.status.success() {
        bail!(
            "git could not read {object}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_generator_fixture(workspace_root: &Path) -> Paths {
        let paths = Paths::new(workspace_root);
        fs::create_dir_all(paths.catalog.parent().unwrap()).unwrap();
        fs::create_dir_all(paths.generated.parent().unwrap()).unwrap();

        let catalog = br#"{
            "DELTA_TEST": {
                "message": ["value <parameter>"],
                "sqlState": "XX000"
            }
        }"#;
        fs::write(&paths.catalog, catalog).unwrap();
        let source = SourceMetadata {
            repository: OSS_REPOSITORY.to_string(),
            commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            path: OSS_CATALOG_PATH.to_string(),
            sha256: sha256(catalog),
        };
        fs::write(&paths.source_metadata, render_source(&source).unwrap()).unwrap();
        paths
    }

    #[test]
    fn rust_string_escapes_controls_and_preserves_unicode() {
        assert_eq!(
            rust_string("quote=\" slash=\\ line=\n tab=\t unit=\u{1f} snowman=☃"),
            "\"quote=\\\" slash=\\\\ line=\\n tab=\\t unit=\\u{1f} snowman=☃\""
        );
    }

    #[test]
    fn subclass_inherits_sqlstate_and_parent_template() {
        let catalog = br#"{
            "DELTA_PARENT": {
                "message": ["parent <first>"],
                "sqlState": "22000",
                "subClass": {
                    "CHILD": {"message": ["child <second> and <first>"]}
                }
            }
        }"#;
        let conditions = parse_catalog(catalog).unwrap();
        assert_eq!(conditions.len(), 2);
        let child = &conditions[1];
        assert_eq!(child.name, "DELTA_PARENT.CHILD");
        assert_eq!(child.sql_state.as_deref(), Some("22000"));
        assert_eq!(child.parameters, ["first", "second"]);
        assert_eq!(child.template, "parent <first>\nchild <second> and <first>");
    }

    #[test]
    fn subclass_can_override_inherited_sqlstate() {
        let catalog = br#"{
            "DELTA_PARENT": {
                "message": ["parent"],
                "sqlState": "22000",
                "subClass": {
                    "CHILD": {
                        "message": ["child"],
                        "sqlState": "23000"
                    }
                }
            }
        }"#;
        let conditions = parse_catalog(catalog).unwrap();
        assert_eq!(conditions[1].sql_state.as_deref(), Some("23000"));
    }

    #[test]
    fn duplicate_catalog_keys_are_rejected() {
        let error = parse_catalog(
            br#"{
                "DELTA_DUP": {"message": ["first"]},
                "DELTA_DUP": {"message": ["second"]}
            }"#,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("duplicate JSON object key"));
    }

    #[test]
    fn unknown_catalog_fields_are_rejected() {
        let error = parse_catalog(
            br#"{
                "DELTA_TEST": {
                    "message": ["message"],
                    "unknownField": true
                }
            }"#,
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("unknown field"));
    }

    #[test]
    fn malformed_placeholders_are_rejected() {
        for template in ["missing <close", "empty <>", "bad <not.valid>", "close >"] {
            assert!(ordered_parameters("DELTA_TEST", template).is_err());
        }
    }

    #[test]
    fn placeholder_names_follow_the_upstream_grammar() {
        assert_eq!(
            ordered_parameters("DELTA_TEST", "<1> <_value> <with-hyphen> <1>").unwrap(),
            ["1", "_value", "with-hyphen"]
        );
    }

    #[test]
    fn reserved_rust_variant_is_rejected() {
        assert!(validate_rust_variant("SELF", "Self").is_err());
    }

    #[test]
    fn generation_and_stale_check_work_end_to_end() {
        let directory = tempfile::tempdir().unwrap();
        let paths = write_generator_fixture(directory.path());

        generate(directory.path(), false).unwrap();
        generate(directory.path(), true).unwrap();

        let generated = fs::read_to_string(&paths.generated).unwrap();
        assert!(generated.contains("// Source commit: 0123456789abcdef0123456789abcdef01234567"));
        assert!(generated.contains("self.metadata().name"));
        assert!(generated.contains("self.metadata().sql_state"));
        assert_eq!(
            generated
                .matches("Self::DeltaTest => ConditionMetadata {")
                .count(),
            1
        );

        fs::write(&paths.generated, "stale").unwrap();
        let error = generate(directory.path(), true).unwrap_err();
        assert!(format!("{error:#}").contains("conditions are stale"));
    }

    #[test]
    fn generation_rejects_catalog_checksum_mismatch() {
        let directory = tempfile::tempdir().unwrap();
        let paths = write_generator_fixture(directory.path());
        fs::write(
            &paths.catalog,
            br#"{"DELTA_CHANGED":{"message":["changed"]}}"#,
        )
        .unwrap();

        let error = generate(directory.path(), false).unwrap_err();
        assert!(format!("{error:#}").contains("catalog checksum mismatch"));
    }

    #[test]
    fn vendored_catalog_has_expected_golden_condition() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let paths = Paths::new(workspace_root);
        let bytes = fs::read(&paths.catalog).unwrap();
        let conditions = parse_catalog(&bytes).unwrap();
        assert_eq!(conditions.len(), 569);
        let condition = conditions
            .iter()
            .find(|item| item.name == "DELTA_CANNOT_WRITE_EMPTY_SCHEMA.STRUCT_NO_FIELDS")
            .unwrap();
        assert_eq!(condition.sql_state.as_deref(), Some("428GU"));
        assert_eq!(condition.parameters, ["columnPath"]);
    }
}
