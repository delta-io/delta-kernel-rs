use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

struct UniqueJson(Value);

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJson;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueJson)
            .ok_or_else(|| E::custom(format!("invalid JSON number {value}")))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::String(value.to_string())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(UniqueJson(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(UniqueJson(Value::Array(values)))
    }

    fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = Map::new();
        while let Some((key, UniqueJson(value))) = entries.next_entry::<String, UniqueJson>()? {
            if object.insert(key.clone(), value).is_some() {
                return Err(<A::Error as de::Error>::custom(format!(
                    "duplicate JSON object key {key:?}"
                )));
            }
        }
        Ok(UniqueJson(Value::Object(object)))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogKind {
    Oss,
    Kernel,
}

impl CatalogKind {
    fn name(self) -> &'static str {
        match self {
            Self::Oss => "OSS",
            Self::Kernel => "kernel",
        }
    }
}

#[derive(Debug)]
struct CatalogEntry {
    condition: String,
    variant: String,
    sql_state: Option<String>,
    message_template: String,
    parameter_names: Vec<String>,
    is_aggregate: bool,
}

#[derive(Debug)]
struct ParsedCatalog {
    entries: Vec<CatalogEntry>,
    top_level_count: usize,
    subclass_count: usize,
}

#[derive(Debug)]
struct Manifest {
    expected_top_level_count: usize,
    expected_subclass_count: usize,
    sha256: String,
}

pub(crate) fn generate(
    oss_catalog_path: &Path,
    kernel_catalog_path: &Path,
    manifest_path: &Path,
    output_path: &Path,
) -> Result<(), String> {
    let manifest_bytes = read_file(manifest_path)?;
    let manifest = parse_manifest(&manifest_bytes, manifest_path)?;
    let oss_bytes = read_file(oss_catalog_path)?;
    verify_oss_catalog_hash(&oss_bytes, &manifest, oss_catalog_path)?;

    let oss = parse_catalog(&oss_bytes, oss_catalog_path, CatalogKind::Oss)?;
    if oss.top_level_count != manifest.expected_top_level_count {
        return Err(format!(
            "{} has {} top-level classes; manifest requires {}",
            oss_catalog_path.display(),
            oss.top_level_count,
            manifest.expected_top_level_count
        ));
    }
    if oss.subclass_count != manifest.expected_subclass_count {
        return Err(format!(
            "{} has {} subclasses; manifest requires {}",
            oss_catalog_path.display(),
            oss.subclass_count,
            manifest.expected_subclass_count
        ));
    }

    let kernel_bytes = read_file(kernel_catalog_path)?;
    let kernel = parse_catalog(&kernel_bytes, kernel_catalog_path, CatalogKind::Kernel)?;
    let entries = merge_catalogs(oss, kernel)?;
    let generated = render_catalog(&entries)?;

    fs::write(output_path, generated)
        .map_err(|error| format!("failed to write {}: {error}", output_path.display()))
}

fn read_file(path: &Path) -> Result<Vec<u8>, String> {
    fs::read(path).map_err(|error| format!("failed to read {}: {error}", path.display()))
}

fn parse_manifest(bytes: &[u8], path: &Path) -> Result<Manifest, String> {
    let root = parse_json(bytes, path)?;
    let root = require_object(&root, "catalog manifest")?;
    reject_unknown_fields(root, &["source", "expectedCounts"], "catalog manifest")?;

    let source = require_field(root, "source", "catalog manifest")?;
    let source = require_object(source, "catalog manifest source")?;
    reject_unknown_fields(
        source,
        &["repository", "path", "commit", "sha256", "license"],
        "catalog manifest source",
    )?;
    for field in ["repository", "path", "commit", "license"] {
        let value = require_string(
            require_field(source, field, "catalog manifest source")?,
            &format!("catalog manifest source.{field}"),
        )?;
        if value.is_empty() {
            return Err(format!("catalog manifest source.{field} must not be empty"));
        }
    }
    let sha256 = require_string(
        require_field(source, "sha256", "catalog manifest source")?,
        "catalog manifest source.sha256",
    )?;
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("catalog manifest source.sha256 must be 64 lowercase hex digits".to_string());
    }

    let expected = require_field(root, "expectedCounts", "catalog manifest")?;
    let expected = require_object(expected, "catalog manifest expectedCounts")?;
    reject_unknown_fields(
        expected,
        &["topLevelClasses", "subclasses"],
        "catalog manifest expectedCounts",
    )?;
    let expected_top_level_count = require_usize(
        require_field(
            expected,
            "topLevelClasses",
            "catalog manifest expectedCounts",
        )?,
        "catalog manifest expectedCounts.topLevelClasses",
    )?;
    let expected_subclass_count = require_usize(
        require_field(expected, "subclasses", "catalog manifest expectedCounts")?,
        "catalog manifest expectedCounts.subclasses",
    )?;

    Ok(Manifest {
        expected_top_level_count,
        expected_subclass_count,
        sha256: sha256.to_string(),
    })
}

fn verify_oss_catalog_hash(bytes: &[u8], manifest: &Manifest, path: &Path) -> Result<(), String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut actual = String::with_capacity(64);
    for byte in digest {
        actual.push(HEX[usize::from(byte >> 4)] as char);
        actual.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    if actual != manifest.sha256 {
        return Err(format!(
            "SHA-256 mismatch for {}: manifest has {}, actual file has {actual}",
            path.display(),
            manifest.sha256
        ));
    }
    Ok(())
}

fn parse_catalog(bytes: &[u8], path: &Path, kind: CatalogKind) -> Result<ParsedCatalog, String> {
    let root = parse_json(bytes, path)?;
    let root = require_object(&root, &format!("{} catalog", kind.name()))?;

    let mut top_level_names: Vec<&String> = root.keys().collect();
    top_level_names.sort_unstable();

    let mut entries = Vec::new();
    let mut subclass_count = 0;
    for condition in top_level_names {
        validate_condition_component(condition, "top-level condition")?;
        if kind == CatalogKind::Kernel && !condition.starts_with("DELTA_KERNEL_") {
            return Err(format!(
                "custom condition {condition} must start with DELTA_KERNEL_"
            ));
        }
        if kind == CatalogKind::Oss && condition.starts_with("DELTA_KERNEL_") {
            return Err(format!(
                "OSS condition {condition} uses the reserved DELTA_KERNEL_ prefix"
            ));
        }

        let context = format!("{} condition {condition}", kind.name());
        let value = root
            .get(condition)
            .ok_or_else(|| format!("missing {context}"))?;
        let object = require_object(value, &context)?;
        reject_unknown_fields(object, &["message", "sqlState", "subClass"], &context)?;

        let parent_fragments = parse_message_fragments(object, &context)?;
        let parent_template = parent_fragments.join("\n");
        let sql_state = parse_sql_state(object, kind, &context)?;
        let is_aggregate = object
            .get("subClass")
            .and_then(Value::as_object)
            .is_some_and(|subclasses| !subclasses.is_empty());
        entries.push(make_entry(
            condition.to_string(),
            sql_state.clone(),
            &parent_fragments,
            parent_template.clone(),
            is_aggregate,
        )?);

        let Some(subclasses) = object.get("subClass") else {
            continue;
        };
        let subclasses = require_object(subclasses, &format!("{context}.subClass"))?;
        let mut subclass_names: Vec<&String> = subclasses.keys().collect();
        subclass_names.sort_unstable();
        for subclass in subclass_names {
            validate_condition_component(subclass, "subclass condition")?;
            let subclass_context = format!("{context}.{subclass}");
            let subclass_value = subclasses
                .get(subclass)
                .ok_or_else(|| format!("missing {subclass_context}"))?;
            let subclass_object = require_object(subclass_value, &subclass_context)?;
            reject_unknown_fields(subclass_object, &["message"], &subclass_context)?;
            let subclass_fragments = parse_message_fragments(subclass_object, &subclass_context)?;

            let full_condition = format!("{condition}.{subclass}");
            let mut all_fragments = parent_fragments.clone();
            all_fragments.extend(subclass_fragments.iter().cloned());
            let subclass_template = format!("{parent_template} {}", subclass_fragments.join("\n"));
            entries.push(make_entry(
                full_condition,
                sql_state.clone(),
                &all_fragments,
                subclass_template,
                false,
            )?);
            subclass_count += 1;
        }
    }

    Ok(ParsedCatalog {
        entries,
        top_level_count: root.len(),
        subclass_count,
    })
}

fn parse_json(bytes: &[u8], path: &Path) -> Result<Value, String> {
    serde_json::from_slice(bytes)
        .map(|UniqueJson(value)| value)
        .map_err(|error| format!("invalid JSON in {}: {error}", path.display()))
}

fn make_entry(
    condition: String,
    sql_state: Option<String>,
    fragments: &[String],
    message_template: String,
    is_aggregate: bool,
) -> Result<CatalogEntry, String> {
    let variant = condition_to_variant(&condition)?;
    let parameter_names = parse_parameter_names(fragments, &condition)?;
    Ok(CatalogEntry {
        condition,
        variant,
        sql_state,
        message_template,
        parameter_names,
        is_aggregate,
    })
}

fn parse_message_fragments(
    object: &Map<String, Value>,
    context: &str,
) -> Result<Vec<String>, String> {
    let message = require_field(object, "message", context)?;
    let fragments = message
        .as_array()
        .ok_or_else(|| format!("{context}.message must be an array"))?;
    if fragments.is_empty() {
        return Err(format!("{context}.message must not be empty"));
    }
    fragments
        .iter()
        .enumerate()
        .map(|(index, fragment)| {
            require_string(fragment, &format!("{context}.message[{index}]")).map(str::to_string)
        })
        .collect()
}

fn parse_sql_state(
    object: &Map<String, Value>,
    kind: CatalogKind,
    context: &str,
) -> Result<Option<String>, String> {
    let value = require_field(object, "sqlState", context)?;
    if value.is_null() {
        if kind == CatalogKind::Oss {
            return Err(format!("{context}.sqlState must not be null"));
        }
        return Ok(None);
    }
    let sql_state = require_string(value, &format!("{context}.sqlState"))?;
    if sql_state.len() != 5
        || !sql_state
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(format!(
            "{context}.sqlState must contain exactly five uppercase ASCII letters or digits"
        ));
    }
    Ok(Some(sql_state.to_string()))
}

fn parse_parameter_names(fragments: &[String], condition: &str) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for fragment in fragments {
        let bytes = fragment.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'<' => {
                    let start = index + 1;
                    let relative_end = bytes[start..]
                        .iter()
                        .position(|byte| *byte == b'>')
                        .ok_or_else(|| {
                            format!("{condition} has an unterminated placeholder in {fragment:?}")
                        })?;
                    let end = start + relative_end;
                    if bytes[start..end].contains(&b'<') {
                        return Err(format!(
                            "{condition} has a nested placeholder in {fragment:?}"
                        ));
                    }
                    let name = &fragment[start..end];
                    validate_parameter_name(name, condition)?;
                    if seen.insert(name.to_string()) {
                        names.push(name.to_string());
                    }
                    index = end + 1;
                }
                b'>' => {
                    return Err(format!(
                        "{condition} has an unmatched '>' in message fragment {fragment:?}"
                    ));
                }
                _ => index += 1,
            }
        }
    }
    Ok(names)
}

fn validate_parameter_name(name: &str, condition: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{condition} contains an empty placeholder"));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(format!(
            "{condition} has invalid placeholder <{name}>; parameter names must match [A-Za-z0-9_-]+"
        ));
    }
    Ok(())
}

fn merge_catalogs(oss: ParsedCatalog, kernel: ParsedCatalog) -> Result<Vec<CatalogEntry>, String> {
    let mut by_condition = BTreeMap::new();
    for entry in oss.entries {
        let condition = entry.condition.clone();
        if by_condition
            .insert(condition.clone(), (CatalogKind::Oss, entry))
            .is_some()
        {
            return Err(format!(
                "condition {condition} occurs more than once in the OSS catalog"
            ));
        }
    }
    for entry in kernel.entries {
        if let Some((existing_kind, _)) = by_condition.get(&entry.condition) {
            return Err(format!(
                "condition {} occurs in both the {} and kernel catalogs",
                entry.condition,
                existing_kind.name()
            ));
        }
        by_condition.insert(entry.condition.clone(), (CatalogKind::Kernel, entry));
    }

    let mut variants = BTreeMap::<String, String>::new();
    for entry in by_condition.values().map(|(_, entry)| entry) {
        if let Some(existing) = variants.insert(entry.variant.clone(), entry.condition.clone()) {
            return Err(format!(
                "conditions {existing} and {} both normalize to Rust variant {}",
                entry.condition, entry.variant
            ));
        }
    }

    Ok(by_condition.into_values().map(|(_, entry)| entry).collect())
}

fn condition_to_variant(condition: &str) -> Result<String, String> {
    let mut variant = String::new();
    for component in condition.split(|character: char| !character.is_ascii_alphanumeric()) {
        if component.is_empty() {
            continue;
        }
        let mut characters = component.chars();
        let Some(first) = characters.next() else {
            continue;
        };
        variant.push(first.to_ascii_uppercase());
        variant.extend(characters.map(|character| character.to_ascii_lowercase()));
    }
    if variant.is_empty()
        || !variant
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic())
    {
        return Err(format!(
            "condition {condition} does not normalize to a valid Rust enum variant"
        ));
    }
    Ok(variant)
}

fn validate_condition_component(value: &str, context: &str) -> Result<(), String> {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(format!("{context} must not be empty"));
    };
    if !first.is_ascii_uppercase()
        || !bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(format!(
            "{context} {value:?} must contain only uppercase ASCII letters, digits, and underscores and start with a letter"
        ));
    }
    Ok(())
}

fn render_catalog(entries: &[CatalogEntry]) -> Result<String, String> {
    let mut output = String::new();
    writeln!(
        output,
        "// @generated by kernel/build/error_catalog.rs. Do not edit.\n"
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        output,
        "/// Stable, string-identified Delta error conditions."
    )
    .map_err(|error| error.to_string())?;
    writeln!(output, "///").map_err(|error| error.to_string())?;
    writeln!(
        output,
        "/// Enum layout and discriminant values are deliberately unspecified. Persist or transmit"
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        output,
        "/// [`Self::condition`] rather than casting this enum to an integer."
    )
    .map_err(|error| error.to_string())?;
    writeln!(output, "#[non_exhaustive]").map_err(|error| error.to_string())?;
    writeln!(output, "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]")
        .map_err(|error| error.to_string())?;
    writeln!(output, "pub enum DeltaErrorCode {{").map_err(|error| error.to_string())?;
    for entry in entries {
        if entry.is_aggregate {
            writeln!(
                output,
                "    /// Aggregate Delta error class `{}` with dotted subclasses.",
                entry.condition
            )
            .map_err(|error| error.to_string())?;
        } else {
            writeln!(
                output,
                "    /// Delta error condition `{}`.",
                entry.condition
            )
            .map_err(|error| error.to_string())?;
        }
        writeln!(output, "    {},", entry.variant).map_err(|error| error.to_string())?;
    }
    writeln!(output, "}}\n").map_err(|error| error.to_string())?;

    writeln!(output, "impl DeltaErrorCode {{").map_err(|error| error.to_string())?;
    writeln!(
        output,
        "    /// Returns every known Delta error code in lexical condition order."
    )
    .map_err(|error| error.to_string())?;
    writeln!(output, "    pub const fn all() -> &'static [Self] {{")
        .map_err(|error| error.to_string())?;
    writeln!(output, "        ALL_DELTA_ERROR_CODES").map_err(|error| error.to_string())?;
    writeln!(output, "    }}\n").map_err(|error| error.to_string())?;

    writeln!(
        output,
        "    /// Returns the stable string identity of this condition."
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        output,
        "    pub const fn condition(self) -> &'static str {{"
    )
    .map_err(|error| error.to_string())?;
    writeln!(output, "        match self {{").map_err(|error| error.to_string())?;
    for entry in entries {
        writeln!(
            output,
            "            Self::{} => {:?},",
            entry.variant, entry.condition
        )
        .map_err(|error| error.to_string())?;
    }
    writeln!(output, "        }}").map_err(|error| error.to_string())?;
    writeln!(output, "    }}\n").map_err(|error| error.to_string())?;

    writeln!(
        output,
        "    /// Returns the SQLSTATE associated with this condition, when defined."
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        output,
        "    pub const fn sql_state(self) -> Option<&'static str> {{"
    )
    .map_err(|error| error.to_string())?;
    writeln!(output, "        match self {{").map_err(|error| error.to_string())?;
    for entry in entries {
        match &entry.sql_state {
            Some(sql_state) => writeln!(
                output,
                "            Self::{} => Some({sql_state:?}),",
                entry.variant
            ),
            None => writeln!(output, "            Self::{} => None,", entry.variant),
        }
        .map_err(|error| error.to_string())?;
    }
    writeln!(output, "        }}").map_err(|error| error.to_string())?;
    writeln!(output, "    }}\n").map_err(|error| error.to_string())?;

    writeln!(
        output,
        "    /// Returns the ordered, deduplicated names of this condition's message parameters."
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        output,
        "    pub const fn parameter_names(self) -> &'static [&'static str] {{"
    )
    .map_err(|error| error.to_string())?;
    writeln!(output, "        match self {{").map_err(|error| error.to_string())?;
    for entry in entries {
        let names = render_string_slice(&entry.parameter_names);
        writeln!(output, "            Self::{} => &{names},", entry.variant)
            .map_err(|error| error.to_string())?;
    }
    writeln!(output, "        }}").map_err(|error| error.to_string())?;
    writeln!(output, "    }}\n").map_err(|error| error.to_string())?;

    writeln!(
        output,
        "    /// Returns the diagnostic message template for this condition."
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        output,
        "    pub(crate) const fn message_template(self) -> &'static str {{"
    )
    .map_err(|error| error.to_string())?;
    writeln!(output, "        match self {{").map_err(|error| error.to_string())?;
    for entry in entries {
        writeln!(
            output,
            "            Self::{} => {:?},",
            entry.variant, entry.message_template
        )
        .map_err(|error| error.to_string())?;
    }
    writeln!(output, "        }}").map_err(|error| error.to_string())?;
    writeln!(output, "    }}\n").map_err(|error| error.to_string())?;

    writeln!(
        output,
        "    /// Resolves a Delta error code from its exact, case-sensitive condition string."
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        output,
        "    pub fn from_condition(condition: &str) -> Option<Self> {{"
    )
    .map_err(|error| error.to_string())?;
    writeln!(output, "        match condition {{").map_err(|error| error.to_string())?;
    for entry in entries {
        writeln!(
            output,
            "            {:?} => Some(Self::{}),",
            entry.condition, entry.variant
        )
        .map_err(|error| error.to_string())?;
    }
    writeln!(output, "            _ => None,").map_err(|error| error.to_string())?;
    writeln!(output, "        }}").map_err(|error| error.to_string())?;
    writeln!(output, "    }}").map_err(|error| error.to_string())?;
    writeln!(output, "}}\n").map_err(|error| error.to_string())?;

    writeln!(
        output,
        "const ALL_DELTA_ERROR_CODES: &[DeltaErrorCode] = &["
    )
    .map_err(|error| error.to_string())?;
    for entry in entries {
        writeln!(output, "    DeltaErrorCode::{},", entry.variant)
            .map_err(|error| error.to_string())?;
    }
    writeln!(output, "];\n").map_err(|error| error.to_string())?;

    Ok(output)
}

fn render_string_slice(values: &[String]) -> String {
    let mut output = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&format!("{value:?}"));
    }
    output.push(']');
    output
}

fn require_object<'a>(value: &'a Value, context: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{context} must be a JSON object"))
}

fn require_field<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    context: &str,
) -> Result<&'a Value, String> {
    object
        .get(field)
        .ok_or_else(|| format!("{context} is missing required field {field:?}"))
}

fn require_string<'a>(value: &'a Value, context: &str) -> Result<&'a str, String> {
    value
        .as_str()
        .ok_or_else(|| format!("{context} must be a string"))
}

fn require_usize(value: &Value, context: &str) -> Result<usize, String> {
    let value = value
        .as_u64()
        .ok_or_else(|| format!("{context} must be a non-negative integer"))?;
    usize::try_from(value).map_err(|_| format!("{context} does not fit in usize"))
}

fn reject_unknown_fields(
    object: &Map<String, Value>,
    allowed: &[&str],
    context: &str,
) -> Result<(), String> {
    let allowed: BTreeSet<&str> = allowed.iter().copied().collect();
    let unknown: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|field| !allowed.contains(field))
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{context} has unsupported field(s): {}",
            unknown.join(", ")
        ))
    }
}
