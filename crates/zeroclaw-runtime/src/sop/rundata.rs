use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::types::{SopStepResult, SopStepStatus};

/// Accumulated step outputs, shared by schema validation and routing.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunData {
    pub outputs: BTreeMap<u32, Value>,
}

impl RunData {
    pub fn from_step_results(results: &[SopStepResult]) -> Self {
        let mut data = Self::default();
        for result in results {
            if result.status == SopStepStatus::Completed {
                data.insert_output_str(result.step_number, &result.output);
            }
        }
        data
    }

    pub fn insert_output(&mut self, step_number: u32, output: Value) {
        self.outputs.insert(step_number, output);
    }

    pub fn insert_output_str(&mut self, step_number: u32, output: &str) {
        self.insert_output(step_number, parse_step_output_value(output));
    }

    pub fn merge(&mut self, other: RunData) {
        self.outputs.extend(other.outputs);
    }

    pub fn to_payload(&self) -> Value {
        let steps = self
            .outputs
            .iter()
            .map(|(step, value)| (step.to_string(), value.clone()))
            .collect();
        json!({ "steps": Value::Object(steps) })
    }

    pub fn get_path(&self, path: &str) -> Option<Value> {
        if path.is_empty() {
            return None;
        }
        let payload = self.to_payload();
        let pointer = path
            .strip_prefix("$.")
            .map(|rest| format!("/{}", rest.replace('.', "/")))
            .unwrap_or_else(|| path.to_string());
        payload.pointer(&pointer).cloned()
    }
}

/// Parse a SOP step's raw output text into a JSON value.
///
/// Step schemas expect a bare JSON object, but chat-tuned models often wrap it in
/// prose and Markdown code fences (` ```json … ``` `) — notably under Anthropic's
/// OAuth "Claude Code" identity, but common across providers. This recovers the
/// JSON object from those wrappers before falling back to the raw string, so a
/// well-formed answer buried in prose validates instead of failing as
/// `expected object, got string`. Purely additive: output that already parses as
/// JSON is returned unchanged, and genuinely non-JSON output still yields
/// `Value::String`.
pub(crate) fn parse_step_output_value(raw: &str) -> Value {
    let trimmed = raw.trim();

    // 1. Fast path: the output is already valid JSON.
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return value;
    }

    // 2. The body of a Markdown code fence, e.g. ```json { … } ```.
    if let Some(body) = fenced_block_body(trimmed)
        && let Ok(value) = serde_json::from_str::<Value>(body.trim())
    {
        return value;
    }

    // 3. The first brace-balanced { … } object embedded in surrounding prose.
    if let Some(candidate) = first_balanced_object(trimmed)
        && let Ok(value) = serde_json::from_str::<Value>(candidate)
    {
        return value;
    }

    // 4. Not recoverable as JSON — preserve the raw text (previous behavior).
    Value::String(raw.into())
}

/// Body of the first Markdown code fence, skipping an optional info string on the
/// opening line (` ```json `). Returns `None` when there is no closing fence.
fn fenced_block_body(text: &str) -> Option<&str> {
    let after_open = &text[text.find("```")? + 3..];
    let body_start = after_open.find('\n').map(|nl| nl + 1)?;
    let body = &after_open[body_start..];
    let close = body.find("```")?;
    Some(&body[..close])
}

/// First brace-balanced `{ … }` substring, ignoring braces inside string literals
/// so an embedded `"{"` / `"}"` cannot unbalance the scan.
fn first_balanced_object(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let start = bytes.iter().position(|&b| b == b'{')?;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, &b) in bytes[start..].iter().enumerate() {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[start..=start + offset]);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_data_parses_json_outputs() {
        let mut data = RunData::default();
        data.insert_output_str(1, r#"{"ok":true}"#);

        assert_eq!(data.outputs[&1]["ok"], true);
    }

    #[test]
    fn run_data_keeps_plain_outputs_as_strings() {
        let mut data = RunData::default();
        data.insert_output_str(2, "plain text");

        assert_eq!(data.outputs[&2], Value::String("plain text".into()));
    }

    #[test]
    fn parses_fenced_json_wrapped_in_prose() {
        // The shape a chat-tuned / "Claude Code"-identity model commonly emits.
        let raw = "The run has ended — final status is **failed**.\n\n\
            ```json\n{\n  \"status\": \"ok\",\n  \"word_count\": 738\n}\n```\n\n\
            Summary: the run stopped safely at Step 1.";
        let value = parse_step_output_value(raw);
        assert_eq!(value["status"], "ok");
        assert_eq!(value["word_count"], 738);
    }

    #[test]
    fn parses_bare_fenced_json_without_lang_tag() {
        let raw = "```\n{\"outcome\": \"stopped\"}\n```";
        assert_eq!(parse_step_output_value(raw)["outcome"], "stopped");
    }

    #[test]
    fn parses_object_embedded_in_prose_without_fences() {
        let raw = "Here is the result: {\"preferences_loaded\": true} — done.";
        assert_eq!(
            parse_step_output_value(raw)["preferences_loaded"],
            Value::Bool(true)
        );
    }

    #[test]
    fn braces_inside_strings_do_not_unbalance_extraction() {
        let raw = "note: {\"summary\": \"contains a } brace and { another\"} tail";
        let value = parse_step_output_value(raw);
        assert_eq!(value["summary"], "contains a } brace and { another");
    }

    #[test]
    fn non_json_prose_still_falls_back_to_string() {
        let raw = "I could not complete the extraction step.";
        assert_eq!(parse_step_output_value(raw), Value::String(raw.into()));
    }

    #[test]
    fn run_data_ignores_failed_and_skipped_outputs() {
        let results = vec![
            SopStepResult {
                effective_agent: None,
                step_number: 1,
                status: SopStepStatus::Failed,
                output: r#"{"failed":true}"#.into(),
                started_at: "now".into(),
                completed_at: Some("now".into()),
                tool_calls: Vec::new(),
            },
            SopStepResult {
                effective_agent: None,
                step_number: 2,
                status: SopStepStatus::Skipped,
                output: r#"{"skipped":true}"#.into(),
                started_at: "now".into(),
                completed_at: Some("now".into()),
                tool_calls: Vec::new(),
            },
            SopStepResult {
                effective_agent: None,
                step_number: 3,
                status: SopStepStatus::Completed,
                output: r#"{"ok":true}"#.into(),
                started_at: "now".into(),
                completed_at: Some("now".into()),
                tool_calls: Vec::new(),
            },
        ];

        let data = RunData::from_step_results(&results);

        assert!(!data.outputs.contains_key(&1));
        assert!(!data.outputs.contains_key(&2));
        assert_eq!(data.outputs[&3]["ok"], true);
    }
}
