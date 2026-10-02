use crate::model::{
    AssertionType, BodyType, ExtractorType, HttpMethod, Project, TestStep, VariableExtractor,
};
use anyhow::{Context, Result};
use goose::prelude::*;
use regex::Regex;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock, mpsc::SyncSender};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use url::Url;

static ACTIVE_RUN: OnceLock<RwLock<Option<Arc<RunContext>>>> = OnceLock::new();

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleRecord {
    pub timestamp_ms: u128,
    pub name: String,
    pub method: String,
    pub url: String,
    pub status: u16,
    pub elapsed_ms: u64,
    pub success: bool,
    pub response_body: String,
    pub error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSummary {
    pub project_name: String,
    pub started_ms: u128,
    pub duration_ms: u128,
    pub requests: u64,
    pub errors: u64,
    pub average_ms: f64,
    pub p90_ms: u64,
    pub status: String,
    pub recent_samples: Vec<SampleRecord>,
}

#[derive(Debug)]
pub enum EngineEvent {
    Started {
        project_name: String,
        started_ms: u128,
    },
    Sample(SampleRecord),
    Finished(RunSummary),
    Failed(String),
}

struct RunContext {
    project: Project,
    event_tx: SyncSender<EngineEvent>,
    csv_rows: Vec<HashMap<String, String>>,
    csv_cursor: AtomicUsize,
    requests: AtomicU64,
    errors: AtomicU64,
    latency_ms: RwLock<Vec<u64>>,
    recent_samples: RwLock<Vec<SampleRecord>>,
}

#[derive(Clone, Debug)]
struct UserSession {
    variables: HashMap<String, String>,
    csv_row: usize,
    iteration: usize,
}

pub async fn run_project(project: Project, event_tx: SyncSender<EngineEvent>) -> Result<()> {
    validate_project(&project)?;
    let csv_rows = load_csv_rows(&project)?;
    let started_ms = epoch_ms();
    let context = Arc::new(RunContext {
        project: project.clone(),
        event_tx: event_tx.clone(),
        csv_rows,
        csv_cursor: AtomicUsize::new(0),
        requests: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        latency_ms: RwLock::new(Vec::new()),
        recent_samples: RwLock::new(Vec::new()),
    });

    let active = ACTIVE_RUN.get_or_init(|| RwLock::new(None));
    *active.write().expect("run context lock poisoned") = Some(context.clone());
    let _ = event_tx.try_send(EngineEvent::Started {
        project_name: project.name.clone(),
        started_ms,
    });

    let result = execute_goose(project.active_base_url(), &project).await;
    let duration_ms = epoch_ms().saturating_sub(started_ms);
    let mut latency = context
        .latency_ms
        .read()
        .expect("latency lock poisoned")
        .clone();
    latency.sort_unstable();
    let average_ms = if latency.is_empty() {
        0.0
    } else {
        latency.iter().map(|value| *value as u128).sum::<u128>() as f64 / latency.len() as f64
    };
    let p90_ms = percentile(&latency, 0.90);
    let recent_samples = context
        .recent_samples
        .read()
        .expect("sample lock poisoned")
        .clone();
    let summary = RunSummary {
        project_name: project.name.clone(),
        started_ms,
        duration_ms,
        requests: context.requests.load(Ordering::Relaxed),
        errors: context.errors.load(Ordering::Relaxed),
        average_ms,
        p90_ms,
        status: if result.is_ok() { "FINISHED" } else { "ERROR" }.into(),
        recent_samples,
    };

    *active.write().expect("run context lock poisoned") = None;
    match result {
        Ok(()) => {
            let _ = event_tx.try_send(EngineEvent::Finished(summary));
            Ok(())
        }
        Err(error) => {
            let message = format!("{error:#}");
            Err(anyhow::anyhow!(message))
        }
    }
}

async fn execute_goose(host: &str, project: &Project) -> Result<()> {
    let config = &project.vuser_config;
    let startup_time = config.ramp_up_seconds;
    let attack = GooseAttack::initialize().context("could not initialize Goose")?;
    let attack = attack.set_default(GooseDefault::Host, host)?;
    let attack = attack.set_default(GooseDefault::Users, config.thread_count.max(1))?;
    let attack = attack.set_default(GooseDefault::StartupTime, startup_time)?;
    let attack = attack.set_default(GooseDefault::NoPrintMetrics, true)?;
    let attack = if config.duration_based {
        attack.set_default(GooseDefault::RunTime, config.duration_seconds.max(1))?
    } else {
        attack.set_default(GooseDefault::Iterations, config.loop_count.max(1))?
    };

    attack
        .register_scenario(
            scenario!("EchoLoad Project")
                .register_transaction(transaction!(initialize_user).set_on_start())
                .register_transaction(transaction!(execute_project)),
        )
        .execute()
        .await
        .map(|_| ())
        .context("Goose load run failed")
}

async fn initialize_user(user: &mut GooseUser) -> TransactionResult {
    let Some(context) = current_context() else {
        return Ok(());
    };
    let row = context.csv_cursor.fetch_add(1, Ordering::Relaxed);
    let mut variables = context
        .project
        .active_variables()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect::<HashMap<_, _>>();
    if let Some(csv_row) = context.csv_rows.get(row % context.csv_rows.len().max(1)) {
        variables.extend(csv_row.clone());
    }
    user.set_session_data(UserSession {
        variables,
        csv_row: row,
        iteration: 0,
    });
    Ok(())
}

async fn execute_project(user: &mut GooseUser) -> TransactionResult {
    let Some(context) = current_context() else {
        return Ok(());
    };
    let Some(mut session) = user.get_session_data::<UserSession>().cloned() else {
        return Ok(());
    };
    session.iteration += 1;
    if !context.csv_rows.is_empty() && session.iteration > 1 {
        let row = if context.project.vuser_config.recycle_on_eof {
            session.csv_row.wrapping_add(session.iteration - 1) % context.csv_rows.len()
        } else {
            (session.csv_row + session.iteration - 1).min(context.csv_rows.len() - 1)
        };
        session.variables.extend(context.csv_rows[row].clone());
    }

    for module in context
        .project
        .modules
        .iter()
        .filter(|module| module.enabled)
    {
        for case in module.test_cases.iter().filter(|case| case.enabled) {
            for step in case.steps.iter().filter(|step| step.enabled) {
                execute_step(user, &context, &mut session, step).await;
                if step.delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(step.delay_ms)).await;
                }
            }
        }
    }
    user.set_session_data(session);
    Ok(())
}

async fn execute_step(
    user: &mut GooseUser,
    context: &RunContext,
    session: &mut UserSession,
    step: &TestStep,
) {
    let method_text = format!("{:?}", step.request.method).to_uppercase();
    let method = match reqwest_method(step.request.method) {
        Ok(method) => method,
        Err(error) => {
            emit_error(context, step, method_text, error.to_string());
            return;
        }
    };
    let raw_url = substitute(&step.request.url, &session.variables);
    let joined_url = match Url::parse(&raw_url).or_else(|_| {
        Url::parse(context.project.active_base_url()).and_then(|base| base.join(&raw_url))
    }) {
        Ok(url) => url,
        Err(error) => {
            emit_error(context, step, method_text, format!("invalid URL: {error}"));
            return;
        }
    };
    let origin = joined_url.origin().ascii_serialization();
    if user.set_base_url(&origin).is_err() {
        emit_error(
            context,
            step,
            method_text,
            "could not set request host".into(),
        );
        return;
    }
    let mut path = format!(
        "{}{}",
        joined_url.path(),
        joined_url
            .query()
            .map(|query| format!("?{query}"))
            .unwrap_or_default()
    );
    for variable in &step.request.path_variables {
        if variable.enabled {
            let value = substitute(&variable.value, &session.variables);
            path = path.replace(&format!(":{}", variable.key), &value);
            path = path.replace(&format!("{{{}}}", variable.key), &value);
        }
    }
    let goose_method = match method {
        Method::GET => GooseMethod::Get,
        Method::POST => GooseMethod::Post,
        Method::PUT => GooseMethod::Put,
        Method::DELETE => GooseMethod::Delete,
        Method::PATCH => GooseMethod::Patch,
        Method::HEAD => GooseMethod::Head,
        Method::OPTIONS => GooseMethod::Get,
        _ => GooseMethod::Get,
    };
    let mut builder = if method == Method::OPTIONS {
        match user.build_url(&path) {
            Ok(url) => user.client.request(Method::OPTIONS, url),
            Err(error) => {
                emit_error(
                    context,
                    step,
                    method_text,
                    format!("request setup failed: {error}"),
                );
                return;
            }
        }
    } else {
        match user.get_request_builder(&goose_method, &path) {
            Ok(builder) => builder,
            Err(error) => {
                emit_error(
                    context,
                    step,
                    method_text,
                    format!("request setup failed: {error}"),
                );
                return;
            }
        }
    };
    builder = builder.timeout(Duration::from_millis(step.request.timeout_ms.max(1)));
    for pair in context
        .project
        .global_headers
        .iter()
        .chain(step.request.headers.iter())
    {
        if pair.enabled && !pair.key.trim().is_empty() {
            builder = builder.header(pair.key.trim(), substitute(&pair.value, &session.variables));
        }
    }
    let query_params = step
        .request
        .query_params
        .iter()
        .filter(|pair| pair.enabled && !pair.key.is_empty())
        .map(|pair| {
            (
                pair.key.clone(),
                substitute(&pair.value, &session.variables),
            )
        })
        .collect::<Vec<_>>();
    if !query_params.is_empty() {
        builder = builder.query(&query_params);
    }
    let body = substitute(&step.request.body, &session.variables);
    if step.request.body_type != BodyType::None && !body.is_empty() {
        if step.request.body_type == BodyType::Json
            && !step
                .request
                .headers
                .iter()
                .any(|h| h.key.eq_ignore_ascii_case("content-type"))
        {
            builder = builder.header("content-type", "application/json");
        }
        if step.request.body_type == BodyType::FormData
            && !step
                .request
                .headers
                .iter()
                .any(|h| h.key.eq_ignore_ascii_case("content-type"))
        {
            builder = builder.header("content-type", "application/x-www-form-urlencoded");
        }
        builder = builder.body(body.clone());
    }

    let request = GooseRequest::builder()
        .path(path.as_str())
        .method(goose_method)
        .name(step.name.as_str())
        .set_request_builder(builder)
        .build();
    let started = Instant::now();
    match user.request(request).await {
        Ok(mut goose_response) => {
            let elapsed_ms = started.elapsed().as_millis() as u64;
            let status = goose_response
                .response
                .as_ref()
                .map(|response| response.status().as_u16())
                .unwrap_or(0);
            let response_body = match goose_response.response {
                Ok(response) => response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("[body read error: {error}]")),
                Err(error) => format!("[request error: {error}]"),
            };
            let assertion_error = check_assertions(step, status, elapsed_ms, &response_body);
            if let Some(reason) = assertion_error.as_deref() {
                let _ = user.set_failure(
                    reason,
                    &mut goose_response.request,
                    None,
                    Some(&response_body),
                );
            }
            let success = status != 0 && (200..300).contains(&status) && assertion_error.is_none();
            let record = SampleRecord {
                timestamp_ms: epoch_ms(),
                name: step.name.clone(),
                method: method_text,
                url: joined_url.to_string(),
                status,
                elapsed_ms,
                success,
                response_body: truncate(&response_body, 16_384),
                error: assertion_error.unwrap_or_default(),
            };
            extract_variables(
                step.extractors.as_slice(),
                &response_body,
                &mut session.variables,
            );
            context.requests.fetch_add(1, Ordering::Relaxed);
            if !success {
                context.errors.fetch_add(1, Ordering::Relaxed);
            }
            if let Ok(mut latencies) = context.latency_ms.write() {
                if latencies.len() < 100_000 {
                    latencies.push(elapsed_ms);
                }
            }
            if let Ok(mut recent) = context.recent_samples.write() {
                if recent.len() >= 1_500 {
                    recent.remove(0);
                }
                recent.push(record.clone());
            }
            let _ = context.event_tx.try_send(EngineEvent::Sample(record));
        }
        Err(error) => emit_error(
            context,
            step,
            method_text,
            format!("request failed: {error}"),
        ),
    }
}

fn check_assertions(
    step: &TestStep,
    status: u16,
    elapsed_ms: u64,
    response_body: &str,
) -> Option<String> {
    for assertion in step.assertions.iter().filter(|assertion| assertion.enabled) {
        let passed = match assertion.assertion_type {
            AssertionType::StatusCode => {
                assertion.expected_value.parse::<u16>().ok() == Some(status)
            }
            AssertionType::ResponseBodyContains => {
                response_body.contains(&assertion.expected_value)
            }
            AssertionType::ResponseTimeLt => assertion
                .expected_value
                .parse::<u64>()
                .map(|limit| elapsed_ms < limit)
                .unwrap_or(false),
            AssertionType::JsonPathExists => {
                json_path_value(response_body, &assertion.expected_value).is_some()
            }
        };
        if !passed {
            return Some(if assertion.description.is_empty() {
                format!(
                    "assertion failed: {:?} expected {}",
                    assertion.assertion_type, assertion.expected_value
                )
            } else {
                assertion.description.clone()
            });
        }
    }
    None
}

fn extract_variables(
    extractors: &[VariableExtractor],
    body: &str,
    variables: &mut HashMap<String, String>,
) {
    for extractor in extractors
        .iter()
        .filter(|extractor| extractor.enabled && !extractor.variable_name.is_empty())
    {
        let value = match extractor.extractor_type {
            ExtractorType::JsonPath => json_path_value(body, &extractor.expression),
            ExtractorType::Regex => Regex::new(&extractor.expression).ok().and_then(|regex| {
                regex.captures(body).and_then(|captures| {
                    captures
                        .get(1)
                        .or_else(|| captures.get(0))
                        .map(|value| value.as_str().to_owned())
                })
            }),
        };
        if let Some(value) = value {
            variables.insert(extractor.variable_name.clone(), value);
        } else if !extractor.default_value.is_empty() {
            variables.insert(
                extractor.variable_name.clone(),
                extractor.default_value.clone(),
            );
        }
    }
}

fn json_path_value(body: &str, path: &str) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(body).ok()?;
    let mut current = &json;
    for part in path
        .trim_start_matches("$")
        .trim_start_matches('.')
        .split('.')
    {
        if part.is_empty() {
            continue;
        }
        if let Some((key, index)) = part.strip_suffix(']').and_then(|v| v.split_once('[')) {
            current = current.get(key)?.get(index.parse::<usize>().ok()?)?;
        } else {
            current = current.get(part)?;
        }
    }
    Some(match current {
        serde_json::Value::String(value) => value.clone(),
        _ => current.to_string(),
    })
}

fn substitute(template: &str, variables: &HashMap<String, String>) -> String {
    let mut output = template.to_owned();
    for (name, value) in variables {
        output = output.replace(&format!("${{{name}}}"), value);
    }
    output
}

fn load_csv_rows(project: &Project) -> Result<Vec<HashMap<String, String>>> {
    let config = &project.vuser_config;
    if !config.use_csv_data || config.csv_file_path.trim().is_empty() {
        return Ok(vec![]);
    }
    let path = Path::new(&config.csv_file_path);
    let delimiter = match config.csv_delimiter.as_str() {
        "\\t" | "\t" => b'\t',
        ";" => b';',
        "|" => b'|',
        _ => b',',
    };
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .from_path(path)
        .with_context(|| format!("could not read CSV file {}", path.display()))?;
    let headers = reader.headers()?.clone();
    reader
        .records()
        .map(|record| {
            let record = record?;
            Ok(headers
                .iter()
                .zip(record.iter())
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect())
        })
        .collect()
}

fn validate_project(project: &Project) -> Result<()> {
    anyhow::ensure!(
        !project.active_base_url().trim().is_empty(),
        "Base URL is required"
    );
    Url::parse(project.active_base_url())
        .context("Base URL must include a valid scheme and host")?;
    anyhow::ensure!(
        project.vuser_config.thread_count > 0,
        "Virtual users must be at least one"
    );
    anyhow::ensure!(
        project.modules.iter().any(|module| module.enabled
            && module
                .test_cases
                .iter()
                .any(|case| case.enabled && case.steps.iter().any(|step| step.enabled))),
        "Enable at least one request step"
    );
    Ok(())
}

fn reqwest_method(method: HttpMethod) -> Result<Method> {
    Ok(match method {
        HttpMethod::Get => Method::GET,
        HttpMethod::Post => Method::POST,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
        HttpMethod::Patch => Method::PATCH,
        HttpMethod::Head => Method::HEAD,
        HttpMethod::Options => Method::OPTIONS,
    })
}

fn current_context() -> Option<Arc<RunContext>> {
    ACTIVE_RUN.get()?.read().ok()?.as_ref().cloned()
}

fn emit_error(context: &RunContext, step: &TestStep, method: String, message: String) {
    let record = SampleRecord {
        timestamp_ms: epoch_ms(),
        name: step.name.clone(),
        method,
        url: step.request.url.clone(),
        status: 0,
        elapsed_ms: 0,
        success: false,
        response_body: String::new(),
        error: message,
    };
    context.requests.fetch_add(1, Ordering::Relaxed);
    context.errors.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut recent) = context.recent_samples.write() {
        if recent.len() >= 1_500 {
            recent.remove(0);
        }
        recent.push(record.clone());
    }
    let _ = context.event_tx.try_send(EngineEvent::Sample(record));
}

fn percentile(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn truncate(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

fn epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
