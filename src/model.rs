use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Project {
    pub name: String,
    pub description: String,
    pub base_url: String,
    pub environments: Vec<EnvironmentProfile>,
    pub active_environment: String,
    pub global_headers: Vec<KeyValuePair>,
    pub vuser_config: VUserConfig,
    pub modules: Vec<TestModule>,
}

impl Default for Project {
    fn default() -> Self {
        Self {
            name: "New Load Test Project".into(),
            description: String::new(),
            base_url: "https://httpbin.org".into(),
            environments: vec![],
            active_environment: "Default".into(),
            global_headers: vec![],
            vuser_config: VUserConfig::default(),
            modules: vec![TestModule::default()],
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EnvironmentProfile {
    pub name: String,
    pub base_url: String,
    pub variables: Vec<KeyValuePair>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TestModule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub test_cases: Vec<TestCase>,
}

impl Default for TestModule {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: "New Module".into(),
            description: String::new(),
            enabled: true,
            test_cases: vec![TestCase::default()],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TestCase {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub steps: Vec<TestStep>,
}

impl Default for TestCase {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: "New Test Case".into(),
            description: String::new(),
            enabled: true,
            steps: vec![TestStep::default()],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TestStep {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub delay_ms: u64,
    pub request: HttpRequestConfig,
    pub extractors: Vec<VariableExtractor>,
    pub assertions: Vec<StepAssertion>,
    pub post_script: String,
}

impl Default for TestStep {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: "GET /get".into(),
            enabled: true,
            delay_ms: 0,
            request: HttpRequestConfig::default(),
            extractors: vec![],
            assertions: vec![],
            post_script: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct HttpRequestConfig {
    pub method: HttpMethod,
    pub url: String,
    pub query_params: Vec<KeyValuePair>,
    pub path_variables: Vec<KeyValuePair>,
    pub headers: Vec<KeyValuePair>,
    pub body_type: BodyType,
    pub body: String,
    pub timeout_ms: u64,
}

impl Default for HttpRequestConfig {
    fn default() -> Self {
        Self {
            method: HttpMethod::Get,
            url: "/get".into(),
            query_params: vec![],
            path_variables: vec![],
            headers: vec![],
            body_type: BodyType::None,
            body: String::new(),
            timeout_ms: 10_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BodyType {
    #[default]
    None,
    Json,
    Raw,
    FormData,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct KeyValuePair {
    pub enabled: bool,
    pub key: String,
    pub value: String,
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct VUserConfig {
    pub thread_count: usize,
    pub ramp_up_seconds: usize,
    pub loop_count: usize,
    pub duration_based: bool,
    pub duration_seconds: usize,
    pub test_mode: String,
    pub use_csv_data: bool,
    pub csv_file_path: String,
    pub csv_variable_names: String,
    pub csv_delimiter: String,
    pub recycle_on_eof: bool,
    pub stop_thread_on_eof: bool,
    pub sharing_mode: String,
}

impl Default for VUserConfig {
    fn default() -> Self {
        Self {
            thread_count: 10,
            ramp_up_seconds: 5,
            loop_count: 1,
            duration_based: false,
            duration_seconds: 60,
            test_mode: "0 Custom".into(),
            use_csv_data: false,
            csv_file_path: String::new(),
            csv_variable_names: String::new(),
            csv_delimiter: ",".into(),
            recycle_on_eof: true,
            stop_thread_on_eof: false,
            sharing_mode: "shareMode.all".into(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct VariableExtractor {
    pub enabled: bool,
    #[serde(rename = "type")]
    pub extractor_type: ExtractorType,
    pub variable_name: String,
    pub expression: String,
    pub default_value: String,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExtractorType {
    #[default]
    JsonPath,
    Regex,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StepAssertion {
    pub enabled: bool,
    #[serde(rename = "type")]
    pub assertion_type: AssertionType,
    pub expected_value: String,
    pub description: String,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AssertionType {
    #[default]
    StatusCode,
    ResponseBodyContains,
    ResponseTimeLt,
    JsonPathExists,
}

impl Project {
    pub fn active_base_url(&self) -> &str {
        self.environments
            .iter()
            .find(|environment| {
                environment.name == self.active_environment && !environment.base_url.is_empty()
            })
            .map(|environment| environment.base_url.as_str())
            .unwrap_or(self.base_url.as_str())
    }

    pub fn active_base_url_mut(&mut self) -> &mut String {
        if let Some(environment) = self
            .environments
            .iter_mut()
            .find(|environment| environment.name == self.active_environment)
        {
            &mut environment.base_url
        } else {
            &mut self.base_url
        }
    }

    pub fn active_variables(&self) -> impl Iterator<Item = (&str, &str)> {
        self.environments
            .iter()
            .find(|environment| environment.name == self.active_environment)
            .into_iter()
            .flat_map(|environment| environment.variables.iter())
            .filter(|pair| pair.enabled && !pair.key.is_empty())
            .map(|pair| (pair.key.as_str(), pair.value.as_str()))
    }
}
