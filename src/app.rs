use crate::engine::{self, EngineEvent, RunSummary, SampleRecord};
use crate::model::{
    AssertionType, BodyType, EnvironmentProfile, ExtractorType, HttpMethod, KeyValuePair, Project,
    StepAssertion, VariableExtractor,
};
use eframe::egui::{self, Color32, RichText, Stroke, Vec2};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Duration;

const MAX_VISIBLE_SAMPLES: usize = 1_500;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Page {
    Plan,
    StepEditor,
    VUsers,
    Monitor,
    Summary,
    History,
}

struct HistoryEntry {
    path: PathBuf,
    summary: RunSummary,
}

pub struct RoadcuseApp {
    project: Project,
    project_path: Option<PathBuf>,
    page: Page,
    selected_case: (usize, usize),
    selected_step: Option<(usize, usize, usize)>,
    step_editor_tab: usize,
    run_config_tab: usize,
    summary_filter: usize,
    status: String,
    is_running: bool,
    stop_requested: bool,
    event_rx: Option<Receiver<EngineEvent>>,
    preview_rx: Option<Receiver<SampleRecord>>,
    records: VecDeque<SampleRecord>,
    selected_record: Option<SampleRecord>,
    preview_record: Option<SampleRecord>,
    preview_pending: bool,
    detail_tab: usize,
    requests: u64,
    errors: u64,
    latencies: VecDeque<u64>,
    timeline_latency: VecDeque<f32>,
    timeline_tps: VecDeque<f32>,
    last_second: u128,
    last_request_count: u64,
    started_ms: u128,
    summary: Option<RunSummary>,
    history: Vec<HistoryEntry>,
}

impl RoadcuseApp {
    pub fn new(_creation: &eframe::CreationContext<'_>) -> Self {
        let mut visuals = egui::Visuals::dark();
        visuals.panel_fill = Color32::from_rgb(16, 23, 34);
        visuals.window_fill = Color32::from_rgb(22, 31, 46);
        visuals.extreme_bg_color = Color32::from_rgb(10, 15, 23);
        visuals.selection.bg_fill = Color32::from_rgb(35, 113, 154);
        _creation.egui_ctx.set_visuals(visuals);
        Self {
            project: Project::default(),
            project_path: None,
            page: Page::Plan,
            selected_case: (0, 0),
            selected_step: None,
            step_editor_tab: 0,
            run_config_tab: 0,
            summary_filter: 0,
            status: "Ready".into(),
            is_running: false,
            stop_requested: false,
            event_rx: None,
            preview_rx: None,
            records: VecDeque::new(),
            selected_record: None,
            preview_record: None,
            preview_pending: false,
            detail_tab: 1,
            requests: 0,
            errors: 0,
            latencies: VecDeque::new(),
            timeline_latency: VecDeque::new(),
            timeline_tps: VecDeque::new(),
            last_second: 0,
            last_request_count: 0,
            started_ms: 0,
            summary: None,
            history: load_history(),
        }
    }

    fn start_run(&mut self) {
        if self.is_running {
            return;
        }
        let (tx, rx) = mpsc::sync_channel(4_096);
        self.event_rx = Some(rx);
        self.records.clear();
        self.selected_record = None;
        self.requests = 0;
        self.errors = 0;
        self.latencies.clear();
        self.timeline_latency.clear();
        self.timeline_tps.clear();
        self.summary = None;
        self.last_second = 0;
        self.last_request_count = 0;
        let project = self.project.clone();
        self.status = "Starting Goose…".into();
        self.is_running = true;
        self.stop_requested = false;
        thread::spawn(move || run_on_worker(project, tx));
    }

    fn receive_engine_events(&mut self) {
        let mut previews = Vec::new();
        if let Some(receiver) = &self.preview_rx {
            while let Ok(record) = receiver.try_recv() {
                previews.push(record);
            }
        }
        for record in previews {
            self.preview_pending = false;
            self.status = if record.error.is_empty() {
                format!(
                    "Request completed · HTTP {} · {} ms",
                    record.status, record.elapsed_ms
                )
            } else {
                format!("Request failed · {}", record.error)
            };
            self.preview_record = Some(record);
        }
        let mut events = Vec::new();
        if let Some(receiver) = &self.event_rx {
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
        }
        for event in events {
            match event {
                EngineEvent::Started {
                    project_name,
                    started_ms,
                } => {
                    self.started_ms = started_ms;
                    self.status = format!("Running {project_name}");
                    self.page = Page::Monitor;
                }
                EngineEvent::Sample(record) => {
                    self.requests += 1;
                    self.errors += u64::from(!record.success);
                    if record.elapsed_ms > 0 {
                        if self.latencies.len() >= 100_000 {
                            self.latencies.pop_front();
                        }
                        self.latencies.push_back(record.elapsed_ms);
                    }
                    if self.records.len() >= MAX_VISIBLE_SAMPLES {
                        self.records.pop_front();
                    }
                    self.records.push_back(record);
                }
                EngineEvent::Finished(summary) => {
                    self.is_running = false;
                    self.requests = summary.requests;
                    self.errors = summary.errors;
                    self.status = format!(
                        "Finished · {} requests · {} errors",
                        summary.requests, summary.errors
                    );
                    let mut summary = summary;
                    if self.stop_requested {
                        summary.status = "CANCELLED".into();
                        self.status = format!(
                            "Stopped · {} requests · {} errors",
                            summary.requests, summary.errors
                        );
                    }
                    self.save_summary(summary.clone());
                    self.summary = Some(summary);
                }
                EngineEvent::Failed(message) => {
                    self.is_running = false;
                    self.status = format!("Run failed: {message}");
                }
            }
        }
        self.update_timeline();
    }

    fn update_timeline(&mut self) {
        if !self.is_running {
            return;
        }
        let current_second = self.started_ms + self.run_elapsed_ms();
        let second = current_second / 1_000;
        if second == self.last_second {
            return;
        }
        self.last_second = second;
        let tps = self.requests.saturating_sub(self.last_request_count) as f32;
        self.last_request_count = self.requests;
        let average = if self.latencies.is_empty() {
            0.0
        } else {
            self.latencies
                .iter()
                .rev()
                .take(1_000)
                .map(|value| *value as u128)
                .sum::<u128>() as f32
                / self.latencies.iter().rev().take(1_000).count().max(1) as f32
        };
        push_point(&mut self.timeline_latency, average);
        push_point(&mut self.timeline_tps, tps);
    }

    fn run_elapsed_ms(&self) -> u128 {
        if self.started_ms == 0 {
            return 0;
        }
        now_ms().saturating_sub(self.started_ms)
    }

    fn save_summary(&mut self, summary: RunSummary) {
        let directory = history_directory();
        if let Err(error) = fs::create_dir_all(&directory) {
            self.status = format!("Finished, but could not create history folder: {error}");
            return;
        }
        let file_path = directory.join(format!("run-{}.json", summary.started_ms));
        match File::create(&file_path).and_then(|file| {
            serde_json::to_writer_pretty(file, &summary).map_err(std::io::Error::other)
        }) {
            Ok(()) => {
                self.history.insert(
                    0,
                    HistoryEntry {
                        path: file_path,
                        summary,
                    },
                );
            }
            Err(error) => self.status = format!("Finished, but saving history failed: {error}"),
        }
    }

    fn send_preview(&mut self) {
        let Some((module_index, case_index, step_index)) = self.selected_step else {
            self.status = "Select an API step first".into();
            return;
        };
        let Some(step) = self
            .project
            .modules
            .get(module_index)
            .and_then(|module| module.test_cases.get(case_index))
            .and_then(|test_case| test_case.steps.get(step_index))
            .cloned()
        else {
            self.status = "Selected API step no longer exists".into();
            return;
        };
        let project = self.project.clone();
        let (preview_tx, preview_rx) = mpsc::sync_channel(4);
        self.preview_rx = Some(preview_rx);
        self.preview_record = None;
        self.preview_pending = true;
        thread::spawn(move || {
            let record = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(engine::preview_request(&project, &step)),
                Err(error) => SampleRecord {
                    timestamp_ms: now_ms(),
                    name: step.name.clone(),
                    method: step.request.method.as_label().into(),
                    url: step.request.url.clone(),
                    request_headers: String::new(),
                    request_body: step.request.body.clone(),
                    status: 0,
                    elapsed_ms: 0,
                    success: false,
                    response_body: String::new(),
                    response_headers: String::new(),
                    error: format!("could not start async runtime: {error}"),
                },
            };
            let _ = preview_tx.try_send(record);
        });
        self.status = "Sending one-off request…".into();
    }

    fn top_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading(
                RichText::new("roadcuse")
                    .strong()
                    .color(Color32::from_rgb(99, 205, 248)),
            );
            ui.label(RichText::new("Rust + Goose Load Tester").weak());
            ui.separator();
            if ui
                .selectable_label(self.page == Page::Plan, "Test Plan")
                .clicked()
            {
                self.page = Page::Plan;
            }
            if self.page == Page::StepEditor {
                ui.label(RichText::new("› Edit API").color(Color32::from_rgb(139, 101, 255)));
            }
            if ui
                .selectable_label(self.page == Page::VUsers, "VUser & CSV")
                .clicked()
            {
                self.page = Page::VUsers;
            }
            if ui
                .selectable_label(self.page == Page::Monitor, "Live Monitor")
                .clicked()
            {
                self.page = Page::Monitor;
            }
            if ui
                .selectable_label(self.page == Page::Summary, "Summary Report")
                .clicked()
            {
                self.page = Page::Summary;
            }
            if ui
                .selectable_label(self.page == Page::History, "History")
                .clicked()
            {
                self.page = Page::History;
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if self.is_running {
                    if ui
                        .add(egui::Button::new("■  Stop").fill(Color32::from_rgb(158, 69, 71)))
                        .clicked()
                    {
                        goose::trigger_killswitch("Stopped from desktop UI");
                        self.stop_requested = true;
                        self.status = "Stopping Goose…".into();
                    }
                } else if ui
                    .add(
                        egui::Button::new("▶  Start Load Test")
                            .fill(Color32::from_rgb(0, 123, 104)),
                    )
                    .clicked()
                {
                    self.start_run();
                }
                if ui.add(egui::Button::new("Save")).clicked() {
                    self.save_project();
                }
                if ui.add(egui::Button::new("Open")).clicked() {
                    self.open_project();
                }
            });
        });
        ui.horizontal(|ui| {
            ui.label(RichText::new(&self.status).color(if self.is_running {
                Color32::LIGHT_BLUE
            } else {
                Color32::GRAY
            }));
            if let Some(path) = &self.project_path {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(RichText::new(path.display().to_string()).small().weak());
                });
            }
        });
    }

    fn plan_page(&mut self, ui: &mut egui::Ui) {
        let available = ui.available_size();
        let left_width = (available.x * 0.69).max(420.0);
        ui.horizontal_top(|ui| {
            ui.allocate_ui_with_layout(
                Vec2::new(left_width, available.y),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.heading(&self.project.name);
                            if ui.small_button("Rename").clicked() {
                                self.status = "Edit the project name in the field below".into();
                            }
                        });
                        ui.horizontal(|ui| {
                            ui.label("Project");
                            ui.add_sized(
                                [220.0, 26.0],
                                egui::TextEdit::singleline(&mut self.project.name),
                            );
                            ui.label("Description");
                            ui.text_edit_singleline(&mut self.project.description);
                        });
                        ui.add_space(12.0);
                        ui.horizontal(|ui| {
                            ui.label("Module");
                            egui::ComboBox::from_id_salt("plan_module")
                                .selected_text(
                                    self.project
                                        .modules
                                        .get(self.selected_case.0)
                                        .map(|module| module.name.as_str())
                                        .unwrap_or("No module"),
                                )
                                .show_ui(ui, |ui| {
                                    for (index, module) in self.project.modules.iter().enumerate() {
                                        ui.selectable_value(
                                            &mut self.selected_case.0,
                                            index,
                                            &module.name,
                                        );
                                    }
                                });
                            if ui.button("＋ Module").clicked() {
                                self.project.modules.push(Default::default());
                                self.selected_case = (self.project.modules.len() - 1, 0);
                            }
                            if let Some(module) = self.project.modules.get_mut(self.selected_case.0)
                            {
                                ui.label("Test case");
                                egui::ComboBox::from_id_salt("plan_case")
                                    .selected_text(
                                        module
                                            .test_cases
                                            .get(self.selected_case.1)
                                            .map(|case| case.name.as_str())
                                            .unwrap_or("No case"),
                                    )
                                    .show_ui(ui, |ui| {
                                        for (index, test_case) in
                                            module.test_cases.iter().enumerate()
                                        {
                                            ui.selectable_value(
                                                &mut self.selected_case.1,
                                                index,
                                                &test_case.name,
                                            );
                                        }
                                    });
                                if ui.button("＋ Case").clicked() {
                                    module.test_cases.push(Default::default());
                                    self.selected_case.1 = module.test_cases.len() - 1;
                                }
                            }
                        });

                        let (module_index, case_index) = self.selected_case;
                        if let Some(test_case) = self
                            .project
                            .modules
                            .get_mut(module_index)
                            .and_then(|module| module.test_cases.get_mut(case_index))
                        {
                            ui.horizontal(|ui| {
                                ui.heading(&test_case.name);
                                ui.checkbox(&mut test_case.enabled, "Enabled");
                                ui.text_edit_singleline(&mut test_case.description);
                            });
                            ui.add_space(8.0);
                            let mut open_step = None;
                            let mut delete_step = None;
                            let mut move_step = None;
                            let step_count = test_case.steps.len();
                            for (index, step) in test_case.steps.iter_mut().enumerate() {
                                egui::Frame::group(ui.style())
                                    .inner_margin(egui::Margin::symmetric(10, 8))
                                    .show(ui, |ui| {
                                        ui.horizontal(|ui| {
                                            ui.label(RichText::new("⠿").weak());
                                            ui.label(
                                                RichText::new((index + 1).to_string())
                                                    .color(Color32::GRAY),
                                            );
                                            ui.checkbox(&mut step.enabled, "");
                                            ui.label(
                                                RichText::new(step.request.method.as_label())
                                                    .strong()
                                                    .color(method_color(step.request.method)),
                                            );
                                            ui.vertical(|ui| {
                                                ui.text_edit_singleline(&mut step.name);
                                                ui.label(
                                                    RichText::new(&step.request.url).small().weak(),
                                                );
                                            });
                                            if ui.button("Edit API").clicked() {
                                                open_step = Some((module_index, case_index, index));
                                            }
                                            if ui.small_button("↑").clicked() && index > 0 {
                                                move_step = Some((index, index - 1));
                                            }
                                            if ui.small_button("↓").clicked()
                                                && index + 1 < step_count
                                            {
                                                move_step = Some((index, index + 1));
                                            }
                                            if ui.small_button("×").clicked() {
                                                delete_step = Some(index);
                                            }
                                        });
                                    });
                                ui.add_space(4.0);
                            }
                            if let Some((from, to)) = move_step {
                                test_case.steps.swap(from, to);
                            }
                            if let Some(index) = delete_step {
                                if index < test_case.steps.len() {
                                    test_case.steps.remove(index);
                                }
                            }
                            if let Some(target) = open_step {
                                self.selected_step = Some(target);
                            }
                            egui::ComboBox::from_id_salt("add_plan_step")
                                .selected_text("＋ Add Steps")
                                .width(ui.available_width())
                                .show_ui(ui, |ui| {
                                    if ui.selectable_label(false, "HTTP Request").clicked() {
                                        test_case.steps.push(Default::default());
                                    }
                                    ui.separator();
                                    ui.add_enabled(false, egui::Label::new("Reference test case"));
                                    ui.add_enabled(false, egui::Label::new("Condition / Loop"));
                                });
                            if let Some(target) = self.selected_step.take() {
                                self.selected_step = Some(target);
                                if target.0 == module_index && target.1 == case_index {
                                    self.page = Page::StepEditor;
                                }
                            }
                        } else {
                            ui.add_space(12.0);
                            ui.label(
                                "Add a module and test case to start building the request flow.",
                            );
                        }
                    });
                },
            );
            ui.separator();
            ui.allocate_ui_with_layout(
                Vec2::new((available.x - left_width - 12.0).max(300.0), available.y),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    ui.horizontal(|ui| {
                        if ui
                            .selectable_label(self.run_config_tab == 0, "Functional")
                            .clicked()
                        {
                            self.run_config_tab = 0;
                        }
                        if ui
                            .selectable_label(self.run_config_tab == 1, "Performance")
                            .clicked()
                        {
                            self.run_config_tab = 1;
                        }
                    });
                    ui.separator();
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        egui::Frame::group(ui.style())
                            .inner_margin(egui::Margin::same(12))
                            .show(ui, |ui| {
                                ui.heading("Run Config");
                                if self.run_config_tab == 0 {
                                    ui.label("Environment");
                                    egui::ComboBox::from_id_salt("run_environment")
                                        .selected_text(&self.project.active_environment)
                                        .width(ui.available_width())
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(
                                                &mut self.project.active_environment,
                                                "Default".into(),
                                                "Default",
                                            );
                                            for environment in &self.project.environments {
                                                ui.selectable_value(
                                                    &mut self.project.active_environment,
                                                    environment.name.clone(),
                                                    &environment.name,
                                                );
                                            }
                                        });
                                    ui.add_space(8.0);
                                    ui.label("Base URL");
                                    let base_url = self
                                        .project
                                        .environments
                                        .iter_mut()
                                        .find(|environment| {
                                            environment.name == self.project.active_environment
                                        })
                                        .map(|environment| &mut environment.base_url);
                                    if let Some(base_url) = base_url {
                                        ui.text_edit_singleline(base_url);
                                    } else {
                                        ui.text_edit_singleline(&mut self.project.base_url);
                                    }
                                    if ui.button("＋ Environment").clicked() {
                                        self.project.environments.push(EnvironmentProfile {
                                            name: format!(
                                                "Environment {}",
                                                self.project.environments.len() + 1
                                            ),
                                            base_url: self.project.base_url.clone(),
                                            ..Default::default()
                                        });
                                        self.project.active_environment = self
                                            .project
                                            .environments
                                            .last()
                                            .map(|environment| environment.name.clone())
                                            .unwrap_or_default();
                                    }
                                    ui.collapsing("Environment variables", |ui| {
                                        if let Some(environment) = self
                                            .project
                                            .environments
                                            .iter_mut()
                                            .find(|environment| {
                                                environment.name == self.project.active_environment
                                            })
                                        {
                                            edit_key_values(
                                                ui,
                                                "run_environment_variables",
                                                &mut environment.variables,
                                            );
                                        }
                                    });
                                    ui.collapsing("Shared headers", |ui| {
                                        edit_key_values(
                                            ui,
                                            "run_shared_headers",
                                            &mut self.project.global_headers,
                                        );
                                    });
                                } else {
                                    ui.label("Virtual users");
                                    ui.add(
                                        egui::DragValue::new(
                                            &mut self.project.vuser_config.thread_count,
                                        )
                                        .range(1..=100_000),
                                    );
                                    ui.label("Ramp-up (seconds)");
                                    ui.add(
                                        egui::DragValue::new(
                                            &mut self.project.vuser_config.ramp_up_seconds,
                                        )
                                        .range(0..=86_400),
                                    );
                                    ui.horizontal(|ui| {
                                        ui.selectable_value(
                                            &mut self.project.vuser_config.duration_based,
                                            false,
                                            "Iterations",
                                        );
                                        ui.selectable_value(
                                            &mut self.project.vuser_config.duration_based,
                                            true,
                                            "Duration",
                                        );
                                    });
                                    if self.project.vuser_config.duration_based {
                                        ui.add(
                                            egui::DragValue::new(
                                                &mut self.project.vuser_config.duration_seconds,
                                            )
                                            .range(1..=86_400)
                                            .suffix(" sec"),
                                        );
                                    } else {
                                        ui.add(
                                            egui::DragValue::new(
                                                &mut self.project.vuser_config.loop_count,
                                            )
                                            .range(1..=1_000_000)
                                            .suffix(" runs/user"),
                                        );
                                    }
                                    ui.separator();
                                    ui.checkbox(
                                        &mut self.project.vuser_config.use_csv_data,
                                        "Use CSV data",
                                    );
                                    if self.project.vuser_config.use_csv_data {
                                        ui.text_edit_singleline(
                                            &mut self.project.vuser_config.csv_file_path,
                                        );
                                        if ui.button("Browse CSV…").clicked() {
                                            if let Some(path) = rfd::FileDialog::new()
                                                .add_filter("CSV", &["csv"])
                                                .pick_file()
                                            {
                                                self.project.vuser_config.csv_file_path =
                                                    path.to_string_lossy().into_owned();
                                            }
                                        }
                                    }
                                }
                            });
                    });
                    ui.with_layout(egui::Layout::bottom_up(egui::Align::Min), |ui| {
                        ui.horizontal(|ui| {
                            if ui
                                .add_enabled(!self.is_running, egui::Button::new("▶ Run"))
                                .clicked()
                            {
                                self.start_run();
                            }
                            if ui.button("Save").clicked() {
                                self.save_project();
                            }
                        });
                    });
                },
            );
        });
    }

    fn vuser_page(&mut self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical().show(ui, |ui| {
            ui.heading("Thread Group");
            ui.label("Set the load profile Goose will run for this project.");
            ui.add_space(12.0);
            egui::Grid::new("vuser_profile")
                .num_columns(2)
                .spacing([16.0, 12.0])
                .show(ui, |ui| {
                    ui.label("Virtual users");
                    ui.add(
                        egui::DragValue::new(&mut self.project.vuser_config.thread_count)
                            .range(1..=100_000),
                    );
                    ui.end_row();
                    ui.label("Ramp-up time (seconds)");
                    ui.add(
                        egui::DragValue::new(&mut self.project.vuser_config.ramp_up_seconds)
                            .range(0..=86_400),
                    );
                    ui.end_row();
                    ui.label("Schedule");
                    ui.horizontal(|ui| {
                        ui.selectable_value(
                            &mut self.project.vuser_config.duration_based,
                            false,
                            "By iterations",
                        );
                        ui.selectable_value(
                            &mut self.project.vuser_config.duration_based,
                            true,
                            "By duration",
                        );
                    });
                    ui.end_row();
                    if self.project.vuser_config.duration_based {
                        ui.label("Duration (seconds)");
                        ui.add(
                            egui::DragValue::new(&mut self.project.vuser_config.duration_seconds)
                                .range(1..=86_400),
                        );
                    } else {
                        ui.label("Iterations per user");
                        ui.add(
                            egui::DragValue::new(&mut self.project.vuser_config.loop_count)
                                .range(1..=1_000_000),
                        );
                    }
                    ui.end_row();
                });

            ui.add_space(18.0);
            ui.separator();
            ui.horizontal(|ui| {
                ui.heading("CSV Data Set");
                ui.checkbox(
                    &mut self.project.vuser_config.use_csv_data,
                    "Enable CSV data",
                );
            });
            ui.label("CSV column headers are available as variables such as ${username}.");
            ui.add_space(8.0);
            ui.horizontal(|ui| {
                ui.add_enabled_ui(self.project.vuser_config.use_csv_data, |ui| {
                    ui.add_sized(
                        [ui.available_width().min(560.0), 28.0],
                        egui::TextEdit::singleline(&mut self.project.vuser_config.csv_file_path)
                            .hint_text("Choose a CSV file…"),
                    );
                });
                if ui
                    .add_enabled(
                        self.project.vuser_config.use_csv_data,
                        egui::Button::new("Browse…"),
                    )
                    .clicked()
                {
                    if let Some(path) = rfd::FileDialog::new()
                        .add_filter("CSV", &["csv"])
                        .pick_file()
                    {
                        self.project.vuser_config.csv_file_path =
                            path.to_string_lossy().into_owned();
                    }
                }
            });
            ui.horizontal(|ui| {
                ui.label("Delimiter");
                egui::ComboBox::from_id_salt("csv_delimiter")
                    .selected_text(match self.project.vuser_config.csv_delimiter.as_str() {
                        ";" => "Semicolon ;",
                        "\\t" => "Tab",
                        "|" => "Pipe |",
                        _ => "Comma ,",
                    })
                    .show_ui(ui, |ui| {
                        for (delimiter, label) in [
                            (",", "Comma ,"),
                            (";", "Semicolon ;"),
                            ("\\t", "Tab"),
                            ("|", "Pipe |"),
                        ] {
                            ui.selectable_value(
                                &mut self.project.vuser_config.csv_delimiter,
                                delimiter.into(),
                                label,
                            );
                        }
                    });
                ui.checkbox(
                    &mut self.project.vuser_config.recycle_on_eof,
                    "Recycle rows at EOF",
                );
            });
        });
    }

    fn step_editor_page(&mut self, ui: &mut egui::Ui) {
        let Some((module_index, case_index, step_index)) = self.selected_step else {
            ui.label("Choose a request from the test plan.");
            if ui.button("Back to Test Plan").clicked() {
                self.page = Page::Plan;
            }
            return;
        };
        let base_url = self.project.active_base_url().to_owned();
        let mut send_request = false;
        let Some(step) = self
            .project
            .modules
            .get_mut(module_index)
            .and_then(|module| module.test_cases.get_mut(case_index))
            .and_then(|test_case| test_case.steps.get_mut(step_index))
        else {
            self.selected_step = None;
            self.page = Page::Plan;
            ui.label("This request step has been removed.");
            return;
        };
        let step_title = step.name.clone();
        let mut save_project = false;

        ui.horizontal(|ui| {
            if ui.button("‹ Test Plan").clicked() {
                self.page = Page::Plan;
            }
            ui.heading(format!("Edit API · {}", step_title));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Save").clicked() {
                    save_project = true;
                }
                if ui.button("Send").clicked() {
                    send_request = true;
                }
            });
        });
        ui.horizontal(|ui| {
            egui::ComboBox::from_id_salt("editor_method")
                .selected_text(step.request.method.as_label())
                .show_ui(ui, |ui| {
                    for method in [
                        HttpMethod::Get,
                        HttpMethod::Post,
                        HttpMethod::Put,
                        HttpMethod::Delete,
                        HttpMethod::Patch,
                        HttpMethod::Head,
                        HttpMethod::Options,
                    ] {
                        ui.selectable_value(&mut step.request.method, method, method.as_label());
                    }
                });
            ui.label(RichText::new(base_url).weak());
            ui.add_sized(
                [ui.available_width(), 32.0],
                egui::TextEdit::singleline(&mut step.request.url)
                    .hint_text("/api/path or full URL"),
            );
        });
        ui.horizontal(|ui| {
            for (index, title) in [
                "Headers",
                "Params",
                "Path",
                "Body",
                "Extractors",
                "Assertions",
            ]
            .iter()
            .enumerate()
            {
                if ui
                    .selectable_label(self.step_editor_tab == index, *title)
                    .clicked()
                {
                    self.step_editor_tab = index;
                }
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.label("HTTP/1.1");
                ui.add(egui::DragValue::new(&mut step.request.timeout_ms).suffix(" ms"));
                ui.label("Timeout");
            });
        });
        ui.separator();
        egui::Frame::group(ui.style())
            .inner_margin(egui::Margin::symmetric(12, 10))
            .show(ui, |ui| match self.step_editor_tab {
                0 => {
                    ui.label(RichText::new("Request Headers").strong());
                    edit_key_values(ui, "api_editor_headers", &mut step.request.headers);
                }
                1 => {
                    ui.label(RichText::new("Query Parameters").strong());
                    edit_key_values(ui, "api_editor_params", &mut step.request.query_params);
                }
                2 => {
                    ui.label(RichText::new("Path Variables").strong());
                    ui.label("Use {name} or :name in the URL. Values can use ${variable}.");
                    edit_key_values(ui, "api_editor_path", &mut step.request.path_variables);
                }
                3 => {
                    ui.horizontal(|ui| {
                        ui.label("Body type");
                        egui::ComboBox::from_id_salt("api_editor_body_type")
                            .selected_text(format!("{:?}", step.request.body_type))
                            .show_ui(ui, |ui| {
                                for body_type in [
                                    BodyType::None,
                                    BodyType::Json,
                                    BodyType::Raw,
                                    BodyType::FormData,
                                ] {
                                    ui.selectable_value(
                                        &mut step.request.body_type,
                                        body_type,
                                        format!("{body_type:?}"),
                                    );
                                }
                            });
                    });
                    if step.request.body_type != BodyType::None {
                        ui.add(
                            egui::TextEdit::multiline(&mut step.request.body)
                                .desired_rows(12)
                                .code_editor()
                                .hint_text("Request body. Variables use ${name}."),
                        );
                    }
                }
                4 => {
                    ui.label("Extract values from the response for later requests.");
                    for (index, extractor) in step.extractors.iter_mut().enumerate() {
                        ui.horizontal(|ui| {
                            ui.checkbox(&mut extractor.enabled, "");
                            ui.text_edit_singleline(&mut extractor.variable_name);
                            egui::ComboBox::from_id_salt(("api_extractor_type", index))
                                .selected_text(format!("{:?}", extractor.extractor_type))
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(
                                        &mut extractor.extractor_type,
                                        ExtractorType::JsonPath,
                                        "JSONPath",
                                    );
                                    ui.selectable_value(
                                        &mut extractor.extractor_type,
                                        ExtractorType::Regex,
                                        "Regex",
                                    );
                                });
                            ui.text_edit_singleline(&mut extractor.expression);
                        });
                    }
                    if ui.button("＋ Add extractor").clicked() {
                        step.extractors.push(VariableExtractor {
                            enabled: true,
                            variable_name: "token".into(),
                            expression: "$.token".into(),
                            ..Default::default()
                        });
                    }
                }
                _ => {
                    ui.label("Validate the response from this step.");
                    for (index, assertion) in step.assertions.iter_mut().enumerate() {
                        ui.horizontal(|ui| {
                            ui.checkbox(&mut assertion.enabled, "");
                            egui::ComboBox::from_id_salt(("api_assertion_type", index))
                                .selected_text(format!("{:?}", assertion.assertion_type))
                                .show_ui(ui, |ui| {
                                    for assertion_type in [
                                        AssertionType::StatusCode,
                                        AssertionType::ResponseBodyContains,
                                        AssertionType::ResponseTimeLt,
                                        AssertionType::JsonPathExists,
                                    ] {
                                        ui.selectable_value(
                                            &mut assertion.assertion_type,
                                            assertion_type,
                                            format!("{assertion_type:?}"),
                                        );
                                    }
                                });
                            ui.text_edit_singleline(&mut assertion.expected_value);
                            ui.text_edit_singleline(&mut assertion.description);
                        });
                    }
                    if ui.button("＋ Add assertion").clicked() {
                        step.assertions.push(StepAssertion {
                            enabled: true,
                            expected_value: "200".into(),
                            ..Default::default()
                        });
                    }
                }
            });
        ui.separator();
        ui.horizontal(|ui| {
            for (index, title) in ["Response", "Headers", "Actual Request", "Console"]
                .iter()
                .enumerate()
            {
                if ui
                    .selectable_label(self.detail_tab == index, *title)
                    .clicked()
                {
                    self.detail_tab = index;
                }
            }
        });
        if let Some(preview) = &self.preview_record {
            ui.label(format!(
                "{} · HTTP {} · {} ms {}",
                preview.method,
                preview.status,
                preview.elapsed_ms,
                if preview.success {
                    "· passed"
                } else {
                    "· failed"
                }
            ));
            let (title, content) = match self.detail_tab {
                1 => ("Response Headers", preview.response_headers.clone()),
                2 => (
                    "Actual Request",
                    format!("{}\n\n{}", preview.request_headers, preview.request_body),
                ),
                3 => ("Console", preview.error.clone()),
                _ => ("Response Body", preview.response_body.clone()),
            };
            ui.label(RichText::new(title).strong());
            let mut content = content;
            ui.add(
                egui::TextEdit::multiline(&mut content)
                    .desired_rows(8)
                    .code_editor()
                    .interactive(false),
            );
        } else {
            ui.label("Send a request to view its response.");
        }
        if send_request {
            self.send_preview();
        }
        if save_project {
            self.save_project();
        }
    }

    fn monitor_page(&mut self, ui: &mut egui::Ui) {
        let elapsed_seconds = if self.is_running {
            self.run_elapsed_ms() as f64 / 1000.0
        } else {
            self.summary
                .as_ref()
                .map(|summary| summary.duration_ms as f64 / 1000.0)
                .unwrap_or(0.0)
        };
        let error_rate = if self.requests == 0 {
            0.0
        } else {
            self.errors as f64 * 100.0 / self.requests as f64
        };
        let average = if let Some(summary) = &self.summary {
            summary.average_ms
        } else if self.latencies.is_empty() {
            0.0
        } else {
            self.latencies
                .iter()
                .map(|value| *value as u128)
                .sum::<u128>() as f64
                / self.latencies.len() as f64
        };
        ui.horizontal_wrapped(|ui| {
            metric_card(
                ui,
                "Active VUsers",
                if self.is_running {
                    self.project.vuser_config.thread_count.to_string()
                } else {
                    "0".into()
                },
            );
            metric_card(ui, "Requests", format!("{}", self.requests));
            metric_card(ui, "Error Rate", format!("{error_rate:.2}%"));
            metric_card(
                ui,
                "Throughput",
                format!(
                    "{:.2} req/s",
                    self.requests as f64 / elapsed_seconds.max(1.0)
                ),
            );
            metric_card(ui, "Avg Latency", format!("{average:.1} ms"));
            metric_card(
                ui,
                "P90 Latency",
                format!(
                    "{} ms",
                    self.summary
                        .as_ref()
                        .map(|summary| summary.p90_ms)
                        .unwrap_or_else(|| percentile(&self.latencies, 0.90))
                ),
            );
        });
        ui.add_space(8.0);
        ui.horizontal(|ui| {
            chart(
                ui,
                "Latency · ms",
                &self.timeline_latency,
                Color32::from_rgb(88, 180, 255),
            );
            chart(
                ui,
                "Requests · /sec",
                &self.timeline_tps,
                Color32::from_rgb(58, 206, 157),
            );
        });
        ui.separator();
        ui.label(RichText::new("Live request stream").strong());
        egui::ScrollArea::vertical()
            .max_height(310.0)
            .show(ui, |ui| {
                egui::Grid::new("live_sample_grid")
                    .striped(true)
                    .min_col_width(70.0)
                    .show(ui, |ui| {
                        ui.strong("Result");
                        ui.strong("Request");
                        ui.strong("Status");
                        ui.strong("Latency");
                        ui.end_row();
                        for record in self.records.iter().rev() {
                            let tint = if record.success {
                                Color32::from_rgb(92, 210, 151)
                            } else {
                                Color32::from_rgb(255, 112, 112)
                            };
                            let selected = self.selected_record.as_ref().is_some_and(|item| {
                                item.timestamp_ms == record.timestamp_ms && item.name == record.name
                            });
                            let response = ui.selectable_label(
                                selected,
                                RichText::new(if record.success { "PASS" } else { "FAIL" })
                                    .color(tint),
                            );
                            if response.clicked() {
                                self.selected_record = Some(record.clone());
                                self.detail_tab = 1;
                            }
                            ui.label(format!("{}  {}", record.method, record.name));
                            ui.label(if record.status == 0 {
                                "ERR".into()
                            } else {
                                record.status.to_string()
                            });
                            ui.label(format!("{} ms", record.elapsed_ms));
                            ui.end_row();
                        }
                    });
            });
        if let Some(record) = &self.selected_record {
            ui.separator();
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new(format!(
                        "{} · {} · {} ms",
                        record.method, record.url, record.elapsed_ms
                    ))
                    .strong(),
                );
            });
            ui.horizontal(|ui| {
                if ui
                    .selectable_label(self.detail_tab == 0, "Request")
                    .clicked()
                {
                    self.detail_tab = 0;
                }
                if ui
                    .selectable_label(self.detail_tab == 1, "Response")
                    .clicked()
                {
                    self.detail_tab = 1;
                }
            });
            ui.add_space(4.0);
            if self.detail_tab == 0 {
                ui.label(RichText::new("Request Headers").strong());
                let mut headers = record.request_headers.clone();
                ui.add(
                    egui::TextEdit::multiline(&mut headers)
                        .desired_rows(4)
                        .code_editor()
                        .interactive(false),
                );
                ui.label(RichText::new("Request Body").strong());
                let mut body = record.request_body.clone();
                ui.add(
                    egui::TextEdit::multiline(&mut body)
                        .desired_rows(5)
                        .code_editor()
                        .interactive(false),
                );
            } else {
                ui.label(format!("Status: {}", record.status));
                ui.label(RichText::new("Response Headers").strong());
                let mut headers = record.response_headers.clone();
                ui.add(
                    egui::TextEdit::multiline(&mut headers)
                        .desired_rows(3)
                        .code_editor()
                        .interactive(false),
                );
                ui.label(RichText::new("Response Body").strong());
                let mut body = record.response_body.clone();
                ui.add(
                    egui::TextEdit::multiline(&mut body)
                        .desired_rows(8)
                        .code_editor()
                        .interactive(false),
                );
            }
            if !record.error.is_empty() {
                ui.colored_label(Color32::LIGHT_RED, &record.error);
            }
        }
    }

    fn summary_page(&mut self, ui: &mut egui::Ui) {
        let mut open_record = None;
        egui::ScrollArea::vertical().show(ui, |ui| {
            ui.heading("Summary Report");
            let Some(summary) = self.summary.as_ref() else {
                ui.add_space(8.0);
                ui.label("Run a load test to see its summary here.");
                return;
            };
            ui.label(format!(
                "{} · {} · started {}",
                summary.project_name,
                summary.status,
                format_epoch(summary.started_ms)
            ));
            ui.add_space(10.0);
            ui.horizontal_wrapped(|ui| {
                metric_card(ui, "Samples", summary.requests.to_string());
                metric_card(ui, "Errors", summary.errors.to_string());
                metric_card(
                    ui,
                    "Duration",
                    format!("{:.1}s", summary.duration_ms as f64 / 1000.0),
                );
                metric_card(ui, "Average", format!("{:.1} ms", summary.average_ms));
                metric_card(ui, "P90", format!("{} ms", summary.p90_ms));
            });
            ui.add_space(12.0);
            ui.separator();
            ui.label(RichText::new("Request Sampler Summary").strong());
            let mut rows = BTreeMap::<String, (u64, u64, u128)>::new();
            for sample in &summary.recent_samples {
                let row = rows.entry(sample.name.clone()).or_default();
                row.0 += 1;
                row.1 += u64::from(!sample.success);
                row.2 += sample.elapsed_ms as u128;
            }
            egui::ScrollArea::vertical()
                .max_height(360.0)
                .show(ui, |ui| {
                    egui::Grid::new("summary_sampler_table")
                        .striped(true)
                        .min_col_width(90.0)
                        .show(ui, |ui| {
                            for heading in ["Sampler", "Samples", "Errors", "Error %", "Avg (ms)"] {
                                ui.strong(heading);
                            }
                            ui.end_row();
                            for (name, (count, errors, total_ms)) in rows {
                                let average_ms = total_ms as f64 / count.max(1) as f64;
                                ui.label(name);
                                ui.label(count.to_string());
                                ui.label(errors.to_string());
                                ui.label(format!(
                                    "{:.2}%",
                                    errors as f64 * 100.0 / count.max(1) as f64
                                ));
                                ui.label(format!("{average_ms:.1}"));
                                ui.end_row();
                            }
                        });
                });
            ui.weak("Sampler rows use the bounded request sample set saved with this run.");
            ui.add_space(12.0);
            ui.separator();
            ui.label(RichText::new("Request Results").strong());
            ui.horizontal(|ui| {
                for (index, label) in [(0, "All"), (1, "Passed"), (2, "Failed")] {
                    if ui
                        .selectable_label(self.summary_filter == index, label)
                        .clicked()
                    {
                        self.summary_filter = index;
                    }
                }
            });
            egui::ScrollArea::vertical()
                .max_height(300.0)
                .show(ui, |ui| {
                    egui::Grid::new("summary_request_results")
                        .striped(true)
                        .show(ui, |ui| {
                            for heading in ["Result", "Method", "Request", "Status", "Latency"] {
                                ui.strong(heading);
                            }
                            ui.end_row();
                            for sample in summary.recent_samples.iter().rev().filter(|sample| {
                                match self.summary_filter {
                                    1 => sample.success,
                                    2 => !sample.success,
                                    _ => true,
                                }
                            }) {
                                let result = ui.selectable_label(
                                    false,
                                    RichText::new(if sample.success { "PASS" } else { "FAIL" })
                                        .color(if sample.success {
                                            Color32::from_rgb(92, 210, 151)
                                        } else {
                                            Color32::from_rgb(255, 112, 112)
                                        }),
                                );
                                if result.clicked() {
                                    open_record = Some(sample.clone());
                                }
                                ui.label(&sample.method);
                                ui.label(&sample.name);
                                ui.label(sample.status.to_string());
                                ui.label(format!("{} ms", sample.elapsed_ms));
                                ui.end_row();
                            }
                        });
                });
        });
        if let Some(record) = open_record {
            self.selected_record = Some(record);
            self.detail_tab = 1;
            self.page = Page::Monitor;
        }
    }

    fn history_page(&mut self, ui: &mut egui::Ui) {
        ui.heading("Test History");
        ui.label("Completed runs are saved locally as JSON summaries with recent request details.");
        ui.separator();
        egui::ScrollArea::vertical().show(ui, |ui| {
            for entry in &self.history {
                let summary = &entry.summary;
                egui::CollapsingHeader::new(format!(
                    "{} · {} · {} requests · {} errors",
                    format_epoch(summary.started_ms),
                    summary.status,
                    summary.requests,
                    summary.errors
                ))
                .id_salt(entry.path.clone())
                .show(ui, |ui| {
                    ui.label(format!(
                        "Duration: {:.1}s · Avg: {:.1}ms · P90: {}ms",
                        summary.duration_ms as f64 / 1000.0,
                        summary.average_ms,
                        summary.p90_ms
                    ));
                    ui.label(entry.path.display().to_string());
                    for sample in summary.recent_samples.iter().rev().take(30) {
                        ui.label(format!(
                            "{} {} · {} · {}ms · {}",
                            sample.method,
                            sample.name,
                            sample.status,
                            sample.elapsed_ms,
                            if sample.success { "PASS" } else { "FAIL" }
                        ));
                    }
                });
            }
        });
    }

    fn open_project(&mut self) {
        let Some(path) = rfd::FileDialog::new()
            .add_filter("roadcuse project", &["json"])
            .pick_file()
        else {
            return;
        };
        match File::open(&path)
            .and_then(|file| serde_json::from_reader(file).map_err(std::io::Error::other))
        {
            Ok(project) => {
                self.project = project;
                self.project_path = Some(path);
                self.status = "Project loaded".into();
            }
            Err(error) => self.status = format!("Could not open project: {error}"),
        }
    }

    fn save_project(&mut self) {
        let path = self.project_path.clone().or_else(|| {
            rfd::FileDialog::new()
                .add_filter("roadcuse project", &["json"])
                .set_file_name("load-test-project.json")
                .save_file()
        });
        let Some(path) = path else {
            return;
        };
        match File::create(&path).and_then(|file| {
            serde_json::to_writer_pretty(file, &self.project).map_err(std::io::Error::other)
        }) {
            Ok(()) => {
                self.project_path = Some(path);
                self.status = "Project saved".into();
            }
            Err(error) => self.status = format!("Could not save project: {error}"),
        }
    }
}

impl eframe::App for RoadcuseApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.receive_engine_events();
        self.top_bar(ui);
        ui.separator();
        ui.add_space(4.0);
        match self.page {
            Page::Plan => self.plan_page(ui),
            Page::StepEditor => self.step_editor_page(ui),
            Page::VUsers => self.vuser_page(ui),
            Page::Monitor => self.monitor_page(ui),
            Page::Summary => self.summary_page(ui),
            Page::History => self.history_page(ui),
        }
        if self.is_running {
            ui.ctx().request_repaint_after(Duration::from_millis(200));
        }
        if self.preview_pending {
            ui.ctx().request_repaint_after(Duration::from_millis(100));
        }
    }
}

fn run_on_worker(project: Project, tx: SyncSender<EngineEvent>) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = tx.try_send(EngineEvent::Failed(format!(
                "could not start async runtime: {error}"
            )));
            return;
        }
    };
    if let Err(error) = runtime.block_on(engine::run_project(project, tx.clone())) {
        let _ = tx.try_send(EngineEvent::Failed(format!("{error:#}")));
    }
}

fn edit_key_values(
    ui: &mut egui::Ui,
    id: impl std::hash::Hash + std::fmt::Debug,
    pairs: &mut Vec<KeyValuePair>,
) {
    let mut remove = None;
    for (index, pair) in pairs.iter_mut().enumerate() {
        ui.horizontal(|ui| {
            ui.checkbox(&mut pair.enabled, "");
            ui.text_edit_singleline(&mut pair.key);
            ui.text_edit_singleline(&mut pair.value);
            if ui.small_button("×").clicked() {
                remove = Some(index);
            }
        });
    }
    if let Some(index) = remove {
        pairs.remove(index);
    }
    if ui.small_button("＋ Add value").clicked() {
        pairs.push(KeyValuePair {
            enabled: true,
            key: String::new(),
            value: String::new(),
            description: format!("{id:?}"),
        });
    }
}

trait MethodLabel {
    fn as_label(self) -> &'static str;
}
impl MethodLabel for HttpMethod {
    fn as_label(self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Head => "HEAD",
            HttpMethod::Options => "OPTIONS",
        }
    }
}

fn method_color(method: HttpMethod) -> Color32 {
    match method {
        HttpMethod::Get => Color32::from_rgb(37, 192, 145),
        HttpMethod::Post => Color32::from_rgb(245, 174, 59),
        HttpMethod::Put => Color32::from_rgb(86, 154, 245),
        HttpMethod::Delete => Color32::from_rgb(240, 102, 112),
        HttpMethod::Patch => Color32::from_rgb(170, 121, 247),
        HttpMethod::Head | HttpMethod::Options => Color32::from_rgb(114, 173, 220),
    }
}

fn metric_card(ui: &mut egui::Ui, title: &str, value: String) {
    egui::Frame::group(ui.style())
        .inner_margin(egui::Margin::same(12))
        .show(ui, |ui| {
            ui.set_min_width(145.0);
            ui.label(RichText::new(title).weak().small());
            ui.label(RichText::new(value).size(20.0).strong());
        });
}

fn chart(ui: &mut egui::Ui, title: &str, values: &VecDeque<f32>, color: Color32) {
    ui.vertical(|ui| {
        ui.label(RichText::new(title).strong());
        let (rect, _) = ui.allocate_exact_size(
            Vec2::new((ui.available_width() * 0.49).max(280.0), 132.0),
            egui::Sense::hover(),
        );
        ui.painter()
            .rect_filled(rect, 4.0, Color32::from_rgb(12, 19, 29));
        ui.painter().rect_stroke(
            rect,
            4.0,
            Stroke::new(1.0, Color32::from_rgb(48, 63, 81)),
            egui::StrokeKind::Inside,
        );
        if values.len() > 1 {
            let max_value = values.iter().copied().fold(1.0_f32, f32::max);
            let points = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let x = rect.left() + (index as f32 / (values.len() - 1) as f32) * rect.width();
                    let y = rect.bottom() - (value / max_value) * (rect.height() - 12.0) - 6.0;
                    egui::pos2(x, y)
                })
                .collect::<Vec<_>>();
            for pair in points.windows(2) {
                ui.painter()
                    .line_segment([pair[0], pair[1]], Stroke::new(2.0, color));
            }
        }
    });
}

fn percentile(values: &VecDeque<u64>, quantile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let index = ((sorted.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn push_point(values: &mut VecDeque<f32>, point: f32) {
    if values.len() >= 60 {
        values.pop_front();
    }
    values.push_back(point);
}

fn history_directory() -> PathBuf {
    std::env::var_os("APPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("roadcuse")
        .join("history")
}

fn load_history() -> Vec<HistoryEntry> {
    let directory = history_directory();
    let Ok(entries) = fs::read_dir(&directory) else {
        return vec![];
    };
    let mut paths = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    paths.sort_by_key(|path| {
        std::cmp::Reverse(
            fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
        )
    });
    paths.truncate(50);
    let mut history = paths
        .into_iter()
        .filter_map(|path| {
            let file = File::open(&path).ok()?;
            let summary = serde_json::from_reader(file).ok()?;
            Some(HistoryEntry { path, summary })
        })
        .collect::<Vec<_>>();
    history.sort_by_key(|entry| std::cmp::Reverse(entry.summary.started_ms));
    history
}

fn format_epoch(epoch_ms: u128) -> String {
    let seconds = (epoch_ms / 1000) as u64;
    let days = seconds / 86_400;
    let hour = (seconds / 3600) % 24;
    let minute = (seconds / 60) % 60;
    let second = seconds % 60;
    format!("Day {days} {hour:02}:{minute:02}:{second:02}")
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
