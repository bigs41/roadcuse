mod app;
mod engine;
mod model;

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_title("roadcuse | Rust + Goose")
            .with_inner_size([1600.0, 1000.0])
            .with_min_inner_size([1200.0, 760.0]),
        ..Default::default()
    };
    eframe::run_native(
        "roadcuse",
        options,
        Box::new(|creation| Ok(Box::new(app::RoadcuseApp::new(creation)))),
    )
}
