# roadcuse

Native desktop HTTP load tester built with Rust, [Goose](https://github.com/tag1consulting/goose), and egui/eframe. This new implementation uses the existing EchoLoad (`load-test`) project as its behavior and data-model reference; the legacy JavaFX checkout is kept separate.

## Current features

- Desktop test-plan view with an ordered, enable/disable request list, step reordering, add-step menu, and Functional/Performance run-configuration panel.
- Dedicated API editor with Headers, Params, Path, Body, Extractors, and Assertions tabs, plus a one-off Send action and response/request detail tabs.
- HTTP methods GET, POST, PUT, DELETE, PATCH, HEAD, and OPTIONS; URL paths, path variables, query parameters, global/request headers, request body, and timeout.
- Per-user variables, CSV data, JSONPath/regex response extractors, status/body/response-time/JSONPath assertions, and step delay.
- Goose execution with concurrent users, ramp-up, iteration or duration mode, cookie-aware users, and Stop control.
- Live request/error counts, throughput, average and P90 latency, charts, recent request details, and response bodies.
- Summary report with sampler aggregates, All/Passed/Failed filters, and links from result rows to request details.
- Open/save project JSON, starter project, and local run history with summary and recent samples.

## Run

Install the stable Rust toolchain, then from this directory:

```powershell
cargo run
```

Use **Open** to load [`sample_project.json`](sample_project.json), or create a plan in the editor. Configure the target base URL, user count, ramp-up, and iteration/duration profile before starting a run. Use **Save** to persist the project as JSON. Run summaries are stored under `%APPDATA%\roadcuse\history`.

For an optimized Windows executable:

```powershell
cargo build --release
```

## Project format and runtime

Project JSON keeps the legacy concepts of modules → test cases → ordered steps and `${variable}` substitutions. The runner registers generic Goose transactions and interprets enabled project steps for each virtual user. Goose provides the load generation and request metrics; roadcuse collects a bounded set of recent request/response details for the live view and history.

The current history stores up to 1,500 recent samples per run and truncates captured request/response bodies to 16 KiB. CSV rows are loaded when a run starts. Form Data currently sends the body as-is with `application/x-www-form-urlencoded` as the default content type; provide an explicit content type header if your endpoint requires a different encoding.

## Reference project

The original JavaFX/JMeter implementation remains in the neighboring `load-test` directory. This repository is the separate Rust + Goose implementation and does not include or modify that checkout.
