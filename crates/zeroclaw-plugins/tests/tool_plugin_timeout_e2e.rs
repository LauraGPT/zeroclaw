//! Real component-boundary regressions for host-owned plugin wall time.

#![cfg(feature = "plugins-wasm-cranelift")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use zeroclaw_plugins::component::PluginLimits;
use zeroclaw_plugins::host::PluginHost;
use zeroclaw_plugins::instance::PluginInstanceScope;
use zeroclaw_plugins::runtime;
use zeroclaw_plugins::{PluginCapability, PluginPermission};

fn fixture() -> PathBuf {
    static FIXTURE: OnceLock<PathBuf> = OnceLock::new();
    FIXTURE
        .get_or_init(|| {
            let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/tool-timeout-fixture");
            let target_dir =
                PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("tool-timeout-fixture");
            let status = Command::new(env!("CARGO"))
                .current_dir(&fixture_dir)
                .args([
                    "build",
                    "--locked",
                    "--quiet",
                    "--package",
                    "zeroclaw-tool-timeout-fixture",
                    "--target",
                    "wasm32-wasip2",
                    "--target-dir",
                ])
                .arg(&target_dir)
                .status()
                .expect("run Cargo for the timeout component fixture");
            assert!(
                status.success(),
                "timeout fixture must build; install the wasm32-wasip2 target"
            );
            let wasm = target_dir.join("wasm32-wasip2/debug/zeroclaw_tool_timeout_fixture.wasm");
            assert!(wasm.is_file(), "timeout fixture WASM was not produced");
            wasm
        })
        .clone()
}

fn limits(call_timeout: Duration, call_fuel: u64) -> PluginLimits {
    PluginLimits {
        call_fuel,
        max_memory_bytes: 64 * 1024 * 1024,
        max_table_elements: 10_000,
        max_instances: 32,
        call_timeout,
    }
}

async fn plugin(call_timeout: Duration) -> runtime::Plugin {
    plugin_with_fuel(call_timeout, 1_000_000_000).await
}

async fn plugin_with_fuel(call_timeout: Duration, call_fuel: u64) -> runtime::Plugin {
    let temp = tempfile::tempdir().expect("temp plugin root");
    let plugin_dir = temp.path().join("tool-timeout-fixture");
    std::fs::create_dir_all(&plugin_dir).expect("create plugin directory");
    std::fs::copy(fixture(), plugin_dir.join("fixture.wasm")).expect("copy fixture component");
    std::fs::write(
        plugin_dir.join("manifest.toml"),
        "name = \"tool-timeout-fixture\"\n\
         version = \"0.0.0\"\n\
         wasm_path = \"fixture.wasm\"\n\
         capabilities = [\"tool\"]\n\
         permissions = [\"http_client\"]\n",
    )
    .expect("write fixture manifest");

    let host = PluginHost::from_plugins_dir(temp.path()).expect("discover fixture");
    let details = host.tool_plugin_details();
    assert_eq!(details.len(), 1);
    let (manifest, path) = details[0];
    assert!(manifest.permissions.contains(&PluginPermission::HttpClient));
    let scope = PluginInstanceScope::from_manifest(
        manifest,
        PluginCapability::Tool,
        "timeout",
        manifest.permissions.iter().copied(),
    )
    .expect("admit fixture scope");
    runtime::create_plugin(path, &scope, limits(call_timeout, call_fuel))
        .await
        .expect("instantiate timeout fixture")
}

async fn server(response: ServerResponse) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture server");
    let address = listener.local_addr().expect("fixture server address");
    let task = ::zeroclaw_spawn::spawn!(async move {
        let (mut stream, _) = listener.accept().await.expect("accept fixture request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await;
        match response {
            ServerResponse::Complete => {
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await
                    .expect("write complete response");
            }
            ServerResponse::Drip => {
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\nx")
                    .await
                    .expect("write dripping response head");
                loop {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if stream.write_all(b"x").await.is_err() {
                        break;
                    }
                }
            }
            ServerResponse::NoResponse => {
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    });
    (format!("http://{address}/body"), task)
}

enum ServerResponse {
    Complete,
    Drip,
    NoResponse,
}

async fn execute(
    plugin: &mut runtime::Plugin,
    value: serde_json::Value,
) -> anyhow::Result<zeroclaw_api::tool::ToolResult> {
    runtime::call_execute(
        plugin,
        &serde_json::to_vec(&value).expect("serialize fixture input"),
        &HashMap::new(),
    )
    .await
}

#[tokio::test]
async fn dripping_response_is_stopped_by_host_deadline() {
    let (url, server) = server(ServerResponse::Drip).await;
    let mut plugin = plugin(Duration::from_millis(250)).await;
    let started = Instant::now();
    let error = execute(&mut plugin, serde_json::json!({"mode": "http", "url": url}))
        .await
        .expect_err("drip must hit the host deadline");
    assert!(
        error.to_string().contains("wall-clock deadline"),
        "unexpected error: {error:#}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    let unavailable = execute(&mut plugin, serde_json::json!({"mode": "spin"}))
        .await
        .expect_err("timed-out tool instance must not resume its store");
    assert!(
        unavailable.to_string().contains("instance is unavailable"),
        "unexpected post-timeout error: {unavailable:#}"
    );
    server.abort();
}

#[tokio::test]
async fn normal_response_completes_before_host_deadline() {
    let (url, server) = server(ServerResponse::Complete).await;
    let mut plugin = plugin(Duration::from_secs(2)).await;
    let result = execute(&mut plugin, serde_json::json!({"mode": "http", "url": url}))
        .await
        .expect("normal response completes");
    assert_eq!(&*result.output, "2 bytes");
    server.await.expect("server task");
}

#[tokio::test]
async fn guest_first_byte_timeout_can_shorten_but_not_extend_host_deadline() {
    let (short_guest_url, short_guest_server) = server(ServerResponse::NoResponse).await;
    let mut guest_first = plugin(Duration::from_secs(2)).await;
    let started = Instant::now();
    let guest_error = execute(
        &mut guest_first,
        serde_json::json!({
            "mode": "raw-first-byte",
            "url": short_guest_url,
            "guest_timeout_ms": 100
        }),
    )
    .await
    .expect_err("guest timeout must fire");
    assert!(
        !guest_error.to_string().contains("wall-clock deadline"),
        "guest timeout was incorrectly replaced by the host ceiling: {guest_error:#}"
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    short_guest_server.abort();

    let (host_url, host_server) = server(ServerResponse::NoResponse).await;
    let mut host_first = plugin(Duration::from_millis(250)).await;
    let host_error = execute(
        &mut host_first,
        serde_json::json!({
            "mode": "raw-first-byte",
            "url": host_url,
            "guest_timeout_ms": 2_000
        }),
    )
    .await
    .expect_err("host deadline must cap the longer guest timeout");
    assert!(
        host_error.to_string().contains("wall-clock deadline"),
        "unexpected error: {host_error:#}"
    );
    host_server.abort();
}

#[tokio::test]
async fn uninterrupted_guest_compute_cannot_starve_wall_clock_deadline() {
    let mut plugin = plugin_with_fuel(Duration::from_millis(250), u64::MAX).await;
    let started = Instant::now();
    let error = execute(&mut plugin, serde_json::json!({"mode": "spin"}))
        .await
        .expect_err("spinning guest must hit wall-clock deadline");
    assert!(
        error.to_string().contains("wall-clock deadline"),
        "unexpected error: {error:#}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}
