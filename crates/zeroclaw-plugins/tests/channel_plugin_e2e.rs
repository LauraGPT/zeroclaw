//! End-to-end fixture for the host's channel-component adapter.
//!
//! The source fixture is a workspace member and is built on demand into a
//! separate target directory so the nested Cargo invocation cannot contend
//! with the host test process's build lock.

#![cfg(feature = "plugins-wasm-cranelift")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use zeroclaw_api::attribution::Attributable;
use zeroclaw_api::channel::{Channel, SendMessage};
use zeroclaw_plugins::component::{HostInboundMessage, PluginLimits};
use zeroclaw_plugins::endpoint::PluginChannelEndpoint;
use zeroclaw_plugins::instance::PluginInstanceScope;
use zeroclaw_plugins::wasm_channel::WasmChannel;
use zeroclaw_plugins::{PluginCapability, PluginManifest};

fn fixture() -> PathBuf {
    static FIXTURE: OnceLock<PathBuf> = OnceLock::new();
    FIXTURE
        .get_or_init(|| {
            let fixture_dir =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/channel-fixture");
            let target_dir =
                PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("channel-plugin-fixture");
            let status = Command::new(env!("CARGO"))
                .current_dir(&fixture_dir)
                .args([
                    "build",
                    "--locked",
                    "--quiet",
                    "--package",
                    "zeroclaw-channel-plugin-fixture",
                    "--target",
                    "wasm32-wasip2",
                    "--target-dir",
                ])
                .arg(&target_dir)
                .status()
                .expect("run Cargo for the channel component fixture");
            assert!(
                status.success(),
                "channel fixture must build; install the wasm32-wasip2 target"
            );

            let wasm = target_dir.join("wasm32-wasip2/debug/zeroclaw_channel_plugin_fixture.wasm");
            assert!(wasm.is_file(), "channel fixture WASM was not produced");
            wasm
        })
        .clone()
}

fn limits() -> PluginLimits {
    limits_with_timeout(Duration::from_secs(30))
}

fn limits_with_timeout(call_timeout: Duration) -> PluginLimits {
    PluginLimits {
        call_fuel: 1_000_000_000,
        max_memory_bytes: 64 * 1024 * 1024,
        max_table_elements: 10_000,
        max_instances: 32,
        call_timeout,
    }
}

async fn channel(binding: &str) -> WasmChannel {
    let manifest = PluginManifest {
        name: "channel-fixture".to_string(),
        version: "0.0.0".to_string(),
        description: None,
        author: None,
        wasm_path: Some("channel-fixture.wasm".to_string()),
        capabilities: vec![PluginCapability::Channel],
        permissions: vec![zeroclaw_plugins::PluginPermission::HttpClient],
        signature: None,
        publisher_key: None,
    };
    let scope = PluginInstanceScope::from_manifest(
        &manifest,
        PluginCapability::Channel,
        binding,
        manifest.permissions.iter().copied(),
    )
    .expect("admit fixture scope");
    let endpoint = PluginChannelEndpoint::new(scope, "plugin").expect("bind fixture endpoint");

    WasmChannel::from_wasm(endpoint, &fixture(), &HashMap::new(), limits())
        .await
        .expect("instantiate fixture channel")
}

async fn channel_with_timeout(binding: &str, timeout: Duration) -> WasmChannel {
    let manifest = PluginManifest {
        name: "channel-fixture".to_string(),
        version: "0.0.0".to_string(),
        description: None,
        author: None,
        wasm_path: Some("channel-fixture.wasm".to_string()),
        capabilities: vec![PluginCapability::Channel],
        permissions: vec![zeroclaw_plugins::PluginPermission::HttpClient],
        signature: None,
        publisher_key: None,
    };
    let scope = PluginInstanceScope::from_manifest(
        &manifest,
        PluginCapability::Channel,
        binding,
        manifest.permissions.iter().copied(),
    )
    .expect("admit fixture scope");
    let endpoint = PluginChannelEndpoint::new(scope, "plugin").expect("bind fixture endpoint");

    WasmChannel::from_wasm(
        endpoint,
        &fixture(),
        &HashMap::new(),
        limits_with_timeout(timeout),
    )
    .await
    .expect("instantiate fixture channel")
}

fn outbound() -> SendMessage {
    SendMessage {
        content: "hello".to_string(),
        recipient: "room".to_string(),
        subject: None,
        thread_ts: None,
        cancellation_token: None,
        attachments: Vec::new(),
        in_reply_to: None,
        suppress_voice: false,
        force_voice: false,
    }
}

#[tokio::test]
async fn channel_component_runs_through_host_ingress() {
    let channel = channel("main").await;

    assert_eq!(channel.name(), "plugin");
    assert_eq!(channel.alias(), "main");
    assert_eq!(channel.self_handle().as_deref(), Some("@fixture"));
    assert!(channel.health_check().await);
    channel
        .send(&outbound())
        .await
        .expect("fixture accepts send");

    let inbound = channel.inbound();
    inbound.enqueue(HostInboundMessage {
        id: "host-1".to_string(),
        sender: "tester".to_string(),
        reply_target: "room".to_string(),
        content: "ping".to_string(),
        channel: "host-channel".to_string(),
        channel_alias: Some("host-alias".to_string()),
        timestamp: 7,
        ..Default::default()
    });

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = zeroclaw_spawn::spawn!(async move { channel.listen(tx).await });
    let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("fixture message arrives before timeout")
        .expect("listener remains connected");

    assert_eq!(message.id, "host-1");
    assert_eq!(message.content, "ping");
    assert_eq!(message.timestamp, 7);
    assert_eq!(message.channel, "plugin");
    assert_eq!(message.channel_alias.as_deref(), Some("main"));

    assert!(
        !listener.is_finished(),
        "listen must retain ownership of its polling loop"
    );
    listener.abort();
    let error = listener
        .await
        .expect_err("aborting listen must cancel its polling loop");
    assert!(error.is_cancelled());
}

#[tokio::test]
async fn channel_listener_stops_when_receiver_closes() {
    let channel = channel("closed").await;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let listener = zeroclaw_spawn::spawn!(async move { channel.listen(tx).await });

    drop(rx);
    tokio::time::timeout(Duration::from_secs(1), listener)
        .await
        .expect("listener observes receiver closure")
        .expect("listener task joins")
        .expect("listener exits cleanly");
}

#[tokio::test]
async fn timed_out_channel_call_releases_lock_and_recreates_instance() {
    let channel = channel_with_timeout("recreate", Duration::from_millis(500)).await;

    let drip_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind drip server");
    let drip_address = drip_listener.local_addr().expect("drip server address");
    let drip_server = ::zeroclaw_spawn::spawn!(async move {
        let (mut stream, _) = drip_listener.accept().await.expect("accept drip request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\nx")
            .await
            .expect("write drip head");
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if stream.write_all(b"x").await.is_err() {
                break;
            }
        }
    });
    let mut dripping = outbound();
    dripping.content = format!("http://{drip_address}/body");
    let error = channel
        .send(&dripping)
        .await
        .expect_err("dripping send must hit the host deadline");
    assert!(
        error.to_string().contains("wall-clock deadline"),
        "unexpected error: {error:#}"
    );
    drip_server.abort();

    let healthy_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind healthy server");
    let healthy_address = healthy_listener
        .local_addr()
        .expect("healthy server address");
    let healthy_server = ::zeroclaw_spawn::spawn!(async move {
        let (mut stream, _) = healthy_listener
            .accept()
            .await
            .expect("accept healthy request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .expect("write healthy response");
    });
    let mut healthy = outbound();
    healthy.content = format!("http://{healthy_address}/body");
    tokio::time::timeout(Duration::from_secs(2), channel.send(&healthy))
        .await
        .expect("recreated channel call is not stranded behind the old lock")
        .expect("recreated channel handles a healthy request");
    healthy_server.await.expect("healthy server task");
}

#[tokio::test]
async fn externally_cancelled_channel_call_discards_store_and_recreates() {
    let channel = Arc::new(channel_with_timeout("cancel", Duration::from_secs(5)).await);

    let stalled_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalled server");
    let stalled_address = stalled_listener
        .local_addr()
        .expect("stalled server address");
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let stalled_server = ::zeroclaw_spawn::spawn!(async move {
        let (mut stream, _) = stalled_listener
            .accept()
            .await
            .expect("accept stalled request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await;
        let _ = accepted_tx.send(());
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let mut stalled = outbound();
    stalled.content = format!("http://{stalled_address}/body");
    let cancelled_channel = Arc::clone(&channel);
    let call = ::zeroclaw_spawn::spawn!(async move { cancelled_channel.send(&stalled).await });
    accepted_rx
        .await
        .expect("guest call reached the async HTTP host import");
    call.abort();
    assert!(
        call.await
            .expect_err("call task was aborted")
            .is_cancelled(),
        "external cancellation must drop the in-flight host call"
    );
    stalled_server.abort();

    let healthy_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind post-cancel server");
    let healthy_address = healthy_listener
        .local_addr()
        .expect("post-cancel server address");
    let healthy_server = ::zeroclaw_spawn::spawn!(async move {
        let (mut stream, _) = healthy_listener
            .accept()
            .await
            .expect("accept post-cancel request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .expect("write post-cancel response");
    });
    let mut healthy = outbound();
    healthy.content = format!("http://{healthy_address}/body");
    tokio::time::timeout(Duration::from_secs(2), channel.send(&healthy))
        .await
        .expect("cancelled call released the channel lock")
        .expect("fresh channel instance served the healthy call");
    healthy_server.await.expect("post-cancel server task");
}
