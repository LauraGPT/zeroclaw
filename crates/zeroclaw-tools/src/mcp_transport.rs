//! MCP transport abstraction — supports stdio, SSE, and HTTP transports.

use std::borrow::Cow;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::time::{Duration, timeout};
use tokio_stream::StreamExt;

use crate::mcp_protocol::{JsonRpcRequest, JsonRpcResponse};
use zeroclaw_config::schema::{McpServerConfig, McpTransport};

/// Maximum bytes for a single JSON-RPC response.
const MAX_LINE_BYTES: usize = 4 * 1024 * 1024; // 4 MB

/// Timeout for init/list operations.
const RECV_TIMEOUT_SECS: u64 = 30;

/// Legacy default HTTP request timeout for non-tool MCP HTTP/SSE requests.
const DEFAULT_HTTP_REQUEST_TIMEOUT_SECS: u64 = 120;

/// JSON-RPC method name for MCP tool calls.
const TOOLS_CALL_METHOD: &str = "tools/call";

/// Streamable HTTP Accept header required by MCP HTTP transport.
const MCP_STREAMABLE_ACCEPT: &str = "application/json, text/event-stream";

/// Default media type for MCP JSON-RPC request bodies.
const MCP_JSON_CONTENT_TYPE: &str = "application/json";
/// Streamable HTTP session header used to preserve MCP server state.
const MCP_SESSION_ID_HEADER: &str = "Mcp-Session-Id";

fn http_request_timeout_secs(
    request: &JsonRpcRequest,
    tool_timeout_secs: Option<u64>,
) -> Option<u64> {
    if request.method == TOOLS_CALL_METHOD {
        tool_timeout_secs
    } else {
        Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS)
    }
}

fn http_sse_read_timeout_secs(
    request: &JsonRpcRequest,
    tool_timeout_secs: Option<u64>,
) -> Option<u64> {
    if request.method == TOOLS_CALL_METHOD {
        tool_timeout_secs
    } else {
        Some(RECV_TIMEOUT_SECS)
    }
}

fn apply_request_timeout(
    req: reqwest::RequestBuilder,
    timeout_secs: Option<u64>,
) -> reqwest::RequestBuilder {
    if let Some(timeout_secs) = timeout_secs {
        req.timeout(Duration::from_secs(timeout_secs))
    } else {
        req
    }
}

fn require_https_url(server_name: &str, url: &str, target: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url)
        .with_context(|| format!("MCP server `{server_name}`: invalid {target} URL"))?;
    if parsed.scheme() != "https" {
        bail!(
            "MCP server `{server_name}`: tls_ca_cert_path requires an HTTPS {target}; \
             refusing plaintext transport"
        );
    }
    Ok(())
}

/// Build the shared HTTP client for remote MCP transports.
///
/// The optional server-specific CA is additive: system/default roots remain
/// enabled, and normal chain and hostname verification stay in force.
fn build_remote_http_client(config: &McpServerConfig) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();

    if let Some(path) = config.tls_ca_cert_path.as_deref() {
        let server_name = config.name.clone();
        builder = builder.redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= 10 {
                attempt.error(std::io::Error::other(format!(
                    "MCP server `{server_name}`: too many redirects"
                )))
            } else if attempt.url().scheme() == "https" {
                attempt.follow()
            } else {
                attempt.error(std::io::Error::other(format!(
                    "MCP server `{server_name}`: tls_ca_cert_path forbids redirecting to plaintext"
                )))
            }
        }));

        if !std::path::Path::new(path).is_absolute() {
            bail!(
                "MCP server `{}`: TLS CA certificate path must be absolute: `{}`",
                config.name,
                path
            );
        }

        let pem = std::fs::read(path).with_context(|| {
            format!(
                "MCP server `{}`: cannot read TLS CA certificate at `{}`",
                config.name, path
            )
        })?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem).with_context(|| {
            format!(
                "MCP server `{}`: invalid PEM CA certificate at `{}`",
                config.name, path
            )
        })?;
        if certificates.is_empty() {
            bail!(
                "MCP server `{}`: CA certificate file `{}` contained no certificates",
                config.name,
                path
            );
        }
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
    }

    builder.build().with_context(|| {
        format!(
            "failed to build HTTP client for MCP server `{}`",
            config.name
        )
    })
}

// ── Transport Errors ───────────────────────────────────────────────────────

/// Transport-level failures that are recoverable by reconnecting — resetting
/// the session and re-running the MCP handshake — rather than surfacing to the
/// caller. Distinct from a genuine tool/application error, which must be
/// reported as-is and never retried.
#[derive(Debug, thiserror::Error)]
pub enum McpTransportError {
    /// The server no longer recognizes our session (typically after it
    /// restarted). Surfaced from HTTP 404/410 responses.
    #[error("MCP session is stale (HTTP {status})")]
    StaleSession { status: u16 },

    /// The underlying stream/connection dropped before a response arrived
    /// (e.g. SSE EOF or connection reset).
    #[error("MCP transport connection closed")]
    TransportClosed,
}

// ── Transport Trait ──────────────────────────────────────────────────────

/// Abstract transport for MCP communication.
#[async_trait::async_trait]
pub trait McpTransportConn: Send + Sync {
    /// Send a JSON-RPC request and receive the response.
    async fn send_and_recv(&mut self, request: &JsonRpcRequest) -> Result<JsonRpcResponse>;

    /// Reset per-connection session state so the next operation re-establishes
    /// a fresh session. Default is a no-op for stateless transports (stdio).
    async fn reset(&mut self) -> Result<()> {
        Ok(())
    }

    /// Check whether the underlying transport is still alive without sending a
    /// real request.  The HTTP and SSE transports always return `Ok(true)` —
    /// connection drops surface through `send_and_recv` errors.  The stdio
    /// transport verifies the child process is still running via `try_wait()`.
    fn health_check(&mut self) -> bool {
        true
    }

    /// Close the connection.
    async fn close(&mut self) -> Result<()>;
}

// ── Stdio Transport ──────────────────────────────────────────────────────

/// Stdio-based transport (spawn local process).
pub struct StdioTransport {
    _child: Child,
    stdin: tokio::process::ChildStdin,
    stdout_lines: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
}

impl StdioTransport {
    pub fn new(config: &McpServerConfig) -> Result<Self> {
        let mut child = Command::new(&config.command)
            .args(&config.args)
            .envs(&config.env)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to spawn MCP server `{}`", config.name))?;

        let stdin = child.stdin.take().ok_or_else(|| {
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({
                        "mcp_server": &config.name,
                        "missing": "stdin",
                    })),
                "mcp_transport: no stdin on spawned MCP server"
            );
            anyhow::Error::msg(format!("no stdin on MCP server `{}`", config.name))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({
                        "mcp_server": &config.name,
                        "missing": "stdout",
                    })),
                "mcp_transport: no stdout on spawned MCP server"
            );
            anyhow::Error::msg(format!("no stdout on MCP server `{}`", config.name))
        })?;
        let stdout_lines = BufReader::new(stdout).lines();

        Ok(Self {
            _child: child,
            stdin,
            stdout_lines,
        })
    }

    async fn send_raw(&mut self, line: &str) -> Result<()> {
        self.stdin
            .write_all(line.as_bytes())
            .await
            .context("failed to write to MCP server stdin")?;
        self.stdin
            .write_all(b"\n")
            .await
            .context("failed to write newline to MCP server stdin")?;
        self.stdin.flush().await.context("failed to flush stdin")?;
        Ok(())
    }

    async fn recv_raw(&mut self) -> Result<String> {
        let line = self.stdout_lines.next_line().await?.ok_or_else(|| {
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure),
                "mcp_transport: MCP server closed stdout"
            );
            anyhow::Error::msg("MCP server closed stdout")
        })?;
        if line.len() > MAX_LINE_BYTES {
            bail!("MCP response too large: {} bytes", line.len());
        }
        Ok(line)
    }
}

#[async_trait::async_trait]
impl McpTransportConn for StdioTransport {
    async fn send_and_recv(&mut self, request: &JsonRpcRequest) -> Result<JsonRpcResponse> {
        let line = serde_json::to_string(request)?;
        self.send_raw(&line).await?;
        if request.id.is_none() {
            return Ok(JsonRpcResponse {
                jsonrpc: crate::mcp_protocol::JSONRPC_VERSION.to_string(),
                id: None,
                result: None,
                error: None,
            });
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(RECV_TIMEOUT_SECS);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                bail!("timeout waiting for MCP response");
            }
            let resp_line = timeout(remaining, self.recv_raw())
                .await
                .context("timeout waiting for MCP response")??;
            let resp: JsonRpcResponse = serde_json::from_str(&resp_line)
                .with_context(|| format!("invalid JSON-RPC response: {}", resp_line))?;
            if resp.id.is_none() {
                // Server-sent notification (e.g. `notifications/initialized`) — skip and
                // keep waiting for the actual response to our request.
                ::zeroclaw_log::record!(
                    DEBUG,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                    "MCP stdio: skipping server notification while waiting for response"
                );
                continue;
            }
            return Ok(resp);
        }
    }

    async fn close(&mut self) -> Result<()> {
        let _ = self.stdin.shutdown().await;
        Ok(())
    }

    fn health_check(&mut self) -> bool {
        // Verify the child process is still running via try_wait().
        // Returns true only when the process is alive (has not exited).
        self._child
            .try_wait()
            .map_or(true, |status| status.is_none())
    }
}

// ── HTTP Transport ───────────────────────────────────────────────────────

/// HTTP-based transport (POST requests).
pub struct HttpTransport {
    url: String,
    /// Per-server tool-call timeout, from `McpServerConfig.tool_timeout_secs`.
    /// Non-tool requests keep the legacy HTTP request timeout and short SSE
    /// read timeout. Tool calls use the configured budget when present; when
    /// absent, the client layer's outer tool-call timeout owns the budget.
    tool_timeout_secs: Option<u64>,
    client: reqwest::Client,
    headers: std::collections::HashMap<String, String>,
    session_id: Option<String>,
}

impl HttpTransport {
    pub fn new(config: &McpServerConfig) -> Result<Self> {
        let url = config
            .url
            .as_ref()
            .ok_or_else(|| {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                        .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                        .with_attrs(::serde_json::json!({
                            "mcp_server": &config.name,
                            "transport": "http",
                        })),
                    "mcp_transport: HTTP transport requires URL"
                );
                anyhow::Error::msg("URL required for HTTP transport")
            })?
            .clone();

        if config.tls_ca_cert_path.is_some() {
            require_https_url(&config.name, &url, "configured remote URL")?;
        }
        let client = build_remote_http_client(config)?;

        Ok(Self {
            url,
            tool_timeout_secs: config.tool_timeout_secs,
            client,
            headers: config.headers.clone(),
            session_id: None,
        })
    }

    fn apply_session_header(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(session_id) = self.session_id.as_deref() {
            req.header(MCP_SESSION_ID_HEADER, session_id)
        } else {
            req
        }
    }

    fn update_session_id_from_headers(&mut self, headers: &reqwest::header::HeaderMap) {
        if let Some(session_id) = headers
            .get(MCP_SESSION_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            self.session_id = Some(session_id.to_string());
        }
    }
}

#[async_trait::async_trait]
impl McpTransportConn for HttpTransport {
    async fn send_and_recv(&mut self, request: &JsonRpcRequest) -> Result<JsonRpcResponse> {
        let body = serde_json::to_string(request)?;

        let has_accept = self
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("Accept"));
        let has_content_type = self
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("Content-Type"));

        let mut req = apply_request_timeout(
            self.client.post(&self.url).body(body),
            http_request_timeout_secs(request, self.tool_timeout_secs),
        );
        if !has_content_type {
            req = req.header("Content-Type", MCP_JSON_CONTENT_TYPE);
        }
        for (key, value) in &self.headers {
            req = req.header(key, value);
        }
        req = self.apply_session_header(req);
        if !has_accept {
            req = req.header("Accept", MCP_STREAMABLE_ACCEPT);
        }

        let resp = req
            .send()
            .await
            .context("HTTP request to MCP server failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            if self.session_id.is_some()
                && (status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::GONE)
            {
                return Err(McpTransportError::StaleSession {
                    status: status.as_u16(),
                }
                .into());
            }
            bail!("MCP server returned HTTP {}", status);
        }

        self.update_session_id_from_headers(resp.headers());

        if request.id.is_none() {
            return Ok(JsonRpcResponse {
                jsonrpc: crate::mcp_protocol::JSONRPC_VERSION.to_string(),
                id: None,
                result: None,
                error: None,
            });
        }

        let is_sse = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_ascii_lowercase().contains("text/event-stream"));
        if is_sse {
            let read_response = read_first_jsonrpc_from_sse_response(resp);
            let maybe_resp = if let Some(sse_timeout) =
                http_sse_read_timeout_secs(request, self.tool_timeout_secs)
            {
                timeout(Duration::from_secs(sse_timeout), read_response)
                    .await
                    .context("timeout waiting for MCP response from streamable HTTP SSE stream")??
            } else {
                read_response.await?
            };
            return maybe_resp.ok_or_else(|| {
                ::zeroclaw_log::record!(
                    ERROR,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                        .with_outcome(::zeroclaw_log::EventOutcome::Failure),
                    "mcp_transport: MCP server returned no response in SSE stream"
                );
                anyhow::Error::msg("MCP server returned no response in SSE stream")
            });
        }

        let resp_text = resp.text().await.context("failed to read HTTP response")?;
        parse_jsonrpc_response_text(&resp_text)
    }

    async fn reset(&mut self) -> Result<()> {
        // Drop the stale session so the next request re-initializes and the
        // server issues a fresh `Mcp-Session-Id`.
        self.session_id = None;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// ── SSE Transport ─────────────────────────────────────────────────────────

/// SSE-based transport (HTTP POST for requests, SSE for responses).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SseStreamState {
    Unknown,
    Connected,
    Unsupported,
}

pub struct SseTransport {
    sse_url: String,
    server_name: String,
    tool_timeout_secs: Option<u64>,
    client: reqwest::Client,
    headers: std::collections::HashMap<String, String>,
    require_https: bool,
    stream_state: SseStreamState,
    shared: std::sync::Arc<Mutex<SseSharedState>>,
    notify: std::sync::Arc<Notify>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    reader_task: Option<tokio::task::JoinHandle<()>>,
}

impl SseTransport {
    pub fn new(config: &McpServerConfig) -> Result<Self> {
        let sse_url = config
            .url
            .as_ref()
            .ok_or_else(|| {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                        .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                        .with_attrs(::serde_json::json!({
                            "mcp_server": &config.name,
                            "transport": "sse",
                        })),
                    "mcp_transport: SSE transport requires URL"
                );
                anyhow::Error::msg("URL required for SSE transport")
            })?
            .clone();

        let require_https = config.tls_ca_cert_path.is_some();
        if require_https {
            require_https_url(&config.name, &sse_url, "configured remote URL")?;
        }
        let client = build_remote_http_client(config)?;

        Ok(Self {
            sse_url,
            server_name: config.name.clone(),
            tool_timeout_secs: config.tool_timeout_secs,
            client,
            headers: config.headers.clone(),
            require_https,
            stream_state: SseStreamState::Unknown,
            shared: std::sync::Arc::new(Mutex::new(SseSharedState::default())),
            notify: std::sync::Arc::new(Notify::new()),
            shutdown_tx: None,
            reader_task: None,
        })
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if self.stream_state == SseStreamState::Unsupported {
            return Ok(());
        }
        if let Some(task) = &self.reader_task
            && !task.is_finished()
        {
            self.stream_state = SseStreamState::Connected;
            return Ok(());
        }

        let has_accept = self
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("Accept"));

        let mut req = self
            .client
            .get(&self.sse_url)
            .header("Cache-Control", "no-cache");
        for (key, value) in &self.headers {
            req = req.header(key, value);
        }
        if !has_accept {
            req = req.header("Accept", MCP_STREAMABLE_ACCEPT);
        }

        let resp = req.send().await.context("SSE GET to MCP server failed")?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND
            || resp.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED
        {
            self.stream_state = SseStreamState::Unsupported;
            return Ok(());
        }
        if !resp.status().is_success() {
            let status = resp.status();
            ::zeroclaw_log::record!(
                ERROR,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Fail)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({"status": status.as_u16()})),
                "mcp_transport: MCP server returned non-success HTTP"
            );
            return Err(anyhow::Error::msg(format!(
                "MCP server returned HTTP {}",
                status
            )));
        }
        let is_event_stream = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_ascii_lowercase().contains("text/event-stream"));
        if !is_event_stream {
            self.stream_state = SseStreamState::Unsupported;
            return Ok(());
        }

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        self.shutdown_tx = Some(shutdown_tx);

        let shared = self.shared.clone();
        let notify = self.notify.clone();
        let sse_url = self.sse_url.clone();
        let server_name = self.server_name.clone();

        self.reader_task = Some(zeroclaw_spawn::spawn!(async move {
            let stream = resp
                .bytes_stream()
                .map(|item| item.map_err(std::io::Error::other));
            let reader = tokio_util::io::StreamReader::new(stream);
            let mut lines = BufReader::new(reader).lines();

            let mut cur_event: Option<String> = None;
            let mut cur_id: Option<String> = None;
            let mut cur_data: Vec<String> = Vec::new();

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    line = lines.next_line() => {
                        let Ok(line_opt) = line else { break; };
                        let Some(mut line) = line_opt else { break; };
                        if line.ends_with('\r') {
                            line.pop();
                        }
                        if line.is_empty() {
                            if cur_event.is_none() && cur_id.is_none() && cur_data.is_empty() {
                                continue;
                            }
                            let event = cur_event.take();
                            let data = cur_data.join("\n");
                            cur_data.clear();
                            let id = cur_id.take();
                            handle_sse_event(&server_name, &sse_url, &shared, &notify, event.as_deref(), id.as_deref(), data).await;
                            continue;
                        }

                        if line.starts_with(':') {
                            continue;
                        }

                        if let Some(rest) = line.strip_prefix("event:") {
                            cur_event = Some(rest.trim().to_string());
                        }
                        if let Some(rest) = line.strip_prefix("data:") {
                            let rest = rest.strip_prefix(' ').unwrap_or(rest);
                            cur_data.push(rest.to_string());
                        }
                        if let Some(rest) = line.strip_prefix("id:") {
                            cur_id = Some(rest.trim().to_string());
                        }
                    }
                }
            }

            // Stream closed: drop every pending sender so each waiter observes a
            // `RecvError`, which `send_and_recv` maps to
            // `McpTransportError::TransportClosed` to trigger a reconnect.
            let pending = {
                let mut guard = shared.lock().await;
                std::mem::take(&mut guard.pending)
            };
            drop(pending);
        }));
        self.stream_state = SseStreamState::Connected;

        Ok(())
    }

    async fn get_message_url(&self) -> Result<(String, bool)> {
        let guard = self.shared.lock().await;
        if let Some(url) = &guard.message_url {
            return Ok((url.clone(), guard.message_url_from_endpoint));
        }
        drop(guard);

        let derived = derive_message_url(&self.sse_url, "messages")
            .or_else(|| derive_message_url(&self.sse_url, "message"))
            .ok_or_else(|| {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                        .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                        .with_attrs(::serde_json::json!({"sse_url": &self.sse_url})),
                    "mcp_transport: invalid SSE URL"
                );
                anyhow::Error::msg("invalid SSE URL")
            })?;
        let mut guard = self.shared.lock().await;
        if guard.message_url.is_none() {
            guard.message_url = Some(derived.clone());
            guard.message_url_from_endpoint = false;
        }
        Ok((derived, false))
    }
}

#[derive(Default)]
struct SseSharedState {
    message_url: Option<String>,
    message_url_from_endpoint: bool,
    pending: std::collections::HashMap<u64, oneshot::Sender<JsonRpcResponse>>,
}

fn derive_message_url(sse_url: &str, message_path: &str) -> Option<String> {
    let url = reqwest::Url::parse(sse_url).ok()?;
    let mut segments: Vec<&str> = url.path_segments()?.collect();
    if segments.is_empty() {
        return None;
    }
    if segments.last().copied() == Some("sse") {
        segments.pop();
        segments.push(message_path);
        let mut new_url = url.clone();
        new_url.set_path(&format!("/{}", segments.join("/")));
        return Some(new_url.to_string());
    }
    let mut new_url = url.clone();
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push('/');
    path.push_str(message_path);
    new_url.set_path(&path);
    Some(new_url.to_string())
}

async fn handle_sse_event(
    server_name: &str,
    sse_url: &str,
    shared: &std::sync::Arc<Mutex<SseSharedState>>,
    notify: &std::sync::Arc<Notify>,
    event: Option<&str>,
    _id: Option<&str>,
    data: String,
) {
    let event = event.unwrap_or("message");
    let trimmed = data.trim();
    if trimmed.is_empty() {
        return;
    }

    if event.eq_ignore_ascii_case("endpoint") || event.eq_ignore_ascii_case("mcp-endpoint") {
        if let Some(url) = parse_endpoint_from_data(sse_url, trimmed) {
            let mut guard = shared.lock().await;
            guard.message_url = Some(url);
            guard.message_url_from_endpoint = true;
            drop(guard);
            notify.notify_waiters();
        }
        return;
    }

    if !event.eq_ignore_ascii_case("message") {
        return;
    }

    let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        return;
    };

    let Ok(resp) = serde_json::from_value::<JsonRpcResponse>(value.clone()) else {
        let _ = serde_json::from_value::<JsonRpcRequest>(value);
        return;
    };

    let Some(id_val) = resp.id.clone() else {
        return;
    };
    let id = match id_val.as_u64() {
        Some(v) => v,
        None => return,
    };

    let tx = {
        let mut guard = shared.lock().await;
        guard.pending.remove(&id)
    };
    if let Some(tx) = tx {
        let _ = tx.send(resp);
    } else {
        ::zeroclaw_log::record!(
            DEBUG,
            ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
            &format!(
                "MCP SSE `{}` received response for unknown id {}",
                server_name, id
            )
        );
    }
}

fn parse_endpoint_from_data(sse_url: &str, data: &str) -> Option<String> {
    if data.starts_with('{') {
        let v: serde_json::Value = serde_json::from_str(data).ok()?;
        let endpoint = v.get("endpoint")?.as_str()?;
        return parse_endpoint_from_data(sse_url, endpoint);
    }
    if data.starts_with("http://") || data.starts_with("https://") {
        return Some(data.to_string());
    }
    let base = reqwest::Url::parse(sse_url).ok()?;
    base.join(data).ok().map(|u| u.to_string())
}

fn extract_json_from_sse_text(resp_text: &str) -> Cow<'_, str> {
    let text = resp_text.trim_start_matches('\u{feff}');
    let mut current_data_lines: Vec<&str> = Vec::new();
    let mut last_event_data_lines: Vec<&str> = Vec::new();

    for raw_line in text.lines() {
        let line = raw_line.trim_end_matches('\r').trim_start();
        if line.is_empty() {
            if !current_data_lines.is_empty() {
                last_event_data_lines = std::mem::take(&mut current_data_lines);
            }
            continue;
        }

        if line.starts_with(':') {
            continue;
        }

        if let Some(rest) = line.strip_prefix("data:") {
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            current_data_lines.push(rest);
        }
    }

    if !current_data_lines.is_empty() {
        last_event_data_lines = current_data_lines;
    }

    if last_event_data_lines.is_empty() {
        return Cow::Borrowed(text.trim());
    }

    if last_event_data_lines.len() == 1 {
        return Cow::Borrowed(last_event_data_lines[0].trim());
    }

    let joined = last_event_data_lines.join("\n");
    Cow::Owned(joined.trim().to_string())
}

fn parse_jsonrpc_response_text(resp_text: &str) -> Result<JsonRpcResponse> {
    let trimmed = resp_text.trim();
    if trimmed.is_empty() {
        bail!("MCP server returned no response");
    }

    let json_text = if looks_like_sse_text(trimmed) {
        extract_json_from_sse_text(trimmed)
    } else {
        Cow::Borrowed(trimmed)
    };

    let mcp_resp: JsonRpcResponse = serde_json::from_str(json_text.as_ref())
        .with_context(|| format!("invalid JSON-RPC response: {}", resp_text))?;
    Ok(mcp_resp)
}

fn looks_like_sse_text(text: &str) -> bool {
    text.starts_with("data:")
        || text.starts_with("event:")
        || text.contains("\ndata:")
        || text.contains("\nevent:")
}

async fn read_first_jsonrpc_from_sse_response(
    resp: reqwest::Response,
) -> Result<Option<JsonRpcResponse>> {
    let stream = resp
        .bytes_stream()
        .map(|item| item.map_err(std::io::Error::other));
    let reader = tokio_util::io::StreamReader::new(stream);
    let mut lines = BufReader::new(reader).lines();

    let mut cur_event: Option<String> = None;
    let mut cur_data: Vec<String> = Vec::new();

    while let Ok(line_opt) = lines.next_line().await {
        let Some(mut line) = line_opt else { break };
        if line.ends_with('\r') {
            line.pop();
        }
        if line.is_empty() {
            if cur_event.is_none() && cur_data.is_empty() {
                continue;
            }
            let event = cur_event.take();
            let data = cur_data.join("\n");
            cur_data.clear();

            let event = event.unwrap_or_else(|| "message".to_string());
            if event.eq_ignore_ascii_case("endpoint") || event.eq_ignore_ascii_case("mcp-endpoint")
            {
                continue;
            }
            if !event.eq_ignore_ascii_case("message") {
                continue;
            }

            let trimmed = data.trim();
            if trimmed.is_empty() {
                continue;
            }
            let json_str = extract_json_from_sse_text(trimmed);
            if let Ok(resp) = serde_json::from_str::<JsonRpcResponse>(json_str.as_ref()) {
                return Ok(Some(resp));
            }
            continue;
        }

        if line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            cur_event = Some(rest.trim().to_string());
        }
        if let Some(rest) = line.strip_prefix("data:") {
            let rest = rest.strip_prefix(' ').unwrap_or(rest);
            cur_data.push(rest.to_string());
        }
    }

    Ok(None)
}

#[async_trait::async_trait]
impl McpTransportConn for SseTransport {
    async fn send_and_recv(&mut self, request: &JsonRpcRequest) -> Result<JsonRpcResponse> {
        self.ensure_connected().await?;

        let id = request.id.as_ref().and_then(|v| v.as_u64());
        let body = serde_json::to_string(request)?;

        let (mut message_url, mut from_endpoint) = self.get_message_url().await?;
        if self.stream_state == SseStreamState::Connected && !from_endpoint {
            for _ in 0..3 {
                {
                    let guard = self.shared.lock().await;
                    if guard.message_url_from_endpoint
                        && let Some(url) = &guard.message_url
                    {
                        message_url = url.clone();
                        from_endpoint = true;
                        break;
                    }
                }
                let _ = timeout(Duration::from_millis(300), self.notify.notified()).await;
            }
        }
        let primary_url = if from_endpoint {
            message_url.clone()
        } else {
            self.sse_url.clone()
        };
        let secondary_url = if message_url == self.sse_url {
            None
        } else if primary_url == message_url {
            Some(self.sse_url.clone())
        } else {
            Some(message_url.clone())
        };
        let has_secondary = secondary_url.is_some();

        let mut rx = None;
        if let Some(id) = id
            && self.stream_state == SseStreamState::Connected
        {
            let (tx, ch) = oneshot::channel();
            {
                let mut guard = self.shared.lock().await;
                guard.pending.insert(id, tx);
            }
            rx = Some((id, ch));
        }

        let mut got_direct = None;
        let mut last_status = None;

        for (i, url) in std::iter::once(primary_url)
            .chain(secondary_url)
            .enumerate()
        {
            if self.require_https {
                require_https_url(&self.server_name, &url, "SSE message endpoint")?;
            }
            let has_accept = self
                .headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case("Accept"));
            let has_content_type = self
                .headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case("Content-Type"));
            let mut req = apply_request_timeout(
                self.client.post(&url).body(body.clone()),
                http_request_timeout_secs(request, self.tool_timeout_secs),
            );
            if !has_content_type {
                req = req.header("Content-Type", MCP_JSON_CONTENT_TYPE);
            }
            for (key, value) in &self.headers {
                req = req.header(key, value);
            }
            if !has_accept {
                req = req.header("Accept", MCP_STREAMABLE_ACCEPT);
            }

            let resp = req.send().await.context("SSE POST to MCP server failed")?;
            let status = resp.status();
            last_status = Some(status);

            if (status == reqwest::StatusCode::NOT_FOUND
                || status == reqwest::StatusCode::METHOD_NOT_ALLOWED)
                && i == 0
            {
                continue;
            }

            if !status.is_success() {
                break;
            }

            if request.id.is_none() {
                got_direct = Some(JsonRpcResponse {
                    jsonrpc: crate::mcp_protocol::JSONRPC_VERSION.to_string(),
                    id: None,
                    result: None,
                    error: None,
                });
                break;
            }

            let is_sse = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.to_ascii_lowercase().contains("text/event-stream"));

            if is_sse {
                if i == 0 && has_secondary {
                    match timeout(
                        Duration::from_secs(3),
                        read_first_jsonrpc_from_sse_response(resp),
                    )
                    .await
                    {
                        Ok(res) => {
                            if let Some(resp) = res? {
                                got_direct = Some(resp);
                            }
                            break;
                        }
                        Err(_) => continue,
                    }
                }
                if let Some(resp) = read_first_jsonrpc_from_sse_response(resp).await? {
                    got_direct = Some(resp);
                }
                break;
            }

            let text = if i == 0 && has_secondary {
                match timeout(Duration::from_secs(3), resp.text()).await {
                    Ok(Ok(t)) => t,
                    Ok(Err(_)) => String::new(),
                    Err(_) => continue,
                }
            } else {
                resp.text().await.unwrap_or_default()
            };
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                let json_str = if trimmed.contains("\ndata:") || trimmed.starts_with("data:") {
                    extract_json_from_sse_text(trimmed)
                } else {
                    Cow::Borrowed(trimmed)
                };
                if let Ok(mcp_resp) = serde_json::from_str::<JsonRpcResponse>(json_str.as_ref()) {
                    got_direct = Some(mcp_resp);
                }
            }
            break;
        }

        if let Some((id, _)) = rx.as_ref() {
            if got_direct.is_some() {
                let mut guard = self.shared.lock().await;
                guard.pending.remove(id);
            } else if let Some(status) = last_status
                && !status.is_success()
            {
                let mut guard = self.shared.lock().await;
                guard.pending.remove(id);
            }
        }

        if let Some(resp) = got_direct {
            return Ok(resp);
        }

        if let Some(status) = last_status {
            if !status.is_success() {
                if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::GONE {
                    return Err(McpTransportError::StaleSession {
                        status: status.as_u16(),
                    }
                    .into());
                }
                bail!("MCP server returned HTTP {}", status);
            }
        } else {
            bail!("MCP request not sent");
        }

        let Some((_id, rx)) = rx else {
            bail!("MCP server returned no response");
        };

        // A dropped receiver means the SSE reader task tore down the stream
        // before our response arrived — recoverable via reconnect.
        rx.await
            .map_err(|_| McpTransportError::TransportClosed.into())
    }

    async fn reset(&mut self) -> Result<()> {
        // Tear down the reader task and clear the cached endpoint/session state
        // so the next send re-handshakes: a fresh GET stream and a new
        // `endpoint` event from the (possibly restarted) server.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.reader_task.take() {
            task.abort();
        }
        self.stream_state = SseStreamState::Unknown;
        let mut guard = self.shared.lock().await;
        guard.message_url = None;
        guard.message_url_from_endpoint = false;
        guard.pending.clear();
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.reader_task.take() {
            task.abort();
        }
        Ok(())
    }
}

// ── Factory ──────────────────────────────────────────────────────────────

/// Create a transport based on config.
pub fn create_transport(config: &McpServerConfig) -> Result<Box<dyn McpTransportConn>> {
    match config.transport {
        McpTransport::Stdio => Ok(Box::new(StdioTransport::new(config)?)),
        McpTransport::Http => Ok(Box::new(HttpTransport::new(config)?)),
        McpTransport::Sse => Ok(Box::new(SseTransport::new(config)?)),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTlsServer {
        url: String,
        ca_pem: String,
        task: tokio::task::JoinHandle<()>,
    }

    fn test_ca_file() -> tempfile::NamedTempFile {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};

        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["ZeroClaw MCP test CA".into()]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), cert.pem()).unwrap();
        file
    }

    async fn spawn_test_tls_server() -> TestTlsServer {
        spawn_test_tls_server_with_san("127.0.0.1").await
    }

    async fn spawn_test_tls_server_with_san(server_san: &str) -> TestTlsServer {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        use rustls::pki_types::PrivatePkcs8KeyDer;
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_rustls::TlsAcceptor;

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(vec!["ZeroClaw MCP test CA".into()]).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let server_key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(vec![server_san.into()]).unwrap();
        server_params.is_ca = IsCa::NoCa;
        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![server_cert.der().clone()],
                PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            )
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = ::zeroclaw_spawn::spawn!(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let Ok(mut stream) = TlsAcceptor::from(Arc::new(server_config))
                .accept(stream)
                .await
            else {
                return;
            };
            let mut request = vec![0_u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let body = r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        TestTlsServer {
            url: format!("https://{addr}/mcp"),
            ca_pem: ca_cert.pem(),
            task,
        }
    }

    #[test]
    fn test_transport_default_is_stdio() {
        let config = McpServerConfig::default();
        assert_eq!(config.transport, McpTransport::Stdio);
    }

    #[test]
    fn test_http_transport_requires_url() {
        let config = McpServerConfig {
            name: "test".into(),
            transport: McpTransport::Http,
            ..Default::default()
        };
        assert!(HttpTransport::new(&config).is_err());
    }

    #[test]
    fn test_sse_transport_requires_url() {
        let config = McpServerConfig {
            name: "test".into(),
            transport: McpTransport::Sse,
            ..Default::default()
        };
        assert!(SseTransport::new(&config).is_err());
    }

    #[test]
    fn remote_transports_without_custom_ca_build_unchanged() {
        let http = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("https://localhost/mcp".into()),
            ..Default::default()
        };
        let sse = McpServerConfig {
            name: "test-sse".into(),
            transport: McpTransport::Sse,
            url: Some("https://localhost/sse".into()),
            ..Default::default()
        };
        assert!(HttpTransport::new(&http).is_ok());
        assert!(SseTransport::new(&sse).is_ok());
    }

    #[test]
    fn remote_transports_with_custom_ca_reject_plaintext_configured_url() {
        let ca_file = test_ca_file();
        for transport in [McpTransport::Http, McpTransport::Sse] {
            let config = McpServerConfig {
                name: "internal".into(),
                transport,
                url: Some("http://internal.example/mcp".into()),
                tls_ca_cert_path: Some(ca_file.path().to_string_lossy().into_owned()),
                ..Default::default()
            };
            let error = create_transport(&config)
                .err()
                .expect("custom CA must reject a plaintext configured URL");
            let message = error.to_string();
            assert!(message.contains("internal"));
            assert!(message.contains("requires an HTTPS configured remote URL"));
            assert!(message.contains("refusing plaintext transport"));
        }
    }

    #[tokio::test]
    async fn custom_ca_rejects_plaintext_endpoint_advertised_by_https_sse_stream() {
        let ca_file = test_ca_file();
        let config = McpServerConfig {
            name: "internal".into(),
            transport: McpTransport::Sse,
            url: Some("https://internal.example/sse".into()),
            tls_ca_cert_path: Some(ca_file.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let mut transport = SseTransport::new(&config).expect("HTTPS SSE transport should build");

        handle_sse_event(
            &transport.server_name,
            &transport.sse_url,
            &transport.shared,
            &transport.notify,
            Some("endpoint"),
            None,
            "http://internal.example/messages".to_string(),
        )
        .await;
        transport.stream_state = SseStreamState::Unsupported;

        let request = JsonRpcRequest::new(1, "initialize", serde_json::json!({}));
        let error = transport
            .send_and_recv(&request)
            .await
            .expect_err("custom CA must reject a plaintext advertised endpoint");
        let message = error.to_string();
        assert!(message.contains("internal"));
        assert!(message.contains("requires an HTTPS SSE message endpoint"));
        assert!(message.contains("refusing plaintext transport"));
    }

    #[test]
    fn remote_transport_rejects_relative_custom_ca_path() {
        let config = McpServerConfig {
            name: "internal".into(),
            transport: McpTransport::Http,
            url: Some("https://localhost/mcp".into()),
            tls_ca_cert_path: Some("internal-ca.pem".into()),
            ..Default::default()
        };
        let error = HttpTransport::new(&config)
            .err()
            .expect("relative path must fail");
        let message = error.to_string();
        assert!(message.contains("internal"));
        assert!(message.contains("must be absolute"));
    }

    #[test]
    fn both_remote_transports_fail_closed_for_missing_custom_ca() {
        for transport in [McpTransport::Http, McpTransport::Sse] {
            let config = McpServerConfig {
                name: "internal".into(),
                transport,
                url: Some("https://localhost/mcp".into()),
                tls_ca_cert_path: Some("/nonexistent/zeroclaw-internal-ca.pem".into()),
                ..Default::default()
            };
            let error = create_transport(&config)
                .err()
                .expect("missing CA must fail");
            let message = error.to_string();
            assert!(message.contains("internal"));
            assert!(message.contains("/nonexistent/zeroclaw-internal-ca.pem"));
            assert!(!message.contains("BEGIN CERTIFICATE"));
        }
    }

    #[test]
    fn remote_transport_fails_closed_for_invalid_custom_ca() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"not a certificate").unwrap();
        let config = McpServerConfig {
            name: "internal".into(),
            transport: McpTransport::Http,
            url: Some("https://localhost/mcp".into()),
            tls_ca_cert_path: Some(file.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let error = HttpTransport::new(&config)
            .err()
            .expect("invalid CA must fail");
        let message = error.to_string();
        assert!(message.contains("internal"));
        assert!(message.contains(&file.path().to_string_lossy().to_string()));
        assert!(!message.contains("not a certificate"));
    }

    #[tokio::test]
    async fn custom_ca_authenticates_a_private_ca_without_disabling_tls_verification() {
        let server = spawn_test_tls_server().await;
        let ca_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(ca_file.path(), &server.ca_pem).unwrap();
        let config = McpServerConfig {
            name: "internal".into(),
            transport: McpTransport::Http,
            url: Some(server.url),
            tls_ca_cert_path: Some(ca_file.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let mut transport = HttpTransport::new(&config).unwrap();
        let request = JsonRpcRequest::new(1, "initialize", serde_json::json!({}));
        let response = transport.send_and_recv(&request).await.unwrap();
        assert_eq!(response.id, Some(serde_json::Value::from(1)));
        assert_eq!(response.result, Some(serde_json::json!({"ok": true})));
        server.task.await.unwrap();

        let server = spawn_test_tls_server().await;
        let wrong_ca_file = tempfile::NamedTempFile::new().unwrap();
        let wrong_ca_key = rcgen::KeyPair::generate().unwrap();
        let mut wrong_ca_params =
            rcgen::CertificateParams::new(vec!["unrelated test CA".into()]).unwrap();
        wrong_ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let wrong_ca = wrong_ca_params.self_signed(&wrong_ca_key).unwrap();
        std::fs::write(wrong_ca_file.path(), wrong_ca.pem()).unwrap();
        let config = McpServerConfig {
            name: "internal".into(),
            transport: McpTransport::Http,
            url: Some(server.url),
            tls_ca_cert_path: Some(wrong_ca_file.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let mut transport = HttpTransport::new(&config).unwrap();
        let error = transport
            .send_and_recv(&request)
            .await
            .expect_err("an unrelated CA must not authenticate the server");
        assert!(
            error
                .to_string()
                .contains("HTTP request to MCP server failed")
        );
        server.task.await.unwrap();

        let server = spawn_test_tls_server_with_san("wrong.example").await;
        let ca_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(ca_file.path(), &server.ca_pem).unwrap();
        let config = McpServerConfig {
            name: "internal".into(),
            transport: McpTransport::Http,
            url: Some(server.url),
            tls_ca_cert_path: Some(ca_file.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let mut transport = HttpTransport::new(&config).unwrap();
        let error = transport
            .send_and_recv(&request)
            .await
            .expect_err("a trusted CA must not bypass hostname verification");
        assert!(
            error
                .to_string()
                .contains("HTTP request to MCP server failed")
        );
        server.task.await.unwrap();
    }

    #[test]
    fn http_request_timeout_defaults_non_tool_requests_to_legacy_value() {
        let request = JsonRpcRequest::new(1, "initialize", serde_json::json!({}));
        assert_eq!(
            http_request_timeout_secs(&request, None),
            Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS)
        );
    }

    #[test]
    fn http_request_timeout_does_not_shorten_non_tool_requests_from_tool_config() {
        let request = JsonRpcRequest::new(1, "tools/list", serde_json::json!({}));
        assert_eq!(
            http_request_timeout_secs(&request, Some(5)),
            Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS)
        );
    }

    #[test]
    fn http_request_timeout_honors_configured_tool_call_timeout_above_legacy_value() {
        let request = JsonRpcRequest::new(1, TOOLS_CALL_METHOD, serde_json::json!({}));
        assert_eq!(
            http_request_timeout_secs(&request, Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS + 60)),
            Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS + 60)
        );
    }

    #[test]
    fn http_request_timeout_leaves_default_tool_call_budget_to_client_wrapper() {
        let request = JsonRpcRequest::new(1, TOOLS_CALL_METHOD, serde_json::json!({}));
        assert_eq!(http_request_timeout_secs(&request, None), None);
    }

    #[test]
    fn http_sse_read_timeout_defaults_non_tool_requests_to_recv_timeout() {
        let request = JsonRpcRequest::new(1, "initialize", serde_json::json!({}));
        assert_eq!(
            http_sse_read_timeout_secs(&request, None),
            Some(RECV_TIMEOUT_SECS)
        );
    }

    #[test]
    fn http_sse_read_timeout_honors_configured_tool_call_timeout() {
        let request = JsonRpcRequest::new(1, TOOLS_CALL_METHOD, serde_json::json!({}));
        assert_eq!(
            http_sse_read_timeout_secs(&request, Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS + 60)),
            Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS + 60)
        );
    }

    #[test]
    fn http_sse_read_timeout_leaves_default_tool_call_budget_to_client_wrapper() {
        let request = JsonRpcRequest::new(1, TOOLS_CALL_METHOD, serde_json::json!({}));
        assert_eq!(http_sse_read_timeout_secs(&request, None), None);
    }

    #[test]
    fn http_transport_stores_configured_tool_timeout() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("http://localhost/mcp".into()),
            tool_timeout_secs: Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS + 60),
            ..Default::default()
        };
        let transport = HttpTransport::new(&config).expect("build transport");
        assert_eq!(
            transport.tool_timeout_secs,
            Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS + 60)
        );
    }

    #[test]
    fn sse_transport_stores_configured_tool_timeout() {
        let config = McpServerConfig {
            name: "test-sse".into(),
            transport: McpTransport::Sse,
            url: Some("http://localhost/sse".into()),
            tool_timeout_secs: Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS + 60),
            ..Default::default()
        };
        let transport = SseTransport::new(&config).expect("build transport");
        assert_eq!(
            transport.tool_timeout_secs,
            Some(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS + 60)
        );
    }

    #[test]
    fn test_extract_json_from_sse_data_no_space() {
        let input = "data:{\"jsonrpc\":\"2.0\",\"result\":{}}\n\n";
        let extracted = extract_json_from_sse_text(input);
        let _: JsonRpcResponse = serde_json::from_str(extracted.as_ref()).unwrap();
    }

    #[test]
    fn test_extract_json_from_sse_with_event_and_id() {
        let input = "id: 1\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"result\":{}}\n\n";
        let extracted = extract_json_from_sse_text(input);
        let _: JsonRpcResponse = serde_json::from_str(extracted.as_ref()).unwrap();
    }

    #[test]
    fn test_extract_json_from_sse_multiline_data() {
        let input = "event: message\ndata: {\ndata:   \"jsonrpc\": \"2.0\",\ndata:   \"result\": {}\ndata: }\n\n";
        let extracted = extract_json_from_sse_text(input);
        let _: JsonRpcResponse = serde_json::from_str(extracted.as_ref()).unwrap();
    }

    #[test]
    fn test_extract_json_from_sse_skips_bom_and_leading_whitespace() {
        let input = "\u{feff}\n\n  data: {\"jsonrpc\":\"2.0\",\"result\":{}}\n\n";
        let extracted = extract_json_from_sse_text(input);
        let _: JsonRpcResponse = serde_json::from_str(extracted.as_ref()).unwrap();
    }

    #[test]
    fn test_extract_json_from_sse_uses_last_event_with_data() {
        let input =
            ": keep-alive\n\nid: 1\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"result\":{}}\n\n";
        let extracted = extract_json_from_sse_text(input);
        let _: JsonRpcResponse = serde_json::from_str(extracted.as_ref()).unwrap();
    }

    #[test]
    fn test_parse_jsonrpc_response_text_handles_plain_json() {
        let parsed = parse_jsonrpc_response_text("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}")
            .expect("plain JSON response should parse");
        assert_eq!(parsed.id, Some(serde_json::json!(1)));
        assert!(parsed.error.is_none());
    }

    #[test]
    fn test_parse_jsonrpc_response_text_handles_sse_framed_json() {
        let sse =
            "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"ok\":true}}\n\n";
        let parsed =
            parse_jsonrpc_response_text(sse).expect("SSE-framed JSON response should parse");
        assert_eq!(parsed.id, Some(serde_json::json!(2)));
        assert_eq!(
            parsed
                .result
                .as_ref()
                .and_then(|v| v.get("ok"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn test_parse_jsonrpc_response_text_rejects_empty_payload() {
        assert!(parse_jsonrpc_response_text(" \n\t ").is_err());
    }

    #[test]
    fn http_transport_updates_session_id_from_response_headers() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("http://localhost/mcp".into()),
            ..Default::default()
        };
        let mut transport = HttpTransport::new(&config).expect("build transport");

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::HeaderName::from_static("mcp-session-id"),
            reqwest::header::HeaderValue::from_static("session-abc"),
        );
        transport.update_session_id_from_headers(&headers);
        assert_eq!(transport.session_id.as_deref(), Some("session-abc"));
    }

    #[test]
    fn http_transport_injects_session_id_header_when_available() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("http://localhost/mcp".into()),
            ..Default::default()
        };
        let mut transport = HttpTransport::new(&config).expect("build transport");
        transport.session_id = Some("session-xyz".to_string());

        let req = transport
            .apply_session_header(reqwest::Client::new().post("http://localhost/mcp"))
            .build()
            .expect("build request");
        assert_eq!(
            req.headers()
                .get(MCP_SESSION_ID_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some("session-xyz")
        );
    }

    // ── derive_message_url tests ──────────────────────────────────────────────

    #[test]
    fn derive_message_url_replaces_sse_segment_with_messages() {
        let url = derive_message_url("http://localhost:3000/mcp/sse", "messages");
        assert_eq!(url, Some("http://localhost:3000/mcp/messages".to_string()));
    }

    #[test]
    fn derive_message_url_appends_when_no_sse_segment() {
        let url = derive_message_url("http://localhost:3000/mcp", "messages");
        assert_eq!(url, Some("http://localhost:3000/mcp/messages".to_string()));
    }

    #[test]
    fn derive_message_url_returns_none_for_invalid_url() {
        let url = derive_message_url("not-a-url", "messages");
        assert!(url.is_none());
    }

    #[test]
    fn derive_message_url_message_path_variant() {
        let url = derive_message_url("http://localhost:3000/mcp/sse", "message");
        assert_eq!(url, Some("http://localhost:3000/mcp/message".to_string()));
    }

    // ── parse_endpoint_from_data tests ───────────────────────────────────────

    #[test]
    fn parse_endpoint_absolute_http_url_returned_as_is() {
        let result = parse_endpoint_from_data("http://base/sse", "http://other/messages");
        assert_eq!(result, Some("http://other/messages".to_string()));
    }

    #[test]
    fn parse_endpoint_absolute_https_url_returned_as_is() {
        let result = parse_endpoint_from_data("https://base/sse", "https://other/messages");
        assert_eq!(result, Some("https://other/messages".to_string()));
    }

    #[test]
    fn parse_endpoint_relative_path_resolved_against_base() {
        let result = parse_endpoint_from_data("http://localhost:3000/sse", "/messages");
        assert_eq!(result, Some("http://localhost:3000/messages".to_string()));
    }

    #[test]
    fn parse_endpoint_json_object_with_endpoint_key() {
        let json_data = r#"{"endpoint":"/messages"}"#;
        let result = parse_endpoint_from_data("http://localhost:3000/sse", json_data);
        assert_eq!(result, Some("http://localhost:3000/messages".to_string()));
    }

    // ── looks_like_sse_text tests ─────────────────────────────────────────────

    #[test]
    fn looks_like_sse_text_detects_data_prefix() {
        assert!(looks_like_sse_text("data:{\"jsonrpc\":\"2.0\"}"));
    }

    #[test]
    fn looks_like_sse_text_detects_event_prefix() {
        assert!(looks_like_sse_text("event: message\ndata: {}"));
    }

    #[test]
    fn looks_like_sse_text_detects_embedded_data_line() {
        assert!(looks_like_sse_text("id: 1\ndata:{\"x\":1}"));
    }

    #[test]
    fn looks_like_sse_text_plain_json_is_not_sse() {
        assert!(!looks_like_sse_text(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}"
        ));
    }

    // ── extract_json_from_sse_text edge cases ─────────────────────────────────

    #[test]
    fn extract_json_skips_comment_lines() {
        let input = ": keep-alive\ndata: {\"jsonrpc\":\"2.0\",\"result\":{}}\n\n";
        let extracted = extract_json_from_sse_text(input);
        let v: serde_json::Value = serde_json::from_str(extracted.as_ref()).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
    }

    #[test]
    fn extract_json_empty_input_returns_empty_trimmed() {
        let result = extract_json_from_sse_text("   ");
        assert!(result.as_ref().trim().is_empty());
    }

    #[test]
    fn extract_json_plain_json_returned_unchanged() {
        let input = "{\"jsonrpc\":\"2.0\",\"result\":{}}";
        let extracted = extract_json_from_sse_text(input);
        // No SSE framing, extracted as-is (trimmed)
        assert_eq!(extracted.as_ref(), input);
    }

    // ── parse_jsonrpc_response_text edge cases ────────────────────────────────

    #[test]
    fn parse_jsonrpc_response_rejects_whitespace_only() {
        assert!(parse_jsonrpc_response_text("   \n\t  ").is_err());
    }

    #[test]
    fn parse_jsonrpc_response_with_error_result() {
        let json = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"not found"}}"#;
        let resp = parse_jsonrpc_response_text(json).unwrap();
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, -32601);
    }

    // ── create_transport factory ──────────────────────────────────────────────

    #[test]
    fn create_transport_stdio_fails_without_valid_command() {
        // Spawning a non-existent binary should fail
        let config = McpServerConfig {
            name: "test-stdio".into(),
            transport: McpTransport::Stdio,
            command: "/usr/bin/zeroclaw_nonexistent_binary_abc123".into(),
            ..Default::default()
        };
        let result = create_transport(&config);
        assert!(result.is_err());
    }

    #[test]
    fn create_transport_http_without_url_fails() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            ..Default::default()
        };
        assert!(create_transport(&config).is_err());
    }

    #[test]
    fn create_transport_sse_without_url_fails() {
        let config = McpServerConfig {
            name: "test-sse".into(),
            transport: McpTransport::Sse,
            ..Default::default()
        };
        assert!(create_transport(&config).is_err());
    }

    #[test]
    fn create_transport_http_with_url_succeeds() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("http://localhost:9999/mcp".into()),
            ..Default::default()
        };
        // Build should succeed even if server isn't running
        assert!(create_transport(&config).is_ok());
    }

    #[test]
    fn create_transport_sse_with_url_succeeds() {
        let config = McpServerConfig {
            name: "test-sse".into(),
            transport: McpTransport::Sse,
            url: Some("http://localhost:9999/sse".into()),
            ..Default::default()
        };
        assert!(create_transport(&config).is_ok());
    }

    // ── HTTP session id whitespace handling ───────────────────────────────────

    #[test]
    fn http_transport_ignores_empty_session_id_header() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("http://localhost/mcp".into()),
            ..Default::default()
        };
        let mut transport = HttpTransport::new(&config).expect("build transport");
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::HeaderName::from_static("mcp-session-id"),
            reqwest::header::HeaderValue::from_static("   "),
        );
        transport.update_session_id_from_headers(&headers);
        // Whitespace-only session id should not be stored
        assert!(transport.session_id.is_none());
    }

    #[test]
    fn http_transport_no_session_header_leaves_none() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("http://localhost/mcp".into()),
            ..Default::default()
        };
        let transport = HttpTransport::new(&config).expect("build transport");
        assert!(transport.session_id.is_none());
    }

    #[test]
    fn http_transport_apply_session_header_noop_when_no_session() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("http://localhost/mcp".into()),
            ..Default::default()
        };
        let transport = HttpTransport::new(&config).expect("build transport");
        let req = transport
            .apply_session_header(reqwest::Client::new().post("http://localhost/mcp"))
            .build()
            .expect("build request");
        assert!(req.headers().get(MCP_SESSION_ID_HEADER).is_none());
    }

    #[tokio::test]
    async fn http_transport_reset_clears_session_id() {
        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some("http://localhost/mcp".into()),
            ..Default::default()
        };
        let mut transport = HttpTransport::new(&config).expect("build transport");
        transport.session_id = Some("stale-session".into());
        transport.reset().await.expect("reset");
        assert!(transport.session_id.is_none());
    }

    #[tokio::test]
    async fn http_transport_maps_404_to_stale_session() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some(server.uri()),
            ..Default::default()
        };
        let mut transport = HttpTransport::new(&config).expect("build transport");
        // A 404 only signals a stale session when the request carried a session id.
        transport.session_id = Some("sess-1".into());
        let req = JsonRpcRequest::new(1, "tools/call", serde_json::json!({}));
        let err = transport
            .send_and_recv(&req)
            .await
            .expect_err("404 should error");
        match err.downcast_ref::<McpTransportError>() {
            Some(McpTransportError::StaleSession { status }) => assert_eq!(*status, 404),
            other => panic!("expected StaleSession, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn http_transport_404_without_session_is_plain_error() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let config = McpServerConfig {
            name: "test-http".into(),
            transport: McpTransport::Http,
            url: Some(server.uri()),
            ..Default::default()
        };
        // No session id was ever issued (stateless server, or a misconfigured url):
        // a 404 here is a missing endpoint, not a stale session — it must NOT map to
        // StaleSession (which would make `call_tool` burn a wasted reconnect).
        let mut transport = HttpTransport::new(&config).expect("build transport");
        assert!(transport.session_id.is_none());
        let req = JsonRpcRequest::new(1, "tools/call", serde_json::json!({}));
        let err = transport
            .send_and_recv(&req)
            .await
            .expect_err("404 should error");
        assert!(
            !matches!(
                err.downcast_ref::<McpTransportError>(),
                Some(McpTransportError::StaleSession { .. })
            ),
            "sessionless 404 must not be classified as StaleSession, got: {err:?}"
        );
        assert!(
            err.to_string().contains("MCP server returned HTTP 404"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn sse_transport_reset_clears_session_and_endpoint_state() {
        let config = McpServerConfig {
            name: "test-sse".into(),
            transport: McpTransport::Sse,
            url: Some("http://localhost:1/sse".into()),
            ..Default::default()
        };
        let mut transport = SseTransport::new(&config).expect("build transport");
        transport.stream_state = SseStreamState::Connected;
        {
            let mut guard = transport.shared.lock().await;
            guard.message_url = Some("http://localhost:1/messages".into());
            guard.message_url_from_endpoint = true;
            let (tx, _rx) = oneshot::channel();
            guard.pending.insert(7, tx);
        }

        transport.reset().await.expect("reset");

        assert_eq!(transport.stream_state, SseStreamState::Unknown);
        let guard = transport.shared.lock().await;
        assert!(guard.message_url.is_none());
        assert!(!guard.message_url_from_endpoint);
        assert!(guard.pending.is_empty());
    }
}
