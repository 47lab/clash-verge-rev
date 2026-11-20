use super::resolve;
use crate::{
    config::{Config, DEFAULT_PAC, IVerge, MixedPort},
    core::lightweight,
    process::AsyncHandler,
    utils::{dirs, window_manager::WindowManager},
};
use anyhow::{Context as _, Result, bail};
use clash_verge_logging::{Type, logging};
use parking_lot::Mutex;
use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::oneshot;
use warp::Filter as _;

const INSTANCE_TOKEN_HEADER: &str = "x-instance-token";
const PAC_CONFIG_MAX_BODY_BYTES: u64 = 256 * 1024;
pub(crate) const INSTANCE_RECORD_FILE: &str = "singleton-instance.json";
pub(crate) const INSTANCE_LOCK_FILE: &str = "singleton-instance.lock";

#[derive(Deserialize, Debug)]
struct QueryParam {
    param: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PacConfigPatchRequest {
    enable_system_proxy: Option<bool>,
    proxy_auto_config: Option<bool>,
    pac_file_content: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PacConfigApplyResult {
    enable_system_proxy: bool,
    proxy_auto_config: bool,
    mixed_port: u16,
    pac_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InstanceRecord {
    port: u16,
    token: String,
}

static SHUTDOWN_SENDER: OnceLock<Mutex<Option<oneshot::Sender<()>>>> = OnceLock::new();
static EMBEDDED_PORT: OnceLock<u16> = OnceLock::new();
static INSTANCE_LOCK: OnceLock<std::fs::File> = OnceLock::new();
const PAC_INITIAL_AVAILABLE: bool = false;
static PAC_AVAILABLE: AtomicBool = AtomicBool::new(PAC_INITIAL_AVAILABLE);
static COMMANDS_READY: AtomicBool = AtomicBool::new(false);
#[cfg(feature = "verge-dev")]
static DEV_QUIT_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingletonDisposition {
    Primary,
    Secondary,
}

pub async fn check_singleton() -> Result<SingletonDisposition> {
    let record_path = instance_record_path().context("failed to resolve singleton instance record path")?;
    let lock = open_instance_lock(&record_path.with_file_name(INSTANCE_LOCK_FILE))
        .context("failed to initialize singleton lock")?;
    if !try_lock_instance(&lock).context("failed to acquire singleton lock")? {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        use crate::utils::retry::{RetryError, RetryPolicy, retry};
        let secondary = retry(
            RetryPolicy::fixed(
                std::num::NonZeroUsize::MIN.saturating_add(400),
                Duration::from_millis(50),
            ),
            |_| async {
                if std::time::Instant::now() >= deadline {
                    return Err(RetryError::Stop(anyhow::anyhow!("another app instance is starting")));
                }
                // A restarting primary releases the lock after spawning us, with its server down.
                if try_lock_instance(&lock)
                    .context("failed to acquire singleton lock")
                    .map_err(RetryError::Stop)?
                {
                    return Ok(false);
                }
                if let Ok(record) = read_instance_record(&record_path)
                    && notify_existing_instance(&record).await
                {
                    return Ok(true);
                }
                Err(RetryError::Retry(anyhow::anyhow!("another app instance is starting")))
            },
        )
        .await?;
        if secondary {
            return Ok(SingletonDisposition::Secondary);
        }
    }

    let preferred = read_instance_record(&record_path).ok().map(|record| record.port);
    let listener = bind_primary_listener(preferred)
        .await
        .context("failed to reserve embedded server port")?;
    let port = listener
        .local_addr()
        .context("failed to read embedded server address")?
        .port();
    let record = InstanceRecord {
        port,
        token: random_token().context("failed to generate singleton instance token")?,
    };
    write_instance_record(&record_path, &record).context("failed to persist singleton instance record")?;
    INSTANCE_LOCK
        .set(lock)
        .map_err(|_| anyhow::anyhow!("singleton lock is already initialized"))?;
    let _ = EMBEDDED_PORT.set(port);
    start_embedded_server(listener, record.token);
    Ok(SingletonDisposition::Primary)
}

async fn bind_primary_listener(preferred: Option<u16>) -> Result<tokio::net::TcpListener> {
    let preferred_port = preferred.unwrap_or(0);
    match tokio::net::TcpListener::bind(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::LOCALHOST,
        preferred_port,
    ))
    .await
    {
        Ok(listener) => Ok(listener),
        Err(_) if preferred.is_some() => {
            Ok(tokio::net::TcpListener::bind(std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 0)).await?)
        }
        Err(error) => Err(error.into()),
    }
}

async fn notify_existing_instance(record: &InstanceRecord) -> bool {
    let client = match ClientBuilder::new().timeout(Duration::from_millis(500)).build() {
        Ok(client) => client,
        Err(_) => return false,
    };
    #[cfg(not(target_os = "macos"))]
    let request = if let Some(arg) = std::env::args().nth(1).as_deref() {
        if arg.starts_with("clash:") {
            client
                .get(format!("http://127.0.0.1:{}/commands/scheme", record.port))
                .query(&[("param", arg)])
        } else {
            client.get(format!("http://127.0.0.1:{}/commands/visible", record.port))
        }
    } else {
        client.get(format!("http://127.0.0.1:{}/commands/visible", record.port))
    };
    #[cfg(target_os = "macos")]
    let request = client.get(format!("http://127.0.0.1:{}/commands/visible", record.port));

    request
        .header(INSTANCE_TOKEN_HEADER, &record.token)
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

fn instance_auth(token: String) -> impl warp::Filter<Extract = (), Error = warp::Rejection> + Clone {
    warp::header::optional::<String>(INSTANCE_TOKEN_HEADER)
        .and_then(move |provided: Option<String>| {
            let token = token.clone();
            async move {
                if provided.as_deref() == Some(token.as_str()) {
                    Ok::<(), warp::Rejection>(())
                } else {
                    Err(warp::reject())
                }
            }
        })
        .untuple_one()
}

#[cfg(feature = "verge-dev")]
fn dev_quit_route<F>(
    token: String,
    commands_ready: &'static AtomicBool,
    quit_requested: &'static AtomicBool,
    trigger: F,
) -> impl warp::Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
    F: Fn() + Clone + Send + Sync + 'static,
{
    instance_auth(token)
        .and(warp::path!("commands" / "dev" / "quit"))
        .and(warp::post())
        .and_then(move || {
            let trigger = trigger.clone();
            async move {
                if !commands_ready.load(Ordering::Acquire) {
                    return Ok::<_, warp::Rejection>(warp::reply::with_status(
                        String::from("starting"),
                        warp::http::StatusCode::SERVICE_UNAVAILABLE,
                    ));
                }
                if quit_requested
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    trigger();
                }
                Ok(warp::reply::with_status(
                    String::from("accepted"),
                    warp::http::StatusCode::ACCEPTED,
                ))
            }
        })
}

#[cfg(feature = "verge-dev")]
fn release_dev_quit_latch(quit_requested: &AtomicBool, outcome: clash_verge_signal::ShutdownOutcome) {
    if matches!(outcome, clash_verge_signal::ShutdownOutcome::Canceled) {
        quit_requested.store(false, Ordering::Release);
    }
}

fn json_error_reply(status: warp::http::StatusCode, message: &str) -> warp::reply::WithStatus<warp::reply::Json> {
    warp::reply::with_status(
        warp::reply::json(&serde_json::json!({
            "status": "error",
            "message": message,
        })),
        status,
    )
}

/// 校验并构造仅包含 PAC / 系统代理白名单字段的 IVerge 补丁，防止误改核心或 TUN 配置。
fn build_pac_config_patch(body: &[u8]) -> std::result::Result<IVerge, String> {
    let req: PacConfigPatchRequest =
        serde_json::from_slice(body).map_err(|error| format!("PAC 配置请求 JSON 无效: {error}"))?;
    if req.enable_system_proxy.is_none() && req.proxy_auto_config.is_none() && req.pac_file_content.is_none() {
        return Err("至少需要提供一个 PAC 配置字段".to_string());
    }
    if let Some(content) = req.pac_file_content.as_deref()
        && (content.trim().is_empty() || !content.contains("FindProxyForURL"))
    {
        return Err("PAC 脚本必须非空且包含 FindProxyForURL 函数".to_string());
    }
    Ok(IVerge {
        enable_system_proxy: req.enable_system_proxy,
        proxy_auto_config: req.proxy_auto_config,
        pac_file_content: req.pac_file_content.map(Into::into),
        ..IVerge::default()
    })
}

/// 受 singleton 令牌保护的 PAC 与系统代理配置热更新路由。
fn pac_config_route<F, Fut>(
    token: String,
    commands_ready: &'static AtomicBool,
    apply_patch: F,
) -> impl warp::Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone
where
    F: Fn(Arc<IVerge>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<PacConfigApplyResult, String>> + Send + 'static,
{
    warp::path!("commands" / "pac-config")
        .and(warp::post())
        .and(warp::header::optional::<String>(INSTANCE_TOKEN_HEADER))
        .and(warp::body::content_length_limit(PAC_CONFIG_MAX_BODY_BYTES))
        .and(warp::body::bytes())
        .and_then(move |provided: Option<String>, body: bytes::Bytes| {
            let token = token.clone();
            let apply_patch = apply_patch.clone();
            async move {
                if provided.as_deref() != Some(token.as_str()) {
                    return Ok::<_, warp::Rejection>(json_error_reply(
                        warp::http::StatusCode::UNAUTHORIZED,
                        "singleton 实例令牌无效",
                    ));
                }
                if !commands_ready.load(Ordering::Acquire) {
                    return Ok(json_error_reply(
                        warp::http::StatusCode::SERVICE_UNAVAILABLE,
                        "嵌入式服务正在启动",
                    ));
                }
                let patch = match build_pac_config_patch(&body) {
                    Ok(patch) => Arc::new(patch),
                    Err(message) => {
                        return Ok(json_error_reply(warp::http::StatusCode::BAD_REQUEST, &message));
                    }
                };
                match Box::pin(apply_patch(patch)).await {
                    Ok(result) => Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({
                            "status": "ok",
                            "enableSystemProxy": result.enable_system_proxy,
                            "proxyAutoConfig": result.proxy_auto_config,
                            "mixedPort": result.mixed_port,
                            "pacAvailable": result.pac_available,
                        })),
                        warp::http::StatusCode::OK,
                    )),
                    Err(message) => Ok(json_error_reply(
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                        &message,
                    )),
                }
            }
        })
}

fn current_mihomo_ipc_path() -> Option<String> {
    let path = match *crate::core::CoreManager::global().get_running_mode() {
        crate::core::manager::RunningMode::Service => dirs::ipc_path().ok(),
        crate::core::manager::RunningMode::Sidecar => dirs::sidecar_ipc_path().ok(),
        crate::core::manager::RunningMode::NotRunning => None,
    }?;
    dirs::path_to_str(&path).ok().map(str::to_owned)
}

fn start_embedded_server(listener: tokio::net::TcpListener, token: String) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let _ = SHUTDOWN_SENDER.set(Mutex::new(Some(shutdown_tx)));

    let auth = instance_auth(token.clone());
    let identity_token = token.clone();

    let identity = warp::path!("commands" / "identity")
        .and(warp::get())
        .and(warp::header::optional::<String>(INSTANCE_TOKEN_HEADER))
        .and_then(move |provided: Option<String>| {
            let token = identity_token.clone();
            async move {
                if provided.as_deref() != Some(token.as_str()) {
                    return Ok::<_, warp::Rejection>(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({
                            "status": "error",
                            "message": "singleton 实例令牌无效"
                        })),
                        warp::http::StatusCode::UNAUTHORIZED,
                    ));
                }
                if !COMMANDS_READY.load(Ordering::Acquire) {
                    return Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({
                            "status": "error",
                            "message": "嵌入式服务正在启动"
                        })),
                        warp::http::StatusCode::SERVICE_UNAVAILABLE,
                    ));
                }
                let profiles = Config::profiles().await.latest_arc();
                let current_profile_uid = profiles.current.clone();
                let current_profile_file = current_profile_uid
                    .as_ref()
                    .and_then(|uid| profiles.get_item(uid).ok())
                    .and_then(|item| item.file.clone());
                let mihomo_ipc_path = current_mihomo_ipc_path();
                Ok(warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({
                        "appVersion": env!("CARGO_PKG_VERSION"),
                        "bundleIdentifier": crate::utils::dirs::APP_ID,
                        "currentProfileUid": current_profile_uid,
                        "currentProfileFile": current_profile_file,
                        "mihomoIpcPath": mihomo_ipc_path,
                        "capabilities": {
                            "profileReloadNoRestart": true,
                            "profileReloadProtocol": "47lab-strict-v1",
                            "pacConfigHotUpdate": true,
                            "pacConfigProtocol": "47lab-pac-v1"
                        }
                    })),
                    warp::http::StatusCode::OK,
                ))
            }
        });

    let visible = warp::path!("commands" / "visible").and_then(|| async {
        if !COMMANDS_READY.load(Ordering::Acquire) {
            return Ok::<_, warp::Rejection>(warp::reply::with_status(
                "starting".to_string(),
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
        // Keep blocking UI calls off HTTP workers.
        AsyncHandler::spawn_blocking(|| {
            AsyncHandler::block_on(async {
                logging!(
                    info,
                    Type::Window,
                    "Restoring the application window from a secondary instance"
                );
                if crate::APP_HANDLE.get().is_some() && !lightweight::exit_lightweight_mode().await {
                    WindowManager::show_main_window().await;
                }
            });
        });
        Ok(warp::reply::with_status("ok".to_string(), warp::http::StatusCode::OK))
    });

    let pac = warp::path!("commands" / "pac").and_then(|| async move {
        if !PAC_AVAILABLE.load(Ordering::Acquire) {
            return Ok::<_, warp::Rejection>(
                warp::http::Response::builder()
                    .status(warp::http::StatusCode::SERVICE_UNAVAILABLE)
                    .body("PAC endpoint is inactive".to_string())
                    .unwrap_or_default(),
            );
        }
        let verge_config = Config::verge().await;
        let verge_data = verge_config.data_arc();
        let pac_content = verge_data.pac_file_content.as_deref().unwrap_or(DEFAULT_PAC);
        // Port changes disable PAC, so the draft port avoids per-request Core queries.
        let pac_port = MixedPort::desired().await;
        let processed_content = pac_content.replace("%mixed-port%", &format!("{pac_port}"));
        Ok::<_, warp::Rejection>(
            warp::http::Response::builder()
                .header("Content-Type", "application/x-ns-proxy-autoconfig")
                .header("Cache-Control", "no-store")
                .body(processed_content)
                .unwrap_or_default(),
        )
    });

    let scheme = warp::path!("commands" / "scheme")
        .and(warp::query::<QueryParam>())
        .and_then(|query: QueryParam| async move {
            if !COMMANDS_READY.load(Ordering::Acquire) {
                return Ok::<_, warp::Rejection>(warp::reply::with_status(
                    "starting".to_string(),
                    warp::http::StatusCode::SERVICE_UNAVAILABLE,
                ));
            }
            AsyncHandler::spawn(|| async move {
                resolve::resolve_scheme(&query.param).await;
            });
            Ok::<_, warp::Rejection>(warp::reply::with_status("ok".to_string(), warp::http::StatusCode::OK))
        });

    let reload = warp::path!("commands" / "reload").and(warp::get()).and_then(|| async {
        if !COMMANDS_READY.load(Ordering::Acquire) {
            return Ok::<_, warp::Rejection>(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({
                    "status": "error",
                    "message": "嵌入式服务正在启动"
                })),
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }

        match crate::feat::enhance_profiles_without_restart().await {
            Ok(outcome) if outcome.is_valid() => {
                crate::core::notify::handle::Handle::refresh_clash();
                let profiles = Config::profiles().await.latest_arc();
                let current_profile_uid = profiles.current.clone();
                let current_profile_file = current_profile_uid
                    .as_ref()
                    .and_then(|uid| profiles.get_item(uid).ok())
                    .and_then(|item| item.file.clone());
                Ok(warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({
                        "status": "ok",
                        "outcome": outcome,
                        "currentProfileUid": current_profile_uid,
                        "currentProfileFile": current_profile_file
                    })),
                    warp::http::StatusCode::OK,
                ))
            }
            Ok(outcome) => Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({
                    "status": "error",
                    "message": outcome.to_string(),
                    "outcome": outcome
                })),
                warp::http::StatusCode::BAD_REQUEST,
            )),
            Err(error) => Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({
                    "status": "error",
                    "message": error.to_string()
                })),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )),
        }
    });

    let pac_config = pac_config_route(token, &COMMANDS_READY, |patch| async move {
        crate::feat::patch_verge(&patch, false)
            .await
            .map_err(|error| error.to_string())?;
        let verge_data = Config::verge().await.data_arc();
        let mixed_port = MixedPort::desired().await;
        let pac_available = PAC_AVAILABLE.load(Ordering::Acquire);
        Ok(PacConfigApplyResult {
            enable_system_proxy: verge_data.enable_system_proxy.unwrap_or_default(),
            proxy_auto_config: verge_data.proxy_auto_config.unwrap_or_default(),
            mixed_port,
            pac_available,
        })
    });

    let commands = auth
        .clone()
        .and(visible)
        .or(auth.clone().and(scheme))
        .or(auth.clone().and(reload))
        .or(identity)
        .or(pac)
        .or(pac_config);
    #[cfg(feature = "verge-dev")]
    let commands = commands.or(dev_quit_route(token, &COMMANDS_READY, &DEV_QUIT_REQUESTED, || {
        AsyncHandler::spawn(|| async {
            release_dev_quit_latch(&DEV_QUIT_REQUESTED, crate::feat::quit().await);
        });
    }));
    AsyncHandler::spawn(move || async move {
        warp::serve(commands)
            .incoming(listener)
            .graceful(async {
                shutdown_rx.await.ok();
            })
            .run()
            .await;
    });
}

pub fn set_pac_available(available: bool) {
    PAC_AVAILABLE.store(available, Ordering::Release);
}

pub fn set_commands_ready() {
    COMMANDS_READY.store(true, Ordering::Release);
}

pub fn embed_server() {
    if EMBEDDED_PORT.get().is_none() {
        logging!(
            error,
            Type::Window,
            "embedded server was not reserved during singleton check"
        );
    }
}

pub fn embedded_server_port() -> Result<u16> {
    EMBEDDED_PORT
        .get()
        .copied()
        .context("embedded server port is not initialized")
}

pub fn shutdown_embedded_server() {
    logging!(info, Type::Window, "shutting down embedded server");
    if let Some(sender) = SHUTDOWN_SENDER.get()
        && let Some(sender) = sender.lock().take()
    {
        sender.send(()).ok();
    }
}

fn instance_record_path() -> Result<PathBuf> {
    Ok(dirs::preinit_app_data_dir()?.join(INSTANCE_RECORD_FILE))
}

fn open_instance_lock(path: &Path) -> Result<std::fs::File> {
    let parent = path.parent().context("singleton lock has no parent")?;
    std::fs::create_dir_all(parent)?;
    #[cfg(windows)]
    {
        crate::core::owner_identity::open_or_create_current_user_lock(path)
    }

    #[cfg(unix)]
    {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true);

        use std::os::unix::fs::OpenOptionsExt as _;
        use std::os::unix::fs::PermissionsExt as _;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);

        let file = options.open(path)?;
        let metadata = file.metadata()?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("singleton lock is not an ordinary file");
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        Ok(file)
    }
}

fn try_lock_instance(file: &std::fs::File) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        if error
            .raw_os_error()
            .is_some_and(|code| code == libc::EWOULDBLOCK || code == libc::EAGAIN)
        {
            return Ok(false);
        }
        Err(error).context("failed to lock singleton file")
    }

    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle as _;
        use windows_sys::Win32::Foundation::{ERROR_LOCK_VIOLATION, GetLastError};
        use windows_sys::Win32::Storage::FileSystem::LockFile;
        let locked = unsafe { LockFile(file.as_raw_handle(), 0, 0, u32::MAX, u32::MAX) };
        if locked != 0 {
            return Ok(true);
        }
        if unsafe { GetLastError() } == ERROR_LOCK_VIOLATION {
            return Ok(false);
        }
        Err(std::io::Error::last_os_error()).context("failed to lock singleton file")
    }
}

fn random_token() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn read_instance_record(path: &Path) -> Result<InstanceRecord> {
    #[cfg(windows)]
    let mut file = crate::core::owner_identity::open_private_current_user_file(path)?;
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut options = std::fs::OpenOptions::new();
        options.read(true).custom_flags(libc::O_NOFOLLOW);
        options.open(path)?
    };
    let metadata = file.metadata()?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("singleton record is not an ordinary file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.permissions().mode() & 0o777 != 0o600 {
            bail!("singleton record permissions are invalid");
        }
    }
    let mut content = Vec::new();
    file.read_to_end(&mut content)?;
    Ok(serde_json::from_slice(&content)?)
}

fn write_instance_record(path: &Path, record: &InstanceRecord) -> Result<()> {
    let parent = path.parent().context("singleton record has no parent")?;
    std::fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("json.tmp-{}-{}", std::process::id(), &random_token()?[..16]));
    #[cfg(windows)]
    let mut file = crate::core::owner_identity::create_private_current_user_file(&temporary)?;
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        options.mode(0o600);
        options.open(&temporary)?
    };
    file.write_all(&serde_json::to_vec(record)?)?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    drop(file);
    std::fs::rename(&temporary, path).context("failed to replace singleton record")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pac_config_patch_validates_whitelist_and_pac_content() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let valid = br#"{
            "enableSystemProxy": true,
            "proxyAutoConfig": true,
            "pacFileContent": "function FindProxyForURL(url, host) { return \"DIRECT\"; }"
        }"#;
        let patch = build_pac_config_patch(valid).map_err(std::io::Error::other)?;
        assert_eq!(patch.enable_system_proxy, Some(true));
        assert_eq!(patch.proxy_auto_config, Some(true));
        assert!(
            patch
                .pac_file_content
                .as_deref()
                .is_some_and(|s| s.contains("FindProxyForURL"))
        );
        assert!(patch.enable_tun_mode.is_none());

        assert!(build_pac_config_patch(br"{}").is_err());
        assert!(build_pac_config_patch(br#"{"enableTunMode":false}"#).is_err());
        assert!(build_pac_config_patch(br#"{"pacFileContent":"return 'DIRECT';"}"#).is_err());
        assert!(build_pac_config_patch(br#"{"pacFileContent":"   "}"#).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn pac_config_route_enforces_auth_readiness_and_payload()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        static TEST_READY: AtomicBool = AtomicBool::new(false);
        let captured = Arc::new(Mutex::new(None::<Arc<IVerge>>));
        let captured_ref = Arc::clone(&captured);

        let route = pac_config_route("secret-token".to_string(), &TEST_READY, move |patch| {
            let captured_ref = Arc::clone(&captured_ref);
            async move {
                *captured_ref.lock() = Some(patch);
                Ok(PacConfigApplyResult {
                    enable_system_proxy: true,
                    proxy_auto_config: true,
                    mixed_port: 2080,
                    pac_available: true,
                })
            }
        });

        // 缺少或错误令牌应返回 401
        let unauth = warp::test::request()
            .method("POST")
            .path("/commands/pac-config")
            .body(r#"{"proxyAutoConfig":true}"#)
            .reply(&route)
            .await;
        assert_eq!(unauth.status(), warp::http::StatusCode::UNAUTHORIZED);

        let bad_token = warp::test::request()
            .method("POST")
            .path("/commands/pac-config")
            .header(INSTANCE_TOKEN_HEADER, "wrong-token")
            .body(r#"{"proxyAutoConfig":true}"#)
            .reply(&route)
            .await;
        assert_eq!(bad_token.status(), warp::http::StatusCode::UNAUTHORIZED);

        // 服务未就绪应返回 503
        let not_ready = warp::test::request()
            .method("POST")
            .path("/commands/pac-config")
            .header(INSTANCE_TOKEN_HEADER, "secret-token")
            .body(r#"{"proxyAutoConfig":true}"#)
            .reply(&route)
            .await;
        assert_eq!(not_ready.status(), warp::http::StatusCode::SERVICE_UNAVAILABLE);

        // 就绪后非法负载应返回 400
        TEST_READY.store(true, Ordering::Release);
        let bad_payload = warp::test::request()
            .method("POST")
            .path("/commands/pac-config")
            .header(INSTANCE_TOKEN_HEADER, "secret-token")
            .body(r#"{"enableTunMode":false}"#)
            .reply(&route)
            .await;
        assert_eq!(bad_payload.status(), warp::http::StatusCode::BAD_REQUEST);

        // 合法请求应返回 200 并仅传递白名单字段
        let ok_resp = warp::test::request()
            .method("POST")
            .path("/commands/pac-config")
            .header(INSTANCE_TOKEN_HEADER, "secret-token")
            .body(r#"{"enableSystemProxy":true,"proxyAutoConfig":true,"pacFileContent":"function FindProxyForURL(url, host) { return \"DIRECT\"; }"}"#)
            .reply(&route)
            .await;
        assert_eq!(ok_resp.status(), warp::http::StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(ok_resp.body())?;
        assert_eq!(json["status"], "ok");
        assert_eq!(json["enableSystemProxy"], true);
        assert_eq!(json["proxyAutoConfig"], true);
        assert_eq!(json["mixedPort"], 2080);
        assert_eq!(json["pacAvailable"], true);

        let saved = captured.lock().take().ok_or("未捕获到 patch")?;
        assert_eq!(saved.enable_system_proxy, Some(true));
        assert_eq!(saved.proxy_auto_config, Some(true));
        assert!(saved.pac_file_content.is_some());
        assert!(saved.enable_tun_mode.is_none());
        Ok(())
    }
}
