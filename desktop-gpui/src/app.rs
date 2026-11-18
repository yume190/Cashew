use std::{path::PathBuf, time::Duration};

use crate::appearance::{Appearance, ThemePreference};
use crate::traffic::TrafficView;

use bazel_cache_server::desktop::{
    DesktopController, DesktopSettings, DesktopSnapshot, ServicePhase,
};
use gpui_kit::component::{
    ActiveTheme, Disableable,
    button::{Button, ButtonVariants},
    input::{Input, InputEvent, InputState},
    radio::{Radio, RadioGroup},
};
use gpui_kit::prelude::FluentBuilder;
use gpui_kit::*;

const ROOT: usize = 0;
const CAPACITY: usize = 1;
const GRPC_HOST: usize = 2;
const GRPC_PORT: usize = 3;
const METRICS_HOST: usize = 4;
const METRICS_PORT: usize = 5;
const CONFIG_FILE: usize = 6;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Page {
    Overview,
    Service,
    Traffic,
    Logs,
    Settings,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListenerScope {
    Local,
    Lan,
    Custom,
}

impl ListenerScope {
    fn from_host(host: &str) -> Self {
        match host {
            "127.0.0.1" => Self::Local,
            "0.0.0.0" => Self::Lan,
            _ => Self::Custom,
        }
    }
}

enum SaveState {
    Unchanged,
    Pending,
    Saved,
    Failed(String),
}

impl SaveState {
    fn label(&self) -> &str {
        match self {
            Self::Unchanged => "修改後會自動儲存。",
            Self::Pending => "正在儲存…",
            Self::Saved => "設定已儲存。",
            Self::Failed(error) => error,
        }
    }
}

pub struct Workspace {
    controller: Option<DesktopController>,
    snapshot: DesktopSnapshot,
    inputs: [Entity<InputState>; 7],
    listener_scopes: [ListenerScope; 2],
    settings_path: PathBuf,
    saved: DesktopSettings,
    appearance: Appearance,
    page: Page,
    pub(crate) traffic: TrafficView,
    notice: Option<String>,
    load_failed: bool,
    save_state: SaveState,
    autosave: Option<Task<()>>,
    _poll: Task<()>,
    _subscriptions: Vec<Subscription>,
}

impl Workspace {
    pub fn new(controller: DesktopController, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let data = std::env::var_os("CASHEW_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                directories::ProjectDirs::from("dev", "yume", "Cashew")
                    .expect("user profile must be available")
                    .config_dir()
                    .to_path_buf()
            });
        let settings_path = data.join("desktop.json");
        let loaded = match std::fs::read(&settings_path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| error.to_string()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut settings = DesktopSettings::default();
                if std::env::var_os("CASHEW_DATA_DIR").is_some() {
                    settings.root = data.join("cache").to_string_lossy().into_owned();
                }
                Ok(settings)
            }
            Err(error) => Err(error.to_string()),
        };
        let load_failed = loaded.is_err();
        let notice = loaded
            .as_ref()
            .err()
            .map(|error| format!("無法讀取設定：{error}。原始檔案已保留。"));
        let settings = loaded.unwrap_or_default();
        let appearance = Appearance::load(data.join("appearance.json"));
        let (grpc_host, grpc_port) = split_listener(&settings.listen);
        let (metrics_host, metrics_port) = split_listener(&settings.metrics_listen);
        let listener_scopes = [
            ListenerScope::from_host(&grpc_host),
            ListenerScope::from_host(&metrics_host),
        ];
        let values = [
            settings.root.clone(),
            settings.max_size.clone(),
            grpc_host,
            grpc_port,
            metrics_host,
            metrics_port,
            settings.config_file.clone(),
        ];
        let inputs = std::array::from_fn(|index| {
            cx.new(|cx| {
                let mut input = InputState::new(window, cx);
                input.set_value(values[index].clone(), window, cx);
                input
            })
        });
        let subscriptions = inputs
            .iter()
            .map(|input| {
                cx.subscribe_in(input, window, |view, _, event: &InputEvent, _, cx| {
                    if matches!(event, InputEvent::Change) {
                        view.schedule_save(cx);
                    }
                    cx.notify();
                })
            })
            .collect();
        let poll = cx.spawn(async move |this, cx| {
            loop {
                smol::Timer::after(Duration::from_millis(500)).await;
                if this
                    .update(cx, |view, cx| {
                        if let Some(controller) = &view.controller {
                            view.snapshot = controller.snapshot();
                        }
                        cx.notify();
                        cx.refresh_windows();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        Self {
            controller: Some(controller),
            snapshot: DesktopSnapshot::default(),
            inputs,
            listener_scopes,
            settings_path,
            saved: settings,
            appearance,
            traffic: TrafficView::default(),
            page: Page::Overview,
            notice,
            load_failed,
            save_state: SaveState::Unchanged,
            autosave: None,
            _poll: poll,
            _subscriptions: subscriptions,
        }
    }

    pub fn shutdown(&mut self, cx: &App) {
        self.autosave.take();
        if matches!(self.save_state, SaveState::Pending) {
            let _ = self.save(cx);
        }
        self.controller.take();
    }

    pub(crate) fn select_traffic_session(&mut self, id: Option<u64>, cx: &mut Context<Self>) {
        if let Some(controller) = &self.controller {
            controller.select_session(id);
        }
        cx.notify();
    }

    fn active(&self) -> bool {
        matches!(
            self.snapshot.phase,
            ServicePhase::Starting | ServicePhase::Running | ServicePhase::Stopping
        )
    }

    fn draft(&self, cx: &App) -> anyhow::Result<DesktopSettings> {
        let value = |index: usize| self.inputs[index].read(cx).value().to_string();
        let root = value(ROOT);
        anyhow::ensure!(!root.trim().is_empty(), "請選擇儲存位置");
        let max_size = value(CAPACITY);
        let bytes = bazel_cache_server::config::parse_size(max_size.trim())
            .map_err(|error| anyhow::anyhow!("容量上限無效：{error}"))?;
        anyhow::ensure!(bytes > 0, "容量上限必須大於 0");
        Ok(DesktopSettings {
            root,
            max_size,
            listen: compose_listener(&value(GRPC_HOST), &value(GRPC_PORT), "gRPC")?,
            metrics_listen: compose_listener(
                &value(METRICS_HOST),
                &value(METRICS_PORT),
                "Metrics / Health",
            )?,
            config_file: value(CONFIG_FILE),
        })
    }

    fn save(&mut self, cx: &App) -> anyhow::Result<DesktopSettings> {
        let result = (|| {
            anyhow::ensure!(
                !self.load_failed,
                "原始設定檔無法讀取，請先修正 {}",
                self.settings_path.display()
            );
            let settings = self.draft(cx)?;
            crate::persistence::write_json(&self.settings_path, &settings)?;
            self.saved = settings.clone();
            Ok(settings)
        })();
        self.save_state = match &result {
            Ok(_) => SaveState::Saved,
            Err(error) => SaveState::Failed(format!("尚未儲存：{error:#}")),
        };
        result
    }

    fn schedule_save(&mut self, cx: &mut Context<Self>) {
        if self.active() {
            return;
        }
        self.save_state = SaveState::Pending;
        self.autosave = Some(cx.spawn(async move |this, cx| {
            smol::Timer::after(Duration::from_millis(400)).await;
            let _ = this.update(cx, |view, cx| {
                let _ = view.save(cx);
                cx.notify();
            });
        }));
    }

    fn toggle(&mut self, cx: &mut Context<Self>) {
        self.notice = None;
        self.autosave.take();
        if self.active() {
            if let Some(controller) = &mut self.controller {
                controller.stop();
                self.snapshot = controller.snapshot();
            }
        } else {
            let result = self.save(cx).and_then(|settings| {
                self.controller
                    .as_mut()
                    .expect("controller alive")
                    .start(settings)
            });
            if let Err(error) = result {
                self.notice = Some(format!("{error:#}"));
            }
            self.snapshot = self
                .controller
                .as_ref()
                .expect("controller alive")
                .snapshot();
        }
        cx.notify();
    }

    fn browse(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        let result = cx.prompt_for_paths(PathPromptOptions {
            files: index == CONFIG_FILE,
            directories: index == ROOT,
            multiple: false,
            prompt: Some(
                if index == ROOT {
                    "選擇快取目錄"
                } else {
                    "選擇 TOML 設定檔"
                }
                .into(),
            ),
        });
        let input = self.inputs[index].clone();
        let handle = window.window_handle();
        cx.spawn(async move |this, cx| match result.await {
            Ok(Ok(Some(paths))) => {
                if let Some(path) = paths.first() {
                    let value = path.to_string_lossy().into_owned();
                    let _ = handle.update(cx, |_, window, cx| {
                        input.update(cx, |input, cx| input.set_value(value, window, cx));
                    });
                    let _ = this.update(cx, |view, cx| {
                        view.schedule_save(cx);
                        cx.notify();
                    });
                }
            }
            Ok(Ok(None)) => {}
            error => {
                let _ = this.update(cx, |view, cx| {
                    view.notice = Some(format!("開啟檔案選擇器失敗：{error:?}"));
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn status(&self) -> &'static str {
        match self.snapshot.phase {
            ServicePhase::Stopped => "已停止",
            ServicePhase::Starting => "啟動中…",
            ServicePhase::Running => "運作中",
            ServicePhase::Stopping => "停止中…",
            ServicePhase::Failed => "啟動失敗",
        }
    }

    fn overview(&self, cx: &mut Context<Self>) -> AnyElement {
        let s = &self.snapshot;
        let total = s.hits + s.misses;
        let ratio = if total == 0 {
            "—".into()
        } else {
            format!("{:.1}%", s.hits as f64 / total as f64 * 100.)
        };
        let stats = div()
            .flex()
            .gap_3()
            .child(stat("快取容量", bytes(s.cas_bytes), "CAS 儲存用量", cx))
            .child(stat("快取物件", s.cas_entries.to_string(), "CAS blobs", cx))
            .child(stat(
                "建置結果",
                s.ac_entries.to_string(),
                "Action Cache",
                cx,
            ))
            .child(stat("命中率", ratio, "CAS + Action Cache", cx));
        let endpoint = self.snapshot.endpoint.clone();
        let command = endpoint
            .as_ref()
            .map(|endpoint| format!("--remote_cache={endpoint}"));
        let unavailable = if matches!(s.phase, ServicePhase::Running) {
            "未找到可用的區網 IP，請確認網路連線。"
        } else {
            "啟動服務後顯示連線參數"
        };
        panel(cx)
            .gap_5()
            .child(stats)
            .child(
                div()
                    .text_lg()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child("本機快取服務"),
            )
            .child(detail(
                "連線位址",
                endpoint.as_deref().unwrap_or(unavailable),
                cx,
            ))
            .child(detail("快取目錄", &self.saved.root, cx))
            .child(detail("容量上限", &self.saved.max_size, cx))
            .child(detail(
                "已上傳 / 已下載",
                &format!(
                    "{} / {}",
                    bytes(s.uploaded_bytes),
                    bytes(s.downloaded_bytes)
                ),
                cx,
            ))
            .child(detail(
                "命中 / 未命中",
                &format!("{} / {}", s.hits, s.misses),
                cx,
            ))
            .child(detail(
                "持續運作",
                &format!(
                    "{}h {}m {}s",
                    s.uptime_secs / 3600,
                    s.uptime_secs / 60 % 60,
                    s.uptime_secs % 60
                ),
                cx,
            ))
            .child(
                div()
                    .mt_3()
                    .text_lg()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child("連接 Bazel"),
            )
            .child(
                div()
                    .text_color(cx.theme().muted_foreground)
                    .child("將下列參數加入 bazel build 或 .bazelrc："),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .p_4()
                    .rounded_lg()
                    .bg(cx.theme().secondary)
                    .child(
                        div()
                            .flex_1()
                            .font_family("monospace")
                            .child(command.clone().unwrap_or_else(|| unavailable.into())),
                    )
                    .child(
                        Button::new("copy-endpoint")
                            .label("複製")
                            .disabled(command.is_none())
                            .on_click(cx.listener(move |view, _, _, cx| {
                                if let Some(command) = &command {
                                    cx.write_to_clipboard(ClipboardItem::new_string(
                                        command.clone(),
                                    ));
                                }
                                view.notice = Some("已複製 Bazel 參數".into());
                                cx.notify();
                            })),
                    ),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("REAPI v2 · SHA-256 · 本機執行建置，共用遠端快取"),
            )
            .into_any_element()
    }

    fn listener_control(&self, index: usize, locked: bool, cx: &mut Context<Self>) -> AnyElement {
        let slot = if index == GRPC_HOST { 0 } else { 1 };
        let choices = [
            ListenerScope::Local,
            ListenerScope::Lan,
            ListenerScope::Custom,
        ];
        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                RadioGroup::horizontal(("listen-scope", index))
                    .selected_index(
                        choices
                            .iter()
                            .position(|scope| *scope == self.listener_scopes[slot]),
                    )
                    .children([
                        Radio::new(("listen-local", index))
                            .label("僅本機 127.0.0.1")
                            .disabled(locked),
                        Radio::new(("listen-lan", index))
                            .label("區網 0.0.0.0")
                            .disabled(locked),
                        Radio::new(("listen-custom", index))
                            .label("自訂 IP")
                            .disabled(locked),
                    ])
                    .on_change(cx.listener(move |view, selected: &usize, window, cx| {
                        if view.active() {
                            return;
                        }
                        view.listener_scopes[slot] = choices[*selected];
                        let host = match choices[*selected] {
                            ListenerScope::Local => Some("127.0.0.1"),
                            ListenerScope::Lan => Some("0.0.0.0"),
                            ListenerScope::Custom => None,
                        };
                        if let Some(host) = host {
                            view.inputs[index]
                                .update(cx, |input, cx| input.set_value(host, window, cx));
                        }
                        view.schedule_save(cx);
                        cx.notify();
                    })),
            )
            .when(
                self.listener_scopes[slot] == ListenerScope::Custom,
                |body| body.child(Input::new(&self.inputs[index]).disabled(locked)),
            )
            .into_any_element()
    }

    fn service(&self, cx: &mut Context<Self>) -> AnyElement {
        let locked = self.active();
        let fields = [
            (
                ROOT,
                "儲存位置",
                "CAS、暫存上傳與 SQLite 存放於此；可輸入路徑或按「選擇…」。",
            ),
            (
                CAPACITY,
                "容量上限",
                "例如 100GB、500GiB；超過水位時依設定執行 GC。",
            ),
            (
                GRPC_HOST,
                "gRPC",
                "127.0.0.1 僅本機；0.0.0.0 允許區網用戶端連入。Port 範圍為 1–65535。",
            ),
            (
                METRICS_HOST,
                "Metrics / Health",
                "HTTP /metrics、/health、/ready；請使用可信任的網路位址。",
            ),
            (
                CONFIG_FILE,
                "進階 TOML 設定檔（選填）",
                "認證、TLS、GC 等進階設定由 TOML 載入；上方欄位優先。TLS 用戶端請使用 grpcs:// 與憑證主機名稱。",
            ),
        ];
        let mut form = panel(cx).gap_5();
        for (index, label, hint) in fields {
            let network = matches!(index, GRPC_HOST | METRICS_HOST);
            let control = if network {
                self.listener_control(index, locked, cx)
            } else {
                Input::new(&self.inputs[index])
                    .disabled(locked)
                    .into_any_element()
            };
            let mut row = div()
                .flex()
                .items_center()
                .gap_2()
                .child(div().flex_1().min_w_0().child(control));
            if network {
                let port = if index == GRPC_HOST {
                    GRPC_PORT
                } else {
                    METRICS_PORT
                };
                row = row.child(
                    div()
                        .w(px(140.))
                        .flex_none()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(div().text_sm().child("Port"))
                        .child(Input::new(&self.inputs[port]).disabled(locked)),
                );
            }
            if index == ROOT || index == CONFIG_FILE {
                row = row.child(
                    Button::new(("browse", index))
                        .label("選擇…")
                        .disabled(locked)
                        .on_click(
                            cx.listener(move |view, _, window, cx| view.browse(index, window, cx)),
                        ),
                );
            }
            form = form.child(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(div().font_weight(FontWeight::MEDIUM).child(label))
                    .child(row)
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(hint),
                    ),
            );
        }
        form.child(
            div()
                .flex()
                .items_center()
                .gap_4()
                .child(
                    Button::new("save-settings")
                        .primary()
                        .label("儲存設定")
                        .disabled(locked || self.load_failed)
                        .on_click(cx.listener(|view, _, _, cx| {
                            view.autosave.take();
                            view.notice = Some(match view.save(cx) {
                                Ok(_) => "設定已儲存".into(),
                                Err(error) => format!("儲存失敗：{error:#}"),
                            });
                            cx.notify();
                        })),
                )
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child(if locked {
                            "停止服務後即可修改。".to_string()
                        } else {
                            "設定會自動儲存，也可按左側按鈕立即儲存。".to_string()
                        }),
                ),
        )
        .into_any_element()
    }

    fn settings(&self, cx: &mut Context<Self>) -> AnyElement {
        let themes = [
            ThemePreference::System,
            ThemePreference::Light,
            ThemePreference::Dark,
        ];
        panel(cx)
            .gap_4()
            .child(
                div()
                    .text_lg()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child("外觀"),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("選擇介面主題，立即套用並自動儲存。"),
            )
            .child(
                RadioGroup::horizontal("appearance-theme")
                    .selected_index(
                        themes
                            .iter()
                            .position(|theme| *theme == self.appearance.theme),
                    )
                    .children([
                        Radio::new("theme-system").label("跟隨系統"),
                        Radio::new("theme-light").label("淺色"),
                        Radio::new("theme-dark").label("深色 / Dark Mode"),
                    ])
                    .on_change(cx.listener(move |view, index: &usize, window, cx| {
                        match view.appearance.set_theme(themes[*index]) {
                            Ok(()) => view.notice = Some("外觀設定已儲存".into()),
                            Err(error) => view.appearance.error = Some(format!("{error:#}")),
                        }
                        view.appearance.apply(window, cx);
                        cx.notify();
                    })),
            )
            .when_some(self.appearance.error.clone(), |body, error| {
                body.child(div().text_color(cx.theme().danger).child(error))
            })
            .into_any_element()
    }

    fn logs(&self, cx: &App) -> AnyElement {
        let mut log = div()
            .id("service-log")
            .flex()
            .flex_col()
            .gap_2()
            .p_4()
            .font_family("monospace")
            .text_size(px(12.))
            .overflow_x_scroll()
            .bg(cx.theme().secondary)
            .rounded_lg();
        if self.snapshot.logs.is_empty() {
            log = log.child("尚無日誌。啟動服務後會在此顯示。");
        }
        for line in &self.snapshot.logs {
            log = log.child(line.clone());
        }
        panel(cx)
            .gap_4()
            .child(div().text_lg().child("服務日誌"))
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("保留最近 200 行；包含服務啟停與錯誤。"),
            )
            .child(log)
            .into_any_element()
    }
}

impl Render for Workspace {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.appearance.apply(window, cx);
        let dark = cx.theme().mode.is_dark();
        let accent = rgb(if dark { 0x3fb5a8 } else { 0x087f75 });
        let bg = rgb(if dark { 0x12181c } else { 0xf4f7f8 });
        let mut sidebar = div()
            .w(px(180.))
            .flex_none()
            .h_full()
            .flex()
            .flex_col()
            .p_5()
            .gap_3()
            .border_r_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(
                div()
                    .text_2xl()
                    .font_weight(FontWeight::BOLD)
                    .text_color(accent)
                    .child("Cashew"),
            )
            .child(
                div()
                    .text_xs()
                    .mb_6()
                    .text_color(cx.theme().muted_foreground)
                    .child("BAZEL REMOTE CACHE"),
            );
        for (page, id, label) in [
            (Page::Overview, "nav-overview", "總覽"),
            (Page::Service, "nav-service", "服務設定"),
            (Page::Traffic, "nav-traffic", "流量／事件"),
            (Page::Logs, "nav-logs", "日誌"),
            (Page::Settings, "nav-settings", "設定"),
        ] {
            sidebar = sidebar.child(
                Button::new(id)
                    .label(label)
                    .w_full()
                    .when(self.page == page, |button| button.primary())
                    .on_click(cx.listener(move |view, _, _, cx| {
                        view.page = page;
                        view.notice = None;
                        cx.notify();
                    })),
            );
        }
        sidebar = sidebar.child(div().flex_1()).child(
            div()
                .text_xs()
                .text_color(cx.theme().muted_foreground)
                .child("REAPI v2 / Rust / GPUI"),
        );
        let content = match self.page {
            Page::Overview => self.overview(cx),
            Page::Service => self.service(cx),
            Page::Traffic => self.traffic.render(&self.snapshot, cx),
            Page::Logs => self.logs(cx),
            Page::Settings => self.settings(cx),
        };
        let error = self.snapshot.error.clone().or_else(|| self.notice.clone());
        div()
            .size_full()
            .flex()
            .bg(bg)
            .text_color(cx.theme().foreground)
            .font_family(cx.theme().font_family.clone())
            .text_size(px(14.))
            .child(sidebar)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .flex()
                    .flex_col()
                    .gap_5()
                    .p_6()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_4()
                            .child(
                                div()
                                    .flex_1()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .child(
                                        div().text_2xl().font_weight(FontWeight::SEMIBOLD).child(
                                            match self.page {
                                                Page::Overview => "快取總覽",
                                                Page::Service => "服務設定",
                                                Page::Traffic => "流量／事件",
                                                Page::Logs => "日誌",
                                                Page::Settings => "設定",
                                            },
                                        ),
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(cx.theme().muted_foreground)
                                            .when(
                                                self.page == Page::Service
                                                    && matches!(
                                                        self.save_state,
                                                        SaveState::Failed(_)
                                                    ),
                                                |label| label.text_color(cx.theme().danger),
                                            )
                                            .child(if self.page == Page::Service {
                                                self.save_state.label().to_string()
                                            } else {
                                                "Cashew · Remote Cache".to_string()
                                            }),
                                    ),
                            )
                            .child(div().text_color(accent).child(self.status()))
                            .child(
                                Button::new("toggle-service")
                                    .primary()
                                    .label(if self.active() {
                                        "停止服務"
                                    } else {
                                        "啟動服務"
                                    })
                                    .disabled(
                                        matches!(
                                            self.snapshot.phase,
                                            ServicePhase::Starting | ServicePhase::Stopping
                                        ) || self.load_failed,
                                    )
                                    .on_click(cx.listener(|view, _, _, cx| view.toggle(cx))),
                            ),
                    )
                    .when_some(error, |body, error| {
                        body.child(
                            div()
                                .p_3()
                                .rounded_lg()
                                .border_1()
                                .border_color(cx.theme().border)
                                .bg(cx.theme().secondary)
                                .child(error),
                        )
                    })
                    .child(
                        div()
                            .id("page-content")
                            .flex_1()
                            .min_h_0()
                            .overflow_y_scroll()
                            .child(content),
                    ),
            )
    }
}

fn panel(cx: &App) -> Div {
    div()
        .flex()
        .flex_col()
        .p_5()
        .rounded_lg()
        .bg(cx.theme().background)
        .border_1()
        .border_color(cx.theme().border)
}

fn stat(label: &'static str, value: String, hint: &'static str, cx: &App) -> Div {
    div()
        .flex_1()
        .min_w_0()
        .flex()
        .flex_col()
        .gap_2()
        .p_3()
        .rounded_lg()
        .bg(cx.theme().secondary)
        .child(
            div()
                .text_sm()
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(
            div()
                .text_2xl()
                .font_weight(FontWeight::SEMIBOLD)
                .child(value),
        )
        .child(
            div()
                .text_xs()
                .text_color(cx.theme().muted_foreground)
                .child(hint),
        )
}

fn detail(label: &'static str, value: &str, cx: &App) -> Div {
    div()
        .flex()
        .gap_4()
        .child(
            div()
                .w(px(140.))
                .flex_none()
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(div().flex_1().min_w_0().child(value.to_string()))
}

fn bytes(value: u64) -> String {
    if value >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", value as f64 / (1024. * 1024. * 1024.))
    } else if value >= 1024 * 1024 {
        format!("{:.1} MiB", value as f64 / (1024. * 1024.))
    } else if value >= 1024 {
        format!("{:.1} KiB", value as f64 / 1024.)
    } else {
        format!("{value} B")
    }
}

fn split_listener(value: &str) -> (String, String) {
    if let Ok(address) = value.parse::<std::net::SocketAddr>() {
        return (address.ip().to_string(), address.port().to_string());
    }
    match value.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.to_string()),
        None => (value.to_string(), String::new()),
    }
}

fn compose_listener(host: &str, port: &str, label: &str) -> anyhow::Result<String> {
    let host = host.trim();
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    let ip = host
        .parse::<std::net::IpAddr>()
        .map_err(|_| anyhow::anyhow!("{label} 的 IP 位址無效"))?;
    let port = port
        .trim()
        .parse::<u16>()
        .map_err(|_| anyhow::anyhow!("{label} 的 Port 必須介於 1–65535"))?;
    anyhow::ensure!(port != 0, "{label} 的 Port 必須介於 1–65535");
    Ok(std::net::SocketAddr::new(ip, port).to_string())
}

#[cfg(test)]
mod tests {
    use super::{compose_listener, split_listener};

    #[test]
    fn editing_listener_ports_preserves_ipv4_and_ipv6_addresses() {
        for original in ["127.0.0.1:50051", "[::1]:50051", "[::]:65535"] {
            let (host, port) = split_listener(original);
            let address = compose_listener(&host, &port, "gRPC").unwrap();
            assert_eq!(
                address.parse::<std::net::SocketAddr>().unwrap(),
                original.parse().unwrap()
            );
        }
        assert_eq!(
            compose_listener("[::1]", " 12345 ", "gRPC").unwrap(),
            "[::1]:12345"
        );
    }

    #[test]
    fn listener_fields_reject_invalid_addresses_and_out_of_range_ports() {
        for port in ["", "0", "-1", "65536", "abc"] {
            assert!(compose_listener("127.0.0.1", port, "gRPC").is_err());
        }
        assert!(compose_listener("not-an-ip", "50051", "gRPC").is_err());
    }
}
