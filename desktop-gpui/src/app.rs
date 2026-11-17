use std::{path::PathBuf, time::Duration};

use bazel_cache_server::desktop::{
    DesktopController, DesktopSettings, DesktopSnapshot, ServicePhase,
};
use gpui_kit::component::{
    ActiveTheme, Disableable,
    button::{Button, ButtonVariants},
    input::{Input, InputEvent, InputState},
};
use gpui_kit::prelude::FluentBuilder;
use gpui_kit::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Page {
    Overview,
    Service,
    Logs,
}

pub struct Workspace {
    controller: Option<DesktopController>,
    snapshot: DesktopSnapshot,
    inputs: [Entity<InputState>; 5],
    settings_path: PathBuf,
    saved: DesktopSettings,
    page: Page,
    notice: Option<String>,
    load_failed: bool,
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
        let values = [
            &settings.root,
            &settings.max_size,
            &settings.listen,
            &settings.metrics_listen,
            &settings.config_file,
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
            .map(|input| cx.subscribe_in(input, window, |_, _, _: &InputEvent, _, cx| cx.notify()))
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
            settings_path,
            saved: settings,
            page: Page::Overview,
            notice,
            load_failed,
            _poll: poll,
            _subscriptions: subscriptions,
        }
    }

    pub fn shutdown(&mut self) {
        self.controller.take();
    }

    fn active(&self) -> bool {
        matches!(
            self.snapshot.phase,
            ServicePhase::Starting | ServicePhase::Running | ServicePhase::Stopping
        )
    }

    fn draft(&self, cx: &App) -> DesktopSettings {
        let value = |index: usize| self.inputs[index].read(cx).value().to_string();
        DesktopSettings {
            root: value(0),
            max_size: value(1),
            listen: value(2),
            metrics_listen: value(3),
            config_file: value(4),
        }
    }

    fn save(&mut self, cx: &App) -> anyhow::Result<DesktopSettings> {
        anyhow::ensure!(
            !self.load_failed,
            "原始設定檔無法讀取，請先修正 {}",
            self.settings_path.display()
        );
        let settings = self.draft(cx);
        let parent = self
            .settings_path
            .parent()
            .expect("settings have a directory");
        std::fs::create_dir_all(parent)?;
        std::fs::write(&self.settings_path, serde_json::to_vec_pretty(&settings)?)?;
        self.saved = settings.clone();
        Ok(settings)
    }

    fn toggle(&mut self, cx: &mut Context<Self>) {
        self.notice = None;
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
            files: index == 4,
            directories: index == 0,
            multiple: false,
            prompt: Some(
                if index == 0 {
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
        panel(cx)
            .gap_5()
            .child(stats)
            .child(
                div()
                    .text_lg()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child("本機快取服務"),
            )
            .child(detail("gRPC", &self.saved.listen, cx))
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
                        div().flex_1().font_family("monospace").child(
                            command
                                .clone()
                                .unwrap_or_else(|| "啟動服務後顯示連線參數".into()),
                        ),
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

    fn service(&self, cx: &mut Context<Self>) -> AnyElement {
        let locked = self.active();
        let labels = [
            "快取目錄",
            "容量上限",
            "gRPC 監聽位址",
            "Metrics / Health 監聽位址",
            "進階 TOML 設定檔（選填）",
        ];
        let hints = [
            "CAS、暫存上傳與 SQLite 存放於此。",
            "例如 100GB、500GiB；超過水位時依設定執行 GC。",
            "127.0.0.1 僅本機；0.0.0.0 允許區網用戶端連入。",
            "HTTP /metrics、/health、/ready；請使用可信任的網路位址。",
            "認證、TLS、GC 等進階設定由 TOML 載入；上方欄位優先。TLS 用戶端請使用 grpcs:// 與憑證主機名稱。",
        ];
        let mut form = panel(cx).gap_5();
        for index in 0..5 {
            let mut row = div().flex().items_center().gap_2().child(
                div()
                    .flex_1()
                    .min_w_0()
                    .child(Input::new(&self.inputs[index]).disabled(locked)),
            );
            if index == 0 || index == 4 {
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
                    .child(div().font_weight(FontWeight::MEDIUM).child(labels[index]))
                    .child(row)
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(hints[index]),
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
                            "停止服務後即可修改。"
                        } else {
                            "啟動時會儲存目前設定。"
                        }),
                ),
        )
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
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
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
            (Page::Logs, "nav-logs", "日誌"),
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
            Page::Logs => self.logs(cx),
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
                                                Page::Logs => "日誌",
                                            },
                                        ),
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(cx.theme().muted_foreground)
                                            .child("Cashew · Remote Cache"),
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
