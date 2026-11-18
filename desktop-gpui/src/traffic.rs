use bazel_cache_server::desktop::{DesktopSnapshot, TrafficEvent, TrafficKind, TrafficSession};
use chrono::{Local, TimeZone};
use gpui_kit::component::{
    ActiveTheme,
    button::{Button, ButtonVariants},
    tooltip::Tooltip,
};
use gpui_kit::prelude::FluentBuilder;
use gpui_kit::*;

use crate::app::Workspace;

/// Tables never get narrower than this; their wrapper scrolls horizontally instead.
const SESSION_TABLE_MIN_WIDTH: f32 = 900.;
const EVENT_TABLE_MIN_WIDTH: f32 = 760.;
/// Lists scroll vertically past this height so the page stays usable.
const SESSION_LIST_MAX_HEIGHT: f32 = 280.;
const EVENT_LIST_MAX_HEIGHT: f32 = 420.;

const SCOL_ID: f32 = 56.;
const SCOL_ADDR: f32 = 176.;
const SCOL_STATE: f32 = 76.;
const SCOL_BYTES: f32 = 92.;
const SCOL_RATE: f32 = 104.;
const SCOL_COUNTS_MIN: f32 = 200.;

const COL_TIME: f32 = 104.;
const COL_KIND: f32 = 76.;
const COL_RPC: f32 = 168.;
const COL_STATUS: f32 = 104.;
const COL_BYTES: f32 = 92.;
const COL_DURATION: f32 = 84.;
const COL_DETAIL_MIN: f32 = 180.;

const KIND_FILTERS: [(&str, Option<TrafficKind>); 4] = [
    ("All", None),
    ("Upload", Some(TrafficKind::Upload)),
    ("Query", Some(TrafficKind::Query)),
    ("Download", Some(TrafficKind::Download)),
];

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SessionFilter {
    #[default]
    All,
    Connected,
    Closed,
}

impl SessionFilter {
    const ALL: [(&'static str, SessionFilter); 3] = [
        ("全部", SessionFilter::All),
        ("連線中", SessionFilter::Connected),
        ("已關閉", SessionFilter::Closed),
    ];

    fn matches(self, session: &TrafficSession) -> bool {
        match self {
            SessionFilter::All => true,
            SessionFilter::Connected => session.ended_at_ms.is_none(),
            SessionFilter::Closed => session.ended_at_ms.is_some(),
        }
    }
}

#[derive(Default)]
pub(crate) struct TrafficView {
    pub(crate) session_filter: SessionFilter,
    pub(crate) kind_filter: Option<TrafficKind>,
}

impl TrafficView {
    pub(crate) fn render(
        &self,
        snapshot: &DesktopSnapshot,
        cx: &mut Context<Workspace>,
    ) -> AnyElement {
        let dashboard = div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                stat_row()
                    .child(stat(
                        "上傳速率",
                        rate(snapshot.network_upload_bytes_per_sec),
                        "實際 socket 速率（客戶端 → 快取）",
                        cx,
                    ))
                    .child(stat(
                        "下載速率",
                        rate(snapshot.network_download_bytes_per_sec),
                        "實際 socket 速率（快取 → 客戶端）",
                        cx,
                    ))
                    .child(stat(
                        "累計上傳",
                        bytes(snapshot.network_uploaded_bytes),
                        "含 HTTP/2 / TLS 通訊開銷，不含 TCP/IP 標頭",
                        cx,
                    ))
                    .child(stat(
                        "累計下載",
                        bytes(snapshot.network_downloaded_bytes),
                        "含 HTTP/2 / TLS 通訊開銷，不含 TCP/IP 標頭",
                        cx,
                    )),
            )
            .child(
                stat_row()
                    .child(stat(
                        "Upload 請求",
                        count(snapshot.upload_requests),
                        "寫入 / 上傳 RPC 次數",
                        cx,
                    ))
                    .child(stat(
                        "Query 請求",
                        count(snapshot.query_requests),
                        "查詢 / 檢查 RPC 次數",
                        cx,
                    ))
                    .child(stat(
                        "Download 請求",
                        count(snapshot.download_requests),
                        "讀取 / 下載 RPC 次數",
                        cx,
                    )),
            );

        let sessions = self.sessions_panel(snapshot, cx);
        let selected = snapshot
            .selected_session_id
            .and_then(|id| snapshot.sessions.iter().find(|session| session.id == id));
        let details = self.details_panel(snapshot, selected, cx);

        div()
            .id("traffic-view")
            .flex()
            .flex_col()
            .gap_4()
            .w_full()
            .min_w_0()
            .child(dashboard)
            .child(sessions)
            .child(details)
            .into_any_element()
    }

    fn sessions_panel(&self, snapshot: &DesktopSnapshot, cx: &mut Context<Workspace>) -> Div {
        let muted = cx.theme().muted_foreground;

        let mut filters = div().flex().flex_wrap().gap_2();
        for (index, (label, filter)) in SessionFilter::ALL.into_iter().enumerate() {
            filters = filters.child(
                Button::new(("traffic-session-filter", index))
                    .label(label)
                    .when(self.session_filter == filter, |button| button.primary())
                    .on_click(cx.listener(move |view, _, _, cx| {
                        view.traffic.session_filter = filter;
                        cx.notify();
                    })),
            );
        }

        let visible: Vec<&TrafficSession> = snapshot
            .sessions
            .iter()
            .filter(|session| self.session_filter.matches(session))
            .collect();
        let connected = snapshot
            .sessions
            .iter()
            .filter(|session| session.ended_at_ms.is_none())
            .count();
        let summary = format!(
            "顯示 {} / {} 個 session（連線中 {}，已關閉 {}；每個 TCP 連線一個 session，已關閉者最多保留最新 200 個）",
            visible.len(),
            snapshot.sessions.len(),
            connected,
            snapshot.sessions.len() - connected,
        );

        let body: AnyElement = if snapshot.sessions.is_empty() {
            empty_state("尚無 session。客戶端與服務建立 TCP 連線後會顯示於此。", cx)
        } else if visible.is_empty() {
            empty_state("目前篩選條件下沒有 session。", cx)
        } else {
            let mut list = div().flex().flex_col();
            for session in visible {
                list = list.child(session_row(
                    session,
                    snapshot.selected_session_id == Some(session.id),
                    cx,
                ));
            }
            let table = div()
                .flex()
                .flex_col()
                .min_w(px(SESSION_TABLE_MIN_WIDTH))
                .child(session_header(cx))
                .child(
                    div()
                        .id("traffic-session-list")
                        .max_h(px(SESSION_LIST_MAX_HEIGHT))
                        .overflow_y_scroll()
                        .child(list),
                );
            div()
                .id("traffic-session-scroll")
                .w_full()
                .overflow_x_scroll()
                .child(table)
                .into_any_element()
        };

        panel(cx)
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .items_center()
                    .justify_between()
                    .gap_3()
                    .child(div().text_lg().child("連線 Session"))
                    .child(filters),
            )
            .child(div().text_sm().text_color(muted).child(summary))
            .child(
                div()
                    .text_xs()
                    .text_color(muted)
                    .child("位元組與速率為實際網路流量，含 HTTP/2 / TLS 通訊開銷，不含 TCP/IP 標頭；請求數為已完成的 RPC 次數。點選 session 查看明細。"),
            )
            .child(body)
    }

    fn details_panel(
        &self,
        snapshot: &DesktopSnapshot,
        selected: Option<&TrafficSession>,
        cx: &mut Context<Workspace>,
    ) -> Div {
        let muted = cx.theme().muted_foreground;
        let title = div().text_lg().child("Session 明細");

        let Some(session) = selected else {
            return panel(cx).child(title).child(empty_state(
                "尚未選取 session。有連線後會自動選取最新的 session。",
                cx,
            ));
        };

        let now_ms = u64::try_from(Local::now().timestamp_millis()).unwrap_or(0);
        let lifetime_end = session.ended_at_ms.unwrap_or(now_ms);
        let lifecycle = stat_row()
            .child(stat(
                "Session",
                format!("#{} · {}", session.id, session.peer_addr),
                if session.ended_at_ms.is_none() {
                    "狀態：連線中"
                } else {
                    "狀態：已關閉"
                },
                cx,
            ))
            .child(stat(
                "連線開始",
                local_time(session.started_at_ms),
                "TCP 連線建立時間",
                cx,
            ))
            .child(stat(
                "連線結束",
                session
                    .ended_at_ms
                    .map_or_else(|| "連線中".to_string(), local_time),
                "socket 關閉時間",
                cx,
            ))
            .child(stat(
                "連線時長",
                duration(lifetime_end.saturating_sub(session.started_at_ms) as f64),
                "開始到結束（連線中則到目前）",
                cx,
            ))
            .child(stat(
                "最後活動",
                local_time(session.last_activity_ms),
                "最近一次 socket 讀寫",
                cx,
            ));

        let mut filters = div().flex().flex_wrap().gap_2();
        for (index, (label, kind)) in KIND_FILTERS.into_iter().enumerate() {
            filters = filters.child(
                Button::new(("traffic-filter", index))
                    .label(label)
                    .when(self.kind_filter == kind, |button| button.primary())
                    .on_click(cx.listener(move |view, _, _, cx| {
                        view.traffic.kind_filter = kind;
                        cx.notify();
                    })),
            );
        }

        let visible: Vec<&TrafficEvent> = snapshot
            .events
            .iter()
            .filter(|event| self.kind_filter.is_none_or(|kind| event.kind == kind))
            .collect();
        let summary = format!(
            "顯示 {} / {} 筆 RPC（最新在前，每個 session 最多保留 200 筆；請求數不受保留上限影響）",
            visible.len(),
            snapshot.events.len()
        );

        let body: AnyElement = if snapshot.events.is_empty() {
            empty_state(
                "此 session 尚無已完成的 RPC。串流中的請求會在結束後才列入。",
                cx,
            )
        } else if visible.is_empty() {
            empty_state("目前篩選條件下沒有 RPC。", cx)
        } else {
            let mut list = div().flex().flex_col();
            for event in visible {
                list = list.child(event_row(event, cx));
            }
            let table = div()
                .flex()
                .flex_col()
                .min_w(px(EVENT_TABLE_MIN_WIDTH))
                .child(event_header(cx))
                .child(
                    div()
                        .id("traffic-event-list")
                        .max_h(px(EVENT_LIST_MAX_HEIGHT))
                        .overflow_y_scroll()
                        .child(list),
                );
            div()
                .id("traffic-event-scroll")
                .w_full()
                .overflow_x_scroll()
                .child(table)
                .into_any_element()
        };

        panel(cx)
            .child(title)
            .child(lifecycle)
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .items_center()
                    .justify_between()
                    .gap_3()
                    .child(div().text_base().child("RPC 操作明細"))
                    .child(filters),
            )
            .child(div().text_sm().text_color(muted).child(summary))
            .child(
                div()
                    .text_xs()
                    .text_color(muted)
                    .child("操作明細的 Bytes 為 payload（blob 或 ActionResult 資料），與上方含 HTTP/2 / TLS 通訊開銷的網路位元組不同。"),
            )
            .child(body)
    }
}

fn panel(cx: &App) -> Div {
    div()
        .flex()
        .flex_col()
        .gap_3()
        .p_4()
        .rounded_lg()
        .border_1()
        .border_color(cx.theme().border)
}

fn stat_row() -> Div {
    div().flex().flex_wrap().gap_3()
}

fn stat(label: &'static str, value: String, hint: &'static str, cx: &App) -> Div {
    div()
        .flex_1()
        .min_w(px(160.))
        .flex()
        .flex_col()
        .gap_1()
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
                .text_xl()
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

fn empty_state(message: &'static str, cx: &App) -> AnyElement {
    div()
        .flex()
        .items_center()
        .justify_center()
        .p_6()
        .rounded_lg()
        .bg(cx.theme().secondary)
        .text_color(cx.theme().muted_foreground)
        .child(message)
        .into_any_element()
}

fn session_header(cx: &App) -> Div {
    let color = cx.theme().muted_foreground;
    row()
        .border_b_1()
        .border_color(cx.theme().border)
        .text_xs()
        .font_weight(FontWeight::SEMIBOLD)
        .text_color(color)
        .child(fixed(SCOL_ID).child("ID"))
        .child(fixed(SCOL_ADDR).child("IP:Port"))
        .child(fixed(SCOL_STATE).child("狀態"))
        .child(fixed(SCOL_BYTES).justify_end().child("上傳"))
        .child(fixed(SCOL_BYTES).justify_end().child("下載"))
        .child(fixed(SCOL_RATE).justify_end().child("上傳速率"))
        .child(fixed(SCOL_RATE).justify_end().child("下載速率"))
        .child(flex_col(SCOL_COUNTS_MIN).child("請求 Upload / Query / Download"))
}

fn session_row(
    session: &TrafficSession,
    selected: bool,
    cx: &mut Context<Workspace>,
) -> Stateful<Div> {
    let theme = cx.theme();
    let id = session.id;
    let key = id as usize;
    let connected = session.ended_at_ms.is_none();
    let (state_label, state_color) = if connected {
        ("連線中", theme.success)
    } else {
        ("已關閉", theme.muted_foreground)
    };
    let download_only = session.upload_requests == 0
        && session.query_requests == 0
        && session.download_requests > 0;
    let mut counts = format!(
        "U {} · Q {} · D {}",
        count(session.upload_requests),
        count(session.query_requests),
        count(session.download_requests),
    );
    if download_only {
        counts.push_str("（僅下載）");
    }

    row()
        .id(("traffic-session", key))
        .cursor_pointer()
        .border_b_1()
        .border_color(theme.border)
        .text_sm()
        .when(selected, |row| row.bg(theme.secondary))
        .hover(|row| row.bg(theme.secondary))
        .on_click(cx.listener(move |view, _, _, cx| {
            view.select_traffic_session(Some(id), cx);
        }))
        .child(fixed(SCOL_ID).child(format!("#{id}")))
        .child(truncated(
            ("traffic-session-addr", key),
            fixed(SCOL_ADDR).font_family("monospace").text_xs(),
            session.peer_addr.to_string(),
        ))
        .child(
            fixed(SCOL_STATE)
                .text_color(state_color)
                .font_weight(FontWeight::SEMIBOLD)
                .child(state_label),
        )
        .child(
            fixed(SCOL_BYTES)
                .justify_end()
                .child(bytes(session.uploaded_bytes)),
        )
        .child(
            fixed(SCOL_BYTES)
                .justify_end()
                .child(bytes(session.downloaded_bytes)),
        )
        .child(
            fixed(SCOL_RATE)
                .justify_end()
                .child(rate(session.upload_bytes_per_sec)),
        )
        .child(
            fixed(SCOL_RATE)
                .justify_end()
                .child(rate(session.download_bytes_per_sec)),
        )
        .child(truncated(
            ("traffic-session-counts", key),
            flex_col(SCOL_COUNTS_MIN),
            counts,
        ))
}

fn event_header(cx: &App) -> Div {
    let color = cx.theme().muted_foreground;
    row()
        .border_b_1()
        .border_color(cx.theme().border)
        .text_xs()
        .font_weight(FontWeight::SEMIBOLD)
        .text_color(color)
        .child(fixed(COL_TIME).child("完成時間"))
        .child(fixed(COL_KIND).child("類型"))
        .child(fixed(COL_RPC).child("RPC"))
        .child(fixed(COL_STATUS).child("狀態"))
        .child(fixed(COL_BYTES).justify_end().child("Payload"))
        .child(fixed(COL_DURATION).justify_end().child("耗時"))
        .child(flex_col(COL_DETAIL_MIN).child("明細"))
}

fn event_row(event: &TrafficEvent, cx: &App) -> Stateful<Div> {
    let theme = cx.theme();
    let (kind_label, kind_color) = match event.kind {
        TrafficKind::Upload => ("Upload", theme.primary),
        TrafficKind::Query => ("Query", theme.info),
        TrafficKind::Download => ("Download", theme.success),
    };
    let status_color = match event.status {
        "OK" => theme.success,
        "PARTIAL" | "NOT_FOUND" | "CANCELLED" => theme.warning,
        _ => theme.danger,
    };
    let id = event.id as usize;
    let detail = if event.detail.is_empty() {
        "—".to_string()
    } else {
        event.detail.clone()
    };
    row()
        .id(("traffic-row", id))
        .border_b_1()
        .border_color(theme.border)
        .text_sm()
        .child(fixed(COL_TIME).child(local_time(event.timestamp_ms)))
        .child(
            fixed(COL_KIND)
                .text_color(kind_color)
                .font_weight(FontWeight::SEMIBOLD)
                .child(kind_label),
        )
        .child(truncated(
            ("traffic-rpc", id),
            fixed(COL_RPC),
            event.method.to_string(),
        ))
        .child(
            fixed(COL_STATUS)
                .text_color(status_color)
                .child(event.status),
        )
        .child(fixed(COL_BYTES).justify_end().child(bytes(event.bytes)))
        .child(
            fixed(COL_DURATION)
                .justify_end()
                .child(duration(event.duration_ms)),
        )
        .child(truncated(
            ("traffic-detail", id),
            flex_col(COL_DETAIL_MIN).font_family("monospace").text_xs(),
            detail,
        ))
}

fn row() -> Div {
    div().flex().items_center().gap_3().py_2().px_1().w_full()
}

fn fixed(width: f32) -> Div {
    div()
        .w(px(width))
        .flex_none()
        .flex()
        .items_center()
        .overflow_hidden()
        .whitespace_nowrap()
}

/// Remaining-width column that never pushes the row wider than its minimum.
fn flex_col(min_width: f32) -> Div {
    div()
        .flex_1()
        .min_w(px(min_width))
        .overflow_hidden()
        .whitespace_nowrap()
}

/// Ellipsised cell whose full text is shown in a tooltip.
fn truncated(id: (&'static str, usize), cell: Div, text: String) -> Stateful<Div> {
    let tip: SharedString = text.clone().into();
    cell.id(id)
        .text_ellipsis()
        .tooltip(move |window, cx| Tooltip::new(tip.clone()).build(window, cx))
        .child(text)
}

fn local_time(timestamp_ms: u64) -> String {
    i64::try_from(timestamp_ms)
        .ok()
        .and_then(|ms| Local.timestamp_millis_opt(ms).single())
        .map(|time| time.format("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| "—".to_string())
}

fn duration(ms: f64) -> String {
    if !ms.is_finite() {
        "—".to_string()
    } else if ms >= 3_600_000. {
        format!("{:.2} h", ms / 3_600_000.)
    } else if ms >= 60_000. {
        format!("{:.1} min", ms / 60_000.)
    } else if ms >= 1000. {
        format!("{:.2} s", ms / 1000.)
    } else {
        format!("{ms:.1} ms")
    }
}

fn count(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn rate(per_sec: f64) -> String {
    let per_sec = if per_sec.is_finite() {
        per_sec.max(0.)
    } else {
        0.
    };
    if per_sec >= 1024. * 1024. {
        format!("{:.2} MiB/s", per_sec / (1024. * 1024.))
    } else if per_sec >= 1024. {
        format!("{:.1} KiB/s", per_sec / 1024.)
    } else {
        format!("{per_sec:.0} B/s")
    }
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
