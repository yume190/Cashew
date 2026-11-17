#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]
mod app;

use gpui_kit::*;

fn main() -> anyhow::Result<()> {
    let controller = bazel_cache_server::desktop::DesktopController::new()?;
    application().with_assets(assets::Assets).run(move |cx| {
        init(cx);
        cx.on_window_closed(|cx, _| {
            if cx.windows().is_empty() {
                cx.quit();
            }
        })
        .detach();
        let bounds = Bounds::centered(None, size(px(1180.), px(820.)), cx);
        let options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            window_min_size: Some(size(px(920.), px(680.))),
            titlebar: Some(TitlebarOptions {
                title: Some("Cashew".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (_, view) = open_window(options, cx, |window, cx| {
            cx.new(|cx| app::Workspace::new(controller, window, cx))
        })
        .expect("open Cashew window");
        // Retain the controller through the quit callback so dropping the window
        // cannot leave the service alive; its Drop joins the service thread.
        cx.on_app_quit(move |cx| {
            view.update(cx, |view, _| view.shutdown());
            async {}
        })
        .detach();
        cx.activate(true);
    });
    Ok(())
}
