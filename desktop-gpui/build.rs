fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=assets/cashew.ico");

    // GPUI's Windows backend loads executable icon resource id 1 for the
    // window class, taskbar and Explorer.
    #[cfg(windows)]
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        let mut resource = winresource::WindowsResource::new();
        resource
            .set_icon("assets/cashew.ico")
            .set("ProductName", "Cashew")
            .set("FileDescription", "Cashew native remote cache manager");
        resource
            .compile()
            .expect("failed to embed assets/cashew.ico as Windows icon resource 1");
    }
}
