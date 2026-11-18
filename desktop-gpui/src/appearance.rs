use std::path::PathBuf;

use anyhow::{Result, ensure};
use gpui_kit::component::{Theme, ThemeMode};
use gpui_kit::{App, Window};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemePreference {
    #[default]
    System,
    Light,
    Dark,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Preferences {
    theme: ThemePreference,
}

pub struct Appearance {
    pub theme: ThemePreference,
    pub error: Option<String>,
    path: PathBuf,
    can_save: bool,
}

impl Appearance {
    pub fn load(path: PathBuf) -> Self {
        let loaded = match std::fs::read(&path) {
            Ok(bytes) => {
                serde_json::from_slice::<Preferences>(&bytes).map_err(|error| error.to_string())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Preferences::default())
            }
            Err(error) => Err(error.to_string()),
        };
        let can_save = loaded.is_ok();
        let error = loaded
            .as_ref()
            .err()
            .map(|error| format!("無法讀取外觀設定：{error}。原始檔案已保留。"));
        Self {
            theme: loaded.unwrap_or_default().theme,
            error,
            path,
            can_save,
        }
    }

    pub fn set_theme(&mut self, theme: ThemePreference) -> Result<()> {
        ensure!(self.can_save, "請先修正外觀設定檔 {}", self.path.display());
        crate::persistence::write_json(&self.path, &Preferences { theme })?;
        self.theme = theme;
        self.error = None;
        Ok(())
    }

    pub fn apply(&self, window: &mut Window, cx: &mut App) {
        let mode = match self.theme {
            ThemePreference::System => ThemeMode::from(window.appearance()),
            ThemePreference::Light => ThemeMode::Light,
            ThemePreference::Dark => ThemeMode::Dark,
        };
        if Theme::global(cx).mode != mode {
            Theme::change(mode, Some(window), cx);
        }
    }
}
