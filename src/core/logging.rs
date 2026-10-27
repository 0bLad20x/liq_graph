//! Minimal, dependency-free, worker-scoped log filtering.
//! LIQ_GRAPH_LOG=info,source_jupiter_mint=debug,liq_graph::workers::completion::manifest=debug
use std::{env, fmt, sync::OnceLock, time::{SystemTime, UNIX_EPOCH}};
use anyhow::{bail, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Level { Error, Warn, Info, Debug }

impl Level {
    fn parse(value:&str)->Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "error"=>Ok(Self::Error),"warn"|"warning"=>Ok(Self::Warn),
            "info"=>Ok(Self::Info),"debug"|"trace"=>Ok(Self::Debug),
            _=>bail!("invalid LIQ_GRAPH_LOG level: {value} (error, warn, info, debug)"),
        }
    }
    fn label(self)->&'static str {
        match self{Self::Error=>"ERROR",Self::Warn=>"WARN",
            Self::Info=>"INFO",Self::Debug=>"DEBUG"}
    }
}
struct Config {default:Level, modules:Vec<(String,Level)>}
static CONFIG:OnceLock<Config>=OnceLock::new();

/// Call after dotenv has loaded. A failed configuration rejects startup.
pub fn init()->Result<()> {
    let mut parsed=Config{default:Level::Info,modules:Vec::new()};
    if let Ok(raw)=env::var("LIQ_GRAPH_LOG") {
        for part in raw.split(',').map(str::trim).filter(|part|!part.is_empty()) {
            if let Some((module,level))=part.split_once('=') {
                if module.trim().is_empty(){bail!("empty LIQ_GRAPH_LOG module name");}
                parsed.modules.push((module.trim().to_owned(),Level::parse(level)?));
            }else{
                parsed.default=Level::parse(part)?;
            }
        }
    }
    let _=CONFIG.set(parsed);
    Ok(())
}
pub fn enabled(level:Level,target:&str)->bool {
    let Some(config)=CONFIG.get() else{return true;};
    let mut limit=config.default;
    for (name,configured) in &config.modules {
        if name=="*" || target.strip_suffix(name)
            .is_some_and(|prefix|prefix.is_empty()||prefix.ends_with("::")) {
            limit=*configured;
        }
    }
    level<=limit
}
pub fn emit(level:Level,target:&str,arguments:fmt::Arguments<'_>) {
    let timestamp=SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d|d.as_millis()).unwrap_or_default();
    eprintln!("ts_ms={timestamp} level={} module={target} {arguments}",level.label());
}

#[macro_export]
macro_rules! log_event {
    ($level:ident, $($message:tt)+) => {{
        if $crate::core::logging::enabled(
            $crate::core::logging::Level::$level,module_path!()) {
            $crate::core::logging::emit(
                $crate::core::logging::Level::$level,
                module_path!(),format_args!($($message)+));
        }
    }};
}
