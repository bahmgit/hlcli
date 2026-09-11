use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{exchange::Network, state::AccountMode};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub data_dir: PathBuf,
    pub network: Network,
    pub default_symbol: String,
    pub allowed_symbols: BTreeSet<String>,
    pub symbol_aliases: BTreeMap<String, String>,
    pub bind: SocketAddr,
    pub allow_remote: bool,
    pub backend_token: Option<String>,
    pub vault_address: Option<String>,
    pub required_account_mode: Option<String>,
    pub market_cross_bps: u32,
}

impl Config {
    pub fn load(data_dir_override: Option<PathBuf>) -> anyhow::Result<Self> {
        let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
        let file = data_dir.join("hl-v2.json");
        let mut cfg = if file.exists() {
            let text = std::fs::read_to_string(&file)?;
            serde_json::from_str::<FileConfig>(&text)?.apply(Self::defaults(data_dir.clone()))?
        } else {
            Self::defaults(data_dir.clone())
        };
        cfg.apply_env()?;
        cfg.validate()?;
        cfg.secure_dirs()?;
        Ok(cfg)
    }

    pub fn defaults(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            network: Network::Mainnet,
            default_symbol: "BTC".to_string(),
            allowed_symbols: BTreeSet::new(),
            symbol_aliases: BTreeMap::new(),
            bind: SocketAddr::from(([127, 0, 0, 1], 8088)),
            allow_remote: false,
            backend_token: None,
            vault_address: None,
            required_account_mode: None,
            market_cross_bps: 50,
        }
    }

    pub fn runtime_dir(&self) -> PathBuf {
        self.data_dir.join("runtime")
    }

    pub fn journal_path(&self) -> PathBuf {
        self.runtime_dir().join("execution_journal_v2.jsonl")
    }

    pub fn managed_state_path(&self) -> PathBuf {
        self.runtime_dir().join("managed_state_v2.json")
    }

    pub fn keybinds_path(&self) -> PathBuf {
        self.data_dir.join("keybinds.json")
    }

    pub fn wallet_dir(&self) -> PathBuf {
        self.data_dir.join("wallets")
    }

    fn apply_env(&mut self) -> anyhow::Result<()> {
        if let Some(value) = env_value("HL_V2_NETWORK")? {
            self.network = parse_network(&value)?;
        }
        if let Some(value) = env_value("HL_V2_DEFAULT_SYMBOL")? {
            self.default_symbol = canonical_symbol(&value)?;
        }
        if let Some(value) = env_value("HL_V2_ALLOWED_SYMBOLS")? {
            self.allowed_symbols = parse_symbol_set(&value)?;
        }
        if let Some(value) = env_value("HL_V2_BIND")? {
            self.bind = value.parse()?;
        }
        if let Some(value) = env_value("HL_V2_ALLOW_REMOTE")? {
            self.allow_remote = parse_bool("HL_V2_ALLOW_REMOTE", &value)?;
        }
        if let Some(value) = env_value("HL_V2_BACKEND_TOKEN")? {
            self.backend_token = Some(non_empty("HL_V2_BACKEND_TOKEN", value)?);
        }
        if let Some(value) = env_value("HL_V2_VAULT_ADDRESS")? {
            self.vault_address = Some(non_empty("HL_V2_VAULT_ADDRESS", value)?);
        }
        if let Some(value) = env_value("HL_V2_REQUIRED_ACCOUNT_MODE")? {
            self.required_account_mode = parse_account_mode(&value)?;
        }
        if let Some(value) = env_value("HL_V2_MARKET_CROSS_BPS")? {
            self.market_cross_bps = value.parse()?;
        }
        Ok(())
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.bind.ip().is_loopback() || self.allow_remote,
            "remote bind requires HL_V2_ALLOW_REMOTE=true"
        );
        anyhow::ensure!(
            self.bind.ip().is_loopback() || self.backend_token.is_some(),
            "remote bind requires HL_V2_BACKEND_TOKEN"
        );
        anyhow::ensure!(
            self.market_cross_bps <= 10_000,
            "market_cross_bps must be <= 10000"
        );
        canonical_symbol(&self.default_symbol)?;
        for symbol in &self.allowed_symbols {
            canonical_symbol(symbol)?;
        }
        validate_symbol_aliases(&self.allowed_symbols, &self.symbol_aliases)?;
        Ok(())
    }

    fn secure_dirs(&self) -> anyhow::Result<()> {
        for dir in [self.data_dir.as_path(), self.runtime_dir().as_path()] {
            std::fs::create_dir_all(dir)?;
            secure_dir(dir)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileConfig {
    network: Option<Network>,
    default_symbol: Option<String>,
    allowed_symbols: Option<BTreeSet<String>>,
    symbol_aliases: Option<BTreeMap<String, String>>,
    bind: Option<SocketAddr>,
    allow_remote: Option<bool>,
    backend_token: Option<String>,
    vault_address: Option<String>,
    required_account_mode: Option<String>,
    market_cross_bps: Option<u32>,
}

impl FileConfig {
    fn apply(self, mut cfg: Config) -> anyhow::Result<Config> {
        if let Some(value) = self.network {
            cfg.network = value;
        }
        if let Some(value) = self.default_symbol {
            cfg.default_symbol = canonical_symbol(&value)?;
        }
        if let Some(value) = self.allowed_symbols {
            cfg.allowed_symbols = value
                .into_iter()
                .map(|symbol| canonical_symbol(&symbol))
                .collect::<anyhow::Result<_>>()?;
        }
        if let Some(value) = self.symbol_aliases {
            cfg.symbol_aliases = canonical_alias_map(value)?;
        }
        if let Some(value) = self.bind {
            cfg.bind = value;
        }
        if let Some(value) = self.allow_remote {
            cfg.allow_remote = value;
        }
        if let Some(value) = self.backend_token {
            cfg.backend_token = Some(non_empty("backendToken", value)?);
        }
        if let Some(value) = self.vault_address {
            cfg.vault_address = Some(non_empty("vaultAddress", value)?);
        }
        if let Some(value) = self.required_account_mode {
            cfg.required_account_mode = parse_account_mode(&value)?;
        }
        if let Some(value) = self.market_cross_bps {
            cfg.market_cross_bps = value;
        }
        Ok(cfg)
    }
}

fn default_data_dir() -> PathBuf {
    env::var_os("HL_V2_DATA_DIR")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".hyperliquid-cli")))
        .unwrap_or_else(|| PathBuf::from(".hyperliquid-cli"))
}

fn env_value(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => anyhow::bail!("{name} must not be empty"),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => anyhow::bail!("{name} must be unicode"),
    }
}

fn parse_network(raw: &str) -> anyhow::Result<Network> {
    match raw.to_ascii_lowercase().as_str() {
        "mainnet" => Ok(Network::Mainnet),
        "testnet" => Ok(Network::Testnet),
        _ => anyhow::bail!("network must be mainnet or testnet"),
    }
}

fn parse_bool(name: &str, raw: &str) -> anyhow::Result<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{name} must be true or false"),
    }
}

fn parse_symbol_set(raw: &str) -> anyhow::Result<BTreeSet<String>> {
    raw.split(',')
        .filter(|part| !part.trim().is_empty())
        .map(canonical_symbol)
        .collect()
}

fn canonical_alias_map(raw: BTreeMap<String, String>) -> anyhow::Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for (alias, target) in raw {
        let alias = canonical_alias(&alias)?;
        let target = canonical_symbol(&target)?;
        if out.insert(alias.clone(), target).is_some() {
            anyhow::bail!("duplicate symbol alias {alias}");
        }
    }
    Ok(out)
}

fn parse_account_mode(raw: &str) -> anyhow::Result<Option<String>> {
    match raw {
        "any" => Ok(None),
        _ => AccountMode::parse(raw)
            .map(|mode| Some(mode.as_str().to_string()))
            .map_err(anyhow::Error::msg),
    }
}

fn canonical_symbol(raw: &str) -> anyhow::Result<String> {
    let symbol = raw.trim().to_ascii_uppercase();
    anyhow::ensure!(!symbol.is_empty(), "symbol must not be empty");
    anyhow::ensure!(
        symbol
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ':' | '/' | '-' | '_')),
        "invalid symbol {raw}"
    );
    Ok(symbol)
}

fn canonical_alias(raw: &str) -> anyhow::Result<String> {
    let alias = raw.trim().to_ascii_uppercase();
    anyhow::ensure!(!alias.is_empty(), "symbol alias must not be empty");
    anyhow::ensure!(
        alias
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_')),
        "invalid symbol alias {raw}"
    );
    Ok(alias)
}

fn validate_symbol_aliases(
    allowed_symbols: &BTreeSet<String>,
    symbol_aliases: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    if symbol_aliases.is_empty() {
        return Ok(());
    }
    anyhow::ensure!(
        !allowed_symbols.is_empty(),
        "symbolAliases requires allowedSymbols"
    );
    for (alias, target) in symbol_aliases {
        canonical_alias(alias)?;
        canonical_symbol(target)?;
        anyhow::ensure!(
            !allowed_symbols.contains(alias),
            "symbol alias {alias} conflicts with allowed symbol"
        );
        anyhow::ensure!(
            allowed_symbols.contains(target),
            "symbol alias {alias} targets {target}, which is not allowed"
        );
    }
    Ok(())
}

fn non_empty(name: &str, value: String) -> anyhow::Result<String> {
    anyhow::ensure!(!value.trim().is_empty(), "{name} must not be empty");
    Ok(value)
}

fn secure_dir(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
