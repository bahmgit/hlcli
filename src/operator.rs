use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainStep {
    Command(String),
    Sleep(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainError(String);

impl fmt::Display for ChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ChainError {}

pub fn parse_chain(input: &str) -> Result<Vec<ChainStep>, ChainError> {
    let mut steps = Vec::new();
    let parts = split_semicolons(input)?;
    if parts.len() > 32 {
        return Err(ChainError("command chain exceeds 32 segments".to_string()));
    }
    let mut total_sleep = Duration::ZERO;
    for part in parts {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            return Err(ChainError(
                "command chain contains an empty segment".to_string(),
            ));
        }
        if let Some(delay) = parse_sleep(trimmed)? {
            if delay > Duration::from_secs(300) {
                return Err(ChainError("single sleep exceeds 300s".to_string()));
            }
            total_sleep += delay;
            if total_sleep > Duration::from_secs(900) {
                return Err(ChainError("total chain sleep exceeds 900s".to_string()));
            }
            steps.push(ChainStep::Sleep(delay));
        } else {
            steps.push(ChainStep::Command(trimmed.to_string()));
        }
    }
    Ok(steps)
}

fn split_semicolons(input: &str) -> Result<Vec<String>, ChainError> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quote = false;
    for ch in input.chars() {
        match ch {
            '"' => {
                quote = !quote;
                cur.push(ch);
            }
            ';' if !quote => {
                parts.push(std::mem::take(&mut cur));
            }
            _ => cur.push(ch),
        }
    }
    if quote {
        return Err(ChainError(
            "unterminated quote in command chain".to_string(),
        ));
    }
    parts.push(cur);
    Ok(parts)
}

fn parse_sleep(input: &str) -> Result<Option<Duration>, ChainError> {
    let words = input.split_whitespace().collect::<Vec<_>>();
    if words.is_empty() || !matches!(words[0].to_ascii_lowercase().as_str(), "sleep" | "wait") {
        return Ok(None);
    }
    if words.len() != 2 {
        return Err(ChainError("sleep requires one duration".to_string()));
    }
    parse_duration(words[1]).map(Some)
}

fn parse_duration(raw: &str) -> Result<Duration, ChainError> {
    if let Some(ms) = raw.strip_suffix("ms") {
        return ms
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|_| ChainError("invalid millisecond sleep".to_string()));
    }
    let seconds = raw.strip_suffix('s').unwrap_or(raw);
    seconds
        .parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| ChainError("invalid second sleep".to_string()))
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Variables {
    values: BTreeMap<String, String>,
}

impl Variables {
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<String>) -> Result<(), String> {
        let name = checked_var_name(name.into())?;
        self.values.insert(name, value.into());
        Ok(())
    }

    pub fn unset(&mut self, name: &str) -> bool {
        self.values.remove(name).is_some()
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    pub fn expand_token(&self, token: &str) -> Result<String, String> {
        self.expand(token, &mut BTreeSet::new())
    }

    fn expand(&self, token: &str, seen: &mut BTreeSet<String>) -> Result<String, String> {
        if !token.starts_with('@') {
            return Ok(token.to_string());
        }
        let name = checked_var_name(token.to_string())?;
        if !seen.insert(name.clone()) {
            return Err(format!("variable cycle at {name}"));
        }
        let value = self
            .values
            .get(&name)
            .ok_or_else(|| format!("unknown variable {name}"))?;
        self.expand(value, seen)
    }
}

fn checked_var_name(name: String) -> Result<String, String> {
    if !name.starts_with('@') {
        return Err("variable names must start with @".to_string());
    }
    if name.len() < 2
        || !name[1..]
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
    {
        return Err(format!("invalid variable name {name}"));
    }
    Ok(name)
}

#[derive(Debug, Clone)]
pub struct KeybindStore {
    path: PathBuf,
    binds: BTreeMap<String, String>,
}

impl KeybindStore {
    pub fn load(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();
        let binds: BTreeMap<String, String> = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(err) => return Err(err.into()),
        };
        for (key, action) in &binds {
            validate_keybind(key, action)?;
        }
        Ok(Self { path, binds })
    }

    pub fn bind(
        &mut self,
        key: impl Into<String>,
        action: impl Into<String>,
    ) -> anyhow::Result<()> {
        let key = key.into();
        let action = action.into();
        validate_keybind(&key, &action)?;
        self.binds.insert(key, action);
        self.save()
    }

    pub fn unbind(&mut self, key: &str) -> anyhow::Result<bool> {
        anyhow::ensure!(
            valid_key(key),
            "keybind key must be one printable ASCII character"
        );
        let existed = self.binds.remove(key).is_some();
        self.save()?;
        Ok(existed)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.binds.get(key).map(String::as_str)
    }

    pub fn binds(&self) -> &BTreeMap<String, String> {
        &self.binds
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn save(&self) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_vec_pretty(&self.binds)?;
        write_secure(&self.path, &data)
    }
}

fn validate_keybind(key: &str, action: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        valid_key(key),
        "keybind key must be one printable ASCII character"
    );
    anyhow::ensure!(
        !action.trim().is_empty(),
        "keybind action must not be empty"
    );
    Ok(())
}

fn valid_key(key: &str) -> bool {
    key.len() == 1 && key.as_bytes()[0].is_ascii_graphic()
}

fn write_secure(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(data)?;
        file.sync_data()?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(tmp, path)?;
    Ok(())
}
