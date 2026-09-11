use std::{
    io::{self, IsTerminal, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use clap::Parser;
use hl_v2::{
    config::Config,
    ipc::{self, IpcRequest, IpcResponse},
    runtime::{CommandExecutionState, CommandResponse},
    state::Fill,
};

#[derive(Debug, Parser)]
#[command(name = "hl", about = "Hyperliquid v2 thin client")]
struct Args {
    #[arg(long, env = "HL_V2_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[arg(long, env = "HL_V2_IPC_PATH")]
    ipc_path: Option<PathBuf>,
    #[arg(long, value_name = "SYMBOL")]
    market: Option<String>,
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cfg = Config::load(args.data_dir)?;
    let ipc_path = args
        .ipc_path
        .unwrap_or_else(|| cfg.runtime_dir().join("backend.sock"));
    let session_id = match args.market {
        Some(market) => Some(open_market_session(&ipc_path, market).await?),
        None => None,
    };
    let result = if args.command.is_empty() {
        shell(&ipc_path, session_id).await
    } else {
        run_once(&ipc_path, args.command, session_id).await
    };
    if let Some(session_id) = session_id {
        let close = close_market_session(&ipc_path, session_id).await;
        if result.is_ok() {
            close?;
        }
    }
    result
}

async fn open_market_session(path: &Path, market: String) -> anyhow::Result<u64> {
    match ipc::request(path, &IpcRequest::MarketSessionOpen { market }, max_frame()).await? {
        IpcResponse::MarketSession(session) => Ok(session.session_id),
        IpcResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("unexpected market session response: {other:?}"),
    }
}

async fn close_market_session(path: &Path, session_id: u64) -> anyhow::Result<()> {
    match ipc::request(
        path,
        &IpcRequest::MarketSessionClose { session_id },
        max_frame(),
    )
    .await?
    {
        IpcResponse::MarketSessionClosed => Ok(()),
        IpcResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("unexpected market session close response: {other:?}"),
    }
}

async fn shell(path: &Path, session_id: Option<u64>) -> anyhow::Result<()> {
    let mut input = ShellInput::new(path, session_id)?;
    loop {
        let prompt = shell_prompt(path, session_id).await;
        let Some(line) = input.read_line(&prompt)? else {
            break;
        };
        let command = line.trim();
        if command.is_empty() {
            continue;
        }
        input.remember(command);
        match submit_shell_command(path, command, session_id).await {
            Ok(ClientControl::Continue) => {}
            Ok(ClientControl::Exit) => break,
            Err(err) => eprintln!("Error: {err}"),
        }
        input.discard_pending_input()?;
    }
    Ok(())
}

async fn shell_prompt(path: &Path, session_id: Option<u64>) -> String {
    let request = session_id.map_or(IpcRequest::Health, |session_id| {
        IpcRequest::MarketSessionGet { session_id }
    });
    match ipc::request(path, &request, max_frame()).await {
        Ok(IpcResponse::Health(payload)) => format!("hl[{}]> ", payload.active),
        Ok(IpcResponse::MarketSession(session)) => format!("hl[{}]> ", session.active),
        _ => "hl[?]> ".to_string(),
    }
}

async fn submit_shell_command(
    path: &Path,
    command: &str,
    session_id: Option<u64>,
) -> anyhow::Result<ClientControl> {
    let command = resolve_shell_keybind(path, command).await?;
    submit_and_print(path, &command, session_id).await
}

async fn run_once(path: &Path, words: Vec<String>, session_id: Option<u64>) -> anyhow::Result<()> {
    let first = words
        .first()
        .map(|word| word.to_ascii_lowercase())
        .unwrap_or_default();
    match first.as_str() {
        "health" => {
            anyhow::ensure!(words.len() == 1, "health does not accept arguments");
            let request = session_id.map_or(IpcRequest::Health, |session_id| {
                IpcRequest::MarketSessionHealth { session_id }
            });
            print_health(ipc::request(path, &request, max_frame()).await?)
        }
        "metrics" => {
            anyhow::ensure!(words.len() == 1, "metrics does not accept arguments");
            print_response(ipc::request(path, &IpcRequest::Metrics, max_frame()).await?)
        }
        "command" => {
            anyhow::ensure!(
                words.len() == 3 && words[1].eq_ignore_ascii_case("get"),
                "command supports exactly: command get <id>"
            );
            let command_id = words[2].parse()?;
            print_response(
                ipc::request(path, &IpcRequest::CommandGet { command_id }, max_frame()).await?,
            )
        }
        "press" => {
            anyhow::ensure!(words.len() == 2, "press requires exactly one key");
            let command = required_keybind(path, &words[1]).await?;
            let _ = submit_and_print(path, &command, session_id).await?;
            Ok(())
        }
        "exec" => {
            anyhow::ensure!(words.len() > 1, "exec requires a command");
            let _ = submit_and_print(path, &words[1..].join(" "), session_id).await?;
            Ok(())
        }
        _ => {
            let _ = submit_and_print(path, &words.join(" "), session_id).await?;
            Ok(())
        }
    }
}

async fn keybind(path: &Path, key: &str) -> anyhow::Result<Option<String>> {
    match ipc::request(path, &IpcRequest::Keybinds, max_frame()).await? {
        IpcResponse::Keybinds(payload) => Ok(payload.keybinds.get(key).cloned()),
        IpcResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("unexpected keybind response: {other:?}"),
    }
}

async fn resolve_shell_keybind(path: &Path, input: &str) -> anyhow::Result<String> {
    Ok(keybind(path, input)
        .await?
        .unwrap_or_else(|| input.to_string()))
}

async fn required_keybind(path: &Path, key: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        valid_key(key),
        "press key must be one printable ASCII character"
    );
    keybind(path, key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("key '{key}' is not bound"))
}

async fn submit_and_print(
    path: &Path,
    command: &str,
    session_id: Option<u64>,
) -> anyhow::Result<ClientControl> {
    let request = match session_id {
        Some(session_id) => IpcRequest::CommandSubmitScoped {
            session_id,
            command: command.to_string(),
        },
        None => IpcRequest::CommandSubmit {
            command: command.to_string(),
        },
    };
    let response = ipc::request(path, &request, max_frame()).await?;
    let command_id = match response {
        IpcResponse::CommandSubmit(payload) => payload.command_id,
        IpcResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("unexpected command submit response: {other:?}"),
    };
    loop {
        let response =
            ipc::request(path, &IpcRequest::CommandGet { command_id }, max_frame()).await?;
        match response {
            IpcResponse::CommandGet(record) => {
                if record.state == CommandExecutionState::Completed {
                    if let Some(output) = record.response {
                        return render_command_response(output);
                    }
                    anyhow::bail!("completed command {command_id} missing response");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            IpcResponse::Error { message } => anyhow::bail!(message),
            other => anyhow::bail!("unexpected command get response: {other:?}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClientControl {
    Continue,
    Exit,
}

fn render_command_response(output: CommandResponse) -> anyhow::Result<ClientControl> {
    if output.clear {
        clear_terminal()?;
    }
    for line in output.lines {
        println!("{line}");
    }
    if !output.ok {
        anyhow::bail!("command failed");
    }
    Ok(if output.exit {
        ClientControl::Exit
    } else {
        ClientControl::Continue
    })
}

fn clear_terminal() -> io::Result<()> {
    print!("\x1b[2J\x1b[H");
    io::stdout().flush()
}

fn print_response(response: IpcResponse) -> anyhow::Result<()> {
    if let IpcResponse::Error { message } = &response {
        anyhow::bail!(message.clone());
    }
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}

fn print_health(response: IpcResponse) -> anyhow::Result<()> {
    let healthy = matches!(&response, IpcResponse::Health(payload) if payload.ok);
    print_response(response)?;
    anyhow::ensure!(healthy, "daemon is not healthy");
    Ok(())
}

fn valid_key(key: &str) -> bool {
    key.len() == 1 && key.as_bytes()[0].is_ascii_graphic()
}

fn max_frame() -> usize {
    8 * 1024 * 1024
}

struct FillWatch {
    path: PathBuf,
    session_id: Option<u64>,
    last_seq: u64,
    error: Option<String>,
}

struct ShellInput {
    history: History,
    interactive: bool,
    fills: Option<FillWatch>,
    _raw: Option<RawMode>,
}

impl ShellInput {
    fn new(path: &Path, session_id: Option<u64>) -> anyhow::Result<Self> {
        let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
        let raw = interactive.then(RawMode::enable).transpose()?;
        let fills = if interactive {
            let initial = fetch_fills_frame(path, session_id, 0)?;
            Some(FillWatch {
                path: path.to_path_buf(),
                session_id,
                last_seq: initial.iter().map(|fill| fill.seq).max().unwrap_or(0),
                error: None,
            })
        } else {
            None
        };
        Ok(Self {
            history: History::default(),
            interactive,
            fills,
            _raw: raw,
        })
    }

    fn read_line(&mut self, prompt: &str) -> anyhow::Result<Option<String>> {
        if self.interactive {
            read_interactive(prompt, &mut self.history, self.fills.as_mut())
        } else {
            print!("{prompt}");
            io::stdout().flush()?;
            let mut line = String::new();
            if io::stdin().read_line(&mut line)? == 0 {
                Ok(None)
            } else {
                Ok(Some(line))
            }
        }
    }

    fn remember(&mut self, command: &str) {
        self.history.push(command.to_string());
    }

    fn discard_pending_input(&mut self) -> anyhow::Result<()> {
        if self.interactive {
            RawMode::drain_stdin()?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct History {
    items: Vec<String>,
    cursor: Option<usize>,
    draft: String,
}

impl History {
    fn push(&mut self, command: String) {
        if self.items.last() != Some(&command) {
            self.items.push(command);
        }
        self.cursor = None;
        self.draft.clear();
    }

    fn older(&mut self, current: &str) -> Option<&str> {
        if self.items.is_empty() {
            return None;
        }
        let next = match self.cursor {
            Some(0) => 0,
            Some(index) => index - 1,
            None => {
                self.draft = current.to_string();
                self.items.len() - 1
            }
        };
        self.cursor = Some(next);
        Some(&self.items[next])
    }

    fn newer(&mut self) -> Option<&str> {
        let cursor = self.cursor?;
        if cursor + 1 < self.items.len() {
            let next = cursor + 1;
            self.cursor = Some(next);
            Some(&self.items[next])
        } else {
            self.cursor = None;
            Some(&self.draft)
        }
    }

    fn reset_navigation(&mut self) {
        self.cursor = None;
        self.draft.clear();
    }
}

struct LineEditor<'a> {
    history: &'a mut History,
    line: String,
    cursor: usize,
    interrupt_armed: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum InputAction {
    Continue,
    Redraw,
    Submit(String),
    Exit,
}

enum InputKey {
    Enter,
    CtrlC,
    CtrlD,
    Backspace,
    Escape(Escape),
    Char(char),
    Other,
}

impl<'a> LineEditor<'a> {
    fn new(history: &'a mut History) -> Self {
        Self {
            history,
            line: String::new(),
            cursor: 0,
            interrupt_armed: false,
        }
    }

    fn line(&self) -> &str {
        &self.line
    }

    fn cursor(&self) -> usize {
        self.cursor
    }

    fn apply(&mut self, key: InputKey) -> InputAction {
        match key {
            InputKey::Enter => InputAction::Submit(std::mem::take(&mut self.line)),
            InputKey::CtrlC => {
                if self.line.is_empty() && self.interrupt_armed {
                    return InputAction::Exit;
                }
                self.line.clear();
                self.cursor = 0;
                self.interrupt_armed = true;
                self.history.reset_navigation();
                InputAction::Redraw
            }
            InputKey::CtrlD if self.line.is_empty() => InputAction::Exit,
            InputKey::CtrlD | InputKey::Other => {
                self.interrupt_armed = false;
                InputAction::Continue
            }
            InputKey::Backspace => {
                self.interrupt_armed = false;
                if self.cursor == 0 {
                    return InputAction::Continue;
                }
                self.cursor -= 1;
                self.line.remove(self.cursor);
                self.history.reset_navigation();
                InputAction::Redraw
            }
            InputKey::Escape(Escape::Up) => {
                self.interrupt_armed = false;
                if let Some(value) = self.history.older(&self.line) {
                    self.line.clear();
                    self.line.push_str(value);
                    self.cursor = self.line.len();
                    InputAction::Redraw
                } else {
                    InputAction::Continue
                }
            }
            InputKey::Escape(Escape::Down) => {
                self.interrupt_armed = false;
                if let Some(value) = self.history.newer() {
                    self.line.clear();
                    self.line.push_str(value);
                    self.cursor = self.line.len();
                    InputAction::Redraw
                } else {
                    InputAction::Continue
                }
            }
            InputKey::Escape(Escape::Left) => {
                self.interrupt_armed = false;
                if self.cursor > 0 {
                    self.cursor -= 1;
                    InputAction::Redraw
                } else {
                    InputAction::Continue
                }
            }
            InputKey::Escape(Escape::Right) => {
                self.interrupt_armed = false;
                if self.cursor < self.line.len() {
                    self.cursor += 1;
                    InputAction::Redraw
                } else {
                    InputAction::Continue
                }
            }
            InputKey::Escape(Escape::Other) => {
                self.interrupt_armed = false;
                InputAction::Continue
            }
            InputKey::Char(ch) => {
                self.interrupt_armed = false;
                self.line.insert(self.cursor, ch);
                self.cursor += 1;
                self.history.reset_navigation();
                InputAction::Redraw
            }
        }
    }
}

fn read_interactive(
    prompt: &str,
    history: &mut History,
    mut fills: Option<&mut FillWatch>,
) -> anyhow::Result<Option<String>> {
    let mut input = io::stdin();
    let mut out = io::stdout();
    let mut editor = LineEditor::new(history);
    write!(out, "{prompt}")?;
    out.flush()?;
    loop {
        let mut byte = [0_u8; 1];
        if input.read(&mut byte)? == 0 {
            if let Some(fills) = fills.as_deref_mut() {
                announce_fills(&mut out, prompt, editor.line(), editor.cursor(), fills)?;
            }
            continue;
        }
        let key = match byte[0] {
            b'\r' | b'\n' => InputKey::Enter,
            3 => InputKey::CtrlC,
            4 => InputKey::CtrlD,
            8 | 127 => InputKey::Backspace,
            27 => InputKey::Escape(read_escape(&mut input)?),
            byte if (0x20..=0x7e).contains(&byte) => InputKey::Char(byte as char),
            _ => InputKey::Other,
        };
        match editor.apply(key) {
            InputAction::Continue => {}
            InputAction::Redraw => redraw(&mut out, prompt, editor.line(), editor.cursor())?,
            InputAction::Submit(line) => {
                writeln!(out)?;
                return Ok(Some(line));
            }
            InputAction::Exit => {
                writeln!(out)?;
                return Ok(None);
            }
        }
    }
}

fn announce_fills(
    out: &mut io::Stdout,
    prompt: &str,
    line: &str,
    cursor: usize,
    fills: &mut FillWatch,
) -> anyhow::Result<()> {
    let new = match fetch_fills_frame(&fills.path, fills.session_id, fills.last_seq) {
        Ok(new) => {
            if fills.error.take().is_some() {
                write!(out, "\r\x1b[2Kfill feed reconnected\r\n")?;
                redraw(out, prompt, line, cursor)?;
            }
            new
        }
        Err(err) => {
            let message = err.to_string();
            if fills.error.as_deref() != Some(&message) {
                fills.error = Some(message.clone());
                write!(out, "\r\x1b[2Kfill feed error: {message}\r\n")?;
                redraw(out, prompt, line, cursor)?;
            }
            return Ok(());
        }
    };
    let Some(max_seq) = new.iter().map(|fill| fill.seq).max() else {
        return Ok(());
    };
    fills.last_seq = max_seq;
    write!(out, "\r\x1b[2K")?;
    for fill in &new {
        write!(out, "{}\r\n", format_fill(fill))?;
    }
    redraw(out, prompt, line, cursor)?;
    Ok(())
}

fn fetch_fills_frame(
    path: &Path,
    session_id: Option<u64>,
    after_seq: u64,
) -> anyhow::Result<Vec<Fill>> {
    let mut stream = std::os::unix::net::UnixStream::connect(path)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let request = rmp_serde::to_vec_named(&match session_id {
        Some(session_id) => IpcRequest::FillsScoped {
            session_id,
            after_seq,
        },
        None => IpcRequest::Fills { after_seq },
    })?;
    stream.write_all(&u32::try_from(request.len())?.to_be_bytes())?;
    stream.write_all(&request)?;
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    anyhow::ensure!(len > 0 && len <= max_frame(), "ipc frame length {len}");
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload)?;
    match rmp_serde::from_slice::<IpcResponse>(&payload)? {
        IpcResponse::Fills(payload) => Ok(payload.fills),
        IpcResponse::Error { message } => anyhow::bail!(message),
        other => anyhow::bail!("unexpected fills response: {other:?}"),
    }
}

fn format_fill(fill: &Fill) -> String {
    let side = if fill.is_buy { "buy" } else { "sell" };
    let pnl = fill.closed_pnl.to_string();
    let pnl = if pnl.starts_with('-') {
        pnl
    } else {
        format!("+{pnl}")
    };
    format!(
        "fill {} {} {} @ {} pnl={}",
        fill.symbol, side, fill.size, fill.price, pnl
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Escape {
    Up,
    Down,
    Left,
    Right,
    Other,
}

fn read_escape(input: &mut io::Stdin) -> io::Result<Escape> {
    let mut bytes = [0_u8; 2];
    if input.read(&mut bytes[..1])? == 0 {
        return Ok(Escape::Other);
    }
    if input.read(&mut bytes[1..2])? == 0 {
        return Ok(Escape::Other);
    }
    Ok(match bytes {
        [b'[', b'A'] | [b'O', b'A'] => Escape::Up,
        [b'[', b'B'] | [b'O', b'B'] => Escape::Down,
        [b'[', b'C'] | [b'O', b'C'] => Escape::Right,
        [b'[', b'D'] | [b'O', b'D'] => Escape::Left,
        _ => Escape::Other,
    })
}

fn redraw(out: &mut io::Stdout, prompt: &str, line: &str, cursor: usize) -> io::Result<()> {
    write!(out, "\r\x1b[2K{prompt}{line}")?;
    let tail = line.len().saturating_sub(cursor);
    if tail > 0 {
        write!(out, "\x1b[{tail}D")?;
    }
    out.flush()
}

struct RawMode {
    saved: String,
}

impl RawMode {
    fn enable() -> anyhow::Result<Self> {
        let saved = Command::new("stty")
            .arg("-g")
            .stdin(Stdio::inherit())
            .output()?;
        anyhow::ensure!(saved.status.success(), "failed to read terminal state");
        let saved = String::from_utf8(saved.stdout)?.trim().to_string();
        anyhow::ensure!(!saved.is_empty(), "terminal state is empty");
        let status = Command::new("stty")
            .args(["-icanon", "-echo", "-isig", "min", "0", "time", "5"])
            .stdin(Stdio::inherit())
            .status()?;
        anyhow::ensure!(
            status.success(),
            "failed to enter noncanonical terminal mode for shell input"
        );
        Ok(Self { saved })
    }

    fn drain_stdin() -> anyhow::Result<()> {
        Self::set_timeout("0")?;
        let result = drain_stdin();
        let restore = Self::set_timeout("5");
        match (result, restore) {
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn set_timeout(value: &str) -> anyhow::Result<()> {
        let status = Command::new("stty")
            .args(["min", "0", "time", value])
            .stdin(Stdio::inherit())
            .status()?;
        anyhow::ensure!(status.success(), "failed to update raw terminal timeout");
        Ok(())
    }
}

fn drain_stdin() -> anyhow::Result<()> {
    let mut input = io::stdin();
    let mut buf = [0_u8; 64];
    loop {
        match input.read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err.into()),
        }
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        let _ = Command::new("stty")
            .arg(&self.saved)
            .stdin(Stdio::inherit())
            .status();
    }
}
