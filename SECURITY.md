# Security

Report vulnerabilities privately through
[GitHub private vulnerability reporting](https://github.com/bahmgit/hlcli/security/advisories/new).
Include the affected revision, impact, and a minimal reproduction stripped of real account,
order, host, and credential data. Do not post exploitable trading failures or secrets in public issues.

Security fixes target the current main branch and pinned toolchain. Protocol behavior is controlled
by Hyperliquid; a successful test run does not guarantee future exchange acceptance.

Use a dedicated API wallet and minimum practical funded exposure. Never give the daemon the main
account private key or seed phrase. Run one signing daemon per API-wallet key. Protect the data
directory, IPC socket, password file, logs, and journals with operating-system permissions.

Stored credentials use AES-256-GCM with an Argon2id-derived key and random salt/nonce. Encryption
does not protect a compromised running daemon, weak password, or equally privileged local user.
HTTP observation exposes account data; prefer loopback or an authenticated private tunnel.
An HTTP token provides authentication, not TLS.

If execution becomes ambiguous, stop submitting commands, preserve the journal, and reconcile
using an independent trusted exchange client. Never delete recovery state to bypass a halt.
For suspected key exposure, stop the daemon and revoke/replace the API wallet through a trusted
interface. Treat profiles, configs, keybinds, runtime files, shell history, and validation logs
as sensitive even when they contain no private key. Review staged content before every push;
`.gitignore` is only a last guard.
