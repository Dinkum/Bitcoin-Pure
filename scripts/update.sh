#!/usr/bin/env bash
set -euo pipefail

MODE=""
STAGE_DIR=""
CURRENT_LINK=""
CONFIG_PATH=""
DATA_DIR=""
LOG_DIR=""
SERVICE_NAME=""
REPO_URL=""
REPO_REF=""
MINING_MODE=""
PROFILE=""
declare -a PEERS=()

usage() {
    cat <<'EOF'
scripts/update.sh prepares a staged Bitcoin Pure release for bootstrap.sh.
It is normally called via ./install.
EOF
}

log() {
	printf '[%s] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" "$*"
}

fail() {
	log "fatal: $*"
	exit 1
}

package_installed() {
	dpkg-query -W -f='${db:Status-Status}\n' "$1" 2>/dev/null | grep -qx 'installed'
}

detect_go_arch() {
	case "$(uname -m)" in
	x86_64|amd64)
		echo "amd64"
		;;
	aarch64|arm64)
		echo "arm64"
		;;
	*)
		fail "unsupported cpu architecture: $(uname -m)"
		;;
	esac
}

required_go_version() {
	awk '/^go /{print $2; exit}' "${STAGE_DIR}/go.mod"
}

current_go_version() {
	if ! command -v go >/dev/null 2>&1; then
		return 1
	fi
	go version | awk '{print $3}' | sed 's/^go//'
}

install_go() {
	local want have arch tarball url tmp
	want="$(required_go_version)"
	have="$(current_go_version || true)"
	if [[ "${have}" == "${want}" ]]; then
		export PATH="/usr/local/go/bin:${PATH}"
		return
	fi
	arch="$(detect_go_arch)"
	tarball="go${want}.linux-${arch}.tar.gz"
	url="https://go.dev/dl/${tarball}"
	tmp="/tmp/${tarball}"
	log "installing Go ${want} (${arch})"
	curl -fsSL "${url}" -o "${tmp}"
	rm -rf /usr/local/go
	tar -C /usr/local -xzf "${tmp}"
	rm -f "${tmp}"
	export PATH="/usr/local/go/bin:${PATH}"
}

ensure_packages() {
	local pkg
	local -a required=(build-essential ca-certificates curl git python3 tar)
	local -a missing=()
	for pkg in "${required[@]}"; do
		if ! package_installed "${pkg}"; then
			missing+=("${pkg}")
		fi
	done
	if [[ "${#missing[@]}" -eq 0 ]]; then
		log "system packages already satisfied"
		return
	fi
	log "installing system packages: ${missing[*]}"
	apt-get update
	DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${missing[@]}"
}

render_metadata() {
	local metadata_path version
	metadata_path="${STAGE_DIR}/.bpu-release.env"
	version="$(python3 - "${STAGE_DIR}/version.json" <<'PY'
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    print(json.load(fh).get("version", "unknown"))
PY
)"
	{
		printf 'repo_url=%s\n' "${REPO_URL}"
		printf 'repo_ref=%s\n' "${REPO_REF}"
		printf 'version=%s\n' "${version}"
		printf 'prepared_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
	} >"${metadata_path}"
}

render_config() {
	local artifacts_dir peers_json mining_override profile_override
	artifacts_dir="${STAGE_DIR}/.artifacts"
	mkdir -p "${artifacts_dir}"
	if [[ "${#PEERS[@]}" -gt 0 ]]; then
		peers_json="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' "${PEERS[@]}")"
	else
		peers_json=""
	fi
	mining_override="${MINING_MODE}"
	profile_override="${PROFILE}"

	python3 - "${CONFIG_PATH}" "${artifacts_dir}/config.json" "${DATA_DIR}" "${LOG_DIR}" "${STAGE_DIR}" "${profile_override}" "${mining_override}" "${peers_json}" <<'PY'
import json
import os
import sys

config_path, out_path, data_dir, log_dir, stage_dir, profile_override, mining_override, peers_json = sys.argv[1:9]
current = {}
if os.path.exists(config_path):
    with open(config_path, "r", encoding="utf-8") as fh:
        current = json.load(fh)

def keep(key, default):
    return current.get(key, default)

profile = profile_override or keep("profile", "regtest")
import secrets

rpc_token = keep("rpc_auth_token", secrets.token_hex(32))
miner_keyhash = keep("miner_keyhash_hex", secrets.token_hex(32))
if mining_override == "on":
    miner_enabled = True
elif mining_override == "off":
    miner_enabled = False
else:
    miner_enabled = bool(keep("miner_enabled", True))

if peers_json:
    peers = json.loads(peers_json)
else:
    peers = keep("peers", [])

cfg = {
    "profile": profile,
    "db_path": keep("db_path", os.path.join(data_dir, "chain")),
    "log_path": keep("log_path", os.path.join(log_dir, "node.log")),
    "log_level": keep("log_level", "info"),
    "rpc_addr": keep("rpc_addr", "0.0.0.0:18443"),
    "rpc_auth_token": rpc_token,
    "rpc_read_timeout_ms": keep("rpc_read_timeout_ms", 5000),
    "rpc_write_timeout_ms": keep("rpc_write_timeout_ms", 5000),
    "rpc_header_timeout_ms": keep("rpc_header_timeout_ms", 2000),
    "rpc_idle_timeout_ms": keep("rpc_idle_timeout_ms", 30000),
    "rpc_max_header_bytes": keep("rpc_max_header_bytes", 8192),
    "rpc_max_body_bytes": keep("rpc_max_body_bytes", 1048576),
    "p2p_addr": keep("p2p_addr", "0.0.0.0:18444"),
    "peers": peers,
    "max_inbound_peers": keep("max_inbound_peers", 32),
    "max_outbound_peers": keep("max_outbound_peers", 8),
    "handshake_timeout_ms": keep("handshake_timeout_ms", 5000),
    "stall_timeout_ms": keep("stall_timeout_ms", 15000),
    "max_message_bytes": keep("max_message_bytes", 8388608),
    "min_relay_fee_per_byte": keep("min_relay_fee_per_byte", 1),
    "miner_enabled": miner_enabled,
    "miner_workers": keep("miner_workers", 0),
    "miner_keyhash_hex": miner_keyhash,
    "genesis_fixture": os.path.join(stage_dir, "fixtures", "genesis", f"{profile}.json"),
}

os.makedirs(os.path.dirname(out_path), exist_ok=True)
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump(cfg, fh, indent=2)
    fh.write("\n")
PY
	chmod 600 "${artifacts_dir}/config.json"
}

render_unit() {
	local artifacts_dir
	artifacts_dir="${STAGE_DIR}/.artifacts"
	cat >"${artifacts_dir}/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Bitcoin Pure Node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${CURRENT_LINK}
ExecStart=${CURRENT_LINK}/bin/bpu-cli serve --config ${CONFIG_PATH}
Restart=always
RestartSec=2
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
}

build_binary() {
	log "building bpu-cli"
	mkdir -p "${STAGE_DIR}/bin"
	(
		cd "${STAGE_DIR}"
		go build -o "${STAGE_DIR}/bin/bpu-cli" ./cmd/bpu-cli
	)
	chmod 755 "${STAGE_DIR}/bin/bpu-cli"
}

verify_stage() {
	[[ -f "${STAGE_DIR}/go.mod" ]] || fail "staged release is missing go.mod"
	[[ -d "${STAGE_DIR}/fixtures/genesis" ]] || fail "staged release is missing fixtures/genesis"
	[[ -f "${STAGE_DIR}/cmd/bpu-cli/main.go" ]] || fail "staged release is missing cmd/bpu-cli"
	[[ -f "${STAGE_DIR}/version.json" ]] || fail "staged release is missing version.json"
}

parse_args() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--mode)
			MODE="$2"
			shift 2
			;;
		--stage-dir)
			STAGE_DIR="$2"
			shift 2
			;;
		--current-link)
			CURRENT_LINK="$2"
			shift 2
			;;
		--config-path)
			CONFIG_PATH="$2"
			shift 2
			;;
		--data-dir)
			DATA_DIR="$2"
			shift 2
			;;
		--log-dir)
			LOG_DIR="$2"
			shift 2
			;;
		--service-name)
			SERVICE_NAME="$2"
			shift 2
			;;
		--repo-url)
			REPO_URL="$2"
			shift 2
			;;
		--ref)
			REPO_REF="$2"
			shift 2
			;;
		--mining)
			MINING_MODE="$2"
			shift 2
			;;
		--profile)
			PROFILE="$2"
			shift 2
			;;
		--peer)
			PEERS+=("$2")
			shift 2
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			fail "unknown argument: $1"
			;;
		esac
	done
}

main() {
	parse_args "$@"
	[[ -n "${MODE}" ]] || fail "--mode is required"
	[[ -n "${STAGE_DIR}" ]] || fail "--stage-dir is required"
	[[ -n "${CURRENT_LINK}" ]] || fail "--current-link is required"
	[[ -n "${CONFIG_PATH}" ]] || fail "--config-path is required"
	[[ -n "${SERVICE_NAME}" ]] || fail "--service-name is required"
	verify_stage
	ensure_packages
	install_go
	render_config
	render_unit
	build_binary
	render_metadata
	log "stage prepared successfully"
}

main "$@"
