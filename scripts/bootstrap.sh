#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/opt/bitcoin-pure"
CURRENT_LINK="${APP_ROOT}/current"
RELEASES_DIR="${APP_ROOT}/releases"
CONFIG_DIR="/etc/bitcoin-pure"
CONFIG_PATH="${CONFIG_DIR}/config.json"
DATA_DIR="/var/lib/bitcoin-pure"
LOG_DIR="/var/log/bitcoin-pure"
BIN_LINK="/usr/local/bin/bpu-cli"
SERVICE_NAME="bitcoin-pure"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
LOCK_PATH="/var/lock/${SERVICE_NAME}-install.lock"

MODE="install"
SOURCE_ROOT=""
REPO_URL=""
REPO_REF=""
MINING_MODE=""
PROFILE=""
declare -a PEERS=()

STAGE_DIR=""
BACKUP_DIR=""
PREVIOUS_RELEASE=""
SERVICE_WAS_ACTIVE=0
ROLLBACK_NEEDED=0

usage() {
	cat <<'EOF'
Usage: ./install [--update] [--repo-url URL] [--ref REF] [--mining on|off] [--profile regtest|mainnet] [--peer host:port]

Default mode installs from the current checkout.

Options:
  --update         Fetch a fresh checkout from the configured Git remote and deploy it atomically
  --repo-url URL   Git remote to use for --update (otherwise uses the stored origin URL)
  --ref REF        Branch, tag, or ref to deploy during --update
  --mining MODE    Override miner_enabled in config with on or off
  --profile NAME   Override chain profile in config
  --peer HOST:PORT Add/replace configured peers
EOF
}

log() {
	printf '[%s] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" "$*"
}

fail() {
	log "fatal: $*"
	exit 1
}

require_root() {
	[[ "$(id -u)" -eq 0 ]] || fail "run as root"
}

looks_like_ubuntu() {
	[[ -f /etc/os-release ]] || return 1
	if grep -qi '^ID=ubuntu$' /etc/os-release; then
		return 0
	fi
	grep -qi '^ID_LIKE=.*ubuntu' /etc/os-release
}

require_command() {
	command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

acquire_lock() {
	mkdir -p "$(dirname "${LOCK_PATH}")"
	exec 9>"${LOCK_PATH}"
	if ! flock -n 9; then
		fail "another install/update is already running"
	fi
}

metadata_value() {
	local file key
	file="$1"
	key="$2"
	[[ -f "${file}" ]] || return 0
	sed -n "s/^${key}=//p" "${file}" | head -n 1
}

resolve_repo_url() {
	if [[ -n "${REPO_URL}" ]]; then
		return
	fi
	if [[ -n "${SOURCE_ROOT}" && -d "${SOURCE_ROOT}/.git" ]]; then
		REPO_URL="$(git -C "${SOURCE_ROOT}" remote get-url origin 2>/dev/null || true)"
	fi
	if [[ -z "${REPO_URL}" && -f "${CURRENT_LINK}/.bpu-release.env" ]]; then
		REPO_URL="$(metadata_value "${CURRENT_LINK}/.bpu-release.env" "repo_url")"
	fi
	if [[ -z "${REPO_URL}" && -d "${CURRENT_LINK}/.git" ]]; then
		REPO_URL="$(git -C "${CURRENT_LINK}" remote get-url origin 2>/dev/null || true)"
	fi
	[[ -n "${REPO_URL}" ]] || fail "--update requires --repo-url or a previously stored origin URL"
}

stage_checkout() {
	mkdir -p "${RELEASES_DIR}"
	STAGE_DIR="${RELEASES_DIR}/release-$(date -u '+%Y%m%d%H%M%S')-$$"
	log "staging release in ${STAGE_DIR}"
	mkdir -p "${STAGE_DIR}"
	if [[ "${MODE}" == "install" ]]; then
		[[ -n "${SOURCE_ROOT}" ]] || fail "install mode requires --source"
		[[ -f "${SOURCE_ROOT}/go.mod" ]] || fail "source checkout is missing go.mod"
		log "copying local checkout from ${SOURCE_ROOT}"
		(
			cd "${SOURCE_ROOT}"
			tar --exclude='.git' --exclude='.codex-tmp' -cf - .
		) | (
			cd "${STAGE_DIR}"
			tar -xf -
		)
	else
		resolve_repo_url
		log "cloning ${REPO_URL}"
		rm -rf "${STAGE_DIR}"
		if [[ -n "${REPO_REF}" ]]; then
			git clone --depth 1 "${REPO_URL}" "${STAGE_DIR}"
			git -C "${STAGE_DIR}" fetch --depth 1 origin "${REPO_REF}"
			git -C "${STAGE_DIR}" checkout --detach FETCH_HEAD
		else
			git clone --depth 1 "${REPO_URL}" "${STAGE_DIR}"
		fi
	fi
	[[ -x "${STAGE_DIR}/scripts/update.sh" ]] || chmod +x "${STAGE_DIR}/scripts/update.sh"
	[[ -x "${STAGE_DIR}/scripts/update.sh" ]] || fail "staged release is missing scripts/update.sh"
}

render_peer_args() {
	local peer
	for peer in "${PEERS[@]}"; do
		printf '%s\0' "${peer}"
	done
}

prepare_stage() {
	log "preparing staged release"
	local -a cmd=(
		"${STAGE_DIR}/scripts/update.sh"
		--mode "${MODE}"
		--stage-dir "${STAGE_DIR}"
		--current-link "${CURRENT_LINK}"
		--config-path "${CONFIG_PATH}"
		--data-dir "${DATA_DIR}"
		--log-dir "${LOG_DIR}"
		--service-name "${SERVICE_NAME}"
	)
	if [[ -n "${REPO_URL}" ]]; then
		cmd+=(--repo-url "${REPO_URL}")
	fi
	if [[ -n "${REPO_REF}" ]]; then
		cmd+=(--ref "${REPO_REF}")
	fi
	if [[ -n "${MINING_MODE}" ]]; then
		cmd+=(--mining "${MINING_MODE}")
	fi
	if [[ -n "${PROFILE}" ]]; then
		cmd+=(--profile "${PROFILE}")
	fi
	local peer
	for peer in "${PEERS[@]}"; do
		cmd+=(--peer "${peer}")
	done
	"${cmd[@]}"
}

install_candidate_file() {
	local src dst mode tmp
	src="$1"
	dst="$2"
	mode="$3"
	tmp="${dst}.new"
	install -D -m "${mode}" "${src}" "${tmp}"
	mv -f "${tmp}" "${dst}"
}

backup_live_state() {
	BACKUP_DIR="/var/tmp/${SERVICE_NAME}-rollback-$(date -u '+%Y%m%d%H%M%S')-$$"
	mkdir -p "${BACKUP_DIR}"
	if [[ -L "${CURRENT_LINK}" || -d "${CURRENT_LINK}" ]]; then
		PREVIOUS_RELEASE="$(readlink -f "${CURRENT_LINK}" || true)"
	fi
	if systemctl is-active --quiet "${SERVICE_NAME}.service"; then
		SERVICE_WAS_ACTIVE=1
	fi
	[[ -f "${CONFIG_PATH}" ]] && cp -a "${CONFIG_PATH}" "${BACKUP_DIR}/config.json"
	[[ -f "${UNIT_PATH}" ]] && cp -a "${UNIT_PATH}" "${BACKUP_DIR}/unit.service"
	[[ -e "${BIN_LINK}" ]] && cp -a "${BIN_LINK}" "${BACKUP_DIR}/bpu-cli"
}

switch_current_link() {
	local tmp_link
	tmp_link="${CURRENT_LINK}.new"
	rm -f "${tmp_link}"
	ln -s "${STAGE_DIR}" "${tmp_link}"
	mv -Tf "${tmp_link}" "${CURRENT_LINK}"
}

switch_bin_link() {
	local tmp_link
	tmp_link="${BIN_LINK}.new"
	mkdir -p "$(dirname "${BIN_LINK}")"
	rm -f "${tmp_link}"
	ln -s "${CURRENT_LINK}/bin/bpu-cli" "${tmp_link}"
	mv -Tf "${tmp_link}" "${BIN_LINK}"
}

apply_release() {
	local artifacts_dir
	artifacts_dir="${STAGE_DIR}/.artifacts"
	[[ -x "${STAGE_DIR}/bin/bpu-cli" ]] || fail "staged release binary is missing"
	[[ -f "${artifacts_dir}/config.json" ]] || fail "staged release config is missing"
	[[ -f "${artifacts_dir}/${SERVICE_NAME}.service" ]] || fail "staged release unit file is missing"

	backup_live_state
	ROLLBACK_NEEDED=1
	mkdir -p "${APP_ROOT}" "${CONFIG_DIR}" "${DATA_DIR}" "${LOG_DIR}"

	log "installing staged config"
	install_candidate_file "${artifacts_dir}/config.json" "${CONFIG_PATH}" 600
	log "installing staged service unit"
	install_candidate_file "${artifacts_dir}/${SERVICE_NAME}.service" "${UNIT_PATH}" 644
	log "switching current release"
	switch_current_link
	log "switching command symlink"
	switch_bin_link

	systemctl daemon-reload
	if [[ -f "${BACKUP_DIR}/unit.service" ]]; then
		systemctl enable "${SERVICE_NAME}.service" >/dev/null
		systemctl restart "${SERVICE_NAME}.service"
	else
		systemctl enable --now "${SERVICE_NAME}.service"
	fi
}

wait_for_http() {
	local deadline
	deadline=$((SECONDS + 30))
	while (( SECONDS < deadline )); do
		if curl -fsS -o /dev/null "http://127.0.0.1/"; then
			return 0
		fi
		sleep 1
	done
	return 1
}

read_rpc_token() {
	python3 - "${CONFIG_PATH}" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
print(data.get("rpc_auth_token", ""))
PY
}

wait_for_rpc() {
	local token deadline response
	token="$(read_rpc_token)"
	[[ -n "${token}" ]] || fail "new config does not contain rpc_auth_token"
	deadline=$((SECONDS + 30))
	while (( SECONDS < deadline )); do
		response="$(curl -fsS -H "Authorization: Bearer ${token}" -H 'Content-Type: application/json' --data '{"method":"getinfo","params":{}}' http://127.0.0.1/ || true)"
		if python3 - "${response}" <<'PY'
import json, sys
raw = sys.argv[1]
try:
    payload = json.loads(raw)
except json.JSONDecodeError:
    raise SystemExit(1)
result = payload.get("result") or {}
raise SystemExit(0 if "tip_height" in result else 1)
PY
		then
			return 0
		fi
		sleep 1
	done
	return 1
}

verify_release() {
	log "verifying systemd state"
	systemctl is-active --quiet "${SERVICE_NAME}.service" || fail "service did not become active"
	log "verifying public dashboard"
	wait_for_http || fail "dashboard health check failed"
	log "verifying authenticated rpc"
	wait_for_rpc || fail "rpc health check failed"
}

restore_or_remove() {
	local backup live mode
	backup="$1"
	live="$2"
	mode="$3"
	if [[ -e "${backup}" || -L "${backup}" ]]; then
		install_candidate_file "${backup}" "${live}" "${mode}"
	else
		rm -f "${live}"
	fi
}

rollback_release() {
	log "rolling back failed deployment"
	if [[ -n "${PREVIOUS_RELEASE}" ]]; then
		local tmp_link
		tmp_link="${CURRENT_LINK}.rollback"
		rm -f "${tmp_link}"
		ln -s "${PREVIOUS_RELEASE}" "${tmp_link}"
		mv -Tf "${tmp_link}" "${CURRENT_LINK}"
	else
		rm -f "${CURRENT_LINK}"
	fi
	if [[ -e "${BACKUP_DIR}/bpu-cli" || -L "${BACKUP_DIR}/bpu-cli" ]]; then
		rm -f "${BIN_LINK}"
		cp -a "${BACKUP_DIR}/bpu-cli" "${BIN_LINK}"
	else
		rm -f "${BIN_LINK}"
	fi
	restore_or_remove "${BACKUP_DIR}/config.json" "${CONFIG_PATH}" 600
	restore_or_remove "${BACKUP_DIR}/unit.service" "${UNIT_PATH}" 644
	systemctl daemon-reload
	if [[ "${SERVICE_WAS_ACTIVE}" -eq 1 ]]; then
		systemctl restart "${SERVICE_NAME}.service" || true
	else
		systemctl stop "${SERVICE_NAME}.service" >/dev/null 2>&1 || true
	fi
}

cleanup_old_releases() {
	local current_target
	current_target="$(readlink -f "${CURRENT_LINK}" || true)"
	mapfile -t releases < <(find "${RELEASES_DIR}" -mindepth 1 -maxdepth 1 -type d | sort)
	local keep=0
	local release
	for (( idx=${#releases[@]}-1; idx>=0; idx-- )); do
		release="${releases[idx]}"
		if [[ "${release}" == "${current_target}" || "${release}" == "${PREVIOUS_RELEASE}" ]]; then
			continue
		fi
		keep=$((keep + 1))
		if (( keep > 2 )); then
			rm -rf "${release}"
		fi
	done
}

on_exit() {
	local status="$1"
	if [[ "${status}" -ne 0 && "${ROLLBACK_NEEDED}" -eq 1 ]]; then
		rollback_release
	fi
}

parse_args() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--source)
			[[ $# -ge 2 ]] || fail "--source requires a path"
			SOURCE_ROOT="$2"
			shift 2
			;;
		--update)
			MODE="update"
			shift
			;;
		--repo-url)
			[[ $# -ge 2 ]] || fail "--repo-url requires a value"
			REPO_URL="$2"
			shift 2
			;;
		--ref)
			[[ $# -ge 2 ]] || fail "--ref requires a value"
			REPO_REF="$2"
			shift 2
			;;
		--mining)
			[[ $# -ge 2 ]] || fail "--mining requires on or off"
			case "$2" in
			on|off)
				MINING_MODE="$2"
				shift 2
				;;
			*)
				fail "--mining must be on or off"
				;;
			esac
			;;
		--profile)
			[[ $# -ge 2 ]] || fail "--profile requires a value"
			case "$2" in
			regtest|mainnet)
				PROFILE="$2"
				shift 2
				;;
			*)
				fail "--profile must be regtest or mainnet"
				;;
			esac
			;;
		--peer)
			[[ $# -ge 2 ]] || fail "--peer requires host:port"
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
	require_root
	looks_like_ubuntu || fail "Ubuntu is required for ./install"
	require_command git
	require_command python3
	require_command systemctl
	require_command curl
	acquire_lock
	stage_checkout
	prepare_stage
	apply_release
	verify_release
	ROLLBACK_NEEDED=0
	cleanup_old_releases
	log "deployment complete"
	log "current release: ${STAGE_DIR}"
	log "config: ${CONFIG_PATH}"
	log "service: ${SERVICE_NAME}.service"
}

trap 'on_exit $?' EXIT
main "$@"
