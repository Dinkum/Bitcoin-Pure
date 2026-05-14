#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/opt/bitcoin-pure"
CURRENT_LINK="${APP_ROOT}/current"
RELEASES_DIR="${APP_ROOT}/releases"
CONFIG_DIR="/etc/bitcoin-pure"
CONFIG_PATH="${CONFIG_DIR}/config.yaml"
LEGACY_CONFIG_PATH="${CONFIG_DIR}/config.json"
DATA_DIR="/var/lib/bitcoin-pure"
LOG_DIR="/var/log/bitcoin-pure"
BIN_LINK="/usr/local/bin/bpu-cli"
SERVICE_NAME="bitcoin-pure"
SERVICE_USER="bitcoin-pure"
SERVICE_GROUP="bitcoin-pure"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
LOCK_PATH="/var/lock/${SERVICE_NAME}-install.lock"
MOTD_PATH="/etc/update-motd.d/95-bitcoin-pure"

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
KEEP_STAGE_DIR=0
DEPLOY_RESULT=""

usage() {
	cat <<'EOF'
Usage: ./install [--update] [--repo-url URL] [--ref REF] [--mining on|off] [--profile regtest|regtest_medium|regtest_hard|mainnet] [--peer host:port]

No flags are required for a normal Ubuntu install from the current checkout.
The installer keeps existing config where possible and uses sane defaults otherwise.

Options:
  --update         Fetch the latest non-rc tagged release from the configured Git remote and deploy it atomically
  --repo-url URL   Git remote to use for --update (otherwise uses the stored origin URL)
  --ref REF        Release tag to deploy during --update; release candidates are rejected
  --mining MODE    Override miner_enabled in config with on or off; enabling auto-provisions a miner wallet when needed
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

ensure_service_account() {
	log "ensuring ${SERVICE_USER} service account"
	if ! getent group "${SERVICE_GROUP}" >/dev/null; then
		groupadd --system "${SERVICE_GROUP}"
	fi
	if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
		useradd --system --gid "${SERVICE_GROUP}" --home-dir "${DATA_DIR}" --shell /usr/sbin/nologin "${SERVICE_USER}"
	fi
	log "service account ready"
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

miner_wallet_marker_value() {
	local key
	key="$1"
	metadata_value "${CURRENT_LINK}/.artifacts/miner-wallet-provisioned" "${key}"
}

resolve_source_repo_url() {
	if [[ -n "${REPO_URL}" ]]; then
		return
	fi
	if [[ -n "${SOURCE_ROOT}" ]] && git -C "${SOURCE_ROOT}" rev-parse --show-toplevel >/dev/null 2>&1; then
		REPO_URL="$(git -C "${SOURCE_ROOT}" remote get-url origin 2>/dev/null || true)"
	fi
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

normalize_release_tag() {
	local tag
	tag="$1"
	tag="${tag#refs/tags/}"
	tag="${tag%^{}}"
	printf '%s' "${tag}"
}

tag_is_release_candidate() {
	local tag
	tag="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
	[[ "${tag}" == *rc* ]]
}

tag_exists_on_remote() {
	local tag
	tag="$1"
	git ls-remote --exit-code --tags --refs "${REPO_URL}" "refs/tags/${tag}" >/dev/null 2>&1
}

tag_matches_version_json() {
	local tag tmp_dir version
	tag="$1"
	tmp_dir="$(mktemp -d "/tmp/${SERVICE_NAME}-tag-check.XXXXXX")"
	if ! git -C "${tmp_dir}" init -q; then
		rm -rf "${tmp_dir}"
		return 1
	fi
	if ! git -C "${tmp_dir}" remote add origin "${REPO_URL}"; then
		rm -rf "${tmp_dir}"
		return 1
	fi
	if ! git -C "${tmp_dir}" fetch -q --depth 1 origin "refs/tags/${tag}"; then
		rm -rf "${tmp_dir}"
		return 1
	fi
	if ! git -C "${tmp_dir}" checkout -q --detach FETCH_HEAD; then
		rm -rf "${tmp_dir}"
		return 1
	fi
	if [[ ! -f "${tmp_dir}/version.json" ]]; then
		rm -rf "${tmp_dir}"
		return 1
	fi
	version="$(python3 - "${tmp_dir}/version.json" <<'PY' 2>/dev/null || true
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    print(str(json.load(fh).get("version", "")).strip())
PY
)"
	rm -rf "${tmp_dir}"
	[[ -n "${version}" && "${tag}" == "v${version}" ]]
}

resolve_update_tag() {
	local tag candidate
	resolve_repo_url
	if [[ -n "${REPO_REF}" ]]; then
		tag="$(normalize_release_tag "${REPO_REF}")"
		[[ -n "${tag}" ]] || fail "--ref must name a release tag"
		if tag_is_release_candidate "${tag}"; then
			fail "--ref ${tag} is a release candidate; use a final release tag"
		fi
		tag_exists_on_remote "${tag}" || fail "--ref ${tag} is not a tag on ${REPO_URL}"
		tag_matches_version_json "${tag}" || fail "--ref ${tag} is not a Bitcoin Pure release tag"
		REPO_REF="${tag}"
		log "using tagged release ${REPO_REF}"
		return
	fi
	while IFS= read -r candidate; do
		tag="$(normalize_release_tag "${candidate}")"
		[[ -n "${tag}" ]] || continue
		if tag_is_release_candidate "${tag}"; then
			continue
		fi
		if tag_matches_version_json "${tag}"; then
			REPO_REF="${tag}"
			log "resolved latest tagged release ${REPO_REF}"
			return
		fi
	done < <(git ls-remote --tags --refs "${REPO_URL}" 'refs/tags/v*' | awk '{print $2}' | sed 's#^refs/tags/##' | LC_ALL=C sort -Vr)
	fail "no final Bitcoin Pure release tag found on ${REPO_URL}"
}

stage_checkout() {
	mkdir -p "${RELEASES_DIR}"
	STAGE_DIR="${RELEASES_DIR}/release-$(date -u '+%Y%m%d%H%M%S')-$$"
	log "staging release in ${STAGE_DIR}"
	mkdir -p "${STAGE_DIR}"
	if [[ "${MODE}" == "install" ]]; then
		[[ -n "${SOURCE_ROOT}" ]] || fail "install mode requires --source"
		[[ -f "${SOURCE_ROOT}/go.mod" ]] || fail "source checkout is missing go.mod"
		resolve_source_repo_url
		log "copying local checkout from ${SOURCE_ROOT}"
		if git -C "${SOURCE_ROOT}" rev-parse --show-toplevel >/dev/null 2>&1; then
			log "copying repository files from ${SOURCE_ROOT}"
			(
				cd "${SOURCE_ROOT}"
				git ls-files --cached --others --exclude-standard -z | tar --null -T - -cf -
			) | (
				cd "${STAGE_DIR}"
				tar -xf -
			)
		else
			log "copying source tree from ${SOURCE_ROOT}"
			(
				cd "${SOURCE_ROOT}"
				tar \
					--exclude='.git' \
					--exclude='.gocache' \
					--exclude='.gopath' \
					--exclude='.DS_Store' \
					--exclude='REFERENCE_NODES' \
					--exclude='Works' \
					-cf - .
			) | (
				cd "${STAGE_DIR}"
				tar -xf -
			)
		fi
	else
		resolve_update_tag
		log "cloning ${REPO_URL} at ${REPO_REF}"
		rm -rf "${STAGE_DIR}"
		git clone --depth 1 --branch "${REPO_REF}" "${REPO_URL}" "${STAGE_DIR}"
		git -C "${STAGE_DIR}" checkout --detach "${REPO_REF}"
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
		--legacy-config-path "${LEGACY_CONFIG_PATH}"
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
	local src dst mode owner group tmp
	src="$1"
	dst="$2"
	mode="$3"
	owner="${4:-}"
	group="${5:-}"
	tmp="${dst}.new"
	install -D -m "${mode}" "${src}" "${tmp}"
	if [[ -n "${owner}" || -n "${group}" ]]; then
		chown "${owner:-root}:${group:-root}" "${tmp}"
	fi
	mv -f "${tmp}" "${dst}"
}

files_match() {
	local left right
	left="$1"
	right="$2"
	[[ -f "${left}" && -f "${right}" ]] || return 1
	cmp -s "${left}" "${right}"
}

release_is_unchanged() {
	local artifacts_dir
	artifacts_dir="${STAGE_DIR}/.artifacts"
	[[ -x "${CURRENT_LINK}/bin/bpu-cli" ]] || return 1
	files_match "${artifacts_dir}/config.yaml" "${CONFIG_PATH}" || return 1
	files_match "${artifacts_dir}/config.json" "${LEGACY_CONFIG_PATH}" || return 1
	files_match "${artifacts_dir}/${SERVICE_NAME}.service" "${UNIT_PATH}" || return 1
	files_match "${artifacts_dir}/${SERVICE_NAME}.motd" "${MOTD_PATH}" || return 1
	files_match "${STAGE_DIR}/bin/bpu-cli" "${CURRENT_LINK}/bin/bpu-cli" || return 1
	return 0
}

refresh_live_metadata() {
	[[ -f "${STAGE_DIR}/.bpu-release.env" ]] || return 0
	[[ -d "${CURRENT_LINK}" ]] || return 0
	if files_match "${STAGE_DIR}/.bpu-release.env" "${CURRENT_LINK}/.bpu-release.env"; then
		return 0
	fi
	log "refreshing live release metadata"
	install_candidate_file "${STAGE_DIR}/.bpu-release.env" "${CURRENT_LINK}/.bpu-release.env" 644 root root
}

backup_live_state() {
	BACKUP_DIR="/var/tmp/${SERVICE_NAME}-rollback-$(date -u '+%Y%m%d%H%M%S')-$$"
	log "saving rollback snapshot in ${BACKUP_DIR}"
	mkdir -p "${BACKUP_DIR}"
	if [[ -L "${CURRENT_LINK}" || -d "${CURRENT_LINK}" ]]; then
		PREVIOUS_RELEASE="$(readlink -f "${CURRENT_LINK}" || true)"
	fi
	if systemctl is-active --quiet "${SERVICE_NAME}.service"; then
		SERVICE_WAS_ACTIVE=1
	fi
	# Fresh installs have no live state yet. Keep the backup step a no-op in
	# that case so `set -e` does not abort the deployment before apply_release.
	if [[ -f "${CONFIG_PATH}" ]]; then
		cp -a "${CONFIG_PATH}" "${BACKUP_DIR}/config.yaml"
	fi
	if [[ -f "${LEGACY_CONFIG_PATH}" ]]; then
		cp -a "${LEGACY_CONFIG_PATH}" "${BACKUP_DIR}/config.json"
	fi
	if [[ -f "${UNIT_PATH}" ]]; then
		cp -a "${UNIT_PATH}" "${BACKUP_DIR}/unit.service"
	fi
	if [[ -f "${MOTD_PATH}" ]]; then
		cp -a "${MOTD_PATH}" "${BACKUP_DIR}/motd"
	fi
	if [[ -e "${BIN_LINK}" ]]; then
		cp -a "${BIN_LINK}" "${BACKUP_DIR}/bpu-cli"
	fi
	log "rollback snapshot ready"
	return 0
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

ensure_live_service() {
	systemctl daemon-reload
	systemctl enable "${SERVICE_NAME}.service" >/dev/null
	if ! systemctl is-active --quiet "${SERVICE_NAME}.service"; then
		log "starting existing service"
		systemctl start "${SERVICE_NAME}.service"
	fi
}

chown_wallet_paths() {
	local wallet_path wallet_file count=0
	[[ -f "$1" ]] || return 0
	while IFS= read -r wallet_path; do
		[[ -n "${wallet_path}" ]] || continue
		[[ "${wallet_path}" = /* ]] || fail "staged wallet path must be absolute: ${wallet_path}"
		mkdir -p "${wallet_path}"
		chown "${SERVICE_USER}:${SERVICE_GROUP}" "${wallet_path}"
		wallet_file="${wallet_path}/wallets.json"
		if [[ -e "${wallet_file}" ]]; then
			chown "${SERVICE_USER}:${SERVICE_GROUP}" "${wallet_file}"
		fi
		count=$((count + 1))
	done <"$1"
	if (( count > 0 )); then
		log "wallet storage ready (${count} path(s))"
	fi
}

chown_runtime_paths() {
	local runtime_path count=0
	[[ -f "$1" ]] || return 0
	log "preparing service runtime directories"
	while IFS= read -r runtime_path; do
		[[ -n "${runtime_path}" ]] || continue
		[[ "${runtime_path}" = /* ]] || fail "staged runtime path must be absolute: ${runtime_path}"
		# systemd requires ReadWritePaths targets to exist before it creates
		# the service mount namespace.
		mkdir -p "${runtime_path}"
		chown "${SERVICE_USER}:${SERVICE_GROUP}" "${runtime_path}"
		count=$((count + 1))
	done <"$1"
	log "service runtime directories ready (${count} path(s))"
}

apply_release() {
	local artifacts_dir
	artifacts_dir="${STAGE_DIR}/.artifacts"
	[[ -x "${STAGE_DIR}/bin/bpu-cli" ]] || fail "staged release binary is missing"
	[[ -f "${artifacts_dir}/config.yaml" ]] || fail "staged release config is missing"
	[[ -f "${artifacts_dir}/config.json" ]] || fail "staged release legacy config sidecar is missing"
	[[ -f "${artifacts_dir}/${SERVICE_NAME}.service" ]] || fail "staged release unit file is missing"
	[[ -f "${artifacts_dir}/${SERVICE_NAME}.motd" ]] || fail "staged release motd helper is missing"

	mkdir -p "${APP_ROOT}" "${CONFIG_DIR}" "${DATA_DIR}" "${LOG_DIR}"
	chown "${SERVICE_USER}:${SERVICE_GROUP}" "${DATA_DIR}" "${LOG_DIR}"
	chown_runtime_paths "${artifacts_dir}/runtime-paths"
	chown_wallet_paths "${artifacts_dir}/wallet-paths"

	if release_is_unchanged; then
		log "staged release matches the live install; leaving binaries and config in place"
		refresh_live_metadata
		ensure_live_service
		DEPLOY_RESULT="unchanged"
		return
	fi

	backup_live_state
	ROLLBACK_NEEDED=1

	log "installing staged config"
	install_candidate_file "${artifacts_dir}/config.yaml" "${CONFIG_PATH}" 640 root "${SERVICE_GROUP}"
	install_candidate_file "${artifacts_dir}/config.json" "${LEGACY_CONFIG_PATH}" 640 root "${SERVICE_GROUP}"
	log "installing staged service unit"
	install_candidate_file "${artifacts_dir}/${SERVICE_NAME}.service" "${UNIT_PATH}" 644 root root
	log "installing ssh monitor banner"
	install_candidate_file "${artifacts_dir}/${SERVICE_NAME}.motd" "${MOTD_PATH}" 755 root root
	log "activating staged release"
	switch_current_link
	log "refreshing bpu-cli command symlink"
	switch_bin_link

	log "reloading systemd"
	systemctl daemon-reload
	if [[ -f "${BACKUP_DIR}/unit.service" ]]; then
		log "restarting ${SERVICE_NAME}.service"
		systemctl enable "${SERVICE_NAME}.service" >/dev/null
		systemctl restart "${SERVICE_NAME}.service"
	else
		log "enabling and starting ${SERVICE_NAME}.service"
		systemctl enable --now "${SERVICE_NAME}.service"
	fi
	KEEP_STAGE_DIR=1
	if [[ "${MODE}" == "update" ]]; then
		DEPLOY_RESULT="updated"
	else
		DEPLOY_RESULT="installed"
	fi
}

rpc_addr_value() {
	python3 - "${LEGACY_CONFIG_PATH}" <<'PY'
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    print(json.load(fh).get("rpc_addr", ""))
PY
}

loopback_http_base() {
	local addr host port
	addr="$(rpc_addr_value)"
	[[ -n "${addr}" ]] || fail "config is missing rpc_addr"
	if [[ "${addr}" =~ ^\[(.*)\]:(.+)$ ]]; then
		host="${BASH_REMATCH[1]}"
		port="${BASH_REMATCH[2]}"
	else
		host="${addr%:*}"
		port="${addr##*:}"
	fi
	if [[ -z "${port}" || "${port}" == "${addr}" ]]; then
		fail "unable to parse rpc_addr: ${addr}"
	fi
	case "${host}" in
		""|0.0.0.0)
			host="127.0.0.1"
			;;
		::|\[::\])
			host="[::1]"
			;;
	esac
	printf 'http://%s:%s' "${host}" "${port}"
}

wait_for_http() {
	local deadline base
	base="$(loopback_http_base)"
	deadline=$((SECONDS + 30))
	while (( SECONDS < deadline )); do
		if curl -fs -o /dev/null "${base}/"; then
			return 0
		fi
		sleep 1
	done
	return 1
}

read_rpc_token() {
	python3 - "${LEGACY_CONFIG_PATH}" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
print(data.get("rpc_auth_token", ""))
PY
}

wait_for_rpc() {
	local token deadline response base
	token="$(read_rpc_token)"
	[[ -n "${token}" ]] || fail "new config does not contain rpc_auth_token"
	base="$(loopback_http_base)"
	deadline=$((SECONDS + 30))
	while (( SECONDS < deadline )); do
		response="$(curl -fs -H "Authorization: Bearer ${token}" -H 'Content-Type: application/json' --data '{"method":"getinfo","params":{}}' "${base}/" || true)"
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

print_service_diagnostics() {
	log "service status follows"
	systemctl status "${SERVICE_NAME}.service" --no-pager -l || true
	log "recent service journal follows"
	journalctl -u "${SERVICE_NAME}.service" --no-pager -n 80 || true
}

verify_release() {
	log "verifying systemd state"
	if ! systemctl is-active --quiet "${SERVICE_NAME}.service"; then
		print_service_diagnostics
		fail "service did not become active"
	fi
	log "systemd service is active"
	log "verifying public dashboard"
	if ! wait_for_http; then
		print_service_diagnostics
		fail "dashboard health check failed"
	fi
	log "public dashboard responded"
	log "verifying authenticated rpc"
	if ! wait_for_rpc; then
		print_service_diagnostics
		fail "rpc health check failed"
	fi
	log "authenticated rpc responded"
}

discover_public_ip() {
	local ip
	ip="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '/src/ {for (i = 1; i <= NF; i++) if ($i == "src") {print $(i + 1); exit}}')"
	if [[ -z "${ip}" ]]; then
		ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
	fi
	printf '%s' "${ip}"
}

ssh_tunnel_user() {
	if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
		printf '%s' "${SUDO_USER}"
		return
	fi
	id -un 2>/dev/null || printf 'root'
}

summary_border() {
	printf '+======================================================================+\n'
}

summary_line() {
	local text width
	text="${1:-}"
	width=69
	while (( ${#text} > width )); do
		printf '| %-69s|\n' "${text:0:width}"
		text="${text:width}"
	done
	printf '| %-69s|\n' "${text}"
}

summary_blank() {
	summary_line ""
}

print_install_summary() {
	local version rpc_addr p2p_addr profile miner_enabled miner_workers service_state monitor_local monitor_public public_ip current_path rpc_host rpc_port tunnel_user tunnel_target miner_wallet_dir miner_receive_address
	local -a config_lines
	version="$(metadata_value "${CURRENT_LINK}/.bpu-release.env" "version")"
	mapfile -t config_lines < <(python3 - "${LEGACY_CONFIG_PATH}" <<'PY'
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as fh:
    cfg = json.load(fh)
print(cfg.get("rpc_addr", ""))
print(cfg.get("p2p_addr", ""))
print(cfg.get("profile", ""))
print("on" if cfg.get("miner_enabled", False) else "off")
workers = cfg.get("miner_workers", 0)
print(str(workers) if workers else "auto")
PY
	)
	rpc_addr="${config_lines[0]:-}"
	p2p_addr="${config_lines[1]:-}"
	profile="${config_lines[2]:-}"
	miner_enabled="${config_lines[3]:-}"
	miner_workers="${config_lines[4]:-}"
	service_state="$(systemctl is-active "${SERVICE_NAME}.service" 2>/dev/null || true)"
	monitor_local="$(loopback_http_base)/"
	public_ip="$(discover_public_ip)"
	monitor_public=""
	rpc_host="${rpc_addr%:*}"
	rpc_port="${rpc_addr##*:}"
	if [[ -n "${public_ip}" && "${rpc_host}" != "127.0.0.1" && "${rpc_host}" != "[::1]" && "${rpc_host}" != "::1" && "${rpc_host}" != "localhost" ]]; then
		monitor_public="http://${public_ip}:${rpc_port}/"
	fi
	tunnel_user="$(ssh_tunnel_user)"
	tunnel_target="${tunnel_user}@${public_ip:-$(hostname -f 2>/dev/null || hostname)}"
	current_path="$(readlink -f "${CURRENT_LINK}" || true)"
	miner_wallet_dir="$(miner_wallet_marker_value "wallet_dir")"
	miner_receive_address="$(miner_wallet_marker_value "receive_address")"

	printf '\n'
	summary_border
	summary_line "Bitcoin Pure install summary"
	summary_border
	summary_line "Result   : ${DEPLOY_RESULT:-complete}"
	summary_line "Version  : ${version:-unknown}"
	summary_line "Service  : ${SERVICE_NAME}.service (${service_state:-unknown})"
	summary_line "Profile  : ${profile:-unknown}"
	summary_line "Mining   : ${miner_enabled:-unknown} (workers: ${miner_workers:-unknown})"
	summary_line "RPC      : ${rpc_addr:-unknown}"
	summary_line "P2P      : ${p2p_addr:-unknown}"
	summary_line "Config   : ${CONFIG_PATH}"
	summary_line "Data     : ${DATA_DIR}"
	summary_line "Release  : ${current_path:-${CURRENT_LINK}}"
	summary_border
	summary_line "Monitor  : ${monitor_local}"
	if [[ -n "${monitor_public}" ]]; then
		summary_line "Public   : ${monitor_public}"
	fi
	summary_border
	summary_line "Next steps"
	summary_blank
	summary_line "Check if the service is healthy:"
	summary_line "  systemctl status ${SERVICE_NAME} --no-pager"
	summary_blank
	summary_line "Watch live node logs:"
	summary_line "  journalctl -u ${SERVICE_NAME} -f"
	summary_blank
	summary_line "Open the monitor from your laptop with an SSH tunnel:"
	summary_line "  ssh -L ${rpc_port}:127.0.0.1:${rpc_port} ${tunnel_target}"
	summary_line "  then open ${monitor_local}"
	summary_blank
	summary_line "Check the node status via CLI:"
	summary_line "  bpu-cli status"
	if [[ "${miner_enabled}" != "on" ]]; then
		summary_blank
		summary_line "Enable mining:"
		summary_line "  sudo bpu-cli config mining on"
		summary_line "  sudo systemctl restart ${SERVICE_NAME}"
	fi
	if [[ -n "${miner_wallet_dir}" ]]; then
		summary_blank
		summary_line "Miner wallet was provisioned. Back it up now:"
		summary_line "  sudo bpu-cli wallet backup --config ${CONFIG_PATH} --out /root/bitcoin-pure-wallets.backup.json"
		summary_line "Keep the backup private; it can spend mined rewards."
		if [[ -n "${miner_receive_address}" ]]; then
			summary_line "Miner receive address: ${miner_receive_address}"
		fi
	fi
	summary_border
	printf '\n'
}

restore_or_remove() {
	local backup live mode owner group
	backup="$1"
	live="$2"
	mode="$3"
	owner="${4:-}"
	group="${5:-}"
	if [[ -e "${backup}" || -L "${backup}" ]]; then
		install_candidate_file "${backup}" "${live}" "${mode}" "${owner}" "${group}"
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
	restore_or_remove "${BACKUP_DIR}/config.yaml" "${CONFIG_PATH}" 640 root "${SERVICE_GROUP}"
	restore_or_remove "${BACKUP_DIR}/config.json" "${LEGACY_CONFIG_PATH}" 640 root "${SERVICE_GROUP}"
	restore_or_remove "${BACKUP_DIR}/unit.service" "${UNIT_PATH}" 644 root root
	restore_or_remove "${BACKUP_DIR}/motd" "${MOTD_PATH}" 755 root root
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

cleanup_stage_dir() {
	local current_target
	[[ -n "${STAGE_DIR}" && -d "${STAGE_DIR}" ]] || return 0
	current_target="$(readlink -f "${CURRENT_LINK}" || true)"
	if [[ "${KEEP_STAGE_DIR}" -eq 1 && "${STAGE_DIR}" == "${current_target}" ]]; then
		return 0
	fi
	rm -rf "${STAGE_DIR}"
}

on_exit() {
	local status="$1"
	if [[ "${status}" -ne 0 && "${ROLLBACK_NEEDED}" -eq 1 ]]; then
		rollback_release
	fi
	if [[ "${status}" -ne 0 ]]; then
		cleanup_stage_dir
		return
	fi
	cleanup_stage_dir
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
			regtest|regtest_medium|regtest_hard|mainnet)
				PROFILE="$2"
				shift 2
				;;
			*)
				fail "--profile must be regtest, regtest_medium, regtest_hard, or mainnet"
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
	log "checking Ubuntu host prerequisites"
	looks_like_ubuntu || fail "Ubuntu is required for ./install"
	require_command git
	require_command python3
	require_command systemctl
	require_command curl
	log "host prerequisites ready"
	acquire_lock
	ensure_service_account
	stage_checkout
	prepare_stage
	apply_release
	verify_release
	ROLLBACK_NEEDED=0
	cleanup_old_releases
	log "deployment complete"
	log "current release: $(readlink -f "${CURRENT_LINK}" || printf '%s' "${CURRENT_LINK}")"
	log "config: ${CONFIG_PATH}"
	log "service: ${SERVICE_NAME}.service"
	print_install_summary
}

trap 'on_exit $?' EXIT
main "$@"
