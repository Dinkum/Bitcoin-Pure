package node

import (
	bpcrypto "bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/types"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"math/big"
	"net"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"
)

func deriveNodeID(cfg ServiceConfig) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	seed := hostname + "\x00" + cfg.DBPath + "\x00" + cfg.RPCAddr + "\x00" + cfg.P2PAddr + "\x00" + cfg.GenesisFixture
	sum := bpcrypto.Sha256d([]byte(seed))
	return strings.ToUpper(hex.EncodeToString(sum[:6]))
}

func blockMinedByPubKey(block *types.Block, pubKey [32]byte) bool {
	if len(block.Txs) == 0 || len(block.Txs[0].Base.Outputs) == 0 {
		return false
	}
	for _, output := range block.Txs[0].Base.Outputs {
		if output.PubKey == pubKey {
			return true
		}
	}
	return false
}

func summarizeFeeSet(fees []uint64) (uint64, uint64, uint64) {
	if len(fees) == 0 {
		return 0, 0, 0
	}
	values := append([]uint64(nil), fees...)
	slices.Sort(values)
	return values[len(values)/2], values[0], values[len(values)-1]
}

func dashboardDifficulty(nBits uint32, powLimitBits uint32) float64 {
	target, ok := compactTargetForDashboard(nBits)
	if !ok || target.Sign() <= 0 {
		return 0
	}
	powLimit, ok := compactTargetForDashboard(powLimitBits)
	if !ok || powLimit.Sign() <= 0 {
		return 0
	}
	ratio := new(big.Rat).SetFrac(powLimit, target)
	value, _ := ratio.Float64()
	return value
}

func compactTargetForDashboard(compact uint32) (*big.Int, bool) {
	size := byte(compact >> 24)
	mantissa := compact & 0x007fffff
	if mantissa == 0 {
		return nil, false
	}
	target := new(big.Int).SetUint64(uint64(mantissa))
	if size <= 3 {
		target.Rsh(target, uint(8*(3-int(size))))
	} else {
		target.Lsh(target, uint(8*(int(size)-3)))
	}
	return target, true
}

func shortHexBytes(hash [32]byte, width int) string {
	return shortHexString(hex.EncodeToString(hash[:]), width)
}

func fullHashString(hash [32]byte) string {
	return hex.EncodeToString(hash[:])
}

func shortUserAgent(ua string) string {
	ua = strings.TrimSpace(ua)
	if ua == "" {
		return "-"
	}
	if len(ua) <= 20 {
		return ua
	}
	return ua[:17] + "..."
}

func shortPeerAddr(addr string) string {
	addr = normalizePeerAddr(addr)
	if addr == "" {
		return "-"
	}
	if len(addr) <= 24 {
		return addr
	}
	return addr[:21] + "..."
}

func formatMaybeTimeAgo(unix int64, now time.Time) string {
	if unix <= 0 {
		return "never"
	}
	return now.Sub(time.Unix(unix, 0)).Round(time.Second).String()
}

func shortHash(hash [32]byte) string {
	return hex.EncodeToString(hash[:4])
}

func shortHexString(raw string, chars int) string {
	if len(raw) <= chars {
		return raw
	}
	if chars <= 3 {
		return raw[:chars]
	}
	return raw[:chars-3] + "..."
}

func shouldServePublicDashboard() bool {
	if runtime.GOOS != "linux" {
		return false
	}
	buf, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return false
	}
	return osReleaseLooksLikeUbuntu(string(buf))
}

func osReleaseLooksLikeUbuntu(raw string) bool {
	info := parseOSRelease(raw)
	id := strings.ToLower(info["ID"])
	if id == "ubuntu" {
		return true
	}
	return strings.Contains(strings.ToLower(info["ID_LIKE"]), "ubuntu")
}

func defaultMinerWorkers() int {
	workers := runtime.NumCPU() / 2
	if workers < 1 {
		return 1
	}
	return workers
}

func parseOSRelease(raw string) map[string]string {
	info := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		info[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), "\"")
	}
	return info
}

func decodeHashHex(raw string) ([32]byte, error) {
	var hash [32]byte
	buf, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return hash, err
	}
	if len(buf) != len(hash) {
		return hash, errors.New("hash must be 32 bytes hex")
	}
	copy(hash[:], buf)
	return hash, nil
}

func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func randomNonce() uint64 {
	var buf [8]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return uint64(buf[0]) |
		uint64(buf[1])<<8 |
		uint64(buf[2])<<16 |
		uint64(buf[3])<<24 |
		uint64(buf[4])<<32 |
		uint64(buf[5])<<40 |
		uint64(buf[6])<<48 |
		uint64(buf[7])<<56
}

func (s *Service) localVersion() p2p.VersionMessage {
	services := p2p.ServiceNodeNetwork | p2p.ServiceErlayTxRelay | p2p.ServiceCompactBlockRelay | p2p.ServiceGrapheneExtended
	if s.avalancheManager().enabled() {
		services |= p2p.ServiceAvalancheOverlay
	}
	return p2p.VersionMessage{
		Protocol:  1,
		Services:  services,
		Height:    s.blockHeight(),
		Nonce:     randomNonce(),
		UserAgent: "bpu/go",
	}
}

func ParseMinerPubKey(raw string) ([32]byte, error) {
	var pubKey [32]byte
	if raw == "" {
		return pubKey, nil
	}
	raw = strings.TrimSpace(raw)
	buf, err := hex.DecodeString(raw)
	if err != nil {
		return pubKey, err
	}
	if len(buf) != 32 {
		return pubKey, errors.New("miner pubkey must be 32 bytes hex")
	}
	copy(pubKey[:], buf)
	if !bpcrypto.IsValidXOnlyPubKey(&pubKey) {
		return pubKey, errors.New("miner pubkey must be a valid x-only secp256k1 public key")
	}
	return pubKey, nil
}
