package node

import (
	"bitcoin-pure/internal/compactfilter"
	"bitcoin-pure/internal/consensus"
	"bitcoin-pure/internal/mempool"
	"bitcoin-pure/internal/p2p"
	"bitcoin-pure/internal/rpcserver"
	"bitcoin-pure/internal/storage"
	"bitcoin-pure/internal/types"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type rpcRequest = rpcserver.Request

type rpcResponse = rpcserver.Response

func (s *Service) handleHTTP(w http.ResponseWriter, r *http.Request) {
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && s.publicPage && s.isPublicDashboardPath(r.URL.Path) {
		s.handlePublicDashboard(w, r)
		return
	}
	if r.Method == http.MethodPost {
		s.handleRPC(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *Service) handleRPC(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	requestID := s.nextRPCRequestID()
	if r.Method != http.MethodPost {
		s.logger.Warn("rpc request rejected",
			slog.String("request_id", requestID),
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("http_method", r.Method),
			slog.Int("status_code", http.StatusMethodNotAllowed),
			slog.Duration("duration", time.Since(started)),
		)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	if !s.authorizeRPC(r) {
		s.logger.Warn("rpc request rejected",
			slog.String("request_id", requestID),
			slog.String("remote_addr", r.RemoteAddr),
			slog.Int("status_code", http.StatusUnauthorized),
			slog.Duration("duration", time.Since(started)),
		)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body := r.Body
	if s.cfg.RPCMaxBodyBytes > 0 {
		body = http.MaxBytesReader(w, r.Body, int64(s.cfg.RPCMaxBodyBytes))
	}
	var req rpcRequest
	decoder := json.NewDecoder(body)
	if err := decoder.Decode(&req); err != nil {
		s.logger.Warn("rpc decode failed",
			slog.String("request_id", requestID),
			slog.String("remote_addr", r.RemoteAddr),
			slog.Int("status_code", http.StatusOK),
			slog.Duration("duration", time.Since(started)),
			slog.Any("error", err),
		)
		s.writeRPCResponse(w, requestID, r.RemoteAddr, "", started, rpcResponse{Error: err.Error()})
		return
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing data after RPC request")
		}
		s.logger.Warn("rpc decode failed",
			slog.String("request_id", requestID),
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("method", req.Method),
			slog.Int("status_code", http.StatusOK),
			slog.Duration("duration", time.Since(started)),
			slog.Any("error", err),
		)
		s.writeRPCResponse(w, requestID, r.RemoteAddr, req.Method, started, rpcResponse{Error: err.Error()})
		return
	}
	s.logger.Debug("rpc request",
		slog.String("request_id", requestID),
		slog.String("remote_addr", r.RemoteAddr),
		slog.String("method", req.Method),
	)
	result, err := s.dispatchRPC(req)
	resp := rpcResponse{Result: result}
	if err != nil {
		if shouldDebugRPCFailure(req.Method, err) {
			s.logger.Debug("rpc request failed",
				slog.String("request_id", requestID),
				slog.String("remote_addr", r.RemoteAddr),
				slog.String("method", req.Method),
				slog.Int("status_code", http.StatusOK),
				slog.Duration("duration", time.Since(started)),
				slog.Any("error", err),
			)
		} else {
			s.logger.Warn("rpc request failed",
				slog.String("request_id", requestID),
				slog.String("remote_addr", r.RemoteAddr),
				slog.String("method", req.Method),
				slog.Int("status_code", http.StatusOK),
				slog.Duration("duration", time.Since(started)),
				slog.Any("error", err),
			)
		}
		resp.Error = err.Error()
		resp.Result = nil
	} else {
		s.logger.Debug("rpc request completed",
			slog.String("request_id", requestID),
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("method", req.Method),
			slog.Int("status_code", http.StatusOK),
			slog.Duration("duration", time.Since(started)),
		)
	}
	s.writeRPCResponse(w, requestID, r.RemoteAddr, req.Method, started, resp)
}

func (s *Service) writeRPCResponse(w http.ResponseWriter, requestID string, remoteAddr string, method string, started time.Time, resp rpcResponse) {
	if err := json.NewEncoder(w).Encode(resp); err != nil && s.logger != nil {
		s.logger.Warn("rpc response write failed",
			slog.String("request_id", requestID),
			slog.String("remote_addr", remoteAddr),
			slog.String("method", method),
			slog.Int("status_code", http.StatusOK),
			slog.Duration("duration", time.Since(started)),
			slog.Any("error", err),
		)
	}
}

func (s *Service) nextRPCRequestID() string {
	if s == nil {
		return "rpc_0"
	}
	return fmt.Sprintf("rpc_%d", s.rpcRequestSeq.Add(1))
}

func shouldDebugRPCFailure(method string, err error) bool {
	if err == nil {
		return false
	}
	return method == "submitblock" && errors.Is(err, ErrBlockAlreadyKnown)
}

type rpcHandler = rpcserver.Handler[Service]

var rpcMethods = rpcserver.Registry[Service]{
	"getinfo":                       rpcNoParams((*Service).rpcGetInfo),
	"getchainstate":                 rpcNoParams((*Service).rpcGetChainState),
	"getmempoolinfo":                rpcNoParams((*Service).rpcGetMempoolInfo),
	"getmininginfo":                 rpcNoParams((*Service).rpcGetMiningInfo),
	"getpeerinfo":                   rpcNoParams((*Service).rpcGetPeerInfo),
	"getavalancheinfo":              rpcNoParams((*Service).rpcGetAvalancheInfo),
	"getmetrics":                    rpcNoParams((*Service).rpcGetMetrics),
	"getheader":                     rpcRequiredParams((*Service).rpcGetHeader),
	"getblockhashbyheight":          rpcRequiredParams((*Service).rpcGetBlockHashByHeight),
	"getblock":                      rpcRequiredParams((*Service).rpcGetBlock),
	"getblockfilter":                rpcRequiredParams((*Service).rpcGetBlockFilter),
	"getfilterheaders":              rpcRequiredParams((*Service).rpcGetFilterHeaders),
	"getfiltercheckpoint":           rpcRequiredParams((*Service).rpcGetFilterCheckpoint),
	"getmempool":                    rpcNoParams((*Service).rpcGetMempool),
	"gettxstatus":                   rpcRequiredParams((*Service).rpcGetTxStatus),
	"getutxosbypubkeys":             rpcRequiredParams((*Service).rpcGetUTXOsByPubKeys),
	"getutxosbywatchitems":          rpcRequiredParams((*Service).rpcGetUTXOsByWatchItems),
	"getutxoproof":                  rpcRequiredParams((*Service).rpcGetUTXOProof),
	"getutxoproofbatch":             rpcRequiredParams((*Service).rpcGetUTXOProofBatch),
	"verifyutxoproof":               rpcRequiredParams((*Service).rpcVerifyUTXOProof),
	"verifyutxoproofbatch":          rpcRequiredParams((*Service).rpcVerifyUTXOProofBatch),
	"getcompactstatepackage":        rpcRequiredParams((*Service).rpcGetCompactStatePackage),
	"verifycompactstatepackage":     rpcRequiredParams((*Service).rpcVerifyCompactStatePackage),
	"getstresslaneinfo":             rpcNoParams((*Service).rpcGetStressLaneInfo),
	"getwalletactivitybypubkeys":    rpcRequiredParams((*Service).rpcGetWalletActivityByPubKeys),
	"getwalletactivitybywatchitems": rpcRequiredParams((*Service).rpcGetWalletActivityByWatchItems),
	"estimatefee":                   rpcOptionalParams((*Service).rpcEstimateFee),
	"submittx":                      rpcRequiredParams((*Service).rpcSubmitTx),
	"submittxbatch":                 rpcRequiredParams((*Service).rpcSubmitTxBatch),
	"submitpackedtxbatch":           rpcRequiredParams((*Service).rpcSubmitPackedTxBatch),
	"mine":                          rpcOptionalParams((*Service).rpcMine),
	"seedstresslanes":               rpcRequiredParams((*Service).rpcSeedStressLanes),
	"submitblock":                   rpcRequiredParams((*Service).rpcSubmitBlock),
	"addpeer":                       rpcRequiredParams((*Service).rpcAddPeer),
	"stop":                          rpcNoParams((*Service).rpcStop),
}

func rpcNoParams[R any](fn func(*Service) (R, error)) rpcHandler {
	return rpcserver.NoParams[Service, R](fn)
}

func rpcRequiredParams[P any, R any](fn func(*Service, P) (R, error)) rpcHandler {
	return rpcserver.RequiredParams[Service, P, R](fn)
}

func rpcOptionalParams[P any, R any](fn func(*Service, P) (R, error)) rpcHandler {
	return rpcserver.OptionalParams[Service, P, R](fn)
}

func (s *Service) dispatchRPC(req rpcRequest) (any, error) {
	return rpcserver.Dispatch(s, rpcMethods, req)
}

type rpcGetHeaderParams struct {
	Hash string `json:"hash"`
}

type rpcGetHeaderResult struct {
	Height uint64 `json:"height"`
	Hash   string `json:"hash"`
	NBits  uint32 `json:"nbits"`
}

type rpcGetBlockHashByHeightParams struct {
	Height uint64 `json:"height"`
}

type rpcGetBlockHashByHeightResult struct {
	Height uint64 `json:"height"`
	Hash   string `json:"hash"`
}

type rpcGetBlockParams struct {
	Hash string `json:"hash"`
}

type rpcGetBlockResult struct {
	Hash   string `json:"hash"`
	Txs    int    `json:"txs"`
	Header string `json:"header"`
}

type rpcGetBlockFilterParams struct {
	Hash   string  `json:"hash"`
	Height *uint64 `json:"height"`
}

type rpcGetFilterHeadersParams struct {
	StartHeight uint64 `json:"start_height"`
	Count       uint64 `json:"count"`
}

type rpcGetFilterCheckpointParams struct {
	Interval uint64 `json:"interval"`
}

type rpcGetMempoolResult []string

type rpcGetTxStatusParams struct {
	TxID string `json:"txid"`
}

type rpcGetTxStatusResult struct {
	TxID      string `json:"txid"`
	Confirmed bool   `json:"confirmed"`
	Mempool   bool   `json:"mempool"`
	BlockHash string `json:"block_hash,omitempty"`
}

type rpcGetUTXOsByPubKeysParams struct {
	PubKeys []string `json:"pubkeys"`
}

type rpcGetUTXOsByWatchItemsParams struct {
	WatchItems []rpcWatchItemParam `json:"watchitems"`
}

type rpcWatchItemParam struct {
	Type      uint64 `json:"type"`
	Payload32 string `json:"payload32"`
}

type rpcPubKeyUTXOResult struct {
	UTXOs []rpcPubKeyUTXO `json:"utxos"`
	Count int             `json:"count"`
}

type rpcWatchItemUTXOResult struct {
	UTXOs []rpcWatchItemUTXO `json:"utxos"`
	Count int                `json:"count"`
}

type rpcPubKeyUTXO struct {
	TxID          string `json:"txid"`
	Vout          uint32 `json:"vout"`
	Value         uint64 `json:"value"`
	PubKey        string `json:"pubkey"`
	Height        uint64 `json:"height"`
	Confirmations uint64 `json:"confirmations"`
	Coinbase      bool   `json:"coinbase"`
	Mature        bool   `json:"mature"`
}

type rpcWatchItemUTXO struct {
	TxID          string `json:"txid"`
	Vout          uint32 `json:"vout"`
	Value         uint64 `json:"value"`
	Type          uint64 `json:"type"`
	Payload32     string `json:"payload32"`
	Height        uint64 `json:"height"`
	Confirmations uint64 `json:"confirmations"`
	Coinbase      bool   `json:"coinbase"`
	Mature        bool   `json:"mature"`
}

type rpcGetUTXOProofParams struct {
	TxID string `json:"txid"`
	Vout uint32 `json:"vout"`
}

type rpcOutPointParam struct {
	TxID string `json:"txid"`
	Vout uint32 `json:"vout"`
}

type rpcGetUTXOProofBatchParams struct {
	OutPoints []rpcOutPointParam `json:"outpoints"`
}

type rpcVerifyUTXOProofParams struct {
	Proof RPCAnchoredUTXOProof `json:"proof"`
}

type rpcVerifyUTXOProofBatchParams struct {
	Proof RPCAnchoredUTXOProofBatch `json:"proof"`
}

type rpcVerifyUTXOProofResult struct {
	Valid              bool `json:"valid"`
	AnchorMatchesLocal bool `json:"anchor_matches_local"`
}

type rpcVerifyUTXOProofBatchResult struct {
	AllValid           bool `json:"all_valid"`
	ValidCount         int  `json:"valid_count"`
	AnchorMatchesLocal bool `json:"anchor_matches_local"`
}

type rpcGetCompactStatePackageParams struct {
	OutPoints []rpcOutPointParam `json:"outpoints"`
}

type rpcVerifyCompactStatePackageParams struct {
	Package RPCCompactStatePackage `json:"package"`
}

type rpcVerifyCompactStatePackageResult struct {
	AllValid           bool `json:"all_valid"`
	ValidCount         int  `json:"valid_count"`
	AnchorMatchesLocal bool `json:"anchor_matches_local"`
	LocalityOrdered    bool `json:"locality_ordered"`
}

type rpcGetWalletActivityByPubKeysParams struct {
	PubKeys []string `json:"pubkeys"`
	Limit   int      `json:"limit"`
}

type rpcGetWalletActivityByWatchItemsParams struct {
	WatchItems []rpcWatchItemParam `json:"watchitems"`
	Limit      int                 `json:"limit"`
}

type rpcWalletActivityResult struct {
	Activity []rpcWalletActivity `json:"activity"`
	Count    int                 `json:"count"`
}

type rpcWalletActivity struct {
	TxID      string `json:"txid"`
	BlockHash string `json:"block_hash"`
	Height    uint64 `json:"height"`
	Timestamp string `json:"timestamp"`
	Coinbase  bool   `json:"coinbase"`
	Received  uint64 `json:"received"`
	Sent      uint64 `json:"sent"`
	Fee       uint64 `json:"fee"`
	Net       int64  `json:"net"`
}

type rpcEstimateFeeParams struct {
	TargetBlocks int `json:"target_blocks"`
}

type rpcEstimateFeeResult struct {
	TargetBlocks int    `json:"target_blocks"`
	FeePerByte   uint64 `json:"fee_per_byte"`
}

type rpcSubmitTxParams struct {
	Hex string `json:"hex"`
}

type rpcSubmitTxBatchParams struct {
	Hexes []string `json:"hexes"`
}

type rpcSubmitPackedTxBatchParams struct {
	Packed string `json:"packed"`
}

type rpcTxSubmissionResult struct {
	Error          string `json:"error,omitempty"`
	TxID           string `json:"txid"`
	Fee            uint64 `json:"fee"`
	Orphaned       bool   `json:"orphaned"`
	AcceptedTxs    int    `json:"accepted_txs"`
	EvictedOrphans int    `json:"evicted_orphans"`
}

func (r rpcTxSubmissionResult) MarshalJSON() ([]byte, error) {
	if r.Error != "" {
		return json.Marshal(struct {
			Error string `json:"error"`
		}{Error: r.Error})
	}
	type success struct {
		TxID           string `json:"txid"`
		Fee            uint64 `json:"fee"`
		Orphaned       bool   `json:"orphaned"`
		AcceptedTxs    int    `json:"accepted_txs"`
		EvictedOrphans int    `json:"evicted_orphans"`
	}
	return json.Marshal(success{
		TxID:           r.TxID,
		Fee:            r.Fee,
		Orphaned:       r.Orphaned,
		AcceptedTxs:    r.AcceptedTxs,
		EvictedOrphans: r.EvictedOrphans,
	})
}

type rpcSubmitTxBatchResult struct {
	Results                 []rpcTxSubmissionResult `json:"results"`
	Submitted               int                     `json:"submitted"`
	Accepted                int                     `json:"accepted"`
	OrphanCount             int                     `json:"orphan_count"`
	MempoolSize             int                     `json:"mempool_size"`
	DecodeDurationMS        float64                 `json:"decode_duration_ms"`
	ValidateAdmitDurationMS float64                 `json:"validate_admit_duration_ms"`
}

type rpcMineParams struct {
	Count int `json:"count"`
}

type rpcSeedStressLanesParams struct {
	PubKeys             []string `json:"pubkeys"`
	ReserveTopUp        *bool    `json:"reserve_top_up,omitempty"`
	WaitForConfirmation *bool    `json:"wait_for_confirmation,omitempty"`
}

type rpcSeedStressLanesResult struct {
	Outputs      []rpcFundingOutput `json:"outputs"`
	Count        int                `json:"count"`
	Confirmed    bool               `json:"confirmed"`
	PendingTxID  string             `json:"pending_txid"`
	ReserveTopUp bool               `json:"reserve_topup"`
	Status       StressLaneInfo     `json:"status"`
}

type rpcFundingOutput struct {
	TxID      string `json:"txid"`
	Vout      uint32 `json:"vout"`
	Value     uint64 `json:"value"`
	PubKey    string `json:"pubkey"`
	BlockHash string `json:"block_hash"`
}

type rpcSubmitBlockParams struct {
	Hex string `json:"hex"`
}

type rpcSubmitBlockResult struct {
	Applied bool `json:"applied"`
}

type rpcAddPeerParams struct {
	Addr string `json:"addr"`
}

type rpcAddPeerResult struct {
	Addr string `json:"addr"`
}

type rpcStopResult struct {
	Stopping bool `json:"stopping"`
}

func (s *Service) rpcGetInfo() (ServiceInfo, error) { return s.Info(), nil }

func (s *Service) rpcGetChainState() (ChainStateInfo, error) { return s.ChainStateInfo(), nil }

func (s *Service) rpcGetMempoolInfo() (MempoolInfo, error) { return s.MempoolInfo(), nil }

func (s *Service) rpcGetMiningInfo() (MiningInfo, error) { return s.MiningInfo(), nil }

func (s *Service) rpcGetPeerInfo() ([]PeerInfo, error) { return s.PeerInfo(), nil }

func (s *Service) rpcGetAvalancheInfo() (AvalancheInfo, error) {
	return s.avalancheManager().info(), nil
}

func (s *Service) rpcGetMetrics() (PerformanceMetrics, error) { return s.PerformanceMetrics(), nil }

func (s *Service) rpcGetHeader(params rpcGetHeaderParams) (rpcGetHeaderResult, error) {
	entry, err := s.blockIndexByHashHex(params.Hash)
	if err != nil {
		return rpcGetHeaderResult{}, err
	}
	return rpcGetHeaderResult{Height: entry.Height, Hash: params.Hash, NBits: entry.Header.NBits}, nil
}

func (s *Service) rpcGetBlockHashByHeight(params rpcGetBlockHashByHeightParams) (rpcGetBlockHashByHeightResult, error) {
	entry, err := s.blockIndexByHeight(params.Height)
	if err != nil {
		return rpcGetBlockHashByHeightResult{}, err
	}
	hash := consensus.HeaderHash(&entry.Header)
	return rpcGetBlockHashByHeightResult{Height: params.Height, Hash: hex.EncodeToString(hash[:])}, nil
}

func (s *Service) rpcGetBlock(params rpcGetBlockParams) (rpcGetBlockResult, error) {
	block, err := s.blockByHashHex(params.Hash)
	if err != nil {
		return rpcGetBlockResult{}, err
	}
	return rpcGetBlockResult{Hash: params.Hash, Txs: len(block.Txs), Header: hex.EncodeToString(block.Header.Encode())}, nil
}

func (s *Service) rpcGetBlockFilter(params rpcGetBlockFilterParams) (CompactFilterInfo, error) {
	switch {
	case params.Hash != "":
		hash, err := decodeCompactFilterHash(params.Hash)
		if err != nil {
			return CompactFilterInfo{}, err
		}
		return s.CompactFilterByHash(hash)
	case params.Height != nil:
		return s.CompactFilterByHeight(*params.Height)
	default:
		return CompactFilterInfo{}, fmt.Errorf("hash or height is required")
	}
}

func (s *Service) rpcGetFilterHeaders(params rpcGetFilterHeadersParams) (CompactFilterHeadersInfo, error) {
	return s.CompactFilterHeaders(params.StartHeight, params.Count)
}

func (s *Service) rpcGetFilterCheckpoint(params rpcGetFilterCheckpointParams) (CompactFilterCheckpointInfo, error) {
	return s.CompactFilterCheckpoint(params.Interval)
}

func (s *Service) rpcGetMempool() (rpcGetMempoolResult, error) {
	entries := s.pool.SnapshotShared()
	out := make(rpcGetMempoolResult, 0, len(entries))
	for _, entry := range entries {
		out = append(out, hex.EncodeToString(entry.TxID[:]))
	}
	return out, nil
}

func (s *Service) rpcGetTxStatus(params rpcGetTxStatusParams) (rpcGetTxStatusResult, error) {
	txid, err := decodeHashHex(params.TxID)
	if err != nil {
		return rpcGetTxStatusResult{}, err
	}
	blockHash, confirmed, err := s.findActiveTxBlockHash(txid)
	if err != nil {
		return rpcGetTxStatusResult{}, err
	}
	result := rpcGetTxStatusResult{
		TxID:      hex.EncodeToString(txid[:]),
		Confirmed: confirmed,
		Mempool:   s.pool.Contains(txid),
	}
	if confirmed {
		result.BlockHash = hex.EncodeToString(blockHash[:])
	}
	return result, nil
}

func (s *Service) rpcGetUTXOsByPubKeys(params rpcGetUTXOsByPubKeysParams) (rpcPubKeyUTXOResult, error) {
	pubKeys, err := parseRPCPubKeys(params.PubKeys)
	if err != nil {
		return rpcPubKeyUTXOResult{}, err
	}
	items := make([]compactfilter.WatchItem, len(pubKeys))
	for i, key := range pubKeys {
		items[i] = compactfilter.WatchItem{Type: types.OutputXOnlyP2PK, Payload32: key}
	}
	utxos, err := s.walletUTXOsByWatchItems(items)
	if err != nil {
		return rpcPubKeyUTXOResult{}, err
	}
	out := make([]rpcPubKeyUTXO, 0, len(utxos))
	for _, utxo := range utxos {
		out = append(out, encodeRPCPubKeyUTXO(utxo))
	}
	return rpcPubKeyUTXOResult{UTXOs: out, Count: len(out)}, nil
}

func (s *Service) rpcGetUTXOsByWatchItems(params rpcGetUTXOsByWatchItemsParams) (rpcWatchItemUTXOResult, error) {
	items, err := decodeRPCWatchItems(params.WatchItems)
	if err != nil {
		return rpcWatchItemUTXOResult{}, err
	}
	utxos, err := s.walletUTXOsByWatchItems(items)
	if err != nil {
		return rpcWatchItemUTXOResult{}, err
	}
	out := make([]rpcWatchItemUTXO, 0, len(utxos))
	for _, utxo := range utxos {
		out = append(out, encodeRPCWatchItemUTXO(utxo))
	}
	return rpcWatchItemUTXOResult{UTXOs: out, Count: len(out)}, nil
}

func (s *Service) rpcGetUTXOProof(params rpcGetUTXOProofParams) (RPCAnchoredUTXOProof, error) {
	txid, err := decodeProofHex32(params.TxID, "txid")
	if err != nil {
		return RPCAnchoredUTXOProof{}, err
	}
	proof, err := s.UTXOProof(types.OutPoint{TxID: txid, Vout: params.Vout})
	if err != nil {
		return RPCAnchoredUTXOProof{}, err
	}
	return EncodeRPCUTXOProof(proof), nil
}

func (s *Service) rpcGetUTXOProofBatch(params rpcGetUTXOProofBatchParams) (RPCAnchoredUTXOProofBatch, error) {
	outPoints, err := decodeRPCOutPointParams(params.OutPoints, "outpoints")
	if err != nil {
		return RPCAnchoredUTXOProofBatch{}, err
	}
	batch, err := s.UTXOProofBatch(outPoints)
	if err != nil {
		return RPCAnchoredUTXOProofBatch{}, err
	}
	return EncodeRPCUTXOProofBatch(batch), nil
}

func (s *Service) rpcVerifyUTXOProof(params rpcVerifyUTXOProofParams) (rpcVerifyUTXOProofResult, error) {
	proof, err := DecodeRPCUTXOProof(params.Proof)
	if err != nil {
		return rpcVerifyUTXOProofResult{}, err
	}
	result, err := s.VerifyAnchoredUTXOProof(proof)
	if err != nil {
		return rpcVerifyUTXOProofResult{}, err
	}
	return rpcVerifyUTXOProofResult{Valid: result.Valid, AnchorMatchesLocal: result.AnchorMatchesLocal}, nil
}

func (s *Service) rpcVerifyUTXOProofBatch(params rpcVerifyUTXOProofBatchParams) (rpcVerifyUTXOProofBatchResult, error) {
	batch, err := DecodeRPCUTXOProofBatch(params.Proof)
	if err != nil {
		return rpcVerifyUTXOProofBatchResult{}, err
	}
	result, err := s.VerifyAnchoredUTXOProofBatch(batch)
	if err != nil {
		return rpcVerifyUTXOProofBatchResult{}, err
	}
	return rpcVerifyUTXOProofBatchResult{AllValid: result.AllValid, ValidCount: result.ValidCount, AnchorMatchesLocal: result.AnchorMatchesLocal}, nil
}

func (s *Service) rpcGetCompactStatePackage(params rpcGetCompactStatePackageParams) (RPCCompactStatePackage, error) {
	outPoints, err := decodeRPCOutPointParams(params.OutPoints, "outpoints")
	if err != nil {
		return RPCCompactStatePackage{}, err
	}
	pkg, err := s.CompactStatePackageForOutPoints(outPoints)
	if err != nil {
		return RPCCompactStatePackage{}, err
	}
	return EncodeRPCCompactStatePackage(pkg), nil
}

func (s *Service) rpcVerifyCompactStatePackage(params rpcVerifyCompactStatePackageParams) (rpcVerifyCompactStatePackageResult, error) {
	pkg, err := DecodeRPCCompactStatePackage(params.Package)
	if err != nil {
		return rpcVerifyCompactStatePackageResult{}, err
	}
	result, err := s.VerifyCompactStatePackage(pkg)
	if err != nil {
		return rpcVerifyCompactStatePackageResult{}, err
	}
	return rpcVerifyCompactStatePackageResult{
		AllValid:           result.AllValid,
		ValidCount:         result.ValidCount,
		AnchorMatchesLocal: result.AnchorMatchesLocal,
		LocalityOrdered:    pkg.LocalityOrdered,
	}, nil
}

func (s *Service) rpcGetStressLaneInfo() (StressLaneInfo, error) {
	if !s.cfg.Profile.IsRegtestLike() {
		return StressLaneInfo{}, errors.New("stress lane info is only available on regtest-style profiles")
	}
	return s.stressLaneInfo(), nil
}

func (s *Service) rpcGetWalletActivityByPubKeys(params rpcGetWalletActivityByPubKeysParams) (rpcWalletActivityResult, error) {
	if err := validateWalletActivityLimit(params.Limit); err != nil {
		return rpcWalletActivityResult{}, err
	}
	pubKeys, err := parseRPCPubKeys(params.PubKeys)
	if err != nil {
		return rpcWalletActivityResult{}, err
	}
	activity, err := s.WalletActivityByPubKeys(pubKeys, params.Limit)
	if err != nil {
		return rpcWalletActivityResult{}, err
	}
	return encodeRPCWalletActivityResult(activity), nil
}

func (s *Service) rpcGetWalletActivityByWatchItems(params rpcGetWalletActivityByWatchItemsParams) (rpcWalletActivityResult, error) {
	if err := validateWalletActivityLimit(params.Limit); err != nil {
		return rpcWalletActivityResult{}, err
	}
	items, err := decodeRPCWatchItems(params.WatchItems)
	if err != nil {
		return rpcWalletActivityResult{}, err
	}
	activity, err := s.WalletActivityByWatchItems(items, params.Limit)
	if err != nil {
		return rpcWalletActivityResult{}, err
	}
	return encodeRPCWalletActivityResult(activity), nil
}

func (s *Service) rpcEstimateFee(params rpcEstimateFeeParams) (rpcEstimateFeeResult, error) {
	return rpcEstimateFeeResult{TargetBlocks: params.TargetBlocks, FeePerByte: s.EstimateFeeRate(params.TargetBlocks)}, nil
}

func (s *Service) rpcSubmitTx(params rpcSubmitTxParams) (rpcTxSubmissionResult, error) {
	tx, err := consensus.DecodeTxHex(params.Hex, types.DefaultCodecLimits())
	if err != nil {
		return rpcTxSubmissionResult{}, err
	}
	admission, err := s.SubmitTx(tx)
	if err != nil {
		return rpcTxSubmissionResult{}, err
	}
	return encodeRPCSubmitTxResult(admission), nil
}

func (s *Service) rpcSubmitTxBatch(params rpcSubmitTxBatchParams) (rpcSubmitTxBatchResult, error) {
	decodeStarted := time.Now()
	txs := make([]types.Transaction, 0, len(params.Hexes))
	for _, raw := range params.Hexes {
		tx, err := consensus.DecodeTxHex(raw, types.DefaultCodecLimits())
		if err != nil {
			return rpcSubmitTxBatchResult{}, err
		}
		txs = append(txs, tx)
	}
	return s.submitRPCDecodedTxBatch(txs, time.Since(decodeStarted), "transaction batch processed"), nil
}

func (s *Service) rpcSubmitPackedTxBatch(params rpcSubmitPackedTxBatchParams) (rpcSubmitTxBatchResult, error) {
	if !s.cfg.Profile.IsRegtestLike() {
		return rpcSubmitTxBatchResult{}, errors.New("submitpackedtxbatch is only available on regtest-style profiles")
	}
	decodeStarted := time.Now()
	txs, err := decodePackedTransactions(params.Packed)
	if err != nil {
		return rpcSubmitTxBatchResult{}, err
	}
	return s.submitRPCDecodedTxBatch(txs, time.Since(decodeStarted), "packed transaction batch processed"), nil
}

func (s *Service) submitRPCDecodedTxBatch(txs []types.Transaction, decodeDuration time.Duration, logMessage string) rpcSubmitTxBatchResult {
	admitStarted := time.Now()
	admissions, errs, orphanCount, mempoolSize := s.submitDecodedTxs(txs)
	validateAdmitDuration := time.Since(admitStarted)
	results := make([]rpcTxSubmissionResult, 0, len(txs))
	accepted := 0
	for i := range txs {
		if errs[i] != nil {
			results = append(results, rpcTxSubmissionResult{Error: errs[i].Error()})
			continue
		}
		admission := admissions[i]
		if !admission.Orphaned {
			accepted++
		}
		results = append(results, encodeRPCSubmitTxResult(admission))
	}
	s.logger.Debug(logMessage,
		slog.Int("submitted", len(txs)),
		slog.Int("accepted", accepted),
		slog.Int("orphan_count", orphanCount),
		slog.Int("mempool_size", mempoolSize),
	)
	return rpcSubmitTxBatchResult{
		Results:                 results,
		Submitted:               len(txs),
		Accepted:                accepted,
		OrphanCount:             orphanCount,
		MempoolSize:             mempoolSize,
		DecodeDurationMS:        float64(decodeDuration.Microseconds()) / 1000,
		ValidateAdmitDurationMS: float64(validateAdmitDuration.Microseconds()) / 1000,
	}
}

func (s *Service) rpcMine(params rpcMineParams) ([]string, error) {
	if params.Count <= 0 {
		params.Count = 1
	}
	return s.MineBlocks(params.Count)
}

func (s *Service) rpcSeedStressLanes(params rpcSeedStressLanesParams) (rpcSeedStressLanesResult, error) {
	if !s.cfg.Profile.IsRegtestLike() {
		return rpcSeedStressLanesResult{}, errors.New("seedstresslanes is only available on regtest-style profiles")
	}
	if len(params.PubKeys) == 0 {
		return rpcSeedStressLanesResult{}, errors.New("pubkeys is required")
	}
	pubKeys, err := parseRPCPubKeys(params.PubKeys)
	if err != nil {
		return rpcSeedStressLanesResult{}, err
	}
	reserveTopUp := true
	if params.ReserveTopUp != nil {
		reserveTopUp = *params.ReserveTopUp
	}
	waitForConfirmation := true
	if params.WaitForConfirmation != nil {
		waitForConfirmation = *params.WaitForConfirmation
	}
	outputs, info, pendingTxID, err := s.SeedStressLanes(pubKeys, reserveTopUp, waitForConfirmation)
	if err != nil {
		return rpcSeedStressLanesResult{}, err
	}
	result := make([]rpcFundingOutput, 0, len(outputs))
	for _, output := range outputs {
		result = append(result, rpcFundingOutput{
			TxID:      hex.EncodeToString(output.OutPoint.TxID[:]),
			Vout:      output.OutPoint.Vout,
			Value:     output.Value,
			PubKey:    hex.EncodeToString(output.PubKey[:]),
			BlockHash: hex.EncodeToString(output.BlockHash[:]),
		})
	}
	return rpcSeedStressLanesResult{
		Outputs:      result,
		Count:        len(result),
		Confirmed:    waitForConfirmation,
		PendingTxID:  hex.EncodeToString(pendingTxID[:]),
		ReserveTopUp: reserveTopUp,
		Status:       info,
	}, nil
}

func (s *Service) rpcSubmitBlock(params rpcSubmitBlockParams) (rpcSubmitBlockResult, error) {
	raw, err := hex.DecodeString(params.Hex)
	if err != nil {
		return rpcSubmitBlockResult{}, err
	}
	if len(raw) < types.BlockHeaderEncodedLen {
		return rpcSubmitBlockResult{}, types.ErrUnexpectedEOF
	}
	header, err := types.DecodeBlockHeader(raw[:types.BlockHeaderEncodedLen])
	if err != nil {
		return rpcSubmitBlockResult{}, err
	}
	parent, err := s.chainState.Store().GetBlockIndex(&header.PrevBlockHash)
	if err != nil {
		return rpcSubmitBlockResult{}, err
	}
	if parent == nil {
		return rpcSubmitBlockResult{}, ErrUnknownParent
	}
	maxBytes := consensus.NextBlockSizeLimit(parent.BlockSizeState, consensus.ParamsForProfile(s.cfg.Profile))
	block, err := types.DecodeBlockWithBudget(raw, maxBytes)
	if err != nil {
		return rpcSubmitBlockResult{}, err
	}
	applied, _, _, err := s.applyPeerBlock(&block)
	if errors.Is(err, ErrBlockHeaderNotIndexed) {
		if _, headerErr := s.applyPeerHeaders([]types.BlockHeader{block.Header}); headerErr == nil {
			applied, _, _, err = s.applyPeerBlock(&block)
		}
	}
	if err != nil {
		return rpcSubmitBlockResult{}, err
	}
	if applied {
		hash := consensus.HeaderHash(&block.Header)
		s.broadcastInv([]p2p.InvVector{{Type: p2p.InvTypeBlock, Hash: hash}})
	}
	return rpcSubmitBlockResult{Applied: applied}, nil
}

func (s *Service) rpcAddPeer(params rpcAddPeerParams) (rpcAddPeerResult, error) {
	if params.Addr == "" {
		return rpcAddPeerResult{}, errors.New("addr is required")
	}
	s.safeGoDetached("rpc-addpeer", func() {
		if err := s.ConnectPeer(params.Addr); err != nil {
			s.logger.Warn("rpc addpeer failed", slog.String("addr", params.Addr), slog.Any("error", err))
		}
	})
	return rpcAddPeerResult{Addr: params.Addr}, nil
}

func (s *Service) rpcStop() (rpcStopResult, error) {
	s.safeGoDetached("rpc-stop", func() {
		_ = s.Close()
	})
	return rpcStopResult{Stopping: true}, nil
}

func parseRPCPubKeys(raw []string) ([][32]byte, error) {
	pubKeys := make([][32]byte, 0, len(raw))
	for _, item := range raw {
		pubKey, err := ParseMinerPubKey(item)
		if err != nil {
			return nil, err
		}
		pubKeys = append(pubKeys, pubKey)
	}
	return pubKeys, nil
}

func decodeRPCWatchItems(raw []rpcWatchItemParam) ([]compactfilter.WatchItem, error) {
	items := make([]compactfilter.WatchItem, 0, len(raw))
	for i, item := range raw {
		payload32, err := decodeProofHex32(item.Payload32, fmt.Sprintf("watchitems[%d].payload32", i))
		if err != nil {
			return nil, err
		}
		items = append(items, compactfilter.WatchItem{Type: item.Type, Payload32: payload32})
	}
	return items, nil
}

func decodeRPCOutPointParams(raw []rpcOutPointParam, field string) ([]types.OutPoint, error) {
	outPoints := make([]types.OutPoint, 0, len(raw))
	for i, item := range raw {
		txid, err := decodeProofHex32(item.TxID, fmt.Sprintf("%s[%d].txid", field, i))
		if err != nil {
			return nil, err
		}
		outPoints = append(outPoints, types.OutPoint{TxID: txid, Vout: item.Vout})
	}
	return outPoints, nil
}

func encodeRPCPubKeyUTXO(utxo PubKeyUTXO) rpcPubKeyUTXO {
	return rpcPubKeyUTXO{
		TxID:          hex.EncodeToString(utxo.OutPoint.TxID[:]),
		Vout:          utxo.OutPoint.Vout,
		Value:         utxo.Value,
		PubKey:        hex.EncodeToString(utxo.PubKey[:]),
		Height:        utxo.Height,
		Confirmations: utxo.Confirmations,
		Coinbase:      utxo.Coinbase,
		Mature:        utxo.Mature,
	}
}

func encodeRPCWatchItemUTXO(utxo PubKeyUTXO) rpcWatchItemUTXO {
	return rpcWatchItemUTXO{
		TxID:          hex.EncodeToString(utxo.OutPoint.TxID[:]),
		Vout:          utxo.OutPoint.Vout,
		Value:         utxo.Value,
		Type:          utxo.Type,
		Payload32:     hex.EncodeToString(utxo.Payload32[:]),
		Height:        utxo.Height,
		Confirmations: utxo.Confirmations,
		Coinbase:      utxo.Coinbase,
		Mature:        utxo.Mature,
	}
}

func encodeRPCWalletActivityResult(activity []WalletActivity) rpcWalletActivityResult {
	out := make([]rpcWalletActivity, 0, len(activity))
	for _, item := range activity {
		out = append(out, rpcWalletActivity{
			TxID:      hex.EncodeToString(item.TxID[:]),
			BlockHash: hex.EncodeToString(item.BlockHash[:]),
			Height:    item.Height,
			Timestamp: item.Timestamp.Format(time.RFC3339),
			Coinbase:  item.Coinbase,
			Received:  item.Received,
			Sent:      item.Sent,
			Fee:       item.Fee,
			Net:       item.Net,
		})
	}
	return rpcWalletActivityResult{Activity: out, Count: len(out)}
}

func encodeRPCSubmitTxResult(admission mempool.Admission) rpcTxSubmissionResult {
	return rpcTxSubmissionResult{
		TxID:           hex.EncodeToString(admission.TxID[:]),
		Fee:            admission.Summary.Fee,
		Orphaned:       admission.Orphaned,
		AcceptedTxs:    len(admission.Accepted),
		EvictedOrphans: admission.EvictedOrphans,
	}
}

func (s *Service) authorizeRPC(r *http.Request) bool {
	if s.cfg.RPCAuthToken == "" {
		return allowUnauthenticatedLoopbackRPC(r)
	}
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if strings.HasPrefix(header, prefix) && strings.TrimPrefix(header, prefix) == s.cfg.RPCAuthToken {
		return true
	}
	return r.Header.Get("X-BPU-Auth") == s.cfg.RPCAuthToken
}

func allowUnauthenticatedLoopbackRPC(r *http.Request) bool {
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		return false
	}
	if site := strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	return originMatchesRequestHost(origin, r)
}

func isJSONContentType(raw string) bool {
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil {
		return false
	}
	return strings.EqualFold(mediaType, "application/json")
}

func originMatchesRequestHost(origin string, r *http.Request) bool {
	parsed, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return parsed.Scheme == scheme && strings.EqualFold(parsed.Host, r.Host)
}

func (s *Service) blockIndexByHashHex(raw string) (*storage.BlockIndexEntry, error) {
	hash, err := decodeHashHex(raw)
	if err != nil {
		return nil, err
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	entry, err := s.chainState.Store().GetBlockIndex(&hash)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, errors.New("unknown block hash")
	}
	return entry, nil
}

func (s *Service) blockIndexByHeight(height uint64) (*storage.BlockIndexEntry, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	entry, err := s.chainState.Store().GetBlockIndexByHeight(height)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, errors.New("unknown block height")
	}
	return entry, nil
}

func (s *Service) blockByHashHex(raw string) (*types.Block, error) {
	hash, err := decodeHashHex(raw)
	if err != nil {
		return nil, err
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	block, err := s.chainState.Store().GetBlock(&hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, errors.New("unknown block hash")
	}
	return block, nil
}
