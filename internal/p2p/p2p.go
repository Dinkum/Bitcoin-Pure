package p2p

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"bitcoin-pure/internal/crypto"
	"bitcoin-pure/internal/types"
)

const (
	headerSize            = 16
	defaultProtocolNumber = 1
	maxHeadersPerMessage  = 2000
	maxInvPerMessage      = 2000
	maxAddrPerMessage     = 1000
	maxTxBatchPerMessage  = 256
	maxThinBlockTxs       = 200_000
	maxAvalanchePollItems = 256
)

var (
	ErrBadMagic        = errors.New("invalid network magic")
	ErrPayloadTooLarge = errors.New("payload too large")
	ErrBadChecksum     = errors.New("invalid payload checksum")
	ErrBadCommand      = errors.New("invalid command")
	ErrBadHandshake    = errors.New("invalid handshake sequence")
)

const (
	// ServiceNodeNetwork marks a peer as a normal full-relay node.
	ServiceNodeNetwork uint64 = 1 << iota
	// ServiceErlayTxRelay enables Erlay-style steady-state tx relay.
	ServiceErlayTxRelay
	// ServiceGrapheneBlockRelay enables the compact block relay planner path.
	ServiceGrapheneBlockRelay
	// ServiceGrapheneExtended enables the poor-overlap block recovery path.
	ServiceGrapheneExtended
	// ServiceGrapheneMempoolRepair enables optional repair-only mempool sync.
	ServiceGrapheneMempoolRepair
	// ServiceAvalancheOverlay enables the non-consensus Avalanche poll/vote path.
	ServiceAvalancheOverlay
)

type Command uint8

const (
	CmdVersion Command = 1 + iota
	CmdVerAck
	CmdPing
	CmdPong
	CmdGetAddr
	CmdAddr
	CmdInv
	CmdGetData
	CmdNotFound
	CmdGetHeaders
	CmdHeaders
	CmdGetBlocks
	CmdBlock
	CmdTx
	CmdTxBatch
	CmdTxRecon
	CmdTxReq
	CmdCompactBlock
	CmdGetBlockTx
	CmdBlockTx
	CmdXThinBlock
	CmdGetXBlockTx
	CmdXBlockTx
	CmdAvaPoll
	CmdAvaVote
)

type InvType uint8

const (
	InvTypeTx InvType = 1 + iota
	InvTypeBlock
	InvTypeBlockFull
	InvTypeBlockExtended
)

type Message interface {
	Command() Command
}

type VersionMessage struct {
	Protocol  uint32
	Services  uint64
	Height    uint64
	Nonce     uint64
	UserAgent string
}

func (VersionMessage) Command() Command { return CmdVersion }

type VerAckMessage struct{}

func (VerAckMessage) Command() Command { return CmdVerAck }

type PingMessage struct {
	Nonce uint64
}

func (PingMessage) Command() Command { return CmdPing }

type PongMessage struct {
	Nonce uint64
}

func (PongMessage) Command() Command { return CmdPong }

type GetAddrMessage struct{}

func (GetAddrMessage) Command() Command { return CmdGetAddr }

type AddrMessage struct {
	Addrs []string
}

func (AddrMessage) Command() Command { return CmdAddr }

type InvVector struct {
	Type InvType
	Hash [32]byte
}

type InvMessage struct {
	Items []InvVector
}

func (InvMessage) Command() Command { return CmdInv }

type GetDataMessage struct {
	Items []InvVector
}

func (GetDataMessage) Command() Command { return CmdGetData }

type NotFoundMessage struct {
	Items []InvVector
}

func (NotFoundMessage) Command() Command { return CmdNotFound }

type GetHeadersMessage struct {
	Locator  [][32]byte
	StopHash [32]byte
}

func (GetHeadersMessage) Command() Command { return CmdGetHeaders }

type HeadersMessage struct {
	Headers []types.BlockHeader
}

func (HeadersMessage) Command() Command { return CmdHeaders }

type GetBlocksMessage struct {
	Locator  [][32]byte
	StopHash [32]byte
}

func (GetBlocksMessage) Command() Command { return CmdGetBlocks }

type BlockMessage struct {
	Block types.Block
}

func (BlockMessage) Command() Command { return CmdBlock }

type TxMessage struct {
	Tx types.Transaction
}

func (TxMessage) Command() Command { return CmdTx }

type TxBatchMessage struct {
	Txs []types.Transaction
}

func (TxBatchMessage) Command() Command { return CmdTxBatch }

type TxReconMessage struct {
	TxIDs [][32]byte
}

func (TxReconMessage) Command() Command { return CmdTxRecon }

type TxRequestMessage struct {
	TxIDs [][32]byte
}

func (TxRequestMessage) Command() Command { return CmdTxReq }

type PrefilledTx struct {
	Index uint32
	Tx    types.Transaction
}

type CompactBlockMessage struct {
	Header    types.BlockHeader
	Nonce     uint64
	Prefilled []PrefilledTx
	ShortIDs  []uint64
}

func (CompactBlockMessage) Command() Command { return CmdCompactBlock }

type GetBlockTxMessage struct {
	BlockHash [32]byte
	Indexes   []uint32
}

func (GetBlockTxMessage) Command() Command { return CmdGetBlockTx }

type BlockTxMessage struct {
	BlockHash [32]byte
	Indexes   []uint32
	Txs       []types.Transaction
}

func (BlockTxMessage) Command() Command { return CmdBlockTx }

type XThinBlockMessage struct {
	Header   types.BlockHeader
	Nonce    uint64
	Coinbase types.Transaction
	ShortIDs []uint64
}

func (XThinBlockMessage) Command() Command { return CmdXThinBlock }

type GetXBlockTxMessage struct {
	BlockHash [32]byte
	Indexes   []uint32
}

func (GetXBlockTxMessage) Command() Command { return CmdGetXBlockTx }

type XBlockTxMessage struct {
	BlockHash [32]byte
	Indexes   []uint32
	Txs       []types.Transaction
}

func (XBlockTxMessage) Command() Command { return CmdXBlockTx }

type AvaPollMessage struct {
	PollID uint64
	Items  []types.OutPoint
}

func (AvaPollMessage) Command() Command { return CmdAvaPoll }

type AvaVote struct {
	HasOpinion bool
	TxID       [32]byte
}

type AvaVoteMessage struct {
	PollID uint64
	Votes  []AvaVote
}

func (AvaVoteMessage) Command() Command { return CmdAvaVote }

type Conn struct {
	net.Conn
	magic      uint32
	maxPayload int
	limits     types.CodecLimits
}

func NewConn(conn net.Conn, magic uint32, maxPayload int) *Conn {
	if maxPayload <= 0 {
		maxPayload = 64_000_000
	}
	return &Conn{
		Conn:       conn,
		magic:      magic,
		maxPayload: maxPayload,
		limits:     types.DefaultCodecLimits(),
	}
}

func Handshake(conn *Conn, local VersionMessage, timeout time.Duration) (VersionMessage, error) {
	if local.Protocol == 0 {
		local.Protocol = defaultProtocolNumber
	}
	if timeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			return VersionMessage{}, err
		}
		defer conn.SetDeadline(time.Time{})
	}
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- conn.WriteMessage(local)
	}()
	msg, err := conn.ReadMessage()
	if err != nil {
		return VersionMessage{}, err
	}
	if err := <-writeErr; err != nil {
		return VersionMessage{}, err
	}
	remote, ok := msg.(VersionMessage)
	if !ok {
		return VersionMessage{}, ErrBadHandshake
	}
	go func() {
		writeErr <- conn.WriteMessage(VerAckMessage{})
	}()
	msg, err = conn.ReadMessage()
	if err != nil {
		return VersionMessage{}, err
	}
	if err := <-writeErr; err != nil {
		return VersionMessage{}, err
	}
	if _, ok := msg.(VerAckMessage); !ok {
		return VersionMessage{}, ErrBadHandshake
	}
	return remote, nil
}

func (c *Conn) ReadMessage() (Message, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(c.Conn, header); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(header[:4]) != c.magic {
		return nil, ErrBadMagic
	}
	cmd := Command(header[4])
	payloadLen := int(binary.LittleEndian.Uint32(header[8:12]))
	if payloadLen < 0 || payloadLen > c.maxPayload {
		return nil, ErrPayloadTooLarge
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(c.Conn, payload); err != nil {
		return nil, err
	}
	checksum := crypto.Sha256d(payload)
	if string(checksum[:4]) != string(header[12:16]) {
		return nil, ErrBadChecksum
	}
	return decodeMessage(cmd, payload, c.limits)
}

func (c *Conn) WriteMessage(msg Message) error {
	payload, err := encodeMessage(msg)
	if err != nil {
		return err
	}
	if len(payload) > c.maxPayload {
		return ErrPayloadTooLarge
	}
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[:4], c.magic)
	header[4] = byte(msg.Command())
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(payload)))
	checksum := crypto.Sha256d(payload)
	copy(header[12:16], checksum[:4])
	if _, err := c.Conn.Write(header); err != nil {
		return err
	}
	_, err = c.Conn.Write(payload)
	return err
}

func MagicForProfile(profile types.ChainProfile) uint32 {
	switch profile {
	case types.Mainnet:
		return 0x4250554d
	case types.Regtest:
		return 0x42505552
	case types.RegtestMedium:
		return 0x42505553
	case types.RegtestHard:
		return 0x42505548
	default:
		return 0
	}
}

func encodeMessage(msg Message) ([]byte, error) {
	switch m := msg.(type) {
	case VersionMessage:
		buf := make([]byte, 28)
		binary.LittleEndian.PutUint32(buf[:4], m.Protocol)
		binary.LittleEndian.PutUint64(buf[4:12], m.Services)
		binary.LittleEndian.PutUint64(buf[12:20], m.Height)
		binary.LittleEndian.PutUint64(buf[20:28], m.Nonce)
		buf = appendString(buf, m.UserAgent)
		return buf, nil
	case VerAckMessage, PingMessage, PongMessage, GetAddrMessage, AddrMessage, InvMessage, GetDataMessage, NotFoundMessage, GetHeadersMessage, HeadersMessage, GetBlocksMessage, BlockMessage, TxMessage, TxBatchMessage, TxReconMessage, TxRequestMessage, CompactBlockMessage, GetBlockTxMessage, BlockTxMessage, XThinBlockMessage, GetXBlockTxMessage, XBlockTxMessage, AvaPollMessage, AvaVoteMessage:
		return encodePayload(msg)
	default:
		return nil, fmt.Errorf("unsupported message type %T", msg)
	}
}

func encodePayload(msg Message) ([]byte, error) {
	switch m := msg.(type) {
	case VerAckMessage:
		return nil, nil
	case PingMessage:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, m.Nonce)
		return buf, nil
	case PongMessage:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, m.Nonce)
		return buf, nil
	case GetAddrMessage:
		return nil, nil
	case AddrMessage:
		return encodeStrings(m.Addrs, maxAddrPerMessage)
	case InvMessage:
		return encodeInvs(m.Items)
	case GetDataMessage:
		return encodeInvs(m.Items)
	case NotFoundMessage:
		return encodeInvs(m.Items)
	case GetHeadersMessage:
		return encodeLocatorPayload(m.Locator, m.StopHash)
	case HeadersMessage:
		return encodeHeaders(m.Headers)
	case GetBlocksMessage:
		return encodeLocatorPayload(m.Locator, m.StopHash)
	case BlockMessage:
		blockBytes := m.Block.Encode()
		return appendBytes(nil, blockBytes), nil
	case TxMessage:
		txBytes := m.Tx.Encode()
		return appendBytes(nil, txBytes), nil
	case TxBatchMessage:
		return encodeTxs(m.Txs)
	case TxReconMessage:
		return encodeHashes(m.TxIDs, maxInvPerMessage)
	case TxRequestMessage:
		return encodeHashes(m.TxIDs, maxInvPerMessage)
	case CompactBlockMessage:
		buf := append([]byte(nil), m.Header.Encode()...)
		nonce := make([]byte, 8)
		binary.LittleEndian.PutUint64(nonce, m.Nonce)
		buf = append(buf, nonce...)
		buf, err := encodePrefilledTxs(buf, m.Prefilled, maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		return encodeU64s(buf, m.ShortIDs, maxThinBlockTxs)
	case GetBlockTxMessage:
		buf := append([]byte(nil), m.BlockHash[:]...)
		return encodeU32s(buf, m.Indexes, maxThinBlockTxs)
	case BlockTxMessage:
		buf := append([]byte(nil), m.BlockHash[:]...)
		buf, err := encodeU32s(buf, m.Indexes, maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		return encodeTxsWithLimit(buf, m.Txs, maxThinBlockTxs)
	case XThinBlockMessage:
		buf := append([]byte(nil), m.Header.Encode()...)
		nonce := make([]byte, 8)
		binary.LittleEndian.PutUint64(nonce, m.Nonce)
		buf = append(buf, nonce...)
		buf = appendBytes(buf, m.Coinbase.Encode())
		return encodeU64s(buf, m.ShortIDs, maxThinBlockTxs)
	case GetXBlockTxMessage:
		buf := append([]byte(nil), m.BlockHash[:]...)
		return encodeU32s(buf, m.Indexes, maxThinBlockTxs)
	case XBlockTxMessage:
		buf := append([]byte(nil), m.BlockHash[:]...)
		buf, err := encodeU32s(buf, m.Indexes, maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		return encodeTxsWithLimit(buf, m.Txs, maxThinBlockTxs)
	case AvaPollMessage:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, m.PollID)
		return encodeOutPoints(buf, m.Items, maxAvalanchePollItems)
	case AvaVoteMessage:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, m.PollID)
		return encodeAvaVotes(buf, m.Votes, maxAvalanchePollItems)
	default:
		return nil, fmt.Errorf("unsupported message payload %T", msg)
	}
}

func decodeMessage(cmd Command, payload []byte, limits types.CodecLimits) (Message, error) {
	r := newReader(payload)
	switch cmd {
	case CmdVersion:
		if len(payload) < 28 {
			return nil, io.ErrUnexpectedEOF
		}
		msg := VersionMessage{
			Protocol: binary.LittleEndian.Uint32(payload[:4]),
			Services: binary.LittleEndian.Uint64(payload[4:12]),
			Height:   binary.LittleEndian.Uint64(payload[12:20]),
			Nonce:    binary.LittleEndian.Uint64(payload[20:28]),
		}
		userAgent, err := r.skip(28).readString()
		if err != nil {
			return nil, err
		}
		msg.UserAgent = userAgent
		return msg, nil
	case CmdVerAck:
		return VerAckMessage{}, nil
	case CmdPing:
		nonce, err := r.readU64()
		if err != nil {
			return nil, err
		}
		return PingMessage{Nonce: nonce}, nil
	case CmdPong:
		nonce, err := r.readU64()
		if err != nil {
			return nil, err
		}
		return PongMessage{Nonce: nonce}, nil
	case CmdGetAddr:
		return GetAddrMessage{}, nil
	case CmdAddr:
		addrs, err := r.readStrings(maxAddrPerMessage)
		if err != nil {
			return nil, err
		}
		return AddrMessage{Addrs: addrs}, nil
	case CmdInv:
		items, err := r.readInvs()
		if err != nil {
			return nil, err
		}
		return InvMessage{Items: items}, nil
	case CmdGetData:
		items, err := r.readInvs()
		if err != nil {
			return nil, err
		}
		return GetDataMessage{Items: items}, nil
	case CmdNotFound:
		items, err := r.readInvs()
		if err != nil {
			return nil, err
		}
		return NotFoundMessage{Items: items}, nil
	case CmdGetHeaders:
		locator, stopHash, err := r.readLocatorPayload()
		if err != nil {
			return nil, err
		}
		return GetHeadersMessage{Locator: locator, StopHash: stopHash}, nil
	case CmdHeaders:
		headers, err := r.readHeaders()
		if err != nil {
			return nil, err
		}
		return HeadersMessage{Headers: headers}, nil
	case CmdGetBlocks:
		locator, stopHash, err := r.readLocatorPayload()
		if err != nil {
			return nil, err
		}
		return GetBlocksMessage{Locator: locator, StopHash: stopHash}, nil
	case CmdBlock:
		blockBytes, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		block, err := types.DecodeBlockWithLimits(blockBytes, limits)
		if err != nil {
			return nil, err
		}
		return BlockMessage{Block: block}, nil
	case CmdTx:
		txBytes, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		tx, err := types.DecodeTransactionWithLimits(txBytes, limits)
		if err != nil {
			return nil, err
		}
		return TxMessage{Tx: tx}, nil
	case CmdTxBatch:
		txs, err := r.readTxs(maxTxBatchPerMessage, limits)
		if err != nil {
			return nil, err
		}
		return TxBatchMessage{Txs: txs}, nil
	case CmdTxRecon:
		txids, err := r.readHashes(maxInvPerMessage)
		if err != nil {
			return nil, err
		}
		return TxReconMessage{TxIDs: txids}, nil
	case CmdTxReq:
		txids, err := r.readHashes(maxInvPerMessage)
		if err != nil {
			return nil, err
		}
		return TxRequestMessage{TxIDs: txids}, nil
	case CmdCompactBlock:
		headerBytes, err := r.take(148)
		if err != nil {
			return nil, err
		}
		header, err := types.DecodeBlockHeader(headerBytes)
		if err != nil {
			return nil, err
		}
		nonce, err := r.readU64()
		if err != nil {
			return nil, err
		}
		prefilled, err := r.readPrefilledTxs(maxThinBlockTxs, limits)
		if err != nil {
			return nil, err
		}
		shortIDs, err := r.readU64s(maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		return CompactBlockMessage{Header: header, Nonce: nonce, Prefilled: prefilled, ShortIDs: shortIDs}, nil
	case CmdGetBlockTx:
		hashBytes, err := r.take(32)
		if err != nil {
			return nil, err
		}
		var blockHash [32]byte
		copy(blockHash[:], hashBytes)
		indexes, err := r.readU32s(maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		return GetBlockTxMessage{BlockHash: blockHash, Indexes: indexes}, nil
	case CmdBlockTx:
		hashBytes, err := r.take(32)
		if err != nil {
			return nil, err
		}
		var blockHash [32]byte
		copy(blockHash[:], hashBytes)
		indexes, err := r.readU32s(maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		txs, err := r.readTxs(maxThinBlockTxs, limits)
		if err != nil {
			return nil, err
		}
		return BlockTxMessage{BlockHash: blockHash, Indexes: indexes, Txs: txs}, nil
	case CmdXThinBlock:
		headerBytes, err := r.take(148)
		if err != nil {
			return nil, err
		}
		header, err := types.DecodeBlockHeader(headerBytes)
		if err != nil {
			return nil, err
		}
		nonce, err := r.readU64()
		if err != nil {
			return nil, err
		}
		coinbaseBytes, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		coinbase, err := types.DecodeTransactionWithLimits(coinbaseBytes, limits)
		if err != nil {
			return nil, err
		}
		shortIDs, err := r.readU64s(maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		return XThinBlockMessage{Header: header, Nonce: nonce, Coinbase: coinbase, ShortIDs: shortIDs}, nil
	case CmdGetXBlockTx:
		hashBytes, err := r.take(32)
		if err != nil {
			return nil, err
		}
		var blockHash [32]byte
		copy(blockHash[:], hashBytes)
		indexes, err := r.readU32s(maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		return GetXBlockTxMessage{BlockHash: blockHash, Indexes: indexes}, nil
	case CmdXBlockTx:
		hashBytes, err := r.take(32)
		if err != nil {
			return nil, err
		}
		var blockHash [32]byte
		copy(blockHash[:], hashBytes)
		indexes, err := r.readU32s(maxThinBlockTxs)
		if err != nil {
			return nil, err
		}
		txs, err := r.readTxs(maxThinBlockTxs, limits)
		if err != nil {
			return nil, err
		}
		return XBlockTxMessage{BlockHash: blockHash, Indexes: indexes, Txs: txs}, nil
	case CmdAvaPoll:
		pollID, err := r.readU64()
		if err != nil {
			return nil, err
		}
		items, err := r.readOutPoints(maxAvalanchePollItems)
		if err != nil {
			return nil, err
		}
		return AvaPollMessage{PollID: pollID, Items: items}, nil
	case CmdAvaVote:
		pollID, err := r.readU64()
		if err != nil {
			return nil, err
		}
		votes, err := r.readAvaVotes(maxAvalanchePollItems)
		if err != nil {
			return nil, err
		}
		return AvaVoteMessage{PollID: pollID, Votes: votes}, nil
	default:
		return nil, ErrBadCommand
	}
}

func encodeOutPoints(buf []byte, items []types.OutPoint, limit int) ([]byte, error) {
	if len(items) > limit {
		return nil, fmt.Errorf("too many items: %d", len(items))
	}
	buf = append(buf, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(items)))
	for _, item := range items {
		buf = append(buf, item.TxID[:]...)
		raw := make([]byte, 4)
		binary.LittleEndian.PutUint32(raw, item.Vout)
		buf = append(buf, raw...)
	}
	return buf, nil
}

func encodeAvaVotes(buf []byte, votes []AvaVote, limit int) ([]byte, error) {
	if len(votes) > limit {
		return nil, fmt.Errorf("too many items: %d", len(votes))
	}
	buf = append(buf, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(votes)))
	for _, vote := range votes {
		if vote.HasOpinion {
			buf = append(buf, 1)
			buf = append(buf, vote.TxID[:]...)
			continue
		}
		buf = append(buf, 0)
		buf = append(buf, make([]byte, 32)...)
	}
	return buf, nil
}

func encodePrefilledTxs(buf []byte, items []PrefilledTx, limit int) ([]byte, error) {
	if len(items) > limit {
		return nil, fmt.Errorf("too many prefilled txs: %d", len(items))
	}
	count := make([]byte, 4)
	binary.LittleEndian.PutUint32(count, uint32(len(items)))
	buf = append(buf, count...)
	for _, item := range items {
		index := make([]byte, 4)
		binary.LittleEndian.PutUint32(index, item.Index)
		buf = append(buf, index...)
		buf = appendBytes(buf, item.Tx.Encode())
	}
	return buf, nil
}

func encodeInvs(items []InvVector) ([]byte, error) {
	if len(items) > maxInvPerMessage {
		return nil, fmt.Errorf("too many inv items: %d", len(items))
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(items)))
	for _, item := range items {
		buf = append(buf, byte(item.Type))
		buf = append(buf, item.Hash[:]...)
	}
	return buf, nil
}

func encodeLocatorPayload(locator [][32]byte, stopHash [32]byte) ([]byte, error) {
	if len(locator) > maxHeadersPerMessage {
		return nil, fmt.Errorf("too many locator hashes: %d", len(locator))
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(locator)))
	for _, hash := range locator {
		buf = append(buf, hash[:]...)
	}
	buf = append(buf, stopHash[:]...)
	return buf, nil
}

func encodeHeaders(headers []types.BlockHeader) ([]byte, error) {
	if len(headers) > maxHeadersPerMessage {
		return nil, fmt.Errorf("too many headers: %d", len(headers))
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(headers)))
	for _, header := range headers {
		buf = append(buf, header.Encode()...)
	}
	return buf, nil
}

func encodeStrings(items []string, limit int) ([]byte, error) {
	if len(items) > limit {
		return nil, fmt.Errorf("too many items: %d", len(items))
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(items)))
	for _, item := range items {
		buf = appendString(buf, item)
	}
	return buf, nil
}

func encodeTxs(txs []types.Transaction) ([]byte, error) {
	return encodeTxsWithLimit(nil, txs, maxTxBatchPerMessage)
}

func encodeTxsWithLimit(buf []byte, txs []types.Transaction, limit int) ([]byte, error) {
	if len(txs) > limit {
		return nil, fmt.Errorf("too many txs: %d", len(txs))
	}
	if buf == nil {
		buf = make([]byte, 4)
	} else {
		buf = append(buf, make([]byte, 4)...)
	}
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(txs)))
	for _, tx := range txs {
		buf = appendBytes(buf, tx.Encode())
	}
	return buf, nil
}

func encodeU64s(buf []byte, items []uint64, limit int) ([]byte, error) {
	if len(items) > limit {
		return nil, fmt.Errorf("too many items: %d", len(items))
	}
	buf = append(buf, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(items)))
	for _, item := range items {
		raw := make([]byte, 8)
		binary.LittleEndian.PutUint64(raw, item)
		buf = append(buf, raw...)
	}
	return buf, nil
}

func encodeHashes(hashes [][32]byte, limit int) ([]byte, error) {
	if len(hashes) > limit {
		return nil, fmt.Errorf("too many hashes: %d", len(hashes))
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(hashes)))
	for _, hash := range hashes {
		buf = append(buf, hash[:]...)
	}
	return buf, nil
}

func encodeU32s(buf []byte, items []uint32, limit int) ([]byte, error) {
	if len(items) > limit {
		return nil, fmt.Errorf("too many items: %d", len(items))
	}
	buf = append(buf, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(items)))
	for _, item := range items {
		raw := make([]byte, 4)
		binary.LittleEndian.PutUint32(raw, item)
		buf = append(buf, raw...)
	}
	return buf, nil
}

func appendString(buf []byte, value string) []byte {
	return appendBytes(buf, []byte(value))
}

func appendBytes(buf []byte, value []byte) []byte {
	size := make([]byte, 4)
	binary.LittleEndian.PutUint32(size, uint32(len(value)))
	buf = append(buf, size...)
	return append(buf, value...)
}

type reader struct {
	buf []byte
	pos int
}

func newReader(buf []byte) *reader {
	return &reader{buf: buf}
}

func (r *reader) skip(n int) *reader {
	r.pos = n
	return r
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	out := r.buf[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *reader) readU32() (uint32, error) {
	buf, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (r *reader) readU64() (uint64, error) {
	buf, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func (r *reader) readBytes() ([]byte, error) {
	n, err := r.readU32()
	if err != nil {
		return nil, err
	}
	return r.take(int(n))
}

func (r *reader) readString() (string, error) {
	buf, err := r.readBytes()
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (r *reader) readStrings(limit int) ([]string, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if int(count) > limit {
		return nil, ErrPayloadTooLarge
	}
	out := make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		item, err := r.readString()
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func (r *reader) readInvs() ([]InvVector, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if count > maxInvPerMessage {
		return nil, ErrPayloadTooLarge
	}
	items := make([]InvVector, 0, count)
	for i := uint32(0); i < count; i++ {
		kind, err := r.take(1)
		if err != nil {
			return nil, err
		}
		hash, err := r.take(32)
		if err != nil {
			return nil, err
		}
		var item InvVector
		item.Type = InvType(kind[0])
		copy(item.Hash[:], hash)
		items = append(items, item)
	}
	return items, nil
}

func (r *reader) readOutPoints(limit int) ([]types.OutPoint, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if int(count) > limit {
		return nil, ErrPayloadTooLarge
	}
	out := make([]types.OutPoint, 0, count)
	for i := uint32(0); i < count; i++ {
		hash, err := r.take(32)
		if err != nil {
			return nil, err
		}
		vout, err := r.readU32()
		if err != nil {
			return nil, err
		}
		var txid [32]byte
		copy(txid[:], hash)
		out = append(out, types.OutPoint{TxID: txid, Vout: vout})
	}
	return out, nil
}

func (r *reader) readAvaVotes(limit int) ([]AvaVote, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if int(count) > limit {
		return nil, ErrPayloadTooLarge
	}
	out := make([]AvaVote, 0, count)
	for i := uint32(0); i < count; i++ {
		flag, err := r.take(1)
		if err != nil {
			return nil, err
		}
		hash, err := r.take(32)
		if err != nil {
			return nil, err
		}
		vote := AvaVote{HasOpinion: flag[0] != 0}
		copy(vote.TxID[:], hash)
		out = append(out, vote)
	}
	return out, nil
}

func (r *reader) readLocatorPayload() ([][32]byte, [32]byte, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, [32]byte{}, err
	}
	if count > maxHeadersPerMessage {
		return nil, [32]byte{}, ErrPayloadTooLarge
	}
	locator := make([][32]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		hash, err := r.take(32)
		if err != nil {
			return nil, [32]byte{}, err
		}
		var out [32]byte
		copy(out[:], hash)
		locator = append(locator, out)
	}
	stop, err := r.take(32)
	if err != nil {
		return nil, [32]byte{}, err
	}
	var stopHash [32]byte
	copy(stopHash[:], stop)
	return locator, stopHash, nil
}

func (r *reader) readHeaders() ([]types.BlockHeader, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if count > maxHeadersPerMessage {
		return nil, ErrPayloadTooLarge
	}
	headers := make([]types.BlockHeader, 0, count)
	for i := uint32(0); i < count; i++ {
		buf, err := r.take(148)
		if err != nil {
			return nil, err
		}
		header, err := types.DecodeBlockHeader(buf)
		if err != nil {
			return nil, err
		}
		headers = append(headers, header)
	}
	return headers, nil
}

func (r *reader) readTxs(limit int, limits types.CodecLimits) ([]types.Transaction, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if int(count) > limit {
		return nil, ErrPayloadTooLarge
	}
	txs := make([]types.Transaction, 0, count)
	for i := uint32(0); i < count; i++ {
		txBytes, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		tx, err := types.DecodeTransactionWithLimits(txBytes, limits)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (r *reader) readPrefilledTxs(limit int, limits types.CodecLimits) ([]PrefilledTx, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if int(count) > limit {
		return nil, ErrPayloadTooLarge
	}
	items := make([]PrefilledTx, 0, count)
	for i := uint32(0); i < count; i++ {
		index, err := r.readU32()
		if err != nil {
			return nil, err
		}
		txBytes, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		tx, err := types.DecodeTransactionWithLimits(txBytes, limits)
		if err != nil {
			return nil, err
		}
		items = append(items, PrefilledTx{Index: index, Tx: tx})
	}
	return items, nil
}

func (r *reader) readHashes(limit int) ([][32]byte, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if int(count) > limit {
		return nil, ErrPayloadTooLarge
	}
	hashes := make([][32]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		raw, err := r.take(32)
		if err != nil {
			return nil, err
		}
		var hash [32]byte
		copy(hash[:], raw)
		hashes = append(hashes, hash)
	}
	return hashes, nil
}

func (r *reader) readU64s(limit int) ([]uint64, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if int(count) > limit {
		return nil, ErrPayloadTooLarge
	}
	values := make([]uint64, 0, count)
	for i := uint32(0); i < count; i++ {
		value, err := r.readU64()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (r *reader) readU32s(limit int) ([]uint32, error) {
	count, err := r.readU32()
	if err != nil {
		return nil, err
	}
	if int(count) > limit {
		return nil, ErrPayloadTooLarge
	}
	values := make([]uint32, 0, count)
	for i := uint32(0); i < count; i++ {
		value, err := r.readU32()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}
