package benchmarks

import (
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	bpcrypto "bitcoin-pure/internal/crypto"
)

type SimulationTopology string

const (
	SimulationTopologyLine       SimulationTopology = "line"
	SimulationTopologyRing       SimulationTopology = "ring"
	SimulationTopologyMesh       SimulationTopology = "mesh"
	SimulationTopologySmallWorld SimulationTopology = "small-world"
)

type SimulationOptions struct {
	NodeCount            int
	Topology             SimulationTopology
	Seed                 int64
	TxCount              int
	BlockCount           int
	BaseLatency          time.Duration
	LatencyJitter        time.Duration
	TxProcessingDelay    time.Duration
	BlockProcessingDelay time.Duration
	TxSpacing            time.Duration
	BlockSpacing         time.Duration
	ChurnEvents          int
	ChurnDuration        time.Duration
	SmallWorldDegree     int
}

type SimulationReport struct {
	StartedAt   time.Time              `json:"started_at"`
	CompletedAt time.Time              `json:"completed_at"`
	Environment Environment            `json:"environment"`
	Config      SimulationConfigReport `json:"config"`
	Summary     SimulationSummary      `json:"summary"`
	Nodes       []SimulationNodeReport `json:"nodes"`
	Notes       []string               `json:"notes,omitempty"`
}

type SimulationConfigReport struct {
	NodeCount              int     `json:"node_count"`
	Topology               string  `json:"topology"`
	Seed                   int64   `json:"seed"`
	TxCount                int     `json:"tx_count"`
	BlockCount             int     `json:"block_count"`
	BaseLatencyMS          float64 `json:"base_latency_ms"`
	LatencyJitterMS        float64 `json:"latency_jitter_ms"`
	TxProcessingDelayMS    float64 `json:"tx_processing_delay_ms"`
	BlockProcessingDelayMS float64 `json:"block_processing_delay_ms"`
	TxSpacingMS            float64 `json:"tx_spacing_ms"`
	BlockSpacingMS         float64 `json:"block_spacing_ms"`
	ChurnEvents            int     `json:"churn_events"`
	ChurnDurationMS        float64 `json:"churn_duration_ms"`
	SmallWorldDegree       int     `json:"small_world_degree"`
}

type SimulationSummary struct {
	TargetCoveragePct  float64                `json:"target_coverage_pct"`
	Tx                 SimulationMetricSet    `json:"tx"`
	Block              SimulationMetricSet    `json:"block"`
	Messages           SimulationMessageStats `json:"messages"`
	ChurnEvents        int                    `json:"churn_events"`
	PeakConcurrentDown int                    `json:"peak_concurrent_down"`
}

type SimulationMetricSet struct {
	Items           int     `json:"items"`
	AvgCoveragePct  float64 `json:"avg_coverage_pct"`
	P50TargetMS     float64 `json:"p50_target_ms,omitempty"`
	P95TargetMS     float64 `json:"p95_target_ms,omitempty"`
	MaxTargetMS     float64 `json:"max_target_ms,omitempty"`
	FullCoveragePct float64 `json:"full_coverage_pct"`
	IncompleteItems int     `json:"incomplete_items"`
}

type SimulationMessageStats struct {
	Sent          int `json:"sent"`
	Delivered     int `json:"delivered"`
	Dropped       int `json:"dropped"`
	MaxQueueDepth int `json:"max_queue_depth"`
}

type SimulationNodeReport struct {
	Name       string `json:"name"`
	Peers      int    `json:"peers"`
	Sent       int    `json:"sent"`
	Delivered  int    `json:"delivered"`
	Dropped    int    `json:"dropped"`
	DownEvents int    `json:"down_events"`
	FinalDown  bool   `json:"final_down"`
}

type simItemKind string

const (
	simItemTx    simItemKind = "tx"
	simItemBlock simItemKind = "block"
)

type simNode struct {
	name       string
	neighbors  []int
	down       bool
	downEvents int
	sent       int
	delivered  int
	dropped    int
	seen       map[int]time.Duration
	known      map[int]struct{}
}

type simItem struct {
	id         int
	kind       simItemKind
	source     int
	injectedAt time.Duration
}

type simEdgeState struct {
	busyUntil time.Duration
	inFlight  int
}

type simEventKind uint8

const (
	simEventInject simEventKind = iota + 1
	simEventDeliver
	simEventNodeDown
	simEventNodeUp
)

type simEvent struct {
	at     time.Duration
	seq    int
	kind   simEventKind
	node   int
	from   int
	to     int
	itemID int
}

type simEventHeap []simEvent

func (h simEventHeap) Len() int { return len(h) }
func (h simEventHeap) Less(i, j int) bool {
	if h[i].at == h[j].at {
		return h[i].seq < h[j].seq
	}
	return h[i].at < h[j].at
}
func (h simEventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *simEventHeap) Push(x any)   { *h = append(*h, x.(simEvent)) }
func (h *simEventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func DefaultSimulationOptions() SimulationOptions {
	return SimulationOptions{
		NodeCount:            128,
		Topology:             SimulationTopologySmallWorld,
		Seed:                 1,
		TxCount:              512,
		BlockCount:           24,
		BaseLatency:          20 * time.Millisecond,
		LatencyJitter:        120 * time.Millisecond,
		TxProcessingDelay:    2 * time.Millisecond,
		BlockProcessingDelay: 12 * time.Millisecond,
		TxSpacing:            15 * time.Millisecond,
		BlockSpacing:         1500 * time.Millisecond,
		ChurnEvents:          24,
		ChurnDuration:        3 * time.Second,
		SmallWorldDegree:     6,
	}
}

func RunSimulation(ctx context.Context, opts SimulationOptions) (*SimulationReport, error) {
	opts = withSimulationDefaults(opts)
	if err := validateSimulationOptions(opts); err != nil {
		return nil, err
	}

	started := time.Now()
	engine := newSimulationEngine(opts)
	if err := engine.run(ctx); err != nil {
		return nil, err
	}

	report := &SimulationReport{
		StartedAt:   started,
		CompletedAt: time.Now(),
		Environment: Environment{
			GoOS:      runtime.GOOS,
			GoArch:    runtime.GOARCH,
			GoVersion: runtime.Version(),
			NumCPU:    runtime.NumCPU(),
		},
		Config: SimulationConfigReport{
			NodeCount:              opts.NodeCount,
			Topology:               string(opts.Topology),
			Seed:                   opts.Seed,
			TxCount:                opts.TxCount,
			BlockCount:             opts.BlockCount,
			BaseLatencyMS:          durationMS(opts.BaseLatency),
			LatencyJitterMS:        durationMS(opts.LatencyJitter),
			TxProcessingDelayMS:    durationMS(opts.TxProcessingDelay),
			BlockProcessingDelayMS: durationMS(opts.BlockProcessingDelay),
			TxSpacingMS:            durationMS(opts.TxSpacing),
			BlockSpacingMS:         durationMS(opts.BlockSpacing),
			ChurnEvents:            opts.ChurnEvents,
			ChurnDurationMS:        durationMS(opts.ChurnDuration),
			SmallWorldDegree:       opts.SmallWorldDegree,
		},
		Summary: engine.summary(),
		Nodes:   engine.nodeReports(),
		Notes: []string{
			"Deterministic relay simulation with seeded topology, asymmetric directed latency, bounded link serialization, and scheduled node churn.",
			"Target latency metrics are measured to 90% network coverage per item to stay meaningful even under churn-heavy partial outages.",
		},
	}
	return report, nil
}

func RenderSimulationMarkdown(report *SimulationReport) string {
	var b strings.Builder
	b.WriteString("# Network Simulation Report\n\n")
	b.WriteString(fmt.Sprintf("- Nodes: `%d`\n", report.Config.NodeCount))
	b.WriteString(fmt.Sprintf("- Topology: `%s`\n", report.Config.Topology))
	b.WriteString(fmt.Sprintf("- Seed: `%d`\n", report.Config.Seed))
	b.WriteString(fmt.Sprintf("- Workload: `%d` tx items, `%d` block items\n", report.Config.TxCount, report.Config.BlockCount))
	b.WriteString(fmt.Sprintf("- Latency: `%.2f ms` base + `%.2f ms` asymmetric jitter\n", report.Config.BaseLatencyMS, report.Config.LatencyJitterMS))
	b.WriteString(fmt.Sprintf("- Churn: `%d` down events, `%.2f ms` down window\n", report.Config.ChurnEvents, report.Config.ChurnDurationMS))
	b.WriteString("\n## Summary\n\n")
	b.WriteString("| Kind | Items | Avg Coverage | P50 to 90% | P95 to 90% | Max to 90% | Full Coverage | Incomplete |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, row := range []struct {
		label string
		data  SimulationMetricSet
	}{
		{"tx", report.Summary.Tx},
		{"block", report.Summary.Block},
	} {
		b.WriteString(fmt.Sprintf("| %s | %d | %.2f%% | %.2f ms | %.2f ms | %.2f ms | %.2f%% | %d |\n",
			row.label,
			row.data.Items,
			row.data.AvgCoveragePct,
			row.data.P50TargetMS,
			row.data.P95TargetMS,
			row.data.MaxTargetMS,
			row.data.FullCoveragePct,
			row.data.IncompleteItems,
		))
	}
	b.WriteString("\n## Message Pressure\n\n")
	b.WriteString("| Sent | Delivered | Dropped | Max Queue |\n")
	b.WriteString("| ---: | ---: | ---: | ---: |\n")
	b.WriteString(fmt.Sprintf("| %d | %d | %d | %d |\n",
		report.Summary.Messages.Sent,
		report.Summary.Messages.Delivered,
		report.Summary.Messages.Dropped,
		report.Summary.Messages.MaxQueueDepth,
	))
	return b.String()
}

func RenderSimulationASCIISummary(report *SimulationReport) string {
	lines := []string{
		fmt.Sprintf("Topology    %s (%d nodes, seed %d)", report.Config.Topology, report.Config.NodeCount, report.Config.Seed),
		fmt.Sprintf("Workload    %d tx items | %d block items", report.Config.TxCount, report.Config.BlockCount),
		fmt.Sprintf("Latency     %.2f ms base + %.2f ms jitter", report.Config.BaseLatencyMS, report.Config.LatencyJitterMS),
		fmt.Sprintf("Churn       %d events | %.2f ms down window | peak %d nodes down",
			report.Config.ChurnEvents,
			report.Config.ChurnDurationMS,
			report.Summary.PeakConcurrentDown,
		),
		fmt.Sprintf("TX          avg %.2f%% reach | p95 %.2f ms to 90%% | %.2f%% full coverage",
			report.Summary.Tx.AvgCoveragePct,
			report.Summary.Tx.P95TargetMS,
			report.Summary.Tx.FullCoveragePct,
		),
		fmt.Sprintf("Block       avg %.2f%% reach | p95 %.2f ms to 90%% | %.2f%% full coverage",
			report.Summary.Block.AvgCoveragePct,
			report.Summary.Block.P95TargetMS,
			report.Summary.Block.FullCoveragePct,
		),
		fmt.Sprintf("Pressure    sent %d | delivered %d | dropped %d | max queue %d",
			report.Summary.Messages.Sent,
			report.Summary.Messages.Delivered,
			report.Summary.Messages.Dropped,
			report.Summary.Messages.MaxQueueDepth,
		),
	}
	return renderASCIIBox("BPU Network Simulation", lines)
}

func WriteSimulationReportFiles(report *SimulationReport, jsonPath, markdownPath string) error {
	if jsonPath != "" {
		if err := writeJSON(jsonPath, report); err != nil {
			return err
		}
	}
	if markdownPath != "" {
		if err := writeText(markdownPath, RenderSimulationMarkdown(report)); err != nil {
			return err
		}
	}
	return nil
}

func withSimulationDefaults(opts SimulationOptions) SimulationOptions {
	defaults := DefaultSimulationOptions()
	if opts.NodeCount <= 0 {
		opts.NodeCount = defaults.NodeCount
	}
	if opts.Topology == "" {
		opts.Topology = defaults.Topology
	}
	if opts.Seed == 0 {
		opts.Seed = defaults.Seed
	}
	if opts.TxCount <= 0 {
		opts.TxCount = defaults.TxCount
	}
	if opts.BlockCount <= 0 {
		opts.BlockCount = defaults.BlockCount
	}
	if opts.BaseLatency <= 0 {
		opts.BaseLatency = defaults.BaseLatency
	}
	if opts.LatencyJitter < 0 {
		opts.LatencyJitter = defaults.LatencyJitter
	}
	if opts.TxProcessingDelay <= 0 {
		opts.TxProcessingDelay = defaults.TxProcessingDelay
	}
	if opts.BlockProcessingDelay <= 0 {
		opts.BlockProcessingDelay = defaults.BlockProcessingDelay
	}
	if opts.TxSpacing <= 0 {
		opts.TxSpacing = defaults.TxSpacing
	}
	if opts.BlockSpacing <= 0 {
		opts.BlockSpacing = defaults.BlockSpacing
	}
	if opts.ChurnEvents < 0 {
		opts.ChurnEvents = defaults.ChurnEvents
	}
	if opts.ChurnDuration <= 0 {
		opts.ChurnDuration = defaults.ChurnDuration
	}
	if opts.SmallWorldDegree <= 0 {
		opts.SmallWorldDegree = defaults.SmallWorldDegree
	}
	return opts
}

func validateSimulationOptions(opts SimulationOptions) error {
	if opts.NodeCount < 2 {
		return fmt.Errorf("simulation requires at least 2 nodes")
	}
	switch opts.Topology {
	case SimulationTopologyLine, SimulationTopologyRing, SimulationTopologyMesh, SimulationTopologySmallWorld:
	default:
		return fmt.Errorf("unsupported simulation topology: %s", opts.Topology)
	}
	if opts.SmallWorldDegree < 2 {
		return fmt.Errorf("small-world degree must be at least 2")
	}
	return nil
}

type simulationEngine struct {
	opts             SimulationOptions
	nodes            []simNode
	items            []simItem
	latency          map[[2]int]time.Duration
	linkState        map[[2]int]*simEdgeState
	events           simEventHeap
	seq              int
	targetCoverage   float64
	messageSent      int
	messageDelivered int
	messageDropped   int
	maxQueueDepth    int
	currentDown      int
	peakDown         int
}

func newSimulationEngine(opts SimulationOptions) *simulationEngine {
	engine := &simulationEngine{
		opts:           opts,
		nodes:          make([]simNode, opts.NodeCount),
		latency:        make(map[[2]int]time.Duration),
		linkState:      make(map[[2]int]*simEdgeState),
		targetCoverage: 90,
	}
	for i := range engine.nodes {
		engine.nodes[i] = simNode{
			name:  fmt.Sprintf("node-%03d", i),
			seen:  make(map[int]time.Duration),
			known: make(map[int]struct{}),
		}
	}
	engine.buildTopology()
	engine.buildWorkload()
	engine.scheduleChurn()
	heap.Init(&engine.events)
	return engine
}

func (e *simulationEngine) run(ctx context.Context) error {
	for e.events.Len() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		event := heap.Pop(&e.events).(simEvent)
		switch event.kind {
		case simEventInject:
			e.handleInject(event)
		case simEventDeliver:
			e.handleDeliver(event)
		case simEventNodeDown:
			e.handleNodeDown(event.node)
		case simEventNodeUp:
			e.handleNodeUp(event.node)
		}
	}
	return nil
}

func (e *simulationEngine) buildTopology() {
	switch e.opts.Topology {
	case SimulationTopologyLine:
		for i := 0; i+1 < e.opts.NodeCount; i++ {
			e.connect(i, i+1)
			e.connect(i+1, i)
		}
	case SimulationTopologyRing:
		for i := 0; i < e.opts.NodeCount; i++ {
			next := (i + 1) % e.opts.NodeCount
			e.connect(i, next)
			e.connect(next, i)
		}
	case SimulationTopologyMesh:
		for i := 0; i < e.opts.NodeCount; i++ {
			for j := 0; j < e.opts.NodeCount; j++ {
				if i == j {
					continue
				}
				e.connect(i, j)
			}
		}
	case SimulationTopologySmallWorld:
		e.buildSmallWorld()
	}
}

func (e *simulationEngine) buildSmallWorld() {
	n := e.opts.NodeCount
	degree := e.opts.SmallWorldDegree
	if degree >= n {
		degree = n - 1
	}
	if degree%2 != 0 {
		degree++
	}
	if degree < 2 {
		degree = 2
	}
	half := degree / 2
	for i := 0; i < n; i++ {
		for step := 1; step <= half; step++ {
			j := (i + step) % n
			k := (i - step + n) % n
			e.connect(i, j)
			e.connect(i, k)
		}
	}
	for i := 0; i < n; i++ {
		shortcut := e.hashInt(int64(i), 0, n-1)
		if shortcut == i {
			shortcut = (shortcut + 1) % n
		}
		e.connect(i, shortcut)
	}
}

func (e *simulationEngine) connect(from, to int) {
	if from == to {
		return
	}
	if slices.Contains(e.nodes[from].neighbors, to) {
		return
	}
	e.nodes[from].neighbors = append(e.nodes[from].neighbors, to)
	key := [2]int{from, to}
	if _, ok := e.linkState[key]; !ok {
		e.linkState[key] = &simEdgeState{}
	}
	e.latency[key] = e.edgeLatency(from, to)
}

func (e *simulationEngine) buildWorkload() {
	for i := 0; i < e.opts.TxCount; i++ {
		item := simItem{
			id:         len(e.items),
			kind:       simItemTx,
			source:     e.hashInt(int64(i)+11, 0, e.opts.NodeCount-1),
			injectedAt: time.Duration(i) * e.opts.TxSpacing,
		}
		e.items = append(e.items, item)
		e.push(simEvent{at: item.injectedAt, kind: simEventInject, node: item.source, itemID: item.id})
	}
	for i := 0; i < e.opts.BlockCount; i++ {
		item := simItem{
			id:         len(e.items),
			kind:       simItemBlock,
			source:     e.hashInt(int64(i)+7_001, 0, e.opts.NodeCount-1),
			injectedAt: time.Duration(i) * e.opts.BlockSpacing,
		}
		e.items = append(e.items, item)
		e.push(simEvent{at: item.injectedAt, kind: simEventInject, node: item.source, itemID: item.id})
	}
}

func (e *simulationEngine) scheduleChurn() {
	if e.opts.ChurnEvents == 0 {
		return
	}
	horizon := time.Duration(max(1, e.opts.TxCount-1))*e.opts.TxSpacing + time.Duration(max(1, e.opts.BlockCount))*e.opts.BlockSpacing + e.opts.ChurnDuration
	for i := 0; i < e.opts.ChurnEvents; i++ {
		node := e.hashInt(int64(i)+9_001, 0, e.opts.NodeCount-1)
		start := time.Duration(e.hashUint64(int64(i)+12_001) % uint64(horizon))
		e.push(simEvent{at: start, kind: simEventNodeDown, node: node})
		e.push(simEvent{at: start + e.opts.ChurnDuration, kind: simEventNodeUp, node: node})
	}
}

func (e *simulationEngine) handleInject(event simEvent) {
	e.observe(event.node, event.itemID, event.at)
	e.forward(event.node, -1, event.itemID, event.at)
}

func (e *simulationEngine) handleDeliver(event simEvent) {
	key := [2]int{event.from, event.to}
	if state, ok := e.linkState[key]; ok && state.inFlight > 0 {
		state.inFlight--
	}
	if e.nodes[event.to].down || e.nodes[event.from].down {
		e.messageDropped++
		e.nodes[event.to].dropped++
		return
	}
	e.messageDelivered++
	e.observe(event.to, event.itemID, event.at)
	e.forward(event.to, event.from, event.itemID, event.at)
}

func (e *simulationEngine) observe(nodeIndex, itemID int, at time.Duration) {
	node := &e.nodes[nodeIndex]
	if _, ok := node.known[itemID]; ok {
		return
	}
	node.known[itemID] = struct{}{}
	node.seen[itemID] = at
}

func (e *simulationEngine) forward(nodeIndex, from, itemID int, at time.Duration) {
	node := &e.nodes[nodeIndex]
	item := e.items[itemID]
	for _, peer := range node.neighbors {
		if peer == from {
			continue
		}
		if _, ok := e.nodes[peer].known[itemID]; ok {
			continue
		}
		key := [2]int{nodeIndex, peer}
		state := e.linkState[key]
		start := at
		if state.busyUntil > start {
			start = state.busyUntil
		}
		processing := e.opts.TxProcessingDelay
		if item.kind == simItemBlock {
			processing = e.opts.BlockProcessingDelay
		}
		serialization := processing / 2
		if serialization <= 0 {
			serialization = time.Millisecond
		}
		deliverAt := start + processing + e.latency[key]
		state.busyUntil = start + serialization
		state.inFlight++
		if state.inFlight > e.maxQueueDepth {
			e.maxQueueDepth = state.inFlight
		}
		node.sent++
		e.messageSent++
		e.push(simEvent{
			at:     deliverAt,
			kind:   simEventDeliver,
			from:   nodeIndex,
			to:     peer,
			itemID: itemID,
		})
	}
}

func (e *simulationEngine) handleNodeDown(node int) {
	if e.nodes[node].down {
		return
	}
	e.nodes[node].down = true
	e.nodes[node].downEvents++
	e.currentDown++
	if e.currentDown > e.peakDown {
		e.peakDown = e.currentDown
	}
}

func (e *simulationEngine) handleNodeUp(node int) {
	if !e.nodes[node].down {
		return
	}
	e.nodes[node].down = false
	if e.currentDown > 0 {
		e.currentDown--
	}
}

func (e *simulationEngine) push(event simEvent) {
	event.seq = e.seq
	e.seq++
	heap.Push(&e.events, event)
}

func (e *simulationEngine) edgeLatency(from, to int) time.Duration {
	if e.opts.LatencyJitter <= 0 {
		return e.opts.BaseLatency
	}
	jitter := time.Duration(e.hashUint64(int64(from)<<32|int64(to)) % uint64(e.opts.LatencyJitter))
	return e.opts.BaseLatency + jitter
}

func (e *simulationEngine) hashUint64(seed int64) uint64 {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(e.opts.Seed))
	binary.LittleEndian.PutUint64(buf[8:], uint64(seed))
	sum := bpcrypto.Sha256d(buf[:])
	return binary.LittleEndian.Uint64(sum[:8])
}

func (e *simulationEngine) hashInt(seed int64, minValue, maxValue int) int {
	if maxValue <= minValue {
		return minValue
	}
	span := uint64(maxValue - minValue + 1)
	return minValue + int(e.hashUint64(seed)%span)
}

func (e *simulationEngine) summary() SimulationSummary {
	return SimulationSummary{
		TargetCoveragePct: e.targetCoverage,
		Tx:                e.metricSet(simItemTx),
		Block:             e.metricSet(simItemBlock),
		Messages: SimulationMessageStats{
			Sent:          e.messageSent,
			Delivered:     e.messageDelivered,
			Dropped:       e.messageDropped,
			MaxQueueDepth: e.maxQueueDepth,
		},
		ChurnEvents:        e.opts.ChurnEvents,
		PeakConcurrentDown: e.peakDown,
	}
}

func (e *simulationEngine) metricSet(kind simItemKind) SimulationMetricSet {
	targetDurations := make([]float64, 0)
	fullCoverage := 0
	incomplete := 0
	var totalCoverage float64
	items := 0
	targetNodes := int(float64(e.opts.NodeCount)*e.targetCoverage/100.0 + 0.9999)
	for _, item := range e.items {
		if item.kind != kind {
			continue
		}
		items++
		seenTimes := make([]time.Duration, 0, e.opts.NodeCount)
		for i := range e.nodes {
			if seenAt, ok := e.nodes[i].seen[item.id]; ok {
				seenTimes = append(seenTimes, seenAt-item.injectedAt)
			}
		}
		totalCoverage += float64(len(seenTimes)) * 100 / float64(e.opts.NodeCount)
		if len(seenTimes) == e.opts.NodeCount {
			fullCoverage++
		}
		if len(seenTimes) < targetNodes {
			incomplete++
			continue
		}
		slices.Sort(seenTimes)
		targetDurations = append(targetDurations, durationMS(seenTimes[targetNodes-1]))
	}
	metrics := SimulationMetricSet{Items: items}
	if items == 0 {
		return metrics
	}
	metrics.AvgCoveragePct = totalCoverage / float64(items)
	metrics.FullCoveragePct = float64(fullCoverage) * 100 / float64(items)
	metrics.IncompleteItems = incomplete
	if len(targetDurations) != 0 {
		slices.Sort(targetDurations)
		metrics.P50TargetMS = percentile(targetDurations, 50)
		metrics.P95TargetMS = percentile(targetDurations, 95)
		metrics.MaxTargetMS = targetDurations[len(targetDurations)-1]
	}
	return metrics
}

func (e *simulationEngine) nodeReports() []SimulationNodeReport {
	out := make([]SimulationNodeReport, 0, len(e.nodes))
	for _, node := range e.nodes {
		out = append(out, SimulationNodeReport{
			Name:       node.name,
			Peers:      len(node.neighbors),
			Sent:       node.sent,
			Delivered:  len(node.seen),
			Dropped:    node.dropped,
			DownEvents: node.downEvents,
			FinalDown:  node.down,
		})
	}
	return out
}

func percentile(samples []float64, pct int) float64 {
	if len(samples) == 0 {
		return 0
	}
	if len(samples) == 1 {
		return samples[0]
	}
	index := (len(samples) - 1) * pct / 100
	return samples[index]
}

func WriteSimulationReportDir(report *SimulationReport, dir string) error {
	root := filepath.Clean(dir)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	return WriteSimulationReportFiles(
		report,
		filepath.Join(root, "simulation.json"),
		filepath.Join(root, "simulation.md"),
	)
}

func marshalSimulationReport(report *SimulationReport) ([]byte, error) {
	return json.MarshalIndent(report, "", "  ")
}
