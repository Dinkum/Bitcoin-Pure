package profiling

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	runtimepprof "runtime/pprof"
	"strings"
)

type Artifact struct {
	Kind string `json:"kind"`
	Path string `json:"path"`
}

type CaptureConfig struct {
	Dir       string
	Prefix    string
	CPU       bool
	Heap      bool
	Mutex     bool
	Block     bool
	MutexRate int
	BlockRate int
}

type Capture struct {
	cfg               CaptureConfig
	cpuFile           *os.File
	artifacts         []Artifact
	previousMutexRate int
}

func DefaultCaptureConfig(dir, prefix string) CaptureConfig {
	return CaptureConfig{
		Dir:       dir,
		Prefix:    prefix,
		CPU:       true,
		Heap:      true,
		Mutex:     true,
		Block:     true,
		MutexRate: 1,
		BlockRate: 1,
	}
}

func StartCapture(cfg CaptureConfig) (*Capture, error) {
	if strings.TrimSpace(cfg.Dir) == "" {
		return nil, nil
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "profile-"
	}
	if cfg.MutexRate <= 0 {
		cfg.MutexRate = 1
	}
	if cfg.BlockRate <= 0 {
		cfg.BlockRate = 1
	}
	if err := os.MkdirAll(filepath.Clean(cfg.Dir), 0o755); err != nil {
		return nil, err
	}
	capture := &Capture{cfg: cfg}
	if cfg.Mutex {
		capture.previousMutexRate = runtime.SetMutexProfileFraction(cfg.MutexRate)
	}
	if cfg.Block {
		runtime.SetBlockProfileRate(cfg.BlockRate)
	}
	if cfg.CPU {
		path := filepath.Join(cfg.Dir, cfg.Prefix+"cpu.pprof")
		file, err := os.Create(filepath.Clean(path))
		if err != nil {
			capture.stopRates()
			return nil, err
		}
		if err := runtimepprof.StartCPUProfile(file); err != nil {
			_ = file.Close()
			capture.stopRates()
			return nil, err
		}
		capture.cpuFile = file
		capture.artifacts = append(capture.artifacts, Artifact{Kind: "cpu", Path: path})
	}
	return capture, nil
}

func (c *Capture) Stop() ([]Artifact, error) {
	if c == nil {
		return nil, nil
	}
	var firstErr error
	if c.cpuFile != nil {
		runtimepprof.StopCPUProfile()
		if err := c.cpuFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.cpuFile = nil
	}
	if c.cfg.Heap {
		runtime.GC()
		if err := c.writeProfile("heap", "heap"); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.cfg.Mutex {
		if err := c.writeProfile("mutex", "mutex"); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.cfg.Block {
		if err := c.writeProfile("block", "block"); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.stopRates()
	return append([]Artifact(nil), c.artifacts...), firstErr
}

func (c *Capture) stopRates() {
	if c.cfg.Mutex {
		runtime.SetMutexProfileFraction(c.previousMutexRate)
	}
	if c.cfg.Block {
		runtime.SetBlockProfileRate(0)
	}
}

func (c *Capture) writeProfile(profileName, artifactKind string) error {
	profile := runtimepprof.Lookup(profileName)
	if profile == nil {
		return fmt.Errorf("runtime profile %q is unavailable", profileName)
	}
	path := filepath.Join(c.cfg.Dir, c.cfg.Prefix+artifactKind+".pprof")
	file, err := os.Create(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer file.Close()
	if err := profile.WriteTo(file, 0); err != nil {
		return err
	}
	c.artifacts = append(c.artifacts, Artifact{Kind: artifactKind, Path: path})
	return nil
}
