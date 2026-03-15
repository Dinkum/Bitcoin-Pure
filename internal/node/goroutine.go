package node

import (
	"log/slog"
	"runtime/debug"
)

// safeGo runs a service-owned goroutine behind a panic barrier so a bad peer,
// accumulator invariant, or RNG failure cannot take down the whole node.
func (s *Service) safeGo(name string, fn func()) {
	s.safeGoWithCleanup(name, nil, fn)
}

func (s *Service) safeGoWithCleanup(name string, cleanup func(), fn func()) {
	s.wg.Add(1)
	go s.runGuarded(name, true, cleanup, fn)
}

func (s *Service) safeGoDetached(name string, fn func()) {
	s.safeGoDetachedWithCleanup(name, nil, fn)
}

func (s *Service) safeGoDetachedWithCleanup(name string, cleanup func(), fn func()) {
	go s.runGuarded(name, false, cleanup, fn)
}

func (s *Service) runGuarded(name string, managed bool, cleanup func(), fn func()) {
	if managed {
		defer s.wg.Done()
	}
	defer s.recoverGoroutine(name, cleanup)
	fn()
}

func (s *Service) recoverGoroutine(name string, cleanup func()) {
	if recovered := recover(); recovered != nil {
		logger := s.logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error("goroutine panic",
			slog.String("goroutine", name),
			slog.Any("panic", recovered),
			slog.String("stack", string(debug.Stack())),
		)
		if cleanup != nil {
			s.runPanicCleanup(logger, name, cleanup)
		}
	}
}

func (s *Service) runPanicCleanup(logger *slog.Logger, name string, cleanup func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Error("goroutine panic cleanup failed",
				slog.String("goroutine", name),
				slog.Any("panic", recovered),
				slog.String("stack", string(debug.Stack())),
			)
		}
	}()
	cleanup()
}
