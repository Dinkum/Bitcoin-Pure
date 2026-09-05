package wallet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// beginMutation serializes reload, mutation, and publication across CLI processes.
// The lock file is never renamed or removed: locking wallets.json would lock an
// obsolete inode as soon as an atomic wallet write replaces that file.
func (s *Store) beginMutation() (func(), error) {
	if s.path == "" {
		return nil, errors.New("wallet store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(s.path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wallet lock: %w", err)
	}
	unlock := func() { _ = lock.Close() }
	info, err := lock.Stat()
	if err != nil {
		unlock()
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
		unlock()
		return nil, errors.New("wallet lock must be a regular file with one link")
	}
	if err := lock.Chmod(0o600); err != nil {
		unlock()
		return nil, err
	}
	// Root-run maintenance must leave the stable lock usable by the wallet owner.
	if os.Geteuid() == 0 {
		uid, gid, ok := existingFileOwner(s.path)
		if !ok {
			uid, gid, ok = existingFileOwner(filepath.Dir(s.path))
		}
		if ok {
			if err := lock.Chown(uid, gid); err != nil {
				unlock()
				return nil, err
			}
		}
	}
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		unlock()
		return nil, fmt.Errorf("lock wallet: %w", err)
	}
	latest, err := OpenWithProfile(s.path, s.profile)
	if err != nil {
		unlock()
		return nil, fmt.Errorf("reload locked wallet: %w", err)
	}
	s.wallets = latest.wallets
	return unlock, nil
}
