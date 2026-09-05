#!/usr/bin/env python3
"""Validate installed runtime paths and prepare ownership without following links."""

import argparse
import grp
import json
import os
import pwd
import re
import stat


def canonical_path(path):
    # These paths are also embedded in a systemd unit. Reject expansion tokens,
    # whitespace and ambiguous spelling instead of trying to quote two grammars.
    if not isinstance(path, str) or not path.startswith("/") or path.startswith("//"):
        raise ValueError("runtime path must be absolute")
    if path != os.path.normpath(path) or not re.fullmatch(r"/[A-Za-z0-9_./-]+", path):
        raise ValueError(f"runtime path must be canonical and contain no expansion characters: {path!r}")
    return path


def beneath(path, root):
    return os.path.commonpath((path, root)) == root


def configured_paths(config, data_root, log_root):
    data_root, log_root = canonical_path(data_root), canonical_path(log_root)
    db = canonical_path(config["db_path"])
    wallet = os.path.join(os.path.dirname(db), "wallets")
    if db == data_root or not beneath(db, data_root) or not beneath(wallet, data_root):
        raise ValueError(f"installed db_path and wallets must be below {data_root}")
    paths = [db, wallet]
    if config.get("log_path"):
        log = canonical_path(config["log_path"])
        if log == log_root or not beneath(log, log_root):
            raise ValueError(f"installed log_path must be below {log_root}")
        paths.append(os.path.dirname(log))
    return list(dict.fromkeys(paths)), wallet


def open_directory(path):
    """Walk from / using pinned descriptors; a swapped symlink never gets followed."""
    path = canonical_path(path)
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.split("/")[1:]:
            try:
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            except FileNotFoundError:
                try:
                    os.mkdir(component, 0o755, dir_fd=fd)
                except FileExistsError:
                    pass  # A racing creator still has to pass O_NOFOLLOW below.
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def own_regular_file(directory_fd, name, uid, gid, mode=None):
    # O_NONBLOCK prevents a malicious FIFO from hanging the privileged updater.
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except FileNotFoundError:
        return
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError(f"{name} must be a regular file with one link")
        os.fchown(fd, uid, gid)
        if mode is not None:
            os.fchmod(fd, mode)
    finally:
        os.close(fd)


def prepare(config, data_root, log_root, config_root, uid, gid):
    paths, wallet = configured_paths(config, data_root, log_root)
    # Config is consumed by root during updates. The daemon must not be able to
    # replace it, including through the ownership of its containing directory.
    fd = open_directory(config_root)
    try:
        os.fchown(fd, 0, 0)
        os.fchmod(fd, 0o755)
        own_regular_file(fd, "config.yaml", 0, gid, 0o640)
        own_regular_file(fd, "config.json", 0, gid, 0o640)
    finally:
        os.close(fd)
    for path in dict.fromkeys([data_root, log_root, *paths]):
        fd = open_directory(path)
        try:
            os.fchown(fd, uid, gid)
            if path == wallet:
                own_regular_file(fd, "wallets.json", uid, gid, 0o600)
                own_regular_file(fd, "wallets.json.lock", uid, gid, 0o600)
        finally:
            os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("list", "prepare"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--log-root", required=True)
    parser.add_argument("--config-root")
    parser.add_argument("--user")
    parser.add_argument("--group")
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as source:
        config = json.load(source)
    if args.mode == "list":
        paths, _ = configured_paths(config, args.data_root, args.log_root)
        print("\n".join(paths))
    else:
        if os.geteuid() != 0 or not all((args.config_root, args.user, args.group)):
            parser.error("prepare requires root, --config-root, --user and --group")
        prepare(config, args.data_root, args.log_root, args.config_root,
                pwd.getpwnam(args.user).pw_uid, grp.getgrnam(args.group).gr_gid)


if __name__ == "__main__":
    main()
