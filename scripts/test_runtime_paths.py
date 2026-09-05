import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import runtime_paths


class RuntimePathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.data = self.root / "data"
        self.logs = self.root / "logs"
        self.config = {"db_path": str(self.data / "chain"), "log_path": str(self.logs / "events.jsonl")}

    def test_config_paths_stay_in_install_roots(self):
        paths, wallet = runtime_paths.configured_paths(self.config, str(self.data), str(self.logs))
        self.assertEqual(paths, [str(self.data / "chain"), str(self.data / "wallets"), str(self.logs)])
        self.assertEqual(wallet, str(self.data / "wallets"))
        for bad in ("/etc/chain", str(self.data) + "-other/chain", str(self.data / ".." / "chain"), str(self.data / "a b"), str(self.data / "%h"), str(self.data / "a\nb")):
            with self.subTest(path=bad), self.assertRaises(ValueError):
                runtime_paths.configured_paths(dict(self.config, db_path=bad), str(self.data), str(self.logs))

    def test_directory_walk_refuses_symlink_components(self):
        outside = self.root / "outside"
        outside.mkdir()
        self.data.symlink_to(outside)
        with self.assertRaises(OSError):
            runtime_paths.open_directory(str(self.data / "new"))
        self.assertFalse((outside / "new").exists())

    def test_ownership_refuses_symlinks_hardlinks_and_fifos(self):
        self.data.mkdir()
        target = self.root / "target"
        target.write_text("unchanged")
        (self.data / "symlink").symlink_to(target)
        os.link(target, self.data / "hardlink")
        os.mkfifo(self.data / "fifo")
        fd = runtime_paths.open_directory(str(self.data))
        self.addCleanup(os.close, fd)
        with patch.object(runtime_paths.os, "fchown") as chown:
            for name in ("symlink", "hardlink", "fifo"):
                with self.subTest(name=name), self.assertRaises((OSError, ValueError)):
                    runtime_paths.own_regular_file(fd, name, os.getuid(), os.getgid())
            chown.assert_not_called()
        self.assertEqual(target.read_text(), "unchanged")

    def test_pinned_directory_survives_path_replacement(self):
        self.data.mkdir()
        (self.data / "wallets.json").write_text("original")
        outside = self.root / "outside"
        outside.mkdir()
        (outside / "wallets.json").write_text("outside")
        expected = (self.data / "wallets.json").stat().st_ino
        fd = runtime_paths.open_directory(str(self.data))
        self.addCleanup(os.close, fd)
        self.data.rename(self.root / "moved")
        self.data.symlink_to(outside)
        touched = []
        with patch.object(runtime_paths.os, "fchown", side_effect=lambda opened, uid, gid: touched.append(os.fstat(opened).st_ino)):
            runtime_paths.own_regular_file(fd, "wallets.json", os.getuid(), os.getgid())
        self.assertEqual(touched, [expected])
        self.assertEqual((outside / "wallets.json").read_text(), "outside")

    def test_preparation_keeps_config_owned_by_root(self):
        config_root = self.root / "config"
        config_root.mkdir()
        config_file = config_root / "config.yaml"
        config_file.write_text("test config")
        wallet_dir = self.data / "wallets"
        wallet_dir.mkdir(parents=True)
        wallet_file = wallet_dir / "wallets.json"
        wallet_file.write_text("[]")
        os.chmod(wallet_file, 0o644)
        touched = {}

        def record(fd, uid, gid):
            touched[os.fstat(fd).st_ino] = (uid, gid)

        service_uid, service_gid = 1234, 1235
        with patch.object(runtime_paths.os, "fchown", side_effect=record):
            runtime_paths.prepare(self.config, str(self.data), str(self.logs), str(config_root), service_uid, service_gid)
        self.assertEqual(touched[config_root.stat().st_ino], (0, 0))
        self.assertEqual(touched[config_file.stat().st_ino], (0, service_gid))
        self.assertEqual(touched[wallet_file.stat().st_ino], (service_uid, service_gid))
        self.assertEqual(config_file.stat().st_mode & 0o777, 0o640)
        self.assertEqual(wallet_file.stat().st_mode & 0o777, 0o600)

    def test_rendered_unit_excludes_config_directory(self):
        stage = self.root / "stage"
        artifacts = stage / ".artifacts"
        artifacts.mkdir(parents=True)
        (stage / "scripts").symlink_to(Path(__file__).resolve().parent)
        (artifacts / "config.json").write_text(json.dumps(self.config))
        source = Path(__file__).with_name("update.sh").read_text()
        function = source[source.index("render_unit() {"):source.index("\nrender_motd() {")]
        script = function + '\nSTAGE_DIR="$1"; DATA_DIR="$2"; LOG_DIR="$3"; CONFIG_PATH="$4"; SERVICE_NAME=bitcoin-pure; CURRENT_LINK=/opt/bitcoin-pure/current; render_unit\n'
        subprocess.run(["bash", "-c", script, "test", str(stage), str(self.data), str(self.logs), str(self.root / "config" / "config.yaml")], check=True)
        unit = (artifacts / "bitcoin-pure.service").read_text()
        writable = next(line for line in unit.splitlines() if line.startswith("ReadWritePaths="))
        self.assertNotIn(str(self.root / "config"), writable)
        self.assertIn(str(self.data / "wallets"), writable)

    def test_install_ignores_planted_fixed_temp_symlink(self):
        source = Path(__file__).with_name("bootstrap.sh").read_text()
        function = source[source.index("install_candidate_file() {"):source.index("\nfiles_match() {")]
        target, destination, candidate = [self.root / name for name in ("target", "config.yaml", "candidate")]
        target.write_text("unchanged")
        candidate.write_text("replacement")
        Path(str(destination) + ".new").symlink_to(target)
        subprocess.run(["bash", "-c", function + '\ninstall_candidate_file "$1" "$2" 600\n', "test", str(candidate), str(destination)], check=True)
        self.assertEqual(target.read_text(), "unchanged")
        self.assertEqual(destination.read_text(), "replacement")


if __name__ == "__main__":
    unittest.main()
