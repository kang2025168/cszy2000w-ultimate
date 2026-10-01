"""Exercise failure/atomicity without connecting to a database or Docker."""
import gzip
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/db_backup.sh'

class BackupScriptTests(unittest.TestCase):
    def run_backup(self, body, existing=None):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        docker = root / 'docker'
        docker.write_text('#!/bin/sh\n' + body + '\n')
        docker.chmod(0o700)
        backups = root / 'backups'
        backups.mkdir()
        today = subprocess.check_output(['date', '+%F'], text=True).strip()
        output = backups / f'cszy2000_{today}.sql.gz'
        if existing:
            output.write_bytes(existing)
        old = backups / 'cszy2000_2000-01-01.sql.gz'
        old.write_bytes(b'old')
        os.utime(old, (1,1))
        env = dict(os.environ, CSZY_BACKUP_PROJECT_DIR=str(root), CSZY_DOCKER_BIN=str(docker))
        result = subprocess.run(['bash',str(SCRIPT)],env=env,text=True,capture_output=True)
        self.assertFalse(list(backups.glob('.cszy*')))
        return result, output, old

    def test_export_failure_cannot_report_success_or_replace_old_backup(self):
        result, output, old = self.run_backup("printf 'partial SQL'; exit 2", b'previous good backup')
        self.assertNotEqual(0,result.returncode)
        self.assertNotIn('OK ',result.stdout)
        self.assertEqual(b'previous good backup',output.read_bytes())
        self.assertTrue(old.exists())

    def test_empty_success_output_is_rejected(self):
        result, output, old = self.run_backup('exit 0')
        self.assertNotEqual(0,result.returncode)
        self.assertFalse(output.exists())
        self.assertTrue(old.exists())

    def test_complete_backup_rotates_only_after_success(self):
        result, output, old = self.run_backup("printf '%s\\n' 'CREATE TABLE test (id INT);' '-- Dump completed on 2026-09-30 23:00:00'")
        self.assertEqual(0,result.returncode, result.stderr)
        self.assertIn('CREATE TABLE',gzip.decompress(output.read_bytes()).decode())
        self.assertFalse(old.exists())
        self.assertEqual(0o600,output.stat().st_mode & 0o777)
