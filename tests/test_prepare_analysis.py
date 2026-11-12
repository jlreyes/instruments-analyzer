"""CLI integration checks using real DuckDB files (no xctrace substitution)."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'prepare_analysis.py'


class PrepareAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Path(self.directory.name) / 'analysis.duckdb'
        with duckdb.connect(str(self.db)) as conn:
            conn.execute('CREATE TABLE marker (value INTEGER)')

    def run_cli(self, *args, database=None, cwd=None):
        return subprocess.run([sys.executable, str(SCRIPT), str(database or self.db), *args],
                              capture_output=True, text=True, cwd=cwd)

    def source(self, rows=()):
        with duckdb.connect(str(self.db)) as conn:
            conn.execute('''CREATE TABLE hitches_frame_lifetimes (
                swap_id BIGINT, start_ns BIGINT, duration_ns BIGINT,
                frame_color VARCHAR, display VARCHAR, surface_id INTEGER)''')
            if rows:
                conn.executemany('INSERT INTO hitches_frame_lifetimes VALUES (?, ?, ?, ?, ?, ?)', rows)

    def status(self, status):
        with duckdb.connect(str(self.db)) as conn:
            conn.execute('''CREATE TABLE _export_status (schema_name VARCHAR, table_name VARCHAR,
                         status VARCHAR, row_count BIGINT, error VARCHAR)''')
            conn.execute('INSERT INTO _export_status VALUES (?, ?, ?, 0, ?)',
                         ['hitches-frame-lifetimes', 'hitches_frame_lifetimes', status,
                          'export parser incompatible' if status == 'failed' else None])

    def test_missing_and_empty_frames_are_unavailable_not_smooth(self):
        for status in ['not_recorded', 'empty', 'failed', 'unsupported']:
            with self.subTest(status=status):
                with duckdb.connect(str(self.db)) as conn:
                    conn.execute('DROP TABLE IF EXISTS _export_status')
                self.status(status)
                result = self.run_cli()
                self.assertEqual(result.returncode, 2 if status in ['empty', 'not_recorded'] else 1)
                self.assertIn(status, result.stderr)
                self.assertNotIn('Traceback', result.stderr)
                self.assertNotIn('Dropped frames:', result.stdout)
        with duckdb.connect(str(self.db)) as conn:
            conn.execute('DROP TABLE _export_status')
        self.source()
        result = self.run_cli()
        self.assertEqual(result.returncode, 2)
        self.assertIn('empty', result.stderr)

    def test_views_survive_reopen_and_second_prepare_replaces_budget_and_window(self):
        self.source([(1, 0, 10_000_000, 'Green', 'A', 1),
                     (2, 1_000_000_000, 30_000_000, 'Red', 'A', 1),
                     (3, 2_000_000_000, 40_000_000, 'Red', 'A', 1)])
        result = self.run_cli('--budget-ms', '10', '--start-s', '1', '--end-s', '2')
        self.assertEqual(result.returncode, 0, result.stderr)
        with duckdb.connect(str(self.db)) as conn:
            self.assertEqual(conn.execute('SELECT swap_id, budget_ms, missed_frames FROM frames').fetchall(),
                             [(2, 10.0, 2.0)])
        result = self.run_cli('--budget-ms', '20')
        self.assertEqual(result.returncode, 0, result.stderr)
        with duckdb.connect(str(self.db)) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*), MAX(budget_ms) FROM frames').fetchone(), (3, 20.0))
        result = self.run_cli('--start-s', '9', '--end-s', '10')
        self.assertEqual(result.returncode, 2, result.stderr)
        with duckdb.connect(str(self.db)) as conn:
            self.assertNotIn(('frames',), conn.execute('SHOW TABLES').fetchall())

    def test_invalid_inputs_do_not_create_derived_views(self):
        self.source([(1, 0, 10_000_000, 'Green', 'A', 1)])
        for args in [('--budget-ms', 'nan'), ('--budget-ms', 'inf'),
                     ('--start-s', '2', '--end-s', '1'), ('--context-frames', '-1')]:
            with self.subTest(args=args):
                result = self.run_cli(*args)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('Traceback', result.stderr)
        with duckdb.connect(str(self.db)) as conn:
            self.assertNotIn(('frames',), conn.execute('SHOW TABLES').fetchall())

    def test_preceding_context_stays_on_target_surface_and_reports_heuristic(self):
        self.source([(1, 0, 40_000_000, 'Red', 'A', 1),
                     (2, 50_000_000, 90_000_000, 'Red', 'B', 2),
                     (3, 200_000_000, 35_000_000, 'Red', 'A', 1)])
        result = self.run_cli('--swap-id', '3', '--budget-ms', '20')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('| -1 | 1 |', result.stdout)
        self.assertNotIn('| -1 | 2 |', result.stdout)
        self.assertIn('heuristic', result.stdout.lower())
        self.assertNotIn('**ROOT CAUSE**', result.stdout)
        self.assertIn('unavailable', result.stdout.lower())  # Optional logs/signposts were not recorded.

    def test_invalid_source_and_ambiguous_frame_fail_with_reason(self):
        self.source([(1, 0, -1, 'Red', 'A', 1)])
        result = self.run_cli()
        self.assertEqual(result.returncode, 1)
        self.assertIn('invalid', result.stderr.lower())
        with duckdb.connect(str(self.db)) as conn:
            conn.execute('UPDATE hitches_frame_lifetimes SET duration_ns = 1')
            conn.execute("INSERT INTO hitches_frame_lifetimes VALUES (1, 2, 1, 'Red', 'B', 2)")
        result = self.run_cli('--swap-id', '1')
        self.assertEqual(result.returncode, 1)
        self.assertIn('ambiguous', result.stderr.lower())

    def test_relative_database_path_with_parquet_metadata_reopens_same_file(self):
        root = Path(self.directory.name)
        (root / 'parquet').mkdir()
        self.source([(1, 0, 10_000_000, 'Green', 'A', 1)])
        with duckdb.connect(str(self.db)) as conn:
            conn.execute('CREATE TABLE metadata (key VARCHAR, value VARCHAR)')
            conn.execute("INSERT INTO metadata VALUES ('parquet_dir', 'parquet')")
        result = self.run_cli(database=self.db.relative_to(root.parent), cwd=root.parent)
        self.assertEqual(result.returncode, 0, result.stderr)
        with duckdb.connect(str(self.db)) as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM frames').fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
