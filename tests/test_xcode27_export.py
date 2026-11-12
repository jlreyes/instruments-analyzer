"""Recorded XML -> real Arrow/Parquet/DuckDB. No mocked boundaries.

These integration checks catch lost references, shifted columns, and wrong CPU
weight units. Small encoding variants are described in fixtures/README.md.
"""
import io
import json
import tempfile
import unittest
from pathlib import Path

import duckdb
from lxml import etree as ET
import pyarrow.parquet as pq

from test_export_to_duckdb import export_to_duckdb as exporter

FIXTURES = Path(__file__).parent / 'fixtures'


class Xcode27ExportTests(unittest.TestCase):
    def parse(self, fixture, parser, **kwargs):
        data = (FIXTURES / fixture).read_bytes()
        stream = io.BytesIO(data)
        refs = exporter.build_ref_map(stream)
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'data.parquet')
            count = parser(stream, refs, path, **kwargs)
            rows = pq.read_table(path).to_pylist()
        self.assertEqual(count, len(rows))
        return rows

    def test_tagged_stacks_and_raw_refs_preserve_cpu_samples(self):
        rows = self.parse('xcode27_time_profile.xml', exporter.parse_time_profile_to_parquet)
        self.assertEqual([r['time_ns'] for r in rows], [1082909541, 1082909541, 0])
        self.assertEqual([r['thread_tid'] for r in rows], [889851] * 3)
        self.assertEqual([r['core_id'] for r in rows], [1] * 3)
        self.assertEqual([r['weight_ns'] for r in rows], [100000] * 3)
        stacks = [json.loads(r['backtrace_json']) for r in rows]
        self.assertEqual(stacks[0], stacks[1])
        self.assertEqual(stacks[0][2]['name'], 'dyld4::APIs::_dyld_find_protocol_conformance(void const*, void const*, void const*) const')
        self.assertEqual(stacks[0][2]['binary'], 'dyld')
        self.assertEqual(stacks[0][2]['path'], '/usr/lib/dyld')

    def test_shifted_update_columns_resolve_node_identity(self):
        rows = self.parse('xcode27_updates.xml', exporter.parse_updates_to_parquet)
        self.assertEqual([r['cause_graph_node_id'] for r in rows], ['21', '21'])
        self.assertEqual(rows[0]['start_ns'], 17627749625)
        self.assertEqual(rows[0]['duration_ns'], 9208)
        self.assertEqual(rows[0]['description'], 'Preference List')
        self.assertEqual(rows[0]['allocations'], 0)

    def test_hang_process_and_thread_refs_resolve(self):
        rows = self.parse('xcode27_hangs.xml', exporter.parse_potential_hangs_to_parquet)
        self.assertEqual([r['process_name'] for r in rows], ['TodoInstruments'] * 2)
        self.assertEqual([r['thread_tid'] for r in rows], [911180] * 2)
        self.assertEqual(rows[0]['start_ns'], 2195788375)
        self.assertEqual(rows[0]['duration_ns'], 140097333)

    def test_typed_array_causal_descriptions_resolve_refs(self):
        rows = self.parse('xcode27_causes.xml', exporter.parse_swiftui_causes_to_parquet)
        self.assertEqual(rows[0]['source_event_id'], 5762)
        self.assertEqual(rows[0]['destination_event_id'], 11966)
        self.assertEqual(rows[0]['source_description'], 'Preference List')
        self.assertEqual(rows[0]['destination_description'], 'Preference List')

    def test_cpu_cycles_are_never_exported_as_nanoseconds(self):
        rows = self.parse('cpu_profile_cycles.xml', exporter.parse_time_profile_to_parquet,
                          table_name='cpu_profile')
        self.assertEqual([r['weight_cycles'] for r in rows], [22] * 3)
        self.assertNotIn('weight_ns', rows[0])
        self.assertEqual([json.loads(r['backtrace_json']) for r in rows],
                         [json.loads(rows[0]['backtrace_json'])] * 3)
        with self.assertRaisesRegex(ValueError, 'weight type'):
            self.parse('cpu_profile_cycles.xml', exporter.parse_time_profile_to_parquet)

    def test_schema_metadata_survives_reordering_and_rejects_missing_columns(self):
        root = ET.parse(str(FIXTURES / 'xcode27_updates.xml'))
        schema = root.find('.//schema')
        schema.insert(0, schema[-1])
        for row in root.findall('.//row'):
            row.insert(0, row[-1])
        data = ET.tostring(root)
        refs = exporter.build_ref_map(io.BytesIO(data))
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'reordered.parquet')
            exporter.parse_updates_to_parquet(io.BytesIO(data), refs, path)
            self.assertEqual(pq.read_table(path)['cause_graph_node_id'].to_pylist(), ['21', '21'])
            schema.find("col[mnemonic='cause-graph-node']/mnemonic").text = 'unknown-node'
            data = ET.tostring(root)
            refs = exporter.build_ref_map(io.BytesIO(data))
            with self.assertRaisesRegex(ValueError, 'Unsupported columns'):
                exporter.parse_updates_to_parquet(io.BytesIO(data), refs, path)

    def test_status_manifest_and_separate_cpu_sources_persist(self):
        with tempfile.TemporaryDirectory() as directory:
            tables = {}
            for fixture, table in [('xcode27_time_profile.xml', 'time_profile'),
                                   ('cpu_profile_cycles.xml', 'cpu_profile')]:
                stream = io.BytesIO((FIXTURES / fixture).read_bytes())
                refs = exporter.build_ref_map(stream)
                path = str(Path(directory) / (table + '.parquet'))
                count = exporter.parse_time_profile_to_parquet(stream, refs, path, table_name=table)
                tables[table] = (path, count)
            statuses = [('time-profile', 'time_profile', 'exported', 3, None),
                        ('cpu-profile', 'cpu_profile', 'exported', 3, None),
                        ('hitches-frame-lifetimes', 'hitches_frame_lifetimes', 'empty', 0, None)]
            path = str(Path(directory) / 'analysis.duckdb')
            with duckdb.connect(path) as conn:
                exporter.create_duckdb_views(conn, directory, tables, statuses)
            with duckdb.connect(path) as conn:
                self.assertEqual(conn.execute('SELECT sum(weight_ns) FROM time_profile').fetchone()[0], 300000)
                self.assertEqual(conn.execute('SELECT sum(weight_cycles) FROM cpu_profile').fetchone()[0], 66)
                self.assertEqual(conn.execute("SELECT status FROM _export_status WHERE schema_name='hitches-frame-lifetimes'").fetchone()[0], 'empty')


if __name__ == '__main__':
    unittest.main()
