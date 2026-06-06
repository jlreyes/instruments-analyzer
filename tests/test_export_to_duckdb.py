import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path

from lxml import etree as ET
import pyarrow.parquet as pq


SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'export_to_duckdb.py'
SPEC = importlib.util.spec_from_file_location('export_to_duckdb', SCRIPT_PATH)
export_to_duckdb = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(export_to_duckdb)


FIXTURE_PATH = Path(__file__).resolve().parent / 'fixtures' / 'time_profile_dedup.xml'


def load_fixture_bytes():
    return FIXTURE_PATH.read_bytes()


def parse_fixture_backtraces(binary_info=None):
    xml_bytes = load_fixture_bytes()
    ref_map = export_to_duckdb.build_ref_map(io.BytesIO(xml_bytes))
    root = ET.fromstring(xml_bytes)
    return [
        json.loads(export_to_duckdb.parse_backtrace(backtrace, ref_map, binary_info))
        for backtrace in root.findall('.//backtrace')
    ]


class BacktraceParsingTests(unittest.TestCase):
    def test_parse_backtrace_resolves_frame_refs_across_rows(self):
        frames = parse_fixture_backtraces()[2]

        self.assertEqual(frames[0]['name'], '<deduplicated_symbol>')
        self.assertEqual(frames[1]['name'], 'stat_with_subsystem')
        self.assertEqual(frames[-1]['name'], 'start')
        self.assertEqual(frames[1]['binary'], 'dyld')
        self.assertEqual(frames[1]['path'], '/usr/lib/dyld')

    def test_parse_backtrace_resolves_whole_backtrace_refs(self):
        parsed = parse_fixture_backtraces()

        self.assertEqual(parsed[3], parsed[2])
        self.assertEqual(parsed[3][1]['name'], 'stat_with_subsystem')

    def test_parse_backtrace_populates_binary_info_from_refs(self):
        binary_info = {}
        parse_fixture_backtraces(binary_info)

        self.assertEqual(
            binary_info,
            {'dyld': {'path': '/usr/lib/dyld', 'load_addr': '0x186781000'}},
        )

    def test_time_profile_parser_resolves_real_xctrace_fixture(self):
        xml_stream = io.BytesIO(load_fixture_bytes())
        ref_map = export_to_duckdb.build_ref_map(xml_stream)

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / 'time_profile.parquet'
            row_count = export_to_duckdb.parse_time_profile_to_parquet(
                xml_stream,
                ref_map,
                str(parquet_path),
            )
            table = pq.read_table(parquet_path, columns=['backtrace_json'])

        backtraces = [
            json.loads(backtrace_json)
            for backtrace_json in table.column('backtrace_json').to_pylist()
        ]

        self.assertEqual(row_count, 4)
        self.assertEqual(backtraces[3], backtraces[2])
        self.assertEqual(backtraces[2][1]['name'], 'stat_with_subsystem')
        self.assertEqual(
            sum(1 for frames in backtraces for frame in frames if not frame.get('name')),
            0,
        )

    def test_parse_backtrace_still_handles_plain_dicts(self):
        backtrace = ET.fromstring(
            b'<backtrace id="1"><frame id="2" name="inline" addr="0x1"/></backtrace>'
        )

        self.assertEqual(
            json.loads(export_to_duckdb.parse_backtrace(backtrace, {})),
            [{'name': 'inline', 'addr': '0x1'}],
        )


if __name__ == '__main__':
    unittest.main()
