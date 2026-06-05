import importlib.util
import io
import json
import unittest
from pathlib import Path

from lxml import etree as ET


SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'export_to_duckdb.py'
SPEC = importlib.util.spec_from_file_location('export_to_duckdb', SCRIPT_PATH)
export_to_duckdb = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(export_to_duckdb)


SAMPLE_XML = b'''<?xml version="1.0"?>
<trace-toc>
  <run>
    <data>
      <table schema="time-profile">
        <row>
          <backtrace id="10">
            <frame id="11" name="App.render()" addr="0x1001">
              <binary id="12" name="Bridge" load-addr="0x1000" path="/Applications/Bridge.app/Bridge"/>
            </frame>
            <frame id="13" name="SwiftUI.update()" addr="0x2001">
              <binary ref="12"/>
            </frame>
          </backtrace>
        </row>
        <row>
          <backtrace id="14">
            <frame ref="13"/>
            <frame ref="11"/>
          </backtrace>
        </row>
        <row>
          <backtrace ref="14"/>
        </row>
      </table>
    </data>
  </run>
</trace-toc>
'''


def parse_sample_backtraces(binary_info=None):
    ref_map = export_to_duckdb.build_ref_map(io.BytesIO(SAMPLE_XML))
    root = ET.fromstring(SAMPLE_XML)
    return [
        json.loads(export_to_duckdb.parse_backtrace(backtrace, ref_map, binary_info))
        for backtrace in root.findall('.//backtrace')
    ]


class BacktraceParsingTests(unittest.TestCase):
    def test_parse_backtrace_resolves_frame_refs_across_rows(self):
        frames = parse_sample_backtraces()[1]

        self.assertEqual(
            frames,
            [
                {
                    'name': 'SwiftUI.update()',
                    'addr': '0x2001',
                    'binary': 'Bridge',
                    'path': '/Applications/Bridge.app/Bridge',
                    'load_addr': '0x1000',
                },
                {
                    'name': 'App.render()',
                    'addr': '0x1001',
                    'binary': 'Bridge',
                    'path': '/Applications/Bridge.app/Bridge',
                    'load_addr': '0x1000',
                },
            ],
        )

    def test_parse_backtrace_resolves_whole_backtrace_refs(self):
        parsed = parse_sample_backtraces()

        self.assertEqual(parsed[2], parsed[1])
        self.assertEqual([frame['name'] for frame in parsed[2]], ['SwiftUI.update()', 'App.render()'])

    def test_parse_backtrace_populates_binary_info_from_refs(self):
        binary_info = {}
        parse_sample_backtraces(binary_info)

        self.assertEqual(
            binary_info,
            {'Bridge': {'path': '/Applications/Bridge.app/Bridge', 'load_addr': '0x1000'}},
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
