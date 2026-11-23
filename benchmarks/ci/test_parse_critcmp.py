"""Keep added and removed benchmarks in the correct PR comparison columns."""

import unittest

from parse_critcmp import parse_rows, render_table


class AddedBenchmarkTests(unittest.TestCase):
    def test_added_and_removed_benchmarks_preserve_baseline_columns(self):
        # Captured from critcmp 0.1.8 with a common, added, and removed benchmark.
        lines = [
            "group      base                                   changes",
            "-----      ----                                   -------",
            "added                                             1.00    10.0±10.00ns        ? ?/sec",
            "common     1.00    10.0±10.00ns        ? ?/sec    1.00    10.0±10.00ns        ? ?/sec",
            "removed    1.00    10.0±10.00ns        ? ?/sec  ",
        ]
        added, common, removed = parse_rows(lines)
        self.assertEqual((added['base_display'], added['chg_display']), ('N/A', '10.0±10.00ns'))
        self.assertIsNone(added['ratio'])
        self.assertEqual(common['ratio'], 1.0)
        self.assertEqual((removed['base_display'], removed['chg_display']), ('10.0±10.00ns', 'N/A'))
        self.assertIsNone(removed['ratio'])
        self.assertIn('| `added` | N/A | N/A | 10.0±10.00ns |', render_table([added]))

    def test_changes_only_comparison_has_no_base_duration(self):
        rows = parse_rows([
            'group    changes',
            '-----    -------',
            'added    1.00    10.0±10.00ns        ? ?/sec',
        ])
        self.assertEqual(rows[0]['base_display'], 'N/A')
        self.assertEqual(rows[0]['chg_display'], '10.0±10.00ns')

    def test_base_only_comparison_has_no_pr_duration(self):
        rows = parse_rows([
            'group      base',
            '-----      ----',
            'removed    1.00    10.0±10.00ns        ? ?/sec',
        ])
        self.assertEqual(rows[0]['base_display'], '10.0±10.00ns')
        self.assertEqual(rows[0]['chg_display'], 'N/A')
