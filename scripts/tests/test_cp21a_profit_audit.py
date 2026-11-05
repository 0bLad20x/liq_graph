"""CP21A: exact V10 fee and cross-settlement net accounting regression."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from execution_analyze import analyze,cp21a_usd_nano,cp21a_v10_profit_fee,WSOL,USDC
from test_cp20_economic_audit import compact_record,decode_many


class CP21AProfitAuditTests(unittest.TestCase):
    def test_wsol_usdc_normalization_uses_exact_integer_nano_usd(self):
        self.assertEqual(cp21a_usd_nano(WSOL,10_000),1_000_000)
        self.assertEqual(cp21a_usd_nano(USDC,2_000),2_000_000)
        self.assertGreater(cp21a_usd_nano(USDC,2_000),
                           cp21a_usd_nano(WSOL,10_000))
        self.assertEqual(cp21a_usd_nano(WSOL,-10_000),-1_000_000)
        self.assertIsNone(cp21a_usd_nano('unknown',123))

    def test_v10_profit_share_is_integer_floor_and_positive_only(self):
        self.assertEqual(cp21a_v10_profit_fee(12_188),1_218)
        self.assertEqual(cp21a_v10_profit_fee(9),0)
        self.assertEqual(cp21a_v10_profit_fee(19),1)
        self.assertEqual(cp21a_v10_profit_fee(0),0)
        self.assertEqual(cp21a_v10_profit_fee(-10),0)

    def test_archive_schema_seven_can_be_reassessed_without_invented_fields(self):
        header,record=compact_record('old')
        report=analyze(iter(decode_many(header,[record])))
        cp=report['cp21a_profit_accounting']
        self.assertEqual(cp['counts']['counterfactual_forecasts'],1)
        self.assertEqual(cp['by_settlement'][WSOL]['legacy_positive'],1)
        self.assertEqual(cp['by_settlement'][WSOL]['adjusted_positive'],1)
        # Old fixture has no V10 fee fields and a pre-fee wallet gain.
        self.assertEqual(cp['counts'].get('new_schema_checked',0),0)

    def test_new_schema_fee_calibration_matches_wallet_share(self):
        header,record=compact_record('new')
        sel=dict(zip(header['selection_columns'],record[1]))
        eco=sel['economic']
        eco.update(v10_profit_fee_raw=61,post_v10_gain_raw=549,
                   predicted_net_gain_raw=49,predicted_net_gain_usd_nano=4900)
        actual=record[2][header['report_columns'].index('balance_changes')]
        actual['settlement_gain_raw']='540' # 600 - floor(600/10)
        report=analyze(iter(decode_many(header,[record])))
        cp=report['cp21a_profit_accounting']
        self.assertEqual(cp['counts']['new_schema_checked'],1)
        self.assertEqual(cp['counts']['wallet_fee_compared'],1)
        self.assertEqual(cp['counts'].get('wallet_fee_mismatch',0),0)
        self.assertEqual(cp['by_settlement'][WSOL]['observed_positive_after_fee'],1)
        self.assertEqual(cp['by_settlement'][WSOL]['observed_net_usd_nano_observation_sum'],4_000)
        self.assertFalse(any(report['violation_counts'].values()),report['violations'])

    def test_invalid_new_schema_profit_accounting_is_rejected(self):
        header,record=compact_record('wrong')
        eco=record[1][header['selection_columns'].index('economic')]
        eco.update(v10_profit_fee_raw=0,post_v10_gain_raw=610,
                   predicted_net_gain_raw=110,predicted_net_gain_usd_nano=11_000)
        report=analyze(iter(decode_many(header,[record])))
        self.assertEqual(report['cp21a_profit_accounting']['counts']['new_schema_mismatch'],1)
        self.assertEqual(report['violation_counts']['calibration'],1)


if __name__=='__main__':
    unittest.main()
