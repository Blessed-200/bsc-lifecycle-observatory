"""Synthetic adversarial tests; these fixtures are NOT observed profitable trades."""
import importlib.util
import pathlib
import unittest

FILE = pathlib.Path(__file__).resolve().parents[1] / "scripts/replay_bsc_v2_economics.py"
SPEC = importlib.util.spec_from_file_location("bsc_v2_economics", FILE)
assert SPEC is not None and SPEC.loader is not None
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)

TOKEN = "0x" + "e" * 40

def birth():
    return {"birth_block":1000,"token0":M.WBNB,"token1":TOKEN}

def sync(block,quote=10**19,token=10**26):
    return {"block":block,"log_index":0,"kind":"SYNC",
            "values":[str(quote),str(token)]}

def swap(block,buys=True):
    return {"block":block,"log_index":1,"kind":"SWAP",
            "values":["10000000000000000","0","0","9000000000000000000000"] if buys
            else ["0","9000000000000000000000","10000000000000000","0"]}

class FailClosedMath(unittest.TestCase):
    def test_unchanged_reserves_roundtrip_loses_after_fees_and_gas(self):
        b=birth()
        e=[sync(1002),swap(1080),swap(1081),sync(1090)]
        ft,reason=M.feature(b,e,1300)
        self.assertEqual(reason,"OK")
        ret=M.after_cost(M.multiple(b,e,ft,1500),tax=0.,adverse=0.)
        self.assertLess(ret,0.)
    def test_future_liquidity_not_visible_before_anchor(self):
        ft,reason=M.feature(birth(),[sync(1600)],1300)
        self.assertIsNone(ft)
        self.assertEqual(reason,"NO_INITIAL_LIQUIDITY")
    def test_total_lp_withdrawal_means_full_price_loss(self):
        b=birth()
        e=[sync(1001),sync(1400,quote=0)]
        ft,_=M.feature(b,e,1300)
        self.assertEqual(M.multiple(b,e,ft,1500),0)
        self.assertLess(M.after_cost(0),-1)
    def test_flow_requires_successful_sells_to_be_counted_as_sell_tested(self):
        b=birth()
        e=[sync(1001)]+[swap(1100+n) for n in range(7)]
        ft,_=M.feature(b,e,1300)
        self.assertTrue(M.selected("FLOW",ft))
        self.assertFalse(M.selected("FLOW_SELL_TESTED",ft))
    def test_adverse_transfer_tax_is_monotone_cost(self):
        self.assertLess(M.after_cost(1.5,tax=.10),M.after_cost(1.5,tax=.05))
    def test_static_zero_liquidity_excluded_even_if_many_swaps(self):
        b=birth()
        e=[sync(1001,quote=0)]+[swap(1100+n) for n in range(10)]
        ft,_=M.feature(b,e,1300)
        self.assertIsNone(ft)

if __name__ == "__main__":
    unittest.main()
