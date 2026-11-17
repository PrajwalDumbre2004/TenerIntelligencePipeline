"""
Unit tests for the stage / cancellation changes. No network, no database.

Run:
    python -m unittest tests.test_extract_stage -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline3.extract import extract_participants          # noqa: E402
from pipeline2.worker import parse_doc_to_tender             # noqa: E402
from pipeline2.config import BY_STATUSES, STAGE_OF, SORT_BY  # noqa: E402


def _tender(stage, bid_number="GEM/2025/B/1234567", fin=None, tech=None, ra=None):
    result = {"technical_evaluation": tech or [], "financial_evaluation": fin or []}
    return {
        "bid_id": 1234567, "bid_id_parent": 0, "bid_number": bid_number,
        "bid_number_parent": "", "ra_number": "", "category": "Cat", "month": "2025-07",
        "end_date": "2025-07-15", "department": "Dept", "ministry": "Min",
        "buyer_details": {"buyer_name": "Buyer"},
        "bid_result": result, "ra_result": ra, "stage": stage,
    }


FIN_ROWS = [
    {"seller_name": "ALPHA TRADERS", "rank": "L1", "total_price": 100.0},
    {"seller_name": "BETA SUPPLIES", "rank": "L2", "total_price": 120.0},
]
TECH_ROWS = [
    {"seller_name": "ALPHA TRADERS", "status": "Qualified"},
    {"seller_name": "BETA SUPPLIES", "status": "Qualified"},
    {"seller_name": "GAMMA CORP", "status": "Disqualified"},
]


def _by_seller(records):
    return {r["seller_id"]: r for r in records}


class ExtractStageTests(unittest.TestCase):

    def test_awarded_l1_is_winner(self):
        rec = _by_seller(extract_participants(_tender(3, fin=FIN_ROWS, tech=TECH_ROWS)))
        self.assertTrue(rec["ALPHA TRADERS"]["is_winner"])
        self.assertEqual(rec["ALPHA TRADERS"]["outcome"], "won")
        self.assertEqual(rec["BETA SUPPLIES"]["outcome"], "lost")
        self.assertEqual(rec["BETA SUPPLIES"]["winner_id"], "ALPHA TRADERS")
        self.assertEqual(rec["GAMMA CORP"]["outcome"], "disqualified")

    def test_stage_none_behaves_like_awarded(self):
        """Rows from before stages existed were all awarded."""
        rec = _by_seller(extract_participants(_tender(None, fin=FIN_ROWS)))
        self.assertTrue(rec["ALPHA TRADERS"]["is_winner"])
        self.assertEqual(rec["ALPHA TRADERS"]["outcome"], "won")

    def test_financial_evaluation_has_no_winner(self):
        rec = _by_seller(extract_participants(_tender(2, fin=FIN_ROWS, tech=TECH_ROWS)))
        for r in rec.values():
            self.assertFalse(r["is_winner"], r["seller_id"])
            self.assertEqual(r["winner_id"], "")
            self.assertEqual(r["winner_name"], "")
            self.assertIsNone(r["winner_price"])
        # rank and price are kept
        self.assertEqual(rec["ALPHA TRADERS"]["rank"], "L1")
        self.assertEqual(rec["ALPHA TRADERS"]["rank_num"], 1)
        self.assertEqual(rec["ALPHA TRADERS"]["total_price"], 100.0)
        # not decided yet: no "won"/"lost"
        self.assertEqual(rec["ALPHA TRADERS"]["outcome"], "no_result")
        self.assertEqual(rec["BETA SUPPLIES"]["outcome"], "no_result")
        self.assertEqual(rec["GAMMA CORP"]["outcome"], "disqualified")

    def test_technical_evaluation(self):
        rec = _by_seller(extract_participants(_tender(1, tech=TECH_ROWS)))
        self.assertEqual(len(rec), 3)
        self.assertFalse(any(r["is_winner"] for r in rec.values()))
        self.assertEqual(rec["ALPHA TRADERS"]["outcome"], "no_result")
        self.assertEqual(rec["GAMMA CORP"]["outcome"], "disqualified")
        self.assertEqual(rec["GAMMA CORP"]["technical_status"], "Disqualified")

    def test_no_table_gives_no_rows(self):
        self.assertEqual(extract_participants(_tender(1)), [])

    def test_ra_bid_with_only_parent_table_gives_no_rows(self):
        """RA bids read ra_result only; a page with just the parent's table yields
        nothing, which 06_require_participants.sql then keeps out of the main DB."""
        t = _tender(2, bid_number="GEM/2025/R/555555", fin=FIN_ROWS, ra=None)
        self.assertEqual(extract_participants(t), [])

    def test_ra_bid_awarded_uses_ra_result(self):
        ra = {"technical_evaluation": [], "financial_evaluation": [
            {"seller_name": "DELTA LTD", "rank": "L1", "total_price": 90.0}]}
        t = _tender(3, bid_number="GEM/2025/R/555555", fin=FIN_ROWS, ra=ra)
        rec = _by_seller(extract_participants(t))
        self.assertEqual(set(rec), {"DELTA LTD"})
        self.assertTrue(rec["DELTA LTD"]["is_winner"])


def _doc(b_status, bid_id=999):
    return {"b_id": [bid_id], "b_bid_number": ["GEM/2025/B/999"], "b_buyer_status": [2],
            "final_end_date_sort": ["2025-07-15T00:00:00Z"], "b_status": b_status}


class ParseDocTests(unittest.TestCase):

    def test_stage_is_stored(self):
        self.assertEqual(parse_doc_to_tender(_doc([1]), "2025-07", 2)["stage"], 2)

    def test_cancelled_codes(self):
        for code in (3, 5):
            t = parse_doc_to_tender(_doc([code]), "2025-07", 2)
            self.assertTrue(t["is_cancelled"], code)
            self.assertEqual(t["bid_status"], code)

    def test_not_cancelled_codes(self):
        for code in (1, 2):
            t = parse_doc_to_tender(_doc([code]), "2025-07", 3)
            self.assertFalse(t["is_cancelled"], code)
            self.assertEqual(t["bid_status"], code)

    def test_scalar_and_string_codes(self):
        self.assertTrue(parse_doc_to_tender(_doc(3), "2025-07", 1)["is_cancelled"])
        self.assertTrue(parse_doc_to_tender(_doc(["5"]), "2025-07", 1)["is_cancelled"])

    def test_missing_code_is_unknown(self):
        doc = _doc([1])
        del doc["b_status"]
        t = parse_doc_to_tender(doc, "2025-07", 2)
        self.assertIsNone(t["bid_status"])
        self.assertIsNone(t["is_cancelled"])

    def test_listing_fields_still_parsed(self):
        t = parse_doc_to_tender(_doc([2], bid_id=4242), "2025-07", 3)
        self.assertEqual(t["bid_id"], 4242)
        self.assertEqual(t["buyer_status"], 2)
        self.assertEqual(t["end_date"], "2025-07-15T00:00:00Z")


class PageLoadedTests(unittest.TestCase):
    """pipeline3.scraper._ensure_page_loaded: 'no table' vs 'page never loaded'."""

    def setUp(self):
        from pipeline3.scraper import _ensure_page_loaded, ResultPageNotLoaded
        self.check, self.Err = _ensure_page_loaded, ResultPageNotLoaded
        self.t = {"bid_id": 1}

    def test_with_participants_never_raises(self):
        self.check({"page_diag": [{"status": 429, "len": 0}]}, {"technical_evaluation": [1]}, None, self.t)
        self.check({"page_diag": []}, None, {"financial_evaluation": [1]}, self.t)

    def test_full_page_without_table_is_genuine_no_data(self):
        self.check({"page_diag": [{"status": 200, "len": 0}, {"status": 200, "len": 123170}]}, None, None, self.t)

    def test_throttled_or_error_pages_raise(self):
        for diag in ([{"status": 429, "len": 150}],
                     [{"status": 200, "len": 0}],
                     [{"status": 503, "len": 900}, {"status": -1, "len": 0}],
                     [{"status": 200, "len": 5000}],
                     []):
            with self.assertRaises(self.Err, msg=str(diag)):
                self.check({"page_diag": diag}, None, None, self.t)


class ConfigTests(unittest.TestCase):

    def test_status_order_and_stages(self):
        self.assertEqual(BY_STATUSES, ["tech_evaluated", "fin_evaluated", "bid_awarded"])
        self.assertEqual([STAGE_OF[s] for s in BY_STATUSES], [1, 2, 3])

    def test_sort_is_official_value(self):
        self.assertIn(SORT_BY, {"Bid-End-Date-Oldest", "Bid-End-Date-Latest",
                                "Bid-Start-Date-Oldest", "Bid-Start-Date-Latest"})


if __name__ == "__main__":
    unittest.main()
