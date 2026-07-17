"""
Data models for Pipeline2.
Same schema as pipeline1 for DB compatibility.
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BidListing:
    """A single bid/tender from the listing API."""
    bid_id: int
    bid_number: str
    bid_number_parent: str
    bid_id_parent: int
    bid_type: int
    eval_type: int
    single_packet: int
    buyer_status: int
    bid_to_ra: int
    ra_to_bid: int
    category: str
    category_id: str
    bid_title: str
    department: str
    ministry: str
    start_date: str
    end_date: str
    total_quantity: int
    is_high_value: bool
    is_rc_bid: int
    is_global_tendering: int
    is_bunch: int
    is_custom_item: int
    has_boq: bool


@dataclass
class MonthStatus:
    """Pipeline status for one month chunk."""
    month: str              # 'YYYY-MM'
    status: str             # pending / in_progress / completed / failed
    total_tenders: int
    unique_captured: int
    passes_done: int
    last_pass_new: int      # new IDs found in last pass
    error_message: Optional[str]
