# Test Suite for Tender Intelligence Pipeline

This folder contains validation and testing scripts for the pipeline.

## Data Consistency Validation

### validate_data_consistency.py

Automated integrity checker that validates data across TenderIntelligence (raw) and TenderAnalytics (derived) databases.

**Usage:**

```bash
# Run all validation tests
python test/validate_data_consistency.py

# Validate specific months only
python test/validate_data_consistency.py --months 2026-01 2026-02

# Use smaller sample size for faster checks
python test/validate_data_consistency.py --sample-size 20

# Skip competitor checks (slow on large datasets)
python test/validate_data_consistency.py --skip-competitors
```

**Tests Performed:**

1. **Count Match** - Verifies `tenders_YYYY` document counts match `pipeline2_status.unique_captured`
2. **No Orphans** - Ensures all tenders with `scraped_at` have `derived_at` set
3. **Seller Math** - Validates seller aggregates (`total_tenders`, `total_wins`, `win_rate`)
4. **No Duplicates** - Checks for duplicate `(seller_id, bid_id)` pairs in `seller_tenders`
5. **Competitor Symmetry** - Verifies competitor relationships are bidirectional and consistent
6. **Price Sanity** - Detects negative, null, or absurdly large prices
7. **Tender Linkage** - Confirms `seller_tenders` bid_ids exist in raw `tenders_YYYY` collections
8. **Winner Consistency** - Validates winner fields are consistent across tender participants
9. **Category Aggregates** - Checks `seller_categories` counts match `seller_tenders`
10. **Buyer Aggregates** - Checks `seller_buyers` counts match `seller_tenders`

**Exit Codes:**
- `0` - All tests passed
- `1` - One or more tests failed

**When to Run:**

- **After every `pipeline_main` run** - Catches pipeline bugs immediately
- **After manual data fixes** - Ensures derived collections stay consistent
- **Before production deployment** - Final safety check
- **Weekly as a cron job** - Catches data drift over time

**Interpreting Results:**

- ✓ **Passed** - Test succeeded, data is consistent
- ⚠ **Warning** - Potential issue but not critical (e.g., missing prices on some tenders)
- ✗ **Error** - Critical failure, data is inconsistent and needs investigation

**Common Failures and Fixes:**

| Error | Likely Cause | Fix |
|-------|--------------|-----|
| Count mismatch | Pipeline2 multi-pass didn't converge | Rerun pipeline2 for that month |
| Orphaned tenders | Pipeline4 not run after pipeline3 | Run `python -m pipeline4.run --month YYYY-MM` |
| Seller math wrong | Pipeline4 didn't recompute from full history | Rerun pipeline4 with `--recompute-only SELLER_ID` |
| Duplicate seller_tenders | Race condition or failed upsert | Check MongoDB unique index, rerun pipeline4 |
| Competitor asymmetry | Incomplete pipeline4 run | Rerun full pipeline4 with competitors enabled |
| Price sanity | Scraper parsing bug or source data issue | Spot-check the tender on GeM portal, fix scraper if needed |

## Additional Test Utilities

### quick_validate.py

Fast validation for development workflows - runs only critical tests.

```bash
# Quick check (runs in seconds)
python test/quick_validate.py

# Quick check for specific month
python test/quick_validate.py --month 2026-01
```

**Tests Run:**
- Count match
- No orphans
- Seller math (sample of 10)
- No duplicates

Use this during active development for fast feedback. Run full validation before deployment.

### spot_check_tender.py

Trace a specific tender through all collections for manual verification.

```bash
# Basic check
python test/spot_check_tender.py 12345678

# Verbose mode (shows full documents)
python test/spot_check_tender.py 12345678 --verbose
```

**Output:**
- Raw tender details from `tenders_YYYY`
- Extracted participants with ranks/winners
- Matching `seller_tenders` records
- Seller profiles
- Competitor relationships

Use this to:
- Verify specific tenders after scraping
- Debug extraction logic issues
- Compare against GeM portal manually

### data_quality_report.py

Comprehensive statistics and quality metrics across both databases.

```bash
# Full report
python test/data_quality_report.py

# Month-filtered report
python test/data_quality_report.py --months 2026-01 2026-02

# Save to file
python test/data_quality_report.py --output report.txt
```

**Report Sections:**
- Raw tender counts and completion rates
- Derived collection statistics
- Data completeness (results, buyers, contracts)
- Winner and price statistics
- Top sellers by tender count
- Potential issues detection

Use this to:
- Monitor pipeline health over time
- Identify data quality trends
- Generate reports for stakeholders
- Detect anomalies early

## Adding New Tests

To add a new validation test:

1. Define a function `validate_<test_name>(source_db, target_db, report, ...)` in the script
2. Use `report.pass_test()`, `report.warn()`, or `report.fail()` to record results
3. Call your function in `main()` after existing tests
4. Update this README with test description

Example:

```python
def validate_my_test(target_db, report: ValidationReport):
    print("\n[TEST XX] My Custom Test")
    
    # Your validation logic here
    if condition_passed:
        report.pass_test("My test", "Details here")
    else:
        report.fail("My test", "Failure details")
```

## Future Enhancements

- [ ] Add unit tests for `extract_participants()` transformation logic
- [ ] Add unit tests for builder functions (`build_seller_profile`, etc.)
- [ ] Add integration tests that run mini-pipeline on sample data
- [ ] Add performance benchmarks (tenders/sec, memory usage)
- [ ] Add data quality metrics (% tenders with results, % with prices)
- [ ] Add automated spot-check against GeM portal (requires browser automation)
