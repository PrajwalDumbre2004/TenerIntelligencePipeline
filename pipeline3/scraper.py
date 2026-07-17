"""
Scraper for Pipeline3: Fetches bid results, RA results, buyer details, and contract details.
Uses the exact same parsing logic as pipeline1/scraper.py.
"""
import re
from typing import Optional

from pipeline3.config import BASE_URL

CONTRACT_URL = "https://gem.gov.in/view_contracts/bid_detail?bid_no="


def parse_price(price_str: str):
    """Parse price string to float or None."""
    if not price_str:
        return None
    cleaned = price_str.replace("₹", "").replace(",", "").strip()
    cleaned = cleaned.replace("(Bid Price)", "").replace("(RA Price)", "").strip()
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def clean_rank(rank_str: str) -> str:
    """Clean rank string: extract L1/L2/... or keep as-is."""
    if not rank_str:
        return ""
    match = re.match(r"L\d+", rank_str.strip())
    if match:
        return match.group()
    if "disqualif" in rank_str.lower():
        return "Disqualified"
    return rank_str.strip()[:20]


def get_result_urls(tender: dict) -> list:
    """Determine the result view URLs to try for a tender."""
    bid_id = tender["bid_id"]
    bid_type = tender.get("bid_type", 0)
    eval_type = tender.get("eval_type", 0)
    single_packet = tender.get("single_packet", 0)
    bid_id_parent = tender.get("bid_id_parent", 0)

    urls = []
    if single_packet == 1:
        urls.append(f"{BASE_URL}/bidding/bid/getSinglePacketResultView/{bid_id}")
    if bid_type == 2 or eval_type > 0:
        urls.append(f"{BASE_URL}/bidding/bid/getBidResultViewSchedule/{bid_id}")
        urls.append(f"{BASE_URL}/bidding/bid/getBidResultView/{bid_id}")
    if not urls:
        urls.append(f"{BASE_URL}/bidding/bid/getBidResultView/{bid_id}")

    # For RA-converted bids, also try the parent bid's result page
    if bid_id_parent and bid_id_parent > 0:
        parent_url = f"{BASE_URL}/bidding/bid/getBidResultView/{bid_id_parent}"
        if parent_url not in urls:
            urls.append(parent_url)

    return urls


# ═══════════════════════════════════════════════════════════════
# BID RESULTS JS — Parses technical + financial tables + buyer details
# Same logic as pipeline1/scraper.py BID_RESULTS_JS
# ═══════════════════════════════════════════════════════════════

BID_RESULTS_JS = """async (urls) => {
    function cleanSeller(raw) {
        let name = raw || '';
        let mse = '';
        const mseMatch = name.match(/\\(\\s*MSE[^)]*\\)/i);
        if (mseMatch) {
            mse = mseMatch[0].replace(/^\\(\\s*/, '').replace(/\\s*\\)$/, '').trim();
            name = name.replace(mseMatch[0], '');
        }
        const miiMatch = name.match(/\\(\\s*MII[^)]*\\)/i);
        if (miiMatch) {
            if (mse) mse += ', ';
            mse += miiMatch[0].replace(/^\\(\\s*/, '').replace(/\\s*\\)$/, '').trim();
            name = name.replace(miiMatch[0], '');
        }
        name = name.replace(/Under PMA/gi, '').trim();
        return {name, mse};
    }

    function findCol(headers, ...keywords) {
        for (const kw of keywords) {
            const idx = headers.findIndex(h => h.toLowerCase().includes(kw.toLowerCase()));
            if (idx >= 0) return idx;
        }
        return -1;
    }

    function findStatusCol(headers, mseCol) {
        for (let i = 0; i < headers.length; i++) {
            if (i === mseCol) continue;
            if (headers[i].toLowerCase() === 'status') return i;
        }
        for (let i = headers.length - 1; i >= 0; i--) {
            if (i === mseCol) continue;
            if (headers[i].toLowerCase().includes('status')) return i;
        }
        return -1;
    }

    function isTableInRASection(table, bodyHTML, raPos) {
        if (raPos < 0) return false;
        const snippet = table.outerHTML.substring(0, 200);
        const tablePos = bodyHTML.indexOf(snippet);
        return tablePos >= 0 && tablePos >= raPos;
    }

    let technical = [];
    let financial = [];
    let ra_technical = [];
    let ra_financial = [];
    let found_ra_number = '';
    let found_buyer = {};

    for (const url of urls) {
        try {
            const resp = await fetch(url, {credentials: 'include'});
            const html = await resp.text();
            if (!html || html.length < 1000) continue;

            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');
            const tables = doc.querySelectorAll('table');

            const pageText = doc.body ? doc.body.textContent.replace(/\\s+/g, ' ') : '';
            if (!found_ra_number) {
                const raMatch = pageText.match(/RA\\s*DETAILS[^A-Za-z0-9]*(GEM\\/\\d{4}\\/RA?\\/\\d+)/i);
                if (raMatch) {
                    found_ra_number = raMatch[1];
                }
            }

            const bodyHTML = doc.body ? doc.body.innerHTML : '';
            const raSectionMatch = bodyHTML.match(/RA\\s*DETAILS|RA\\s*EVALUATION/i);
            const raPos = raSectionMatch ? raSectionMatch.index : -1;

            for (const table of tables) {
                const headerRow = table.querySelector('tr');
                if (!headerRow) continue;

                const ths = headerRow.querySelectorAll('th');
                if (ths.length < 3) continue;

                const headers = Array.from(ths).map(th => th.textContent.replace(/\\s+/g, ' ').trim());
                const hasSeller = headers.some(h => h.includes('Seller'));
                if (!hasSeller) continue;
                const isTech = headers.some(h => h.includes('Participated') || h.includes('EMD') || h.includes('Offer submitted') || h.includes('Offer Made') || h.includes('Qualified'));
                const isFin = headers.some(h => h.includes('Rank') || h.includes('Total Price') || h.includes('Price') || h.includes('Amount') || h.includes('Awarded'));

                if (!isTech && !isFin) continue;

                const inRA = isTableInRASection(table, bodyHTML, raPos);

                const colSNo = findCol(headers, 'S.No');
                const colSeller = findCol(headers, 'Seller Name', 'Seller');
                const colItem = findCol(headers, 'Offered Item', 'Item');
                const colParticipated = findCol(headers, 'Participated', 'Offer submitted', 'Offer Made');
                const colEMD = findCol(headers, 'EMD');
                const colMSE = findCol(headers, 'MSE', 'MII');
                const colStatus = findStatusCol(headers, colMSE);
                const colPrice = findCol(headers, 'Total Price', 'Price', 'Amount');
                const colRank = findCol(headers, 'Rank', 'Awarded');

                const rows = table.querySelectorAll('tr');
                for (let i = 1; i < rows.length; i++) {
                    const cells = rows[i].querySelectorAll('td');
                    if (cells.length < 3) continue;

                    const cellTexts = Array.from(cells).map(td =>
                        td.textContent.replace(/\\s+/g, ' ').trim()
                    );
                    const get = (idx) => (idx >= 0 && idx < cellTexts.length) ? cellTexts[idx] : '';

                    const {name: sellerName, mse: sellerMSE} = cleanSeller(get(colSeller));

                    if (isTech && !isFin) {
                        const row = {
                            seller_name: sellerName,
                            mse_status: sellerMSE || get(colMSE),
                            offered_item: get(colItem),
                            participated_on: get(colParticipated),
                            emd_status: get(colEMD),
                            status: get(colStatus)
                        };
                        if (inRA) ra_technical.push(row);
                        else technical.push(row);
                    } else if (isFin) {
                        let price = get(colPrice);
                        price = price.replace(/[`\\u20b9,]/g, '').replace(/\\(Bid Price\\)/gi, '').trim();

                        let rank = get(colRank);
                        const rankM = rank.match(/L\\d+/);
                        if (rankM) rank = rankM[0];

                        const row = {
                            seller_name: sellerName,
                            mse_status: sellerMSE || get(colMSE),
                            offered_item: get(colItem),
                            total_price: price,
                            rank: rank,
                            status: get(colStatus)
                        };
                        if (inRA) ra_financial.push(row);
                        else financial.push(row);
                    }
                }
            }

            // Extract buyer details from first page that has content
            if (!found_buyer.buyer_name) {
                const fullText = doc.body ? doc.body.textContent : '';

                // Scope to Buyer Details section to avoid false matches
                let buyerText = fullText;
                const buyerStart = fullText.indexOf('Buyer Details');
                if (buyerStart >= 0) {
                    // Take text from "Buyer Details" to the next major section or end
                    const sectionEnd = fullText.indexOf('Technical Evaluation', buyerStart);
                    buyerText = sectionEnd > 0
                        ? fullText.substring(buyerStart, sectionEnd)
                        : fullText.substring(buyerStart, buyerStart + 3000);
                }

                const extract = (label, text) => {
                    const re = new RegExp(label + '\\\\s*:\\\\s*([^\\\\n]+)', 'i');
                    const m = text.match(re);
                    return m ? m[1].replace(/\\s+/g, ' ').trim().substring(0, 300) : '';
                };

                found_buyer.buyer_name = extract('Name', buyerText);
                found_buyer.buyer_address = extract('Address', buyerText);
                found_buyer.buyer_state = extract('State', buyerText);
                found_buyer.buyer_department = extract('Department', buyerText);
                found_buyer.buyer_organisation = extract('Organisation', buyerText);
                found_buyer.buyer_office = extract('Office', buyerText);
                found_buyer.buyer_ministry = extract('Ministry', buyerText);

                // These are from the Bid Details section (top of page), not buyer section
                const validityMatch = fullText.match(/Bid Validity[^:]*:\\s*([^\\n]+)/i);
                found_buyer.bid_validity_days = validityMatch ? validityMatch[1].replace(/\\s+/g, ' ').trim() : '';

                const openingMatch = fullText.match(/(?:Bid|RA) Opening Date[^:]*:\\s*([^\\n]+)/i);
                found_buyer.bid_opening_date = openingMatch ? openingMatch[1].replace(/\\s+/g, ' ').trim() : '';

                const turnoverMatch = fullText.match(/Average Turn Over[^:]*:\\s*([^\\n]+)/i);
                found_buyer.turnover_requirement = turnoverMatch ? turnoverMatch[1].replace(/\\s+/g, ' ').trim() : '';

                const expMatch = fullText.match(/Experience with Gov[^:]*:\\s*([^\\n]+)/i);
                found_buyer.experience_required = expMatch ? expMatch[1].replace(/\\s+/g, ' ').trim() : '';

                const lifecycleMatch = fullText.match(/Bid Life Cycle[^:]*:\\s*([^\\n]+)/i);
                found_buyer.bid_lifecycle = lifecycleMatch ? lifecycleMatch[1].replace(/\\s+/g, ' ').trim() : '';
            }

            // If we have both bid and RA data, no need to try more URLs
            const hasBid = technical.length > 0 || financial.length > 0;
            const hasRA = ra_technical.length > 0 || ra_financial.length > 0;
            if (hasBid && hasRA) break;

        } catch(e) {
            continue;
        }
    }

    const hasData = technical.length > 0 || financial.length > 0
                 || ra_technical.length > 0 || ra_financial.length > 0;
    return {
        technical_evaluation: technical,
        financial_evaluation: financial,
        ra_technical_evaluation: ra_technical,
        ra_financial_evaluation: ra_financial,
        ra_number: found_ra_number,
        buyer_details: found_buyer
    };
}"""


# ═══════════════════════════════════════════════════════════════
# RA SCHEDULE JS — Fallback for RA results
# ═══════════════════════════════════════════════════════════════

RA_SCHEDULE_JS = """async (url) => {
    const SPEC_WORDS = ['mode', 'cable', 'core', 'size', 'type', 'printing',
        'cartridge', 'paper', 'resolution', 'speed', 'dpi', 'ppm', 'voltage',
        'wattage', 'capacity', 'dimension', 'weight', 'connectivity', 'interface',
        'warranty', 'specification', 'feature', 'material', 'frequency', 'rating',
        'thickness', 'diameter', 'length', 'width', 'height', 'density',
        'number of', 'buying', 'quantity'];

    function isLikelySeller(name) {
        if (!name || name.length < 4) return false;
        const lower = name.toLowerCase();
        if (SPEC_WORDS.some(sw => lower.includes(sw))) return false;
        if (/\\b(sq\\.?\\s*mm|mm|cm|kg|nos|kv|mv|hp|kw)\\b/i.test(name)) return false;
        return true;
    }

    try {
        const resp = await fetch(url, {credentials: 'include'});
        if (resp.status === 404) return null;
        const html = await resp.text();
        if (!html || html.length < 500) return null;

        const parser = new DOMParser();
        const doc = parser.parseFromString(html, 'text/html');
        const tables = doc.querySelectorAll('table');

        let technical = [];
        let financial = [];

        for (const table of tables) {
            const headerRow = table.querySelector('tr');
            if (!headerRow) continue;
            const ths = headerRow.querySelectorAll('th');
            if (ths.length < 3) continue;
            const headers = Array.from(ths).map(th => th.textContent.replace(/\\s+/g, ' ').trim());

            const hasSeller = headers.some(h => h.includes('Seller'));
            if (!hasSeller) continue;

            const isTech = headers.some(h => h.includes('Participated') || h.includes('EMD'));
            const isFin = headers.some(h => h.includes('Rank') || h.includes('Price') || h.includes('Amount') || h.includes('Awarded'));

            if (!isTech && !isFin) continue;

            const rows = table.querySelectorAll('tr');
            for (let i = 1; i < rows.length; i++) {
                const cells = rows[i].querySelectorAll('td');
                if (cells.length < 3) continue;
                const cellTexts = Array.from(cells).map(td => td.textContent.replace(/\\s+/g, ' ').trim());

                const sellerName = cellTexts[1] || '';
                if (!isLikelySeller(sellerName)) continue;

                if (isTech && !isFin) {
                    technical.push({
                        seller_name: sellerName,
                        mse_status: '',
                        offered_item: cellTexts[2] || '',
                        participated_on: cellTexts[3] || '',
                        emd_status: cellTexts[4] || '',
                        status: cellTexts[5] || ''
                    });
                } else if (isFin) {
                    let rankText = cellTexts[4] || cellTexts[cellTexts.length - 1] || '';
                    const rm = rankText.match(/L\\d+/);
                    if (rm) rankText = rm[0];
                    financial.push({
                        seller_name: sellerName,
                        mse_status: '',
                        offered_item: cellTexts[2] || '',
                        total_price: (cellTexts[3] || '').replace(/[`\\u20b9,]/g, '').trim(),
                        rank: rankText
                    });
                }
            }
        }

        if (technical.length > 0 || financial.length > 0) {
            return {technical_evaluation: technical, financial_evaluation: financial};
        }
        return null;
    } catch(e) { return null; }
}"""


# ═══════════════════════════════════════════════════════════════
# CONTRACT DETAILS JS — Parses contract page on gem.gov.in
# ═══════════════════════════════════════════════════════════════
CONTRACT_JS = """() => {
    function getText(label) {
        const strongs = document.querySelectorAll('strong');
        for (const s of strongs) {
            if (s.textContent.includes(label)) {
                const span = s.parentElement.querySelector('span');
                if (span) return span.textContent.trim();
                // Fallback: get text after the strong tag
                const parent = s.parentElement;
                const text = parent.textContent.replace(s.textContent, '').trim();
                return text;
            }
        }
        return '';
    }

    const body = document.body ? document.body.innerText : '';
    if (!body.includes('Contract NO') && !body.includes('Bid Detail')) return null;

    // Extract contract number from the specific element
    let contractNo = '';
    const cnEl = document.querySelector('.ajxtag_order_number');
    if (cnEl) contractNo = cnEl.textContent.trim();

    const result = {
        contract_no: contractNo,
        contract_status: getText('Status of the Contract'),
        seller_name: getText('Seller Organisation Name'),
        organization_type: getText('Organization Type'),
        ministry: getText('Ministry'),
        department: getText('Department'),
        organization_name: getText('Organization Name'),
        office_zone: getText('Office Zone'),
        buyer_designation: getText('Buyer Designation'),
        buying_mode: getText('Buying Mode'),
        bid_number: getText('Bid Number'),
        contract_date: getText('Contract Date'),
        total: getText('Total'),
    };

    // Parse product table
    const table = document.querySelector('table');
    result.products = [];
    if (table) {
        const rows = table.querySelectorAll('tr');
        for (let i = 1; i < rows.length; i++) {
            const cells = rows[i].querySelectorAll('td');
            if (cells.length >= 5) {
                result.products.push({
                    product: cells[0].textContent.trim(),
                    brand: cells[1].textContent.trim(),
                    model: cells[2].textContent.trim(),
                    quantity: cells[3].textContent.trim(),
                    price: cells[4].textContent.trim(),
                });
            }
        }
    }
    return result;
}"""


def fetch_contract_details(page, bid_number: str) -> Optional[dict]:
    """
    Navigate to gem.gov.in contract page and extract contract details.
    Returns dict with contract info or None if not found.
    """
    if not bid_number:
        return None
    url = f"{CONTRACT_URL}{bid_number}"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle", timeout=15000)
        result = page.evaluate(CONTRACT_JS)
        return result
    except Exception:
        return None


def fetch_bid_results(page, tender: dict) -> dict:
    """
    Fetch bid results (Technical + Financial + Buyer Details) for a tender.
    Returns dict with all result data ready for DB update.
    """
    urls = get_result_urls(tender)
    raw = page.evaluate(BID_RESULTS_JS, urls)

    ra_number = raw.get("ra_number", "")

    # Build buyer_details
    buyer_raw = raw.get("buyer_details", {})
    buyer_details = None
    if buyer_raw and buyer_raw.get("buyer_name"):
        buyer_details = {
            "buyer_name": buyer_raw.get("buyer_name", ""),
            "buyer_address": buyer_raw.get("buyer_address", ""),
            "buyer_department": buyer_raw.get("buyer_department", ""),
            "buyer_organisation": buyer_raw.get("buyer_organisation", ""),
            "buyer_ministry": buyer_raw.get("buyer_ministry", ""),
            "bid_validity_days": buyer_raw.get("bid_validity_days", ""),
            "bid_opening_date": buyer_raw.get("bid_opening_date", ""),
            "turnover_requirement": buyer_raw.get("turnover_requirement", ""),
            "experience_required": buyer_raw.get("experience_required", ""),
            "bid_lifecycle": buyer_raw.get("bid_lifecycle", ""),
        }
        # Only include state/office if explicitly present on page
        if buyer_raw.get("buyer_state"):
            buyer_details["buyer_state"] = buyer_raw["buyer_state"]
        if buyer_raw.get("buyer_office"):
            buyer_details["buyer_office"] = buyer_raw["buyer_office"]

    # Build bid_result
    tech_rows = []
    for entry in raw.get("technical_evaluation", []):
        tech_rows.append({
            "seller_name": entry.get("seller_name", "").strip(),
            "mse_status": entry.get("mse_status", ""),
            "offered_item": entry.get("offered_item", ""),
            "participated_on": entry.get("participated_on", ""),
            "emd_status": entry.get("emd_status", ""),
            "status": entry.get("status", ""),
        })

    fin_rows = []
    for entry in raw.get("financial_evaluation", []):
        rank = clean_rank(entry.get("rank", ""))
        fin_rows.append({
            "seller_name": entry.get("seller_name", "").strip(),
            "mse_status": entry.get("mse_status", ""),
            "offered_item": entry.get("offered_item", ""),
            "total_price": parse_price(entry.get("total_price", "")),
            "rank": rank,
            "status": entry.get("status", ""),
        })

    bid_result = {
        "technical_evaluation": tech_rows,
        "financial_evaluation": fin_rows,
    } if (tech_rows or fin_rows) else None

    # Build ra_result
    ra_result = None
    ra_tech_from_page = raw.get("ra_technical_evaluation", [])
    ra_fin_from_page = raw.get("ra_financial_evaluation", [])

    if ra_tech_from_page or ra_fin_from_page:
        ra_tech = []
        for entry in ra_tech_from_page:
            ra_tech.append({
                "seller_name": entry.get("seller_name", "").strip(),
                "mse_status": entry.get("mse_status", ""),
                "offered_item": entry.get("offered_item", ""),
                "participated_on": entry.get("participated_on", ""),
                "emd_status": entry.get("emd_status", ""),
                "status": entry.get("status", ""),
            })
        ra_fin = []
        for entry in ra_fin_from_page:
            rank = clean_rank(entry.get("rank", ""))
            ra_fin.append({
                "seller_name": entry.get("seller_name", "").strip(),
                "mse_status": entry.get("mse_status", ""),
                "offered_item": entry.get("offered_item", ""),
                "total_price": parse_price(entry.get("total_price", "")),
                "rank": rank,
                "status": entry.get("status", ""),
            })
        if ra_tech or ra_fin:
            ra_result = {
                "technical_evaluation": ra_tech,
                "financial_evaluation": ra_fin,
            }
    else:
        # Fallback: check if this bid has RA and try dedicated endpoints
        has_ra = (tender.get("ra_to_bid", 0) == 1 or
                  tender.get("bid_to_ra", 0) >= 1 or
                  tender.get("bid_type", 0) in (2, 5))
        if has_ra or ra_number:
            bid_id = tender["bid_id"]
            url = f"{BASE_URL}/list-ra-schedules/{bid_id}"
            ra_data = page.evaluate(RA_SCHEDULE_JS, url)
            if not ra_data:
                url = f"{BASE_URL}/showradocumentPdf/{bid_id}"
                ra_data = page.evaluate(RA_SCHEDULE_JS, url)
            if ra_data:
                ra_tech = []
                for entry in ra_data.get("technical_evaluation", []):
                    ra_tech.append({
                        "seller_name": entry.get("seller_name", "").strip(),
                        "mse_status": entry.get("mse_status", ""),
                        "offered_item": entry.get("offered_item", ""),
                        "participated_on": entry.get("participated_on", ""),
                        "emd_status": entry.get("emd_status", ""),
                        "status": entry.get("status", ""),
                    })
                ra_fin = []
                for entry in ra_data.get("financial_evaluation", []):
                    rank = clean_rank(entry.get("rank", ""))
                    ra_fin.append({
                        "seller_name": entry.get("seller_name", "").strip(),
                        "mse_status": entry.get("mse_status", ""),
                        "offered_item": entry.get("offered_item", ""),
                        "total_price": parse_price(entry.get("total_price", "")),
                        "rank": rank,
                        "status": entry.get("status", ""),
                    })
                if ra_tech or ra_fin:
                    ra_result = {
                        "technical_evaluation": ra_tech,
                        "financial_evaluation": ra_fin,
                    }

    # ─── Contract Details ─────────────────────────────────────────
    contract_details = None
    bid_number = tender.get("bid_number", "")
    if bid_number:
        contract_details = fetch_contract_details(page, bid_number)
        # Navigate back to bidplus to keep session alive for next tender
        try:
            page.goto(f"{BASE_URL}/all-bids", wait_until="domcontentloaded", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

    return {
        "buyer_details": buyer_details,
        "bid_result": bid_result,
        "ra_result": ra_result,
        "ra_number": ra_number,
        "contract_details": contract_details,
    }


# ═══════════════════════════════════════════════════════════════
# ASYNC VERSIONS — For use with async_playwright multi-page workers
# ═══════════════════════════════════════════════════════════════

async def fetch_contract_details_async(page, bid_number: str) -> Optional[dict]:
    """Async version: Navigate to contract page and extract contract details."""
    if not bid_number:
        return None
    url = f"{CONTRACT_URL}{bid_number}"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=15000)
        result = await page.evaluate(CONTRACT_JS)
        return result
    except Exception:
        return None


async def fetch_bid_results_async(page, tender: dict) -> dict:
    """
    Async version: Fetch bid results for a tender.
    Returns dict with all result data ready for DB update.
    """
    urls = get_result_urls(tender)
    raw = await page.evaluate(BID_RESULTS_JS, urls)

    ra_number = raw.get("ra_number", "")

    # Build buyer_details
    buyer_raw = raw.get("buyer_details", {})
    buyer_details = None
    if buyer_raw and buyer_raw.get("buyer_name"):
        buyer_details = {
            "buyer_name": buyer_raw.get("buyer_name", ""),
            "buyer_address": buyer_raw.get("buyer_address", ""),
            "buyer_department": buyer_raw.get("buyer_department", ""),
            "buyer_organisation": buyer_raw.get("buyer_organisation", ""),
            "buyer_ministry": buyer_raw.get("buyer_ministry", ""),
            "bid_validity_days": buyer_raw.get("bid_validity_days", ""),
            "bid_opening_date": buyer_raw.get("bid_opening_date", ""),
            "turnover_requirement": buyer_raw.get("turnover_requirement", ""),
            "experience_required": buyer_raw.get("experience_required", ""),
            "bid_lifecycle": buyer_raw.get("bid_lifecycle", ""),
        }
        if buyer_raw.get("buyer_state"):
            buyer_details["buyer_state"] = buyer_raw["buyer_state"]
        if buyer_raw.get("buyer_office"):
            buyer_details["buyer_office"] = buyer_raw["buyer_office"]

    # Build bid_result
    tech_rows = []
    for entry in raw.get("technical_evaluation", []):
        tech_rows.append({
            "seller_name": entry.get("seller_name", "").strip(),
            "mse_status": entry.get("mse_status", ""),
            "offered_item": entry.get("offered_item", ""),
            "participated_on": entry.get("participated_on", ""),
            "emd_status": entry.get("emd_status", ""),
            "status": entry.get("status", ""),
        })

    fin_rows = []
    for entry in raw.get("financial_evaluation", []):
        rank = clean_rank(entry.get("rank", ""))
        fin_rows.append({
            "seller_name": entry.get("seller_name", "").strip(),
            "mse_status": entry.get("mse_status", ""),
            "offered_item": entry.get("offered_item", ""),
            "total_price": parse_price(entry.get("total_price", "")),
            "rank": rank,
            "status": entry.get("status", ""),
        })

    bid_result = {
        "technical_evaluation": tech_rows,
        "financial_evaluation": fin_rows,
    } if (tech_rows or fin_rows) else None

    # Build ra_result
    ra_result = None
    ra_tech_from_page = raw.get("ra_technical_evaluation", [])
    ra_fin_from_page = raw.get("ra_financial_evaluation", [])

    if ra_tech_from_page or ra_fin_from_page:
        ra_tech = []
        for entry in ra_tech_from_page:
            ra_tech.append({
                "seller_name": entry.get("seller_name", "").strip(),
                "mse_status": entry.get("mse_status", ""),
                "offered_item": entry.get("offered_item", ""),
                "participated_on": entry.get("participated_on", ""),
                "emd_status": entry.get("emd_status", ""),
                "status": entry.get("status", ""),
            })
        ra_fin = []
        for entry in ra_fin_from_page:
            rank = clean_rank(entry.get("rank", ""))
            ra_fin.append({
                "seller_name": entry.get("seller_name", "").strip(),
                "mse_status": entry.get("mse_status", ""),
                "offered_item": entry.get("offered_item", ""),
                "total_price": parse_price(entry.get("total_price", "")),
                "rank": rank,
                "status": entry.get("status", ""),
            })
        if ra_tech or ra_fin:
            ra_result = {
                "technical_evaluation": ra_tech,
                "financial_evaluation": ra_fin,
            }
    else:
        # Fallback: check if this bid has RA and try dedicated endpoints
        has_ra = (tender.get("ra_to_bid", 0) == 1 or
                  tender.get("bid_to_ra", 0) >= 1 or
                  tender.get("bid_type", 0) in (2, 5))
        if has_ra or ra_number:
            bid_id = tender["bid_id"]
            url = f"{BASE_URL}/list-ra-schedules/{bid_id}"
            ra_data = await page.evaluate(RA_SCHEDULE_JS, url)
            if not ra_data:
                url = f"{BASE_URL}/showradocumentPdf/{bid_id}"
                ra_data = await page.evaluate(RA_SCHEDULE_JS, url)
            if ra_data:
                ra_tech = []
                for entry in ra_data.get("technical_evaluation", []):
                    ra_tech.append({
                        "seller_name": entry.get("seller_name", "").strip(),
                        "mse_status": entry.get("mse_status", ""),
                        "offered_item": entry.get("offered_item", ""),
                        "participated_on": entry.get("participated_on", ""),
                        "emd_status": entry.get("emd_status", ""),
                        "status": entry.get("status", ""),
                    })
                ra_fin = []
                for entry in ra_data.get("financial_evaluation", []):
                    rank = clean_rank(entry.get("rank", ""))
                    ra_fin.append({
                        "seller_name": entry.get("seller_name", "").strip(),
                        "mse_status": entry.get("mse_status", ""),
                        "offered_item": entry.get("offered_item", ""),
                        "total_price": parse_price(entry.get("total_price", "")),
                        "rank": rank,
                        "status": entry.get("status", ""),
                    })
                if ra_tech or ra_fin:
                    ra_result = {
                        "technical_evaluation": ra_tech,
                        "financial_evaluation": ra_fin,
                    }

    # ─── Contract Details ─────────────────────────────────────────
    contract_details = None
    bid_number = tender.get("bid_number", "")
    if bid_number:
        contract_details = await fetch_contract_details_async(page, bid_number)
        # Navigate back to bidplus to keep session alive for next tender
        try:
            await page.goto(f"{BASE_URL}/all-bids", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

    return {
        "buyer_details": buyer_details,
        "bid_result": bid_result,
        "ra_result": ra_result,
        "ra_number": ra_number,
        "contract_details": contract_details,
    }
