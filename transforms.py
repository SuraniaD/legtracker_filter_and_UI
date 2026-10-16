"""
Transforms from raw Congress.gov responses to table rows.

The functions for bills, actions, cosponsors, text versions and sponsors are
line-for-line mirrors of the ingestion repo's src/transform.py, returning dicts
(for SQLAlchemy Core) instead of ORM objects, so rows written here are
identical to rows the ingestion repo writes. Keep them in step with that file.

One deliberate difference: timestamps with a "Z" suffix are stored as naive UTC.
The repo's _parse_datetime returns an aware datetime for those, which Postgres
converts to the session time zone when writing to a `timestamp` column; on a
UTC server (Railway's default) the stored values are the same.
"""

import html
import re
from datetime import date, datetime, timezone


# ── date parsing (src/transform.py) ──────────────────────────────────────────

def parse_date(value):
    """Plain YYYY-MM-DD, or a full timestamp whose date part is taken."""
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()


def parse_datetime(value):
    """Date-only or full timestamp; returned as naive UTC."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.combine(date.fromisoformat(value), datetime.min.time())
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


# ── mirrors of src/transform.py ──────────────────────────────────────────────

def transform_bill(raw: dict) -> dict:
    """transform_bill(): raises KeyError/ValueError on missing/malformed required fields."""
    latest_action = raw.get("latestAction") or {}
    return {
        "congress": raw["congress"],
        "bill_type": raw["type"],
        "bill_number": int(raw["number"]),
        "title": raw.get("title"),
        "origin_chamber": raw.get("originChamber"),
        "latest_action_date": parse_date(latest_action.get("actionDate")),
        "latest_action_text": latest_action.get("text"),
        "update_date": parse_datetime(raw.get("updateDate")),
        "update_date_including_text": parse_datetime(raw.get("updateDateIncludingText")),
        "api_url": raw.get("url"),
    }


def transform_action(raw: dict, congress: int, bill_type: str, bill_number: int) -> dict:
    source_system = raw.get("sourceSystem") or {}
    return {
        "congress": congress,
        "bill_type": bill_type,
        "bill_number": bill_number,
        "action_date": parse_date(raw.get("actionDate")),
        "action_text": raw.get("text"),
        "action_type": raw.get("type"),
        "source_system": source_system.get("name"),
        "action_code": raw.get("actionCode"),
    }


def transform_cosponsor(raw: dict, congress: int, bill_type: str, bill_number: int) -> dict:
    return {
        "congress": congress,
        "bill_type": bill_type,
        "bill_number": bill_number,
        "bioguide_id": raw["bioguideId"],
        "full_name": raw.get("fullName"),
        "first_name": raw.get("firstName"),
        "last_name": raw.get("lastName"),
        "party": raw.get("party"),
        "state": raw.get("state"),
        "sponsorship_date": parse_date(raw.get("sponsorshipDate")),
        "is_original_cosponsor": raw.get("isOriginalCosponsor"),
        "withdrawn_date": parse_date(raw.get("sponsorshipWithdrawnDate")),
        "raw_json": raw,
    }


def transform_text_versions(raw_versions: list, congress: int, bill_type: str, bill_number: int):
    """One version fans out into one row per format. Returns (rows, errors)."""
    rows, errors = [], []
    for raw in raw_versions:
        try:
            version_type = raw.get("type")
            version_date = parse_date(raw.get("date"))
            for fmt in raw.get("formats") or []:
                rows.append({
                    "congress": congress,
                    "bill_type": bill_type,
                    "bill_number": bill_number,
                    "version_type": version_type,
                    "version_date": version_date,
                    "format_type": fmt.get("type", "unknown"),
                    "text_url": fmt.get("url"),
                    "raw_json": raw,
                })
        except (KeyError, ValueError, TypeError) as exc:
            errors.append({"version": raw, "error": str(exc)})
    return rows, errors


def transform_sponsor(raw_detail: dict):
    """Primary sponsor as a member row, or None if the bill has no sponsor."""
    bill_detail = raw_detail.get("bill", {})
    sponsors = bill_detail.get("sponsors") or []
    if not sponsors:
        return None
    sponsor = sponsors[0]
    return {
        "bioguide_id": sponsor["bioguideId"],
        "full_name": sponsor.get("fullName"),
        "first_name": sponsor.get("firstName"),
        "last_name": sponsor.get("lastName"),
        "party": sponsor.get("party"),
        "state": sponsor.get("state"),
        "district": sponsor.get("district"),
    }


def transform_many(fn, raws, *args):
    """Skip-and-continue over a list, like transform_actions/transform_cosponsors."""
    rows, errors = [], []
    for raw in raws:
        try:
            rows.append(fn(raw, *args))
        except (KeyError, ValueError, TypeError) as exc:
            errors.append({"item": raw, "error": str(exc)})
    return rows, errors


# ── gap endpoints (new) ──────────────────────────────────────────────────────

_TAG_RE = re.compile(r"<[^>]+>")
_BLOCK_RE = re.compile(r"</(p|li|h\d|div)>|<br\s*/?>", re.IGNORECASE)


def html_to_text(value):
    if not value:
        return None
    text_ = _BLOCK_RE.sub("\n", value)
    text_ = html.unescape(_TAG_RE.sub("", text_))
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text_.splitlines()]
    return "\n".join(line for line in lines if line) or None


def transform_detail_fields(raw_detail: dict) -> dict:
    bill = raw_detail.get("bill") or {}
    return {
        "policy_area": (bill.get("policyArea") or {}).get("name"),
        "introduced_date": parse_date(bill.get("introducedDate")),
        "legislation_url": bill.get("legislationUrl"),
    }


def transform_subjects(raws: list) -> list:
    seen, rows = set(), []
    for s in raws:
        name = s.get("name")
        if name and name not in seen:
            seen.add(name)
            rows.append({"subject_name": name, "update_date": parse_datetime(s.get("updateDate"))})
    return rows


def transform_summaries(raws: list) -> list:
    return [{
        "version_code": s.get("versionCode"),
        "action_date": parse_date(s.get("actionDate")),
        "action_desc": s.get("actionDesc"),
        "text_html": s.get("text"),
        "text_plain": html_to_text(s.get("text")),
        "update_date": parse_datetime(s.get("updateDate")),
        "raw_json": s,
    } for s in raws]


def transform_titles(raws: list) -> list:
    return [{
        "title": t["title"],
        "title_type": t.get("titleType"),
        "title_type_code": str(t["titleTypeCode"]) if t.get("titleTypeCode") is not None else None,
        "bill_text_version_code": t.get("billTextVersionCode"),
        "bill_text_version_name": t.get("billTextVersionName"),
        "chamber_code": t.get("chamberCode"),
        "update_date": parse_datetime(t.get("updateDate")),
    } for t in raws if t.get("title")]
