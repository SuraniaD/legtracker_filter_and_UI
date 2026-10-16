"""
Models for the ingestion gaps the tracker's filter depends on.

This file is the single definition of the new columns and tables:
  - backfill_gaps.py creates them from it (if they don't exist yet), and
  - the same classes should be pasted into the ingestion repo's
    src/models.py, so the repo's own Alembic knows about them and
    `alembic revision --autogenerate` does not try to drop them.

Conventions follow src/models.py exactly: Mapped types without lengths
(VARCHAR), the (congress, bill_type, bill_number) identity instead of a
bills.id foreign key, raw_json kept where a raw payload exists.

── Add to the existing Bill class in src/models.py ────────────────────────
    # From the item-level detail response (/bill/{congress}/{type}/{number})
    policy_area: Mapped[str | None] = mapped_column(nullable=True)
    introduced_date: Mapped[date | None] = mapped_column(nullable=True)
    legislation_url: Mapped[str | None] = mapped_column(nullable=True)
───────────────────────────────────────────────────────────────────────────
Then add the four classes below (BillSubject, BillSummary, BillTitle,
BillFetchLog) to src/models.py, using the repo's existing Base.
"""

from datetime import date, datetime

from sqlalchemy import Index, JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):  # in the repo: use the existing Base instead
    pass


class BillSubject(Base):
    """
    One row per CRS legislative subject, per bill (/subjects endpoint).
    Maintained by DELETE-then-INSERT per bill (same as bill_action), so a
    subject Congress.gov removes also disappears here.
    """

    __tablename__ = "bill_subject"
    __table_args__ = (
        Index("ix_bill_subject_identity", "congress", "bill_type", "bill_number"),
        Index("ix_bill_subject_name", "subject_name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    congress: Mapped[int] = mapped_column(nullable=False)
    bill_type: Mapped[str] = mapped_column(nullable=False)
    bill_number: Mapped[int] = mapped_column(nullable=False)
    subject_name: Mapped[str] = mapped_column(nullable=False)
    update_date: Mapped[datetime | None] = mapped_column(nullable=True)


class BillSummary(Base):
    """
    One row per CRS summary version, per bill (/summaries endpoint).
    A bill can have several (introduced, reported, passed...); the latest
    by action_date/update_date is the current one. text_html is exactly as
    returned; text_plain is the same with tags removed.
    DELETE-then-INSERT per bill.
    """

    __tablename__ = "bill_summary"
    __table_args__ = (
        Index("ix_bill_summary_identity", "congress", "bill_type", "bill_number"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    congress: Mapped[int] = mapped_column(nullable=False)
    bill_type: Mapped[str] = mapped_column(nullable=False)
    bill_number: Mapped[int] = mapped_column(nullable=False)
    version_code: Mapped[str | None] = mapped_column(nullable=True)
    action_date: Mapped[date | None] = mapped_column(nullable=True)
    action_desc: Mapped[str | None] = mapped_column(nullable=True)
    text_html: Mapped[str | None] = mapped_column(nullable=True)
    text_plain: Mapped[str | None] = mapped_column(nullable=True)
    update_date: Mapped[datetime | None] = mapped_column(nullable=True)
    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class BillTitle(Base):
    """
    One row per title, per bill (/titles endpoint): display, short and
    official titles, per text version. The "official title" the tracker
    uses is the longest row whose title_type starts with "Official Title".
    DELETE-then-INSERT per bill (no stable natural key).
    """

    __tablename__ = "bill_title"
    __table_args__ = (
        Index("ix_bill_title_identity", "congress", "bill_type", "bill_number"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    congress: Mapped[int] = mapped_column(nullable=False)
    bill_type: Mapped[str] = mapped_column(nullable=False)
    bill_number: Mapped[int] = mapped_column(nullable=False)
    title: Mapped[str] = mapped_column(nullable=False)
    title_type: Mapped[str | None] = mapped_column(nullable=True)
    title_type_code: Mapped[str | None] = mapped_column(nullable=True)
    bill_text_version_code: Mapped[str | None] = mapped_column(nullable=True)
    bill_text_version_name: Mapped[str | None] = mapped_column(nullable=True)
    chamber_code: Mapped[str | None] = mapped_column(nullable=True)
    update_date: Mapped[datetime | None] = mapped_column(nullable=True)


class BillFetchLog(Base):
    """
    One row per (bill, endpoint): when it was last fetched, against which
    version of the bill, and how many items came back.

    Needed because "no rows yet" can't tell "not fetched" from "fetched,
    genuinely empty" (many bills have no summary yet). A bill is re-fetched
    when bills.update_date_including_text moves past source_update_date,
    so later runs only touch bills that changed.
    endpoint: detail | actions | cosponsors | text | subjects | summaries | titles
    status:   ok | error | not_found  (404 at Congress.gov; not retried unless the bill changes)
    """

    __tablename__ = "bill_fetch_log"

    congress: Mapped[int] = mapped_column(primary_key=True)
    bill_type: Mapped[str] = mapped_column(primary_key=True)
    bill_number: Mapped[int] = mapped_column(primary_key=True)
    endpoint: Mapped[str] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column(nullable=False)
    item_count: Mapped[int | None] = mapped_column(nullable=True)
    source_update_date: Mapped[datetime | None] = mapped_column(nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    error_message: Mapped[str | None] = mapped_column(nullable=True)


# Columns added to the existing `bills` table (ALTER TABLE ... IF NOT EXISTS).
# Types match what SQLAlchemy generates for the Mapped annotations above.
BILL_NEW_COLUMNS = {
    "policy_area": "VARCHAR",
    "introduced_date": "DATE",
    "legislation_url": "VARCHAR",
}

NEW_TABLES = [BillSubject.__table__, BillSummary.__table__, BillTitle.__table__, BillFetchLog.__table__]
