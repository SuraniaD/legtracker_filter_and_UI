"""
Filter rules: gates 1-3 of the old LegTracker ingestion, copied verbatim.

Everything between the COPIED markers below was extracted unchanged (by
`ast.get_source_segment`) from the old codebase:
  backend/app/services/matching.py        gate 1 (dead bills), gate 2 rules 1-4
  backend/app/services/semantic_scoring.py gate 3 candidate text, anchors, scale
so the tracker's filter makes the same decisions as the old ingestion. Change
filtering by editing this file (and say so in a commit), not the job.
"""
from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Set, Tuple

# ======================= COPIED from matching.py =======================
def _contains_keyword(text: str, keyword: str) -> bool:
    """Whole-word/phrase match, not a raw substring check.

    Plain `keyword in text` false-positives badly on short keywords — e.g.
    "dead" matches inside "deadline", which is extremely common in real
    Congress.gov action text ("extends the deadline for comments"). That
    false match caused is_action_active() to misclassify active bills as
    dead, which deletes them from the database in sync.py.
    """
    return re.search(rf"\b{re.escape(keyword)}\b", text) is not None


ALDF_POLICY_AREAS: Set[str] = {"Animals", "Agriculture and Food"}


ANIMAL_KEYWORDS: List[str] = [
    # general
    "animal", "animals", "wildlife", "humane", "cruelty", "vivisection",
    # farmed land animals
    "livestock", "cattle", "cow", "cows", "calf", "calves", "herd", "herds",
    "ranch", "ranching", "rancher", "ranchers", "poultry", "chicken", "chickens",
    "hen", "hens", "egg", "eggs", "dairy", "milk", "butter", "cheese",
    "meat", "meatpacking", "beef", "pork", "swine", "hog", "hogs", "pig", "pigs",
    "lamb", "sheep", "goat", "goats", "slaughter", "slaughterhouse", "butcher",
    # companion and working animals
    "dog", "dogs", "canine", "cats", "feline", "pet", "pets", "puppy", "puppies",
    "kitten", "kittens", "horse", "horses", "equine", "service animal",
    # aquatic
    "fish", "fishes", "fishery", "fisheries", "fishing", "shrimp", "lobster",
    "crab", "oyster", "oysters", "seafood", "aquaculture", "aquatic",
    "marine mammal", "whale", "whales", "dolphin", "dolphins", "sea turtle",
    # wild
    "bird", "birds", "avian", "migratory bird", "bee", "bees", "pollinator",
    "pollinators", "insect", "insects", "primate", "primates", "elephant",
    "elephants", "wolf", "wolves", "bison", "deer", "elk",
    "endangered", "habitat", "habitats", "refuge", "refuges",
    "hunting", "trapping", "poaching", "fur",
    # captivity and blood sports
    "zoo", "zoos", "aquarium", "aquariums", "circus", "rodeo", "greyhound",
    "cockfighting", "dogfighting", "soring",
    # veterinary
    "veterinary", "veterinarian", "veterinarians",
]


def keyword_match(*texts: Optional[str]) -> Tuple[bool, List[str]]:
    """Whole-word scan of any bill text for animal-related vocabulary.

    Pass every title variant and the summary; Congress.gov's display title is
    frequently a content-free acronym ("WATCH Act", "LEASH Act"), so the
    official title is usually where the subject matter actually appears.

    Returns (matched, sorted distinct keywords found).
    """
    blob = " ".join(t for t in texts if t).lower()
    if not blob.strip():
        return False, []
    hits = {kw for kw in ANIMAL_KEYWORDS if _contains_keyword(blob, kw)}
    return bool(hits), sorted(hits)


ANIMAL_COMMITTEE_PATTERNS: List[str] = [
    "committee on agriculture",
    "committee on natural resources",
    "committee on agriculture, nutrition, and forestry",
    "committee on environment and public works",
    "subcommittee on livestock",
    "subcommittee on conservation",
    "subcommittee on water, wildlife and fisheries",
    "subcommittee on fisheries, wildlife",
]


def committee_match(action_text: Optional[str]) -> Tuple[bool, List[str]]:
    """Detect referral to a committee whose remit covers animals or habitat.

    Reads the latest-action text ("Referred to the House Committee on
    Agriculture."). Substring matching is safe here because the patterns are
    long multi-word committee names, not short words.
    """
    if not action_text:
        return False, []
    lowered = action_text.lower()
    hits = sorted({p for p in ANIMAL_COMMITTEE_PATTERNS if p in lowered})
    return bool(hits), hits


def match_bill(
    policy_area: Optional[str],
    legislative_subjects: List[str],
    active_animal_subjects: Set[str]
) -> Tuple[bool, List[str]]:
    """
    Checks if a bill is animal-related.
    
    A bill matches if:
    1. policy_area == "Animals"
    OR
    2. Any subject name in legislative_subjects matches an active record in active_animal_subjects.
    
    Returns a tuple of (is_matched, matching_subjects).
    """
    matching_subjects = []
    is_matched = False

    # Rule 1: Policy area is one of the ALDF-tracked areas (Animals, Agriculture and Food).
    if policy_area in ALDF_POLICY_AREAS:
        is_matched = True

    # Rule-2: At least one legislative subject matches our active list
    for subject in legislative_subjects:
        if subject in active_animal_subjects:
            matching_subjects.append(subject)
            is_matched = True

    return is_matched, matching_subjects


DEAD_KEYWORDS = [
    "vetoed",
    "veto",
    "failed",
    "rejected",
    "tabled",
    "indefinitely postponed",
    "withdrawn",
    "dead",
    "died",
]


def is_action_active(action_text: Optional[str]) -> bool:
    """
    Checks if a bill's latest action indicates it should still be tracked.

    A bill is INACTIVE (and dropped) only if it ended without becoming law:
    - Vetoed (e.g. "vetoed", "veto")
    - Failed (e.g. "failed", "rejected", "tabled", "indefinitely postponed")
    - Withdrawn (e.g. "withdrawn")
    - Dead (e.g. "dead", "died")

    Bills that became law are NOT inactive here — get_current_stage() below
    maps that text to the "Became Law" stage instead — since that's the
    successful, final outcome we want to keep and surface, not discard like
    a bill that died in committee.

    Otherwise (including "became law"), it is considered ACTIVE.
    """
    if not action_text:
        return True  # If no action recorded, assume active (e.g. newly introduced)

    text_lower = action_text.lower()

    for kw in DEAD_KEYWORDS:
        if _contains_keyword(text_lower, kw):
            return False

    return True

# ================== COPIED from semantic_scoring.py ==================
def build_candidate_text(
    title: Optional[str],
    policy_area: Optional[str],
    subject_names: List[str],
    summary: Optional[str],
    official_title: Optional[str] = None,
) -> str:
    """Build the text blob used to embed a bill (stored or not-yet-stored).

    `title` is Congress.gov's display title, which is frequently a content-free
    acronym — a bill could previously be embedded as
    "WATCH Act | WATCH Act | Policy area: Health" and score near zero against
    every animal-welfare anchor despite being an animal-testing bill. The
    official title carries the substance, so it is repeated as the high-signal
    component whenever it is available, and the display title is kept as a
    secondary term rather than being doubled on its own.
    """
    parts = []
    headline = official_title or title
    if headline:
        parts.append(headline)
        parts.append(headline)  # repeat — highest signal
    if official_title and title and title != official_title:
        parts.append(title)
    if policy_area:
        parts.append(f"Policy area: {policy_area}")
    if subject_names:
        parts.append("Subjects: " + ", ".join(subject_names))
    if summary:
        parts.append(summary[:1500])
    return " | ".join(parts)


def cosine_similarity(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


ANCHOR_PHRASES: List[str] = [
    "Animal welfare and protection of animals from cruelty and abuse",
    "Regulation of animal testing and the use of animals in scientific research",
    "Welfare and treatment of farmed animals, livestock, and factory farming practices",
    "Protection of wildlife, endangered species, and habitat conservation",
    "Regulation of pets, companion animals, and service animals",
    "Wildlife trafficking, animal smuggling, and crimes against animals",
    "Animal shelters, rescue operations, and humane treatment standards",
    "Veterinary medicine, animal health, and infectious or parasitic animal diseases",
    "Aquaculture, seafood, meat, and food supply safety and labeling standards",
    "Hunting, fishing, and wildlife management regulations",
    "Conservation of marine, coastal, and freshwater ecosystems, fisheries, and habitats",
    "Protection of birds, fish, insects, mammals, and reptiles and their natural habitats",
    "Zoos, aquariums, circuses, and the treatment of captive or exhibited animals",
    "Animal fighting, dogfighting, cockfighting, and horse soring or racing welfare",
    "Humane slaughter methods and standards for livestock processing",
    "Funding, appropriations, and enforcement programs for animal welfare agencies",
]


_PREFILTER_SIM_LOW = 0.12


_PREFILTER_SIM_HIGH = 0.50


PREFILTER_THRESHOLD = 20

# ======================= end of copied code =======================


# ── helpers reproducing inline logic of the old sync.process_bill ────────────

def official_summary_from_html(raw_text: Optional[str]) -> Optional[str]:
    """Old sync.py: strip tags to spaces, collapse whitespace (entities kept)."""
    raw_text = raw_text or ""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", raw_text)).strip() or None


def official_title_from_titles(titles: List[Tuple[Optional[str], Optional[str]]]) -> Optional[str]:
    """Old sync.py: the longest title whose type contains 'official title'.
    `titles` is a list of (title, title_type) in API order."""
    candidates = [(t or "").strip() for t, ttype in titles if "official title" in (ttype or "").lower()]
    return max(candidates, key=len) if candidates else None


def all_title_text(display_title: Optional[str], titles: List[Tuple[Optional[str], Optional[str]]]) -> Optional[str]:
    """Old sync.py: display title + every title variant, space-joined."""
    return " ".join([display_title or ""] + [(t or "") for t, _ in titles]).strip() or display_title


def prefilter_score(text_vec: List[float], anchor_vecs: List[List[float]]) -> int:
    """Old semantic_prefilter(), given the vectors: best similarity to any
    anchor, scaled 0.12 -> 0 and 0.50 -> 100, rounded like the old code."""
    best_sim = max(cosine_similarity(text_vec, a) for a in anchor_vecs)
    clamped = max(0.0, min(1.0, (best_sim - _PREFILTER_SIM_LOW) / (_PREFILTER_SIM_HIGH - _PREFILTER_SIM_LOW)))
    return round(clamped * 100)
