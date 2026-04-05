import re
import os

def normalize_idx(raw_indexes):
    # Clean and flatten
    clean_indexes = []
    for item in raw_indexes:
        if isinstance(item, int):
            clean_indexes.append(item)
        elif isinstance(item, str):
            # Strip parentheses and split by comma
            nums = item.strip("()").split(",")
            for n in nums:
                clean_indexes.append(int(n.strip()))
    return clean_indexes

def strip_trailing_parenthetical(name: str) -> str:
    """Remove a trailing parenthetical like '(Brendan)' and trim spaces."""
    if not isinstance(name, str):
        return ""
    return re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
        
def clean_unatural_indexes(needs_cleaning, special_maps=None):
    clean_special = {}
    cleaned_normal = {}
    
    abuse_map = {
        122: {"question": "Four or more voting rights violations in past 25 years", "score": 2},
        123: {"question": "One to three voting rights violations in past 25 years", "score": 1},
        124: {"question": "No history of voting rights violations in past 25 years", "score": 0},
    }

    Post_office_map = {
        80: {"question": "All residents have access to post offices within 10 miles (0)", "score": 0},
        81: {"question": "Most residents have access to post offices within 10 miles (1)", "score": 1},
        82: {"question": "Some residents have access to post offices within 10 miles (2)", "score": 2},
        83: {"question": "Few residents have access to post offices within 10 miles (3)", "score": 3},
        84: {"question": "No residents have access to post offices within 10 miles (4)", "score": 4},
        85: {"question": "No post office in the area (5)", "score": 5},
    }

    Funding_Map = {
        100: {"question": "State provides no funding (0)", "score": 0},
        101: {"question": "State provides limited funding for some elections (1)", "score": 1},
        102: {"question": "State provides a portion of costs of all elections (2)", "score": 2},
        103: {"question": "State provides full funding for elections (3)", "score": 3},
    }

    special_maps = [abuse_map, Post_office_map, Funding_Map]

    # Normalize input: allow passing a single dict or list of dicts
    if special_maps is None:
        special_maps = []
    elif isinstance(special_maps, dict):
        special_maps = [special_maps]

    # Merge all maps into one lookup
    merged_map = {}
    for m in special_maps:
        merged_map.update(m)

    for key, value in needs_cleaning.items():
        print(f"Cleaning index {key}: {value}")
        if not value:
            continue

        if not str(value[0]).isdigit():
            continue

        k = int(key)

        # Handle special mapped indexes (e.g., abuse_map, civics_map, etc.)
        if k in merged_map:
            info = merged_map[k]
            link = value[2] if len(value) > 2 else None
            clean_special[k] = [info["score"], info["question"], link]
            continue

        # Handle normal indexes
        try:
            score = int(str(value[0]).strip())
        except (IndexError, TypeError, ValueError):
            score = 0

        definition = value[1] if len(value) > 1 else None
        link = value[2] if len(value) > 2 else None
        cleaned_normal[k] = [score, definition, link]

    # Edge case: nothing mapped at all → apply a default if desired
    if not clean_special and not cleaned_normal and merged_map:
        # Use the highest key in the special map as fallback
        max_key = max(merged_map.keys())
        fallback = merged_map[max_key]
        clean_special[max_key] = [fallback["score"], fallback["question"], None]

    combined = {**clean_special, **cleaned_normal}
    print(f"Combined cleaned indexes: {combined}")
    return combined
