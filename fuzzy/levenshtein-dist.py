import pandas as pd
from rapidfuzz import process
from rapidfuzz.distance import Levenshtein

# try to fuzzy match the names of "all" (missing contact or mbz)
# with the names of contacts.csv and mbz_deezer.csv

# Import data
df_missing = pd.read_csv("/Users/pol/Downloads/missing_contact_after_consol.csv")
df_contacts = pd.read_csv("/Users/pol/Downloads/contacts.csv")

## drop duplicates for now
df_contacts = df_contacts.drop_duplicates(subset=['contact_name'], 
                                          keep=False) # drops ALL the duplicates


df_missing = df_missing[["name", "deezer_id", "f_n_play"]].dropna(subset=["name"])
df_contacts = df_contacts[["contact_id", "contact_name"]].dropna(subset=["contact_name"])

#df_missing["name_norm"] = df_missing["name"]# .str.lower().str.strip()
#df_contacts["contact_name_norm"] = df_contacts["contact_name"]# .str.lower().str.strip()


# blocking key (first letter)
df_missing["key"] = df_missing["name"].str[0]
df_contacts["key"] = df_contacts["contact_name"].str[0]

contact_groups = {}

for k, g in df_contacts.groupby("key"):
    contact_groups[k] = {
        "names": g["contact_name"].tolist(),
        "rows": g.reset_index(drop=True)
    }



# ---------- token logic ----------

def _tokens(s):
    return s.split()


def _token_edit_distance(a, b):
    ta = _tokens(a)
    tb = _tokens(b)

    cost = 0
    used = set()

    for x in ta:
        best_d = None
        best_j = None
        for j, y in enumerate(tb):
            if j in used:
                continue
            d = Levenshtein.distance(x, y)
            if best_d is None or d < best_d:
                best_d = d
                best_j = j

        if best_d is not None:
            cost += best_d
            used.add(best_j)
        else:
            cost += len(x)

    # penalty for extra tokens in b
    for j, y in enumerate(tb):
        if j not in used:
            cost += len(y)

    return cost


# ---------- STRICT scorer ----------
# 100 only if tokens are exactly identical

# Must accept **kwargs for RapidFuzz compatibility

def strict_token_equality_scorer(a, b, *, max_dist=5, **kwargs):
    # 100 ONLY if tokens identical
    if _tokens(a) == _tokens(b):
        return 100

    d = _token_edit_distance(a, b)
    if d > max_dist:
        return None

    # convert distance → score
    return 100 - 10 * d


# ---------- integrated best match ----------
# DROP-IN replacement for your function

def best_match_single(name, key, max_dist=5):
    group = contact_groups.get(key)
    if group is None:
        return pd.Series([None, None, None])

    match = process.extractOne(
        name,
        group["names"],
        scorer=lambda a, b, **kw: strict_token_equality_scorer(
            a, b, max_dist=max_dist, **kw
        ),
    )

    if match is None:
        return pd.Series([None, None, None])

    matched_name, score, pos = match
    row = group["rows"].iloc[pos]

    return pd.Series([row["contact_id"], row["contact_name"], score])


df_missing[["contact_id", "contact_name", "score"]] = df_missing.apply(
    lambda r: best_match_single(r["name"], r["key"], max_dist=5),
    axis=1
)