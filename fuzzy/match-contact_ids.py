import pandas as pd
from rapidfuzz import process, fuzz

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



def best_match_single(name_norm, key):
    group = contact_groups.get(key)
    if group is None:
        return pd.Series([None, None, None])

    match = process.extractOne(
        name_norm,
        group["names"],
        scorer=fuzz.token_set_ratio,
        score_cutoff=80
    )

    if match is None:
        return pd.Series([None, None, None])

    matched_name, score, pos = match
    row = group["rows"].iloc[pos]

    return pd.Series([row["contact_id"], row["contact_name"], score])



df_missing[["contact_id", "contact_name", "score"]] = df_missing.apply(
    lambda r: best_match_single(r["name"], r["key"]),
    axis=1
)


df_missing.head()
df_missing.info()

df_result = df_missing.sort_values("score", ascending=False)
df_result

df_result.to_excel("/Users/pol/Downloads/result.xlsx")