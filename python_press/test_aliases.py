import pandas as pd
import os
import spacy
from spacy import displacy
from spacy.matcher import PhraseMatcher
from spacy.util import filter_spans

telerama = pd.read_csv("/Users/pol/Downloads/telerama_clean.csv")

aliases = pd.read_csv("/Users/pol/Downloads/aliases.csv")  # or however you import
aliases = aliases.dropna(subset=["mbz_alias"])

alias_to_id = dict(
    zip(aliases["mbz_alias"], aliases["dz_name"])
)

def keep_alias(alias):
    tokens = alias.split()
    
    if len(tokens) > 1:
        return True
    return len(alias) >= 5

alias_to_id = {
    alias: artist_id
    for alias, artist_id in alias_to_id.items()
    if keep_alias(alias)
}


nlp = spacy.load("en_core_web_sm", disable=["ner", "parser", "tagger"])


matcher = PhraseMatcher(nlp.vocab)

patterns = [nlp.make_doc(alias) for alias in alias_to_id.keys()]
matcher.add("ARTIST", patterns)



telerama.info()

telerama["reviewed_artist"]



df = telerama[1:100]
df = df.reset_index().rename(columns={"index": "article_id"})

results = []

for doc, reviewed_artist in zip(nlp.pipe(df["article_text"]), df["reviewed_artist"]):
    matches = matcher(doc)
    
    spans = [doc[start:end] for _, start, end in matches]
    spans = filter_spans(spans)  # keeps longest, removes overlaps
    
    for span in spans:
        results.append({
            "reviewed_artist": reviewed_artist,
            "matched_string": span.text,
            "from_alias": alias_to_id.get(span.text)
        })


matches_df = pd.DataFrame(results)

agg_matches = (
    matches_df
    .groupby("reviewed_artist")["matched_string"]
    .apply(list)
    .reset_index()
)

df = df.merge(agg_matches, on="reviewed_artist", how="left")

df

df.to_csv("/Users/pol/Downloads/test.csv", sep=";")

[s for s in matches_df["article_title"].loc[:100]]


matches_df = matches_df.groupby(["article_id", "artist_id"]).size()
