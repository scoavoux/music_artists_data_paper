import pandas as pd
import os
import spacy
from spacy import displacy
from spacy.matcher import PhraseMatcher
from spacy.util import filter_spans
import unicodedata


press = pd.read_excel("/Users/pol/Downloads/press_corpus.xlsx")

press.info()

aliases = pd.read_excel("/Users/pol/Downloads/aliases.xlsx")  # or however you import
aliases = aliases.dropna(subset=["mbz_alias"])

aliases = aliases.drop(columns="alias_regex")

# normalize both texts
def clean_text(text):
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = text.replace("\xa0", " ")
    text = text.strip()
    return text


press["article_text"] = press["article_text"].apply(clean_text)
aliases["mbz_alias"] = aliases["mbz_alias"].apply(clean_text)


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


nlp = spacy.load("fr_core_news_sm", disable=["ner", "parser", "tagger"])


matcher = PhraseMatcher(nlp.vocab, attr='LOWER')

patterns = [nlp.make_doc(alias) for alias in alias_to_id.keys()]
matcher.add("ARTIST", patterns)


df = press
df = df.reset_index().rename(columns={"index": "article_id"})


results = []


docs = list(nlp.pipe(df["article_text"].fillna("")))

for row, doc in zip(df.itertuples(index=False), docs):
    matches = matcher(doc)
    spans = filter_spans([doc[start:end] for _, start, end in matches])
    
    for span in spans:
        results.append({
            "article_id": row.article_id,
            "reviewed_artist": row.reviewed_artist,
            "matched_string": span.text,
            "from_alias": alias_to_id.get(span.text)
        })

results

matches_df = pd.DataFrame(results)

agg_matches = (
    matches_df
    .groupby("article_id")["matched_string"]
    .apply(list)
    .reset_index()
)

df = df.merge(agg_matches, on="article_id", how="left")

df
df.to_excel("/Users/pol/Downloads/telerama_phrase_matches.xlsx")


