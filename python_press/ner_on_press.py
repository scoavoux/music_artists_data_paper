from tqdm import tqdm
import pandas as pd
import spacy
import re

# python -m spacy download fr_core_news_lg

pd.set_option('display.max_columns', 30)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

presse = pd.read_csv("/Users/pol/Downloads/press_corpus.csv")
presse

nlp = spacy.load('fr_core_news_lg')

df = presse

# create spacy doc object with article_text, keep article id
# parallelize
docs = nlp.pipe(
    zip(df["article_text"], df["article_id"]),
    as_tuples=True,
    disable=['tok2vec', 'parser', 'lemmatizer'],
    batch_size=8,
    n_process=4
)

result = []

# for all ents that are not LOC, extract ents contained in each article
with tqdm(total=len(df)) as pbar:
    for doc, idx in docs: 
        for ent in doc.ents:
            if  ent.label_ != 'LOC':
                result.append((idx, ent.text, ent.label_))
            
        pbar.update(1)
        
ner_df = pd.DataFrame(result, columns=['article_id', 'ent_name', 'ner_type'])
ner_df.to_csv("/Users/pol/Downloads/ner_df_raw.csv", 
                      sep=";",
                      encoding="utf-8-sig")

ner_df = pd.read_csv("/Users/pol/Downloads/ner_df_raw.csv",
                     sep=";",
                      encoding="utf-8-sig")

ner_df = ner_df.drop(columns="Unnamed: 0")
ner_df = ner_df.dropna(subset="ent_name")


# solve tiny issue with names starting with - (that lead to csv encoding error)
def remove_tiretdusix(string):
    string = re.sub(pattern="^-", repl="", string=string)
    string = string.strip()
    return string

ner_df["ent_name"] = ner_df["ent_name"].apply(remove_tiretdusix)

# count occurrences of each name
ner_df["name_count"] = ner_df["ent_name"].map(ner_df["ent_name"].value_counts()) # in total
# ner_df["article_count"] = ner_df.groupby("name")["article_id"].transform("nunique") # in N articles

# join source again to prepare decomposition into media outlets
ner_df = ner_df.merge(
    presse[["article_id", "source"]],
    on="article_id",
    how="left"
)


# decompose name and article count into media outlet
source_stats = (
    ner_df.groupby(["ent_name", "source"])
    .agg(
        name_count=("ent_name", "size"),
        #article_count=("article_id", "nunique")
    )
)

wide = source_stats.unstack(fill_value=0)
wide.columns = [f"{metric}_{source}" for metric, source in wide.columns]
wide = wide.reset_index()

# df of unique extracted entities
extracted_ents = (
    ner_df
    .groupby("ent_name")
    .agg({"article_id": "first",
          "name_count": "first",
          #"article_count": "first"
          })
    .sort_values(by='name_count', ascending=False)
    .reset_index()
    .merge(wide,
           on="ent_name",
           how="left")
)

extracted_ents["name_id"] = extracted_ents.index + 1

# save
extracted_ents.to_csv("/Users/pol/Downloads/extracted_ents_1203.csv", 
                      sep=";",
                      encoding="utf-8-sig")

# press_corpus with column listing all entities for each text
article_text = presse[["article_id", "article_text"]]


press_corpus_ents = (
    ner_df
    .groupby("article_id").agg({
            "ent_name": list,
            #"name_count": "first",
            #"article_count": "first"
        })
    .join(article_text, on="article_id")
)

press_corpus_ents = press_corpus_ents[["article_id", "article_text", "ent_name"]]

press_corpus_ents.to_csv("/Users/pol/Downloads/press_corpus_ents_1103.csv", 
                   sep=";",
                   encoding="utf-8-sig")


