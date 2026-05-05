# ------------------------------------------

library(tidyr)
library(purrr)
library(igraph)

# assume your data is mbz_genre

# collapse to one row per artist-genre (in case duplicates exist)
df <- mbz_genre %>%
  group_by(mbz_artist_id, mbz_genre) %>%
  summarise(n_albums = sum(n_albums), .groups = "drop")

# remove rare genres
df <- df %>%
  group_by(mbz_genre) %>%
  filter(n() > 100)


# create all genre pairs per artist
edges <- df %>%
  arrange(mbz_artist_id) %>%
  group_by(mbz_artist_id) %>%
  summarise(
    pairs = list({
      g <- mbz_genre
      w <- n_albums
      
      if (length(g) < 2) {
        NULL
      } else {
        comb <- t(combn(seq_along(g), 2))
        
        tibble(
          g1 = g[comb[,1]],
          g2 = g[comb[,2]],
          weight = pmin(w[comb[,1]], w[comb[,2]])
        )
      }
    }),
    .groups = "drop"
  ) %>%
  tidyr::unnest(pairs)


edges_agg <- edges %>%
  group_by(g1, g2) %>%
  summarise(weight = sum(weight), .groups = "drop")


g <- graph_from_data_frame(edges_agg, directed = FALSE)

# simplify just in case
g <- simplify(g, edge.attr.comb = list(weight = "sum"))


# compute node strength
strengths <- strength(g, weights = E(g)$weight)

E(g)$weight_norm <- E(g)$weight / 
  (strengths[ends(g, E(g))[,1]] * strengths[ends(g, E(g))[,2]])

cl <- cluster_louvain(g, weights = E(g)$weight)

membership <- membership(cl)


# keep strongest edges only (e.g. top 5%)
threshold <- quantile(E(g)$weight, 0.6)

g_plot <- subgraph_from_edges(g, 
                              E(g)[weight >= threshold], 
                              delete.vertices = TRUE)

plot(
  g_plot,
  vertex.size = 5,
  vertex.label.cex = 0.3,
  vertex.color = membership[V(g_plot)],
  edge.width = E(g_plot)$weight / max(E(g_plot)$weight) * 5,
  layout = layout_with_fr
)

genre_clusters <- tibble(
  genre = names(membership),
  cluster = membership
)


genre_clusters <- genre_clusters %>% 
  arrange(cluster) %>% 
  View()













