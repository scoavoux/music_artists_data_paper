-- musicbrainz_classical_composer_filter.sql
--
-- Restrict the ISRC-documented composer set (~103k) to likely art-music
-- composers using a UNION of three independent signals, so that no single
-- tail is dropped. One row per surviving composer, with the signal(s) that
-- fired, so the boundary can be audited.
--
--   sig_worktype : composer of >=1 work typed as a classical form. Precision
--                  signal; survives into early music when works are typed.
--   sig_birth    : born before 1900. Captures the historical canon even when
--                  works are untyped (no recorded-pop confound that early).
--   sig_tag      : carries a classical genre/tag. The only signal that catches
--                  LIVING classical composers (whom birth-year cannot separate).
--
-- A composer is KEPT if ANY signal fires (union, not intersection): each signal
-- is blind to a different tail, so intersecting would gut recall. Bias is
-- deliberately toward recall -- a stray pop composer is prunable downstream, a
-- silently dropped classical composer is not.
--
-- BEFORE TRUSTING: review the two vocabularies below against your own data:
--     SELECT name FROM work_type ORDER BY name;
--     SELECT name, ref_count FROM tag WHERE lower(name) LIKE '%classic%';
-- Names not present simply no-op in the IN lists, so erring generous is safe.
-- 'Song' and 'Soundtrack' are intentionally EXCLUDED as classical work types:
-- art song is captured by 'Song-cycle' / sig_tag / sig_birth, while 'Song'
-- alone is indistinguishable from pop and 'Soundtrack' is film/TV/game.

COPY (
WITH
-- classical work-type vocabulary (edit/extend to match work_type table)
classical_work_type AS (
  SELECT id FROM work_type
  WHERE name IN (
    'Symphony','Concerto','Sonata','Suite','Partita','Overture',
    'Symphonic poem','Quartet','Quintet','Aria','Cantata','Oratorio',
    'Mass','Motet','Madrigal','Opera','Operetta','Zarzuela','Ballet',
    'Song-cycle','Étude'
  )
),
-- classical tag/genre vocabulary (tunable)
classical_tag AS (
  SELECT id FROM tag
  WHERE lower(name) IN (
    'classical','baroque','romantic','classical period','contemporary classical',
    'modern classical','early music','renaissance','medieval','opera','choral',
    'lied','art song','orchestral'
  )
  OR lower(name) LIKE '%classical%'
),
-- population: composers with at least one ISRC-documented recording
composer_isrc AS (
  SELECT DISTINCT law.entity0 AS artist_id
  FROM isrc
  JOIN l_recording_work lrw ON lrw.entity0 = isrc.recording
  JOIN l_artist_work    law ON law.entity1 = lrw.entity1
  JOIN link      lk ON lk.id = law.link
  JOIN link_type lt ON lt.id = lk.link_type AND lt.name = 'composer'
),
-- signal 1: composer of any classically-typed work (not restricted to ISRC,
-- so the classifier sees the composer's whole catalogue)
sig_worktype AS (
  SELECT DISTINCT law.entity0 AS artist_id
  FROM l_artist_work law
  JOIN link      lk  ON lk.id = law.link
  JOIN link_type lt  ON lt.id = lk.link_type AND lt.name = 'composer'
  JOIN work      w   ON w.id = law.entity1
  JOIN classical_work_type cwt ON cwt.id = w.type
),
-- signal 3: classical tag on the artist
sig_tag AS (
  SELECT DISTINCT at.artist AS artist_id
  FROM artist_tag at
  JOIN classical_tag ct ON ct.id = at.tag
  WHERE at.count > 0
)
SELECT
  a.gid             AS artist_mbid,
  a.name            AS artist_name,
  a.begin_date_year AS birth_year,
  (sw.artist_id IS NOT NULL)                                    AS sig_worktype,
  (a.begin_date_year IS NOT NULL AND a.begin_date_year < 1900)  AS sig_birth,
  (st.artist_id IS NOT NULL)                                    AS sig_tag
FROM composer_isrc ci
JOIN artist a            ON a.id = ci.artist_id
LEFT JOIN sig_worktype sw ON sw.artist_id = ci.artist_id
LEFT JOIN sig_tag      st ON st.artist_id = ci.artist_id
WHERE
     sw.artist_id IS NOT NULL
  OR st.artist_id IS NOT NULL
  OR (a.begin_date_year IS NOT NULL AND a.begin_date_year < 1900)
ORDER BY sig_worktype DESC, sig_tag DESC, artist_name
) TO STDOUT WITH CSV HEADER;