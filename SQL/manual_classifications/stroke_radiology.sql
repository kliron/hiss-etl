-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! IMPORTANT NOTES !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
-- 1. Do not add foreign key constraints to any of the automatically-created tables as these will prevent them from being
--    dropped and recreated by the scripts.
-- 2. The svar_uid column is the ONLY RELIABLE, CONSTANT IDENTIFIER for a specific record.
--    The svar_uid is unique for a specific utlåtande and never changes. The id on the other hand might change from extraction to
--    extraction since it is an autogenerated serial and depends on the order of row insertion in the radiologi_klin_fys table.
DROP TABLE IF EXISTS stroke_features;
CREATE TABLE stroke_features (
  report_uid INT NOT NULL,
  eid INT NOT NULL,
  pid INT NOT NULL,
  kind TEXT NOT NULL,
  temporal TEXT NOT NULL,
  location TEXT NOT NULL,
  side TEXT NOT NULL,
  extent TEXT NOT NULL,
  id SERIAL PRIMARY KEY
);
-- COPY stroke_features FROM '/Users/kliron/Projects/HISS/karda/manually_annotated/stroke_features.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);

-- If you import from csv, make sure to set indices to next value:
-- SELECT setval('stroke_features_id_seq', (SELECT COALESCE(MAX(id),0) FROM stroke_features));
-- SELECT setval('angio_features_id_seq', (SELECT COALESCE(MAX(id),0) FROM angio_features));
-- SELECT setval('degenerative_features_id_seq', (SELECT COALESCE(MAX(id),0) FROM degenerative_features));

DROP TABLE IF EXISTS angio_features;
CREATE TABLE angio_features (
  report_uid INT NOT NULL,
  eid INT NOT NULL,
  pid INT NOT NULL,
  vessel TEXT NOT NULL,
  side TEXT NOT NULL,
  finding TEXT NOT NULL,
  id SERIAL PRIMARY KEY
);
-- COPY angio_features FROM '/Users/kliron/Projects/HISS/karda/manually_annotated/angio_features.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);

DROP TABLE IF EXISTS degenerative_features;
CREATE TABLE degenerative_features (
  report_uid INT NOT NULL,
  eid INT NOT NULL,
  pid INT NOT NULL,
  cortical_atrophy TEXT NOT NULL,
  cortical_atrophy_description TEXT NOT NULL,
  central_atrophy TEXT NOT NULL,
  microangiopathy TEXT NOT NULL,
  id SERIAL PRIMARY KEY
);
-- COPY degenerative_features FROM '/Users/kliron/Projects/HISS/karda/manually_annotated/degenerative_features.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);

-- EXPORT TO CSV
-- COPY stroke_features TO '/Users/kliron/Projects/hiss/karda/SQLite/import/stroke_features.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
-- COPY angio_features TO '/Users/kliron/Projects/hiss/karda/SQLite/import/angio_features.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
-- COPY degenerative_features TO '/Users/kliron/Projects/hiss/karda/SQLite/import/degenerative_features.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);