-- This is the mother of all queries for the existing data in karda. All patients are selected according to their having
-- (at ANY point in time) one of these diagnoses:
--
-- SELECT [Diagnos_kod]
--       ,[Diagnos_namn]
--   into #uttag_diagnoser
--   --drop table  #uttag_diagnoser
--   FROM [kardanormalized].[dbo].[kod_diagnos]  --58 st
--   where (diagnos_kod like 'I61%' or
--   diagnos_kod like 'I60%' or
--   diagnos_kod like 'I63%' or
--   diagnos_kod like 'I65%' or
--   diagnos_kod like 'I69%' or
--   diagnos_kod like 'G45%' or
--   diagnos_kod like 'G46%')
--   order by 1

DROP INDEX IF EXISTS patients_idx;
DROP TABLE IF EXISTS patients;
CREATE TABLE patients (
  pnr VARCHAR UNIQUE NOT NULL, -- THIS COLUMN IS DROPPED IN ONLINE COPIES OF THIS DATABASE
  pid INTEGER PRIMARY KEY,
  hiss_id INTEGER NULL UNIQUE,
  birth_date TIMESTAMP NULL,
  gender CHARACTER NOT NULL,
  death_date TIMESTAMP NULL
);
COPY patients FROM '/Users/kliron/Projects/hiss/karda/meta/index.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
CREATE UNIQUE INDEX patients_idx ON patients (pid, hiss_id, death_date);

DROP TABLE IF EXISTS vardhandelser_stroke;
CREATE TABLE vardhandelser_stroke (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  akut_ankomsttidpunkt TIMESTAMP NULL,
  vardkedja_position VARCHAR NULL,
  inskrivningstidpunkt TIMESTAMP NOT NULL,
  inskrivningstidpunkt_avd TIMESTAMP NULL,
  utskrivningstidpunkt TIMESTAMP NOT NULL,
  vardenhet_id VARCHAR NULL,
  vardenhet_kombika VARCHAR NULL,
  ekonomisk_kombika VARCHAR NULL,
  akut BOOLEAN NOT NULL,
  varddagar INTEGER NOT NULL,
  aldersindelning_id INTEGER NOT NULL
);
COPY vardhandelser_stroke FROM '/Users/kliron/Projects/hiss/karda/raw/vardhandelser_stroke.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP INDEX IF EXISTS events_idx;
DROP TABLE IF EXISTS events;
SELECT
  p.pid as pid,
  v.pas_vardhandelse_id as eid,
  v.akut_ankomsttidpunkt as acute_arrival_at,
  v.vardkedja_position as ward_chain_position,
  v.inskrivningstidpunkt as admission_at,
  v.inskrivningstidpunkt_avd as admission_department_at,
  v.utskrivningstidpunkt as discharge_at,
  v.vardenhet_id as department_id,
  v.vardenhet_kombika as department_code,
  v.ekonomisk_kombika as economic_code,
  v.akut as acute,
  v.varddagar as days,
  v.aldersindelning_id as age_division_id
INTO events
FROM vardhandelser_stroke v
LEFT JOIN patients p ON v.pnr = p.pnr;
CREATE UNIQUE INDEX events_idx ON events (pid, eid, admission_at, discharge_at);
ALTER TABLE events ADD COLUMN id SERIAL PRIMARY KEY;
COPY events TO '/Users/kliron/Projects/hiss/karda/clean/events.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP TABLE vardhandelser_stroke;

DROP TABLE IF EXISTS vardhandelser_atgard;
CREATE TABLE vardhandelser_atgard (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  atgard_kod VARCHAR NOT NULL,
  atgard_kortnamn VARCHAR NOT NULL,
  atgardsdatum TIMESTAMP NOT NULL,
  ZATC1 VARCHAR NULL
);
COPY vardhandelser_atgard FROM '/Users/kliron/Projects/hiss/karda/raw/vardhandelser_atgard.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP INDEX IF EXISTS measures_idx;
DROP TABLE IF EXISTS measures;
SELECT
  p.pid,
  v.pas_vardhandelse_id as eid,
  v.atgard_kod as measure,
  v.atgard_kortnamn as name,
  v.atgardsdatum as at,
  v.ZATC1 as zatc1
INTO measures
FROM vardhandelser_atgard v
LEFT JOIN patients p ON p.pnr = v.pnr;
CREATE UNIQUE INDEX measures_idx ON measures (pid, eid, measure, at, zatc1);
ALTER TABLE vardhandelser_atgard ADD COLUMN id SERIAL PRIMARY KEY;
COPY measures TO '/Users/kliron/Projects/hiss/karda/clean/measures.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP TABLE vardhandelser_atgard;


DROP TABLE IF EXISTS vardhandelser_diagnos;
CREATE TABLE vardhandelser_diagnos (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  diagnos_kod VARCHAR NOT NULL,
  diagnos_kortnamn VARCHAR NOT NULL,
  huvuddiagnos BOOLEAN NOT NULL,
  dodsorsak BOOLEAN NOT NULL
);
COPY vardhandelser_diagnos FROM '/Users/kliron/Projects/hiss/karda/raw/vardhandelser_diagnos.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP INDEX IF EXISTS diagnoses_idx;
DROP TABLE IF EXISTS diagnoses;
SELECT
  p.pid,
  v.pas_vardhandelse_id as eid,
  v.diagnos_kod as diagnosis,
  v.diagnos_kortnamn as name,
  v.huvuddiagnos as main_diagnosis,
  v.dodsorsak as cause_of_death
INTO diagnoses
FROM vardhandelser_diagnos v
LEFT JOIN patients p ON p.pnr = v.pnr;
CREATE UNIQUE INDEX diagnoses_idx ON diagnoses (pid, eid, diagnosis, main_diagnosis, cause_of_death);
ALTER TABLE vardhandelser_diagnos ADD COLUMN id SERIAL PRIMARY KEY;
COPY diagnoses TO '/Users/kliron/Projects/hiss/karda/clean/diagnoses.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP TABLE vardhandelser_diagnos;


DROP TABLE IF EXISTS vardhandelser_utplac;
CREATE TABLE vardhandelser_utplac (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  utplacerad_pa_vardenhet_id VARCHAR NOT NULL,
  namn VARCHAR NOT NULL,
  utplacering_timmar INTEGER NOT NULL,
  utplacering_tidpunkt TIMESTAMP NOT NULL,
  atertagning_tidpunkt TIMESTAMP NOT NULL
);
COPY vardhandelser_utplac FROM '/Users/kliron/Projects/hiss/karda/raw/vardhandelser_utplac.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP INDEX IF EXISTS events_outplaced_idx;
DROP TABLE IF EXISTS events_outplaced;
SELECT
  p.pid,
  v.pas_vardhandelse_id as eid,
  v.utplacerad_pa_vardenhet_id as outplaced_at_department_id,
  v.namn as name,
  v.utplacering_timmar as outplaced_hours,
  v.utplacering_tidpunkt as outplaced_at,
  v.atertagning_tidpunkt as taken_back_at
INTO events_outplaced
FROM vardhandelser_utplac v
LEFT JOIN patients p ON p.pnr = v.pnr;
CREATE UNIQUE INDEX events_outplaced_idx ON events_outplaced (pid, eid, outplaced_at_department_id, name, outplaced_hours, outplaced_at, taken_back_at);
ALTER TABLE events_outplaced ADD COLUMN id SERIAL PRIMARY KEY;
COPY outplaced_events TO '/Users/kliron/Projects/hiss/karda/clean/outplaced_events.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP TABLE vardhandelser_utplac;


DROP TABLE IF EXISTS stroke_alla_diagnoser;
CREATE TABLE stroke_alla_diagnoser (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  inskrivningstidpunkt TIMESTAMP NOT NULL,
  utskrivningstidpunkt TIMESTAMP NOT NULL,
  diagnos_kod VARCHAR NOT NULL,
  huvuddiagnos BOOLEAN NOT NULL
);
-- alla_diagnoser original csv file contains a lot of duplicates (>200,000).
-- De-duplicating here before create unique index is called.
COPY stroke_alla_diagnoser FROM '/Users/kliron/Projects/hiss/karda/raw/stroke_alla_diagnoser.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
ALTER TABLE stroke_alla_diagnoser ADD COLUMN id SERIAL PRIMARY KEY;
DELETE FROM stroke_alla_diagnoser WHERE id IN (SELECT duplicates.id FROM
  (SELECT
     id,
     row_number() OVER (PARTITION BY pas_vardhandelse_id, inskrivningstidpunkt, utskrivningstidpunkt, diagnos_kod, huvuddiagnos  ORDER BY pas_vardhandelse_id ASC) AS nrow FROM stroke_alla_diagnoser
  ) AS duplicates
WHERE duplicates.nrow > 1);
DROP INDEX IF EXISTS all_diagnoses_idx;
DROP TABLE IF EXISTS all_diagnoses;
SELECT
  p.pid,
  d.pas_vardhandelse_id as eid,
  d.inskrivningstidpunkt as admission_at,
  d.utskrivningstidpunkt as discharge_at,
  d.diagnos_kod as diagnosis,
  d.huvuddiagnos as main_diagnosis
INTO all_diagnoses
FROM stroke_alla_diagnoser d
LEFT JOIN patients p ON p.pnr = d.pnr;
CREATE UNIQUE INDEX all_diagnoses_idx ON all_diagnoses (pid, eid, diagnosis, discharge_at, main_diagnosis);
-- Recreate the index so we don't have mysteriously missing ids
ALTER TABLE all_diagnoses ADD COLUMN id SERIAL PRIMARY KEY;
DROP TABLE stroke_alla_diagnoser;



DROP TABLE IF EXISTS stroke_glukos;
CREATE TABLE stroke_glukos (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  term_id VARCHAR NOT NULL,
  termnamn VARCHAR NOT NULL,
  handelsetidpunkt TIMESTAMP NOT NULL,
  varde VARCHAR
);
-- glukos original csv contains duplicates. Remove them here before creating unique index
COPY stroke_glukos FROM '/Users/kliron/Projects/hiss/karda/raw/stroke_glukos.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
ALTER TABLE stroke_glukos ADD COLUMN id SERIAL PRIMARY KEY;
DELETE FROM stroke_glukos WHERE id IN (SELECT duplicates.id FROM
  (SELECT
     id,
     row_number() OVER (PARTITION BY pas_vardhandelse_id, handelsetidpunkt, varde ORDER BY pas_vardhandelse_id ASC) AS nrow FROM stroke_glukos
    WHERE varde IS NOT NULL  -- original data contain nulls in `varde`
  ) AS duplicates
WHERE duplicates.nrow > 1);
DROP INDEX IF EXISTS glucose_idx;
DROP TABLE IF EXISTS glucose;
SELECT
  p.pid,
  g.pas_vardhandelse_id as eid,
  g.term_id as term,
  g.termnamn as term_name,
  g.handelsetidpunkt as at,
  replace(g.varde, ',', '.')::REAL as value
INTO glucose
FROM stroke_glukos g
LEFT JOIN patients p ON p.pnr = g.pnr;
CREATE UNIQUE INDEX glucose_idx ON glucose (pid, eid, at, value);
ALTER TABLE glucose ADD COLUMN id SERIAL PRIMARY KEY;
COPY glucose TO '/Users/kliron/Projects/hiss/karda/clean/glucose.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP TABLE stroke_glukos;


DROP TABLE IF EXISTS stroke_matvarde;
CREATE TABLE stroke_matvarde (
  pas_vardhandelse_id INTEGER NOT NULL,
  handelsetidpunkt TIMESTAMP NOT NULL,
  term_id VARCHAR NOT NULL,
  termnamn VARCHAR NOT NULL,
  varde VARCHAR,
  pnr VARCHAR NOT NULL
);
COPY stroke_matvarde FROM '/Users/kliron/Projects/hiss/karda/raw/stroke_matvarde.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP INDEX IF EXISTS measurements_idx;
DROP TABLE IF EXISTS measurements;
SELECT
  p.pid,
  m.pas_vardhandelse_id as eid,
  m.handelsetidpunkt as at,
  m.term_id as term,
  m.termnamn as term_name,
  replace(m.varde, ',', '.')::REAL as value
INTO measurements
FROM stroke_matvarde m
LEFT JOIN patients p ON p.pnr = m.pnr
WHERE m.varde IS NOT NULL AND m.varde <> 'NA';
CREATE UNIQUE INDEX measurements_idx ON measurements (pid, eid, at, term, value);
ALTER TABLE measurements ADD COLUMN id SERIAL PRIMARY KEY;
COPY measurements TO '/Users/kliron/Projects/hiss/karda/clean/measurements.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP TABLE stroke_matvarde;


DROP TABLE IF EXISTS stroke_lab;
CREATE TABLE stroke_lab (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  svar_uid VARCHAR NOT NULL,
  provtagningstidpunkt_enl_lab TIMESTAMP NOT NULL,
  analys_kemlab VARCHAR NOT NULL,
  svar_kemlab VARCHAR NOT NULL,
  enhet VARCHAR NULL,
  disciplin VARCHAR NOT NULL,
  utanfor_referensomrade BOOLEAN NOT NULL,
  referensomrade1 VARCHAR NULL
);
COPY stroke_lab FROM '/Users/kliron/Projects/hiss/karda/raw/stroke_lab.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP INDEX IF EXISTS lab_idx;
DROP TABLE IF EXISTS lab;
SELECT
  p.pid,
  l.pas_vardhandelse_id as eid,
  l.svar_uid as result_uid,
  l.provtagningstidpunkt_enl_lab as at,
  l.analys_kemlab as analysis,
  l.svar_kemlab as result,
  l.enhet as unit,
  l.disciplin as discipline,
  l.utanfor_referensomrade as out_of_reference_range,
  l.referensomrade1 as reference_range
INTO lab
FROM stroke_lab l
LEFT JOIN patients p ON p.pnr = l.pnr;
CREATE UNIQUE INDEX lab_idx ON lab (pid, eid, result_uid, at, analysis, result, discipline);
COPY lab TO '/Users/kliron/Projects/hiss/karda/clean/lab.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
ALTER TABLE lab ADD COLUMN id SERIAL PRIMARY KEY;
DROP TABLE stroke_lab;


------------------------------------------- ABANDON ALL HOPE YE WHO ENTER HERE ----------------------------------------
--
-- While trying to make sense of this clusterfuck keep in mind:
-- administreringstidpunkt -> time point the order was executed and the drug was administered.
-- ordinationstidpunkt -> time point the order for this drug was signed by the attending physician
-- forsta_dos_tidpunkt -> time point the FIRST EVER dose was administered for this order. If a drug existed before
-- admission, this time will be in the past compared to ord.
-- sista_dos_tidpunkt -> time point of LAST EVER dose for this order. Future (or past) orders for the same drug can
-- exist, in each case with different ordination, forsta, sista times.
-- ordination, first and last are the same for all different administrations of a drug order. Only
-- administreringstidpunkt changes.
--
-- Adding to the above, administreringstidpunkt and forsta_dos_tidpunkt for once-only administrations are often
-- completely unreliable. Often the time part of the timestamp is missing entirely (entered as 00:00:00) and only the
-- date exists.
--
DROP TABLE IF EXISTS stroke_lakemedel;
CREATE TABLE stroke_lakemedel (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  lakemedel_id VARCHAR NOT NULL,
  matchningsniva INTEGER NULL,
  ATC VARCHAR NULL,     -- most of the times this is not null
  atc_kod VARCHAR NULL,  -- most of the times this is null but sometimes when ATC is null, this is not null
  ordinationstyp_id VARCHAR NOT NULL,
  namn VARCHAR NULL,
  preparat VARCHAR NULL,
  beredningsform VARCHAR NULL,
  styrka VARCHAR NULL,
  styrke_enhet VARCHAR NULL,
  enhetskod VARCHAR NULL,
  dosenhet VARCHAR NULL,
  rad VARCHAR NULL,
  ordinationstidpunkt TIMESTAMP NOT NULL,
  administreringstidpunkt TIMESTAMP NOT NULL,
  forsta_dos_tidpunkt TIMESTAMP NOT NULL,
  sista_dos_tidpunkt TIMESTAMP NULL,
  dos_numerisk VARCHAR NULL,
  dos_fritext VARCHAR NULL
);
COPY stroke_lakemedel FROM '/Users/kliron/Projects/hiss/karda/raw/stroke_lakemedel.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
DROP INDEX IF EXISTS drugs_idx;
DROP TABLE IF EXISTS drugs;
SELECT
  p.pid,
  l.pas_vardhandelse_id as eid,
  l.lakemedel_id as drug_id,
  l.matchningsniva as match_level,
  COALESCE(l.ATC, l.atc_kod) as atc,     -- most of the times ATC is not null
  l.ordinationstyp_id as order_type,
  l.namn as name,
  l.preparat as preparation,
  l.beredningsform as form,
  l.styrka as strength,
  l.styrke_enhet as strengh_unit,
  l.enhetskod as unit_code,
  l.dosenhet as dose_unit,
  l.rad as row,
  l.ordinationstidpunkt as ordered_at,
  l.administreringstidpunkt as administered_at,
  l.forsta_dos_tidpunkt as first_dose_at,
  l.sista_dos_tidpunkt as last_dose_at,
  replace(l.dos_numerisk, ',', '.')::REAL as numeric_dose,
  l.dos_fritext as dose_text
INTO drugs
FROM stroke_lakemedel l
LEFT JOIN patients p ON p.pnr = l.pnr;
-- lakemedel contains duplicates. We don't bother to remove them
CREATE INDEX drugs_idx ON drugs (pid, eid, atc, ordered_at, administered_at, first_dose_at, last_dose_at);
COPY drugs TO '/Users/kliron/Projects/hiss/karda/clean/drugs.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
ALTER TABLE drugs ADD COLUMN id SERIAL PRIMARY KEY;
DROP TABLE stroke_lakemedel;


DROP TABLE IF EXISTS stroke_rontgen;
CREATE TABLE stroke_rontgen (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  bestallning_uid INTEGER NOT NULL,
  undersokning_text VARCHAR NULL,
  onskad_undersokning VARCHAR NULL,
  bestallning_skickad TIMESTAMP NOT NULL,
  disciplin VARCHAR NOT NULL,
  svar_uid INTEGER NOT NULL,
  kommentar TEXT NOT NULL,
  undersokningsstart_datum TIMESTAMP NOT NULL,
  svarstyp VARCHAR NULL,
  medicinsk_bedomning TEXT NOT NULL
);
COPY stroke_rontgen FROM '/Users/kliron/Projects/hiss/karda/raw/stroke_rontgen.csv' WITH (DELIMITER '|', HEADER true, QUOTE E'\b', FORMAT CSV);
DROP INDEX IF EXISTS radiology_idx;
DROP TABLE IF EXISTS radiology;
SELECT
  p.pid,
  r.pas_vardhandelse_id as eid,
  r.bestallning_uid as order_uid,
  r.undersokning_text as examination,
  r.onskad_undersokning as request,
  r.bestallning_skickad as ordered_at,
  r.disciplin as discipline,
  r.svar_uid as report_uid,   -- THIS IS THE UNIQUELY IDENTIFYING KEY FOR THIS TABLE
  r.kommentar as comment,
  r.undersokningsstart_datum as examination_started_at,
  r.svarstyp as report_type,
  r.medicinsk_bedomning as report
INTO radiology
FROM stroke_rontgen r
LEFT JOIN patients p ON p.pnr = r.pnr;
CREATE UNIQUE INDEX radiology_idx ON radiology (pid, eid, ordered_at, examination, discipline, report_uid, examination_started_at);
COPY radiology TO '/Users/kliron/Projects/hiss/karda/clean/radiology.csv' WITH (DELIMITER '|', HEADER true, QUOTE E'\b', FORMAT CSV);
ALTER TABLE radiology ADD COLUMN id SERIAL PRIMARY KEY;
DROP TABLE stroke_rontgen;


DROP TABLE IF EXISTS stroke_sokord;
CREATE TABLE stroke_sokord (
  pnr VARCHAR NOT NULL,
  pas_vardhandelse_id INTEGER NOT NULL,
  journalanteckning_id VARCHAR NOT NULL,
  skapad_pa_vardenhet_id VARCHAR NOT NULL,
  namn VARCHAR NOT NULL,
  handelsetidpunkt TIMESTAMP NOT NULL,
  handelsedatum TIMESTAMP NOT NULL,
  sokord_term_id VARCHAR NOT NULL,
  termnamn VARCHAR NOT NULL,
  sokord_fritext TEXT NULL,
  matvarde VARCHAR NULL,
  fritext_matvarde VARCHAR NULL,
  matvarde_beskrivning VARCHAR NULL,
  vardeterm_id VARCHAR NULL,
  vardeterm_termnamn VARCHAR NULL
);
COPY stroke_sokord FROM '/Users/kliron/Projects/hiss/karda/raw/stroke_sokord.csv' WITH (DELIMITER '|', HEADER true, QUOTE E'\b', FORMAT CSV);
DROP INDEX IF EXISTS journal_idx;
DROP TABLE IF EXISTS journal;
SELECT
  p.pid,
  o.pas_vardhandelse_id as eid,
  o.journalanteckning_id as journal_id,
  o.skapad_pa_vardenhet_id as created_at_department_id,
  o.namn as name,
  o.handelsetidpunkt as at,
  o.handelsedatum as date_at,
  o.sokord_term_id as term,
  o.termnamn as term_name,
  o.sokord_fritext as text,
  o.matvarde as measurement,
  o.fritext_matvarde as measurement_text,
  o.matvarde_beskrivning as measurement_description,
  o.vardeterm_id as ward_term_id,
  o.vardeterm_termnamn as ward_term_name
INTO journal
FROM stroke_sokord o
LEFT JOIN patients p ON p.pnr = o.pnr;
-- sokord contains duplicates. We don't bother to remove them
CREATE UNIQUE INDEX journal_idx ON journal (pid, eid, journal_id, created_at_department_id, at, date_at, term, measurement);
COPY journal TO '/Users/kliron/Projects/hiss/karda/clean/journal.csv' WITH (DELIMITER '|', HEADER true, QUOTE E'\b', FORMAT CSV);
ALTER TABLE journal ADD COLUMN id SERIAL PRIMARY KEY;
DROP TABLE stroke_sokord;

-- Export to CSV so that we can import in SQLite
COPY patients TO '/Users/kliron/Projects/hiss/karda/SQLite/patients.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY events TO '/Users/kliron/Projects/hiss/karda/SQLite/events.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY diagnoses TO '/Users/kliron/Projects/hiss/karda/SQLite/diagnoses.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY all_diagnoses TO '/Users/kliron/Projects/hiss/karda/SQLite/all_diagnoses.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY measures TO '/Users/kliron/Projects/hiss/karda/SQLite/measures.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY measurements TO '/Users/kliron/Projects/hiss/karda/SQLite/measurements.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY lab TO '/Users/kliron/Projects/hiss/karda/SQLite/lab.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY glucose TO '/Users/kliron/Projects/hiss/karda/SQLite/glucose.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY drugs TO '/Users/kliron/Projects/hiss/karda/SQLite/drugs.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY radiology TO '/Users/kliron/Projects/hiss/karda/SQLite/radiology.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);
COPY journal TO '/Users/kliron/Projects/hiss/karda/SQLite/journal.csv' WITH (DELIMITER '|', HEADER true, FORMAT CSV);