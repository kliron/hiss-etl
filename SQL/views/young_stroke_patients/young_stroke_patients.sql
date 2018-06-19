-- stroke_events in young patients with pfo
DROP INDEX IF EXISTS pfo_young_stroke_events_idx;
DROP TABLE IF EXISTS pfo_young_stroke_events;
SELECT
  p.pnr,
  s.*
INTO pfo_young_stroke_events
FROM stroke_events s
  LEFT JOIN patients p ON p.pid = s.pid
  LEFT JOIN diagnoses_wide d ON d.eid = s.eid
  WHERE s.pid IN (SELECT DISTINCT(d.pid) FROM all_diagnoses d WHERE d.diagnosis ~ 'Q211.*') AND (s.age_at_event BETWEEN 18 AND 59) AND (d.carotid_stenosis = 0 AND d.vertebral_stenosis = 0 AND d.basilar_stenosis = 0 AND d.atrial_fibrillation = 0 AND d.carotid_dissection = 0 AND vertebral_dissection =  0) -- Enrich with those patients probably having cryptogenic stroke
ORDER BY s.pid ASC, s.age_at_event ASC;
CREATE UNIQUE INDEX pfo_young_stroke_events_idx ON pfo_young_stroke_events(pid, eid, admission_at);


DROP INDEX IF EXISTS non_pfo_young_stroke_events_idx;
DROP TABLE IF EXISTS non_pfo_young_stroke_events;
SELECT
  p.pnr,
  s.*
INTO non_pfo_young_stroke_events
FROM stroke_events s
  LEFT JOIN patients p ON p.pid = s.pid
  LEFT JOIN diagnoses_wide d ON d.eid = s.eid
WHERE (s.age_at_event BETWEEN 18 AND 59) AND s.eid NOT IN (SELECT DISTINCT(pf.eid) FROM pfo_young_stroke_events pf) AND (d.carotid_stenosis = 0 AND d.vertebral_stenosis = 0 AND d.basilar_stenosis = 0 AND d.atrial_fibrillation = 0 AND d.carotid_dissection = 0 AND vertebral_dissection =  0) -- Enrich with those patients probably having cryptogenic stroke
ORDER BY s.pid ASC, s.age_at_event ASC;
CREATE UNIQUE INDEX non_pfo_young_stroke_events_idx ON non_pfo_young_stroke_events(pid, eid, admission_at);


-- all transesofageal eko, even if done in other vårdtillfälle
DROP INDEX IF EXISTS pfo_transesofageal_eko_idx;
DROP TABLE IF EXISTS pfo_transesofageal_eko;
SELECT
  r.*
INTO pfo_transesofageal_eko
FROM radiology r
WHERE r.examination_text = 'Eko transesofageal' AND r.pid IN (SELECT DISTINCT(n.pid) FROM pfo_young_stroke_events n)
ORDER BY r.pid ASC;
-- Not UNIQUE because of dubbelremisser, canceled exams, etc
CREATE INDEX pfo_transesofageal_eko_idx ON pfo_transesofageal_eko(pid, eid);


DROP INDEX IF EXISTS non_pfo_transesofageal_eko_idx;
DROP TABLE IF EXISTS non_pfo_transesofageal_eko;
SELECT
  r.*
INTO non_pfo_transesofageal_eko
FROM radiology r
WHERE r.examination_text = 'Eko transesofageal' AND r.pid IN (SELECT DISTINCT(n.pid) FROM non_pfo_young_stroke_events n)
ORDER BY r.pid ASC;
-- Not UNIQUE because of dubbelremisser, canceled exams, etc
CREATE INDEX non_pfo_transesofageal_eko_idx ON non_pfo_transesofageal_eko(pid, eid);


DROP INDEX IF EXISTS pfo_radiology_idx;
DROP TABLE IF EXISTS pfo_radiology;
SELECT
  r.*
INTO pfo_radiology
FROM radiology r
WHERE r.discipline = 'R' AND r.requested_examination ~ '(.*[Mm][Rr].*)|(.*?[Hh][Jj][Ää][Rr][Nn][Aa].*)|(.*[Ss][Kk][Aa][Ll]{1,2}[Ee].*)|(.*[Aa][Nn][Gg][Ii][Oo].*)|(.*[Hh][Aa][Ll][Ss][Kk][Ää][Rr][Ll].*)' AND r.pid IN (SELECT DISTINCT(n.pid) FROM pfo_young_stroke_events n)
ORDER BY r.pid ASC;
-- Not UNIQUE because of dubbelremisser, canceled exams, etc
CREATE INDEX pfo_radiology_idx ON pfo_radiology(pid, eid);


DROP INDEX IF EXISTS non_pfo_radiology_idx;
DROP TABLE IF EXISTS non_pfo_radiology;
SELECT
  r.*
INTO non_pfo_radiology
FROM radiology r
WHERE r.discipline = 'R' AND r.requested_examination ~ '(.*[Mm][Rr].*)|(.*?[Hh][Jj][Ää][Rr][Nn][Aa].*)|(.*[Ss][Kk][Aa][Ll]{1,2}[Ee].*)|(.*[Aa][Nn][Gg][Ii][Oo].*)|(.*[Hh][Aa][Ll][Ss][Kk][Ää][Rr][Ll].*)' AND r.pid IN (SELECT DISTINCT(n.pid) FROM non_pfo_young_stroke_events n)
ORDER BY r.pid ASC;
-- Not UNIQUE because of dubbelremisser, canceled exams, etc
CREATE INDEX non_pfo_radiology_idx ON non_pfo_radiology(pid, eid);


DROP TABLE IF EXISTS pfo_cholesterol;
SELECT
  c.*
INTO pfo_cholesterol
FROM cholesterol_data c
WHERE c.pid IN (SELECT DISTINCT (pid) FROM pfo_young_stroke_events)
ORDER BY c.pid ASC;


DROP TABLE IF EXISTS non_pfo_cholesterol;
SELECT
  c.*
INTO non_pfo_cholesterol
FROM cholesterol_data c
WHERE c.pid IN (SELECT DISTINCT (pid) FROM non_pfo_young_stroke_events)
ORDER BY c.pid ASC;


DROP TABLE IF EXISTS pfo_homocysteine;
SELECT
  h.*
INTO pfo_homocysteine
FROM homocysteine_data h
WHERE h.pid IN (SELECT DISTINCT (pid) FROM pfo_young_stroke_events)
ORDER BY h.pid ASC;


DROP TABLE IF EXISTS non_pfo_homocysteine;
SELECT
  h.*
INTO non_pfo_homocysteine
FROM homocysteine_data h
WHERE h.pid IN (SELECT DISTINCT (pid) FROM non_pfo_young_stroke_events)
ORDER BY h.pid ASC;


DROP TABLE IF EXISTS pfo_coagulation;
SELECT
  c.*
INTO pfo_coagulation
FROM coagulation_studies c
WHERE c.pid IN (SELECT DISTINCT (pid) FROM pfo_young_stroke_events)
ORDER BY c.pid ASC;


DROP TABLE IF EXISTS non_pfo_coagulation;
SELECT
  c.*
INTO non_pfo_coagulation
FROM coagulation_studies c
WHERE c.pid IN (SELECT DISTINCT (pid) FROM non_pfo_young_stroke_events)
ORDER BY c.pid ASC;


COPY (SELECT
    e.pid,
    p.pnr
  FROM pfo_young_stroke_events e
    LEFT JOIN patients p ON p.pid = e.pid
  WHERE p.pid NOT IN (SELECT DISTINCT(pid) FROM pfo_transesofageal_eko)
  ORDER BY e.pid)
TO '/Users/kliron/Projects/hiss/karda/clean/pfo_missing_transesofageal_eko.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);

-- select count(*) as events, count(distinct(pid)) as unique_patients from pfo_young_stroke_events; -- 92, 83
-- select count(*) as events, count(distinct(pid)) as unique_patients from non_pfo_young_stroke_events; -- 517, 465
-- select count (*) from pfo_transesofageal_eko; -- 55
-- select count (*) from non_pfo_transesofageal_eko; -- 106


-- Write down all events where we dont have TEE data so we retrieve it manually
COPY (SELECT p.pnr, p.admission_at, p.discharge_at from pfo_young_stroke_events p where p.eid not in (select distinct(eid) from pfo_transesofageal_eko))
TO '/Users/kliron/Projects/hiss/karda/clean/pfo_missing_TEE.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);

COPY (SELECT p.pnr, p.admission_at, p.discharge_at from non_pfo_young_stroke_events p where p.eid not in (select distinct(eid) from non_pfo_transesofageal_eko))
TO '/Users/kliron/Projects/hiss/karda/clean/non_pfo_missing_TEE.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);

-- Select only the events where we have TEE data and export for work
COPY (SELECT * FROM pfo_young_stroke_events WHERE eid in (SELECT DISTINCT(eid) FROM pfo_transesofageal_eko)) TO '/Users/kliron/Projects/hiss/karda/clean/pfo_young_stroke_events.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);
COPY (SELECT * FROM non_pfo_young_stroke_events WHERE eid in (SELECT DISTINCT(eid) FROM non_pfo_transesofageal_eko)) TO '/Users/kliron/Projects/hiss/karda/clean/non_pfo_young_stroke_events.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);

COPY pfo_transesofageal_eko TO '/Users/kliron/Projects/hiss/karda/clean/pfo_transesofageal_eko.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);
COPY non_pfo_transesofageal_eko TO '/Users/kliron/Projects/hiss/karda/clean/non_pfo_transesofageal_eko.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);

COPY (SELECT * FROM pfo_cholesterol WHERE eid IN (SELECT DISTINCT(eid) FROM pfo_transesofageal_eko)) TO '/Users/kliron/Projects/hiss/karda/clean/pfo_cholesterol.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);
COPY (SELECT * FROM non_pfo_cholesterol WHERE eid IN (SELECT DISTINCT(eid) FROM non_pfo_transesofageal_eko)) TO '/Users/kliron/Projects/hiss/karda/clean/non_pfo_cholesterol.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);
COPY (SELECT * FROM pfo_homocysteine WHERE eid_1 IN (SELECT DISTINCT(eid) FROM pfo_transesofageal_eko)) TO '/Users/kliron/Projects/hiss/karda/clean/pfo_homocysteine.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);
COPY (SELECT * FROM non_pfo_homocysteine WHERE eid_1 IN (SELECT DISTINCT(eid) FROM non_pfo_transesofageal_eko)) TO '/Users/kliron/Projects/hiss/karda/clean/non_pfo_homocysteine.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);
COPY (SELECT * FROM pfo_coagulation WHERE eid IN (SELECT DISTINCT(eid) FROM pfo_transesofageal_eko)) TO '/Users/kliron/Projects/hiss/karda/clean/pfo_coagulation.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);
COPY (SELECT * FROM non_pfo_coagulation WHERE eid IN (SELECT DISTINCT(eid) FROM non_pfo_transesofageal_eko)) TO '/Users/kliron/Projects/hiss/karda/clean/non_pfo_coagulation.csv' WITH(DELIMITER '|', HEADER true, FORMAT CSV);

