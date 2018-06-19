-- These tables are useful in all extractions.


-- Interesting diagnoses in wide format. Each event id (eid) will be represented by a single row. These events are NOT
-- necessarily stroke events neither have they occured in a neuro department.
-- They are ALL EVENTS RECORDED IN KAROLINSKA where one of our patients got some diagnosis. Diagnoses from other events
-- outside of Karolinska are NOT included.

-- Convert the diagnoses we need to wide format and save in a table
-- Hypertension                'I1%'
-- Diabetes mellitus           'E1%'
-- Hyperlipidemia              'E78%'
-- Carotid stenosis            'I652'
-- Heart failure               'I50%'
-- Previous PCI/CABG           'Z955', 'Z951'
-- History of MI/angina        'I20%', 'I21%'
-- Intermittent claudication   'I739%'
-- History of cancer           'C%'
-- Asthma                      'J45%'
-- COPD                        'J44%'
-- DVT/LE                      'I829', 'Z867B', 'I26%'
-- Depression                  'F32%', 'F33%', 'F412%'
-- Dementia                    'F00%', 'F01%', 'F02%', 'F03%'
-- Migraine                    'G43%'
DROP INDEX IF EXISTS diagnoses_wide_idx;
DROP TABLE IF EXISTS diagnoses_wide;

WITH diag AS (
  SELECT
    d1.pid,
    d1.eid,
    d1.discharge_at,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I48.*')) IS NULL THEN 0 ELSE 1 END) AS atrial_fibrillation,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I652')) IS NULL THEN 0 ELSE 1 END) AS carotid_stenosis,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I650')) IS NULL THEN 0 ELSE 1 END) AS vertebral_stenosis,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I651')) IS NULL THEN 0 ELSE 1 END) AS basilar_stenosis,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I720')) IS NULL THEN 0 ELSE 1 END) AS carotid_dissection,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I726')) IS NULL THEN 0 ELSE 1 END) AS vertebral_dissection,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I725')) IS NULL THEN 0 ELSE 1 END) AS precerebral_dissection,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I1.*')) IS NULL THEN 0 ELSE 1 END) AS hypertension,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'E1.*')) IS NULL THEN 0 ELSE 1 END) AS diabetes,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'E78.*')) IS NULL THEN 0 ELSE 1 END) AS hyperlipidemia,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I50.*')) IS NULL THEN 0 ELSE 1 END) AS heart_failure,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, '(Z951)|(Z9861)')) IS NULL THEN 0 ELSE 1 END) AS cabg_or_pci,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, '(I20.*)|(I21.*)')) IS NULL THEN 0 ELSE 1 END) AS mi_or_angina,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'I739.*')) IS NULL THEN 0 ELSE 1 END) AS claudication,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'C.*')) IS NULL THEN 0 ELSE 1 END) AS cancer,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'J45.*')) IS NULL THEN 0 ELSE 1 END) AS asthma,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'J44.*')) IS NULL THEN 0 ELSE 1 END) AS copd,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, '(I829)|(Z867B)|(I26.*)')) IS NULL THEN 0 ELSE 1 END) AS dvt_or_le,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, '(F32.*)|(F33.*)|(F412.*)')) IS NULL THEN 0 ELSE 1 END) AS depression,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, '(F00.*)|(F01.*)|(F02.*)|(F03.*)')) IS NULL THEN 0 ELSE 1 END) AS dementia,
    (CASE WHEN (SELECT regexp_matches(d1.diagnosis, 'G43.*')) IS NULL THEN 0 ELSE 1 END) AS migraine
  FROM all_diagnoses d1
)
SELECT DISTINCT ON(d.eid)
  d.pid,
  d.eid,
  d.discharge_at,
  first_value(d.atrial_fibrillation) OVER(PARTITION BY d.eid ORDER BY d.atrial_fibrillation DESC) AS atrial_fibrillation,
  first_value(d.carotid_stenosis) OVER(PARTITION BY d.eid ORDER BY d.carotid_stenosis DESC) AS carotid_stenosis,
  first_value(d.vertebral_stenosis) OVER(PARTITION BY d.eid ORDER BY d.vertebral_stenosis DESC) AS vertebral_stenosis,
  first_value(d.basilar_stenosis) OVER(PARTITION BY d.eid ORDER BY d.basilar_stenosis DESC) AS basilar_stenosis,
  first_value(d.carotid_dissection) OVER(PARTITION BY d.eid ORDER BY d.carotid_dissection DESC) AS carotid_dissection,
  first_value(d.vertebral_dissection) OVER(PARTITION BY d.eid ORDER BY d.vertebral_dissection DESC) AS vertebral_dissection,
  first_value(d.hypertension) OVER(PARTITION BY d.eid ORDER BY d.hypertension DESC) AS hypertension,
  first_value(d.diabetes) OVER(PARTITION BY d.eid ORDER BY d.diabetes DESC) AS diabetes,
  first_value(d.hyperlipidemia) OVER(PARTITION BY d.eid ORDER BY d.hyperlipidemia DESC) AS hyperlipidemia,
  first_value(d.heart_failure) OVER(PARTITION BY d.eid ORDER BY d.heart_failure DESC) AS heart_failure,
  first_value(d.cabg_or_pci) OVER(PARTITION BY d.eid ORDER BY d.cabg_or_pci DESC) AS cabg_or_pci,
  first_value(d.mi_or_angina) OVER(PARTITION BY d.eid ORDER BY d.mi_or_angina DESC) AS mi_or_angina,
  first_value(d.claudication) OVER(PARTITION BY d.eid ORDER BY d.claudication DESC) AS claudication,
  first_value(d.cancer) OVER(PARTITION BY d.eid ORDER BY d.cancer DESC) AS cancer,
  first_value(d.asthma) OVER(PARTITION BY d.eid ORDER BY d.asthma DESC) AS asthma,
  first_value(d.copd) OVER(PARTITION BY d.eid ORDER BY d.copd DESC) AS copd,
  first_value(d.dvt_or_le) OVER(PARTITION BY d.eid ORDER BY d.dvt_or_le DESC) AS dvt_or_le,
  first_value(d.depression) OVER(PARTITION BY d.eid ORDER BY d.depression DESC) AS depression,
  first_value(d.dementia) OVER(PARTITION BY d.eid ORDER BY d.dementia DESC) AS dementia,
  first_value(d.migraine) OVER(PARTITION BY d.eid ORDER BY d.migraine DESC) AS migraine
INTO diagnoses_wide
FROM diag d;
CREATE INDEX diagnoses_wide_idx ON diagnoses_wide (pid, eid, discharge_at);


-- B12 and Folate 'B03B.*'
DROP INDEX IF EXISTS vitamins_eid_idx;
DROP TABLE IF EXISTS vitamins;

SELECT DISTINCT ON (l.eid)
  l.eid,
  l.pid,
  l.first_dose_at,
  l.last_dose_at
INTO vitamins
FROM drugs l
WHERE l.atc ~ 'B03B.*';

CREATE UNIQUE INDEX vitamins_eid_idx ON vitamins (eid);

-- Interesting laboratory parameters, in wide format.
DROP INDEX IF EXISTS lab_wide_idx;
DROP TABLE IF EXISTS lab_wide;

WITH lab_pivot AS (
    SELECT
      eid,
      at,
      -- The resulting grouped table is like a diagonal matrix.
      -- MAX is used to collapse the rows into a single row containing each column's non-null value.
      MAX("B-Erytrocyter") AS erythrocytes,
      MAX("B-HbA1c") AS hba1c,
      MAX("B-HbA1c (IFCC)") AS hba1c_ifcc,
      MAX("B-Hemoglobin") AS hemoglobin,
      MAX("B-Leukocyter") AS wbc,
      MAX("B-Trombocyter") AS platelets,
      MAX("P-APT-tid") AS aptt,
      MAX("P-Glukos") AS glucose,
      MAX("P-HDL-kolesterol") AS hdl,
      MAX("P-Kalium") AS potassium,
      MAX("P-Kolesterol") AS cholesterol,
      MAX("P-Homocystein") AS homocysteine,
      MAX("P-Kreatinin") AS creatinine,
      MAX("P-Natrium") AS sodium,
      MAX("P-PK(INR)") AS inr,
      MAX("fP-LDL-kolesterol") AS ldl,
      MAX("fP-Triglycerid") AS triglycerides
    -- The row values of the last two columns will be the name of the new column and its row value
    FROM crosstab($$
    SELECT l.id, l.eid, l.at, l.analysis, l.result
    FROM lab l
    WHERE l.analysis IN (
    'P-Homocystein',
    'P-Kolesterol',
    'fP-LDL-kolesterol',
    'P-HDL-kolesterol',
    'fP-Triglycerid',
    'P-Glukos',
    'B-HbA1c',
    'B-HbA1c (IFCC)',
    'P-Natrium',
    'P-Kalium',
    'P-Kreatinin',
    'B-Hemoglobin',
    'B-Leukocyter',
    'B-Trombocyter',
    'B-Erytrocyter',
    'P-APT-tid',
    'P-PK(INR)'
    )
    ORDER BY 1, 2
    $$,

    $$
    SELECT DISTINCT l.analysis
    FROM lab l
    WHERE l.analysis IN (
    'P-Homocystein',
    'P-Kolesterol',
    'fP-LDL-kolesterol',
    'P-HDL-kolesterol',
    'fP-Triglycerid',
    'P-Glukos',
    'B-HbA1c',
    'B-HbA1c (IFCC)',
    'P-Natrium',
    'P-Kalium',
    'P-Kreatinin',
    'B-Hemoglobin',
    'B-Leukocyter',
    'B-Trombocyter',
    'B-Erytrocyter',
    'P-APT-tid',
    'P-PK(INR)'
    )
    ORDER BY 1
    $$) AS (
    -- IMPORTANT!!! The order of column definitions here MUST match the order returned by ORDER BY above
    -- (in this case, the alphabetical order)
      id INT,
      eid INT,
      at TIMESTAMP,
      "B-Erytrocyter" TEXT,
      "B-HbA1c" TEXT,
      "B-HbA1c (IFCC)" TEXT,
      "B-Hemoglobin" TEXT,
      "B-Leukocyter" TEXT,
      "B-Trombocyter" TEXT,
      "P-APT-tid" TEXT,
      "P-Glukos" TEXT,
      "P-HDL-kolesterol" TEXT,
      "P-Homocystein" TEXT,
      "P-Kalium" TEXT,
      "P-Kolesterol" TEXT,
      "P-Kreatinin" TEXT,
      "P-Natrium" TEXT,
      "P-PK(INR)" TEXT,
      "fP-LDL-kolesterol" TEXT,
      "fP-Triglycerid" TEXT
  )
  GROUP BY eid, at
  ORDER BY eid, at
)
SELECT
  DISTINCT l.eid,
  l.at,
  -- Unfortunately IGNORE NULLS (which is part of the SQL sandard) is not supported by postgres.
  -- We emulate 'ignore nulls' functionality with the much slower CASE ... WHEN ... statement in the window function part.
  first_value(l.erythrocytes) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.erythrocytes IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS erythrocytes,
  first_value(l.hba1c) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.hba1c IS NULL THEN 0 ELSE 1 END DESC, l.at  ASC) AS hba1c,
  first_value(l.hba1c_ifcc) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.hba1c_ifcc IS NULL THEN 0 ELSE 1 END DESC, l.at  ASC) AS hba1c_ifcc,
  first_value(l.hemoglobin) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.hemoglobin IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS hemoglobin,
  first_value(l.wbc) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.wbc IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS wbc,
  first_value(l.platelets) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.platelets IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS platelets,
  first_value(l.aptt) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.aptt IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS aptt,
  first_value(l.glucose) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.glucose IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS glucose,
  first_value(l.hdl) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.hdl IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS hdl,
  first_value(l.potassium) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.potassium IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS potassium,
  first_value(l.cholesterol) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.cholesterol IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS cholesterol,
  first_value(l.creatinine) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.creatinine IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS creatinine,
  first_value(l.sodium) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.sodium IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS sodium,
  first_value(l.inr) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.inr IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS inr,
  first_value(l.ldl) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.ldl IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS ldl,
  first_value(l.triglycerides) OVER(PARTITION BY l.eid ORDER BY CASE WHEN l.triglycerides IS NULL THEN 0 ELSE 1 END DESC, l.at ASC) AS triglycerides
INTO lab_wide
FROM lab_pivot l;

CREATE INDEX lab_wide_idx ON lab_wide (eid, at);


DROP INDEX IF EXISTS coagulation_studies_idx;
DROP TABLE IF EXISTS coagulation_studies;

SELECT
  l.*
INTO coagulation_studies
FROM lab l WHERE l.analysis IN (
  'P-APT-tid',
  'P-PK(INR)',
  'P-Trombintid',
  'S-Kardiolipin IgM Multiplex',
  'S-Kardiolipin IgG Multiplex',
  'S-Kardiolipin-ak (IgG)',
  'S-Kardiolipin-ak (IgM)',
  'P-Lupusantikoagulans',
  'P-Lupus antikoag.',
  'P-Protein S, fritt',
  'P-Protein C (enz)',
  'S-Beta2-glykoprot.1(IgG)',
  'S-Beta2GP1 IgG Multiplex',
  'S-Beta2GP1 IgM Multiplex',
  'kontroll, Faktor V',
  'DNA-FaktorV1691G-A',
  'DNA-FaktorII20210G-A',
  'P-vWillebr.RCoF akt.',
  'P-VWF: Ag (antigen)',
  'P-VWF: Ag',
  'P-VWF GP1bA, akut',
  'P-vWillebrandF Ag',
  'P-VWF GP1bA (aktiv.)',
  'P-VWF: RCoF',
  'P-Protrombin (F II)',
  'P-Homocystein',
  'P-D-Dimer, snabbtest',
  'P-Fibrin,lösl.,kval',
  'P-Faktor XIII (enz)',
  'Trc-ADP (Multiplate)',
  'Trc-RIST(Multiplate)',
  'Trc-TRAP(Multiplate)',
  'Trc-ASPI(Multiplate)',
  'P-Antitrombin(enzFX)',
  'P-Protrombin (F II)',
  'P-Faktor V',
  'P-Faktor V (koag)',
  'P-Faktor VII',
  'P-Faktor VIII',
  'P-Faktor VIII, akut',
  'P-Faktor VIII (enz)',
  'P-Faktor IX',
  'P-Faktor IX (koag)',
  'P-Faktor X',
  'P-Faktor X (koag)',
  'P-Faktor XI',
  'P-Faktor XI (koag)',
  'P-Faktor XII',
  'P-Faktor XIII',
  'P-Antitrombin (enz)',
  'Trombocytantikroppscr. IgG',
  'Trombocytantikroppscr. IgM',
  'P-Fibrin-D-Dimer',
  'P-Fibrinogen (koag)',
  'Rotem med Fibrinogen och Heparinas.',
  'P-Anti faktor Xa',
  'P-Heparin, LM (FXa)',
  'P-AntiFXa,LMWH',
  'Rivaroxaban, Anti Xa',
  'P-AntiFXa,LMWH, akut',
  'P-Apixaban, Anti Xa',
  'GPIIb/IIIa',
  'GPIa/IIa',
  'GPIb/IX',
  'P-Koag.yt-induc.anti',
  'B-Trombocyter,citrat',
  'B-Tromboc (citrat)',
  'B-Trombocyter'
)
ORDER BY l.analysis;
CREATE INDEX coagulation_studies_idx ON coagulation_studies (pid, eid);

-- Total NIHSS values at admission
DROP INDEX IF EXISTS nihss_at_admission_idx;
DROP TABLE IF EXISTS nihss_at_admission;

SELECT DISTINCT ON (nih.eid)
  nih.eid,
  first_value(nih.nihss) OVER (PARTITION BY nih.eid ORDER BY CASE WHEN nih.nihss IS NULL THEN 0 ELSE 1 END DESC, nih.at ASC) AS nihss,
  first_value(nih.at) OVER (PARTITION BY nih.eid ORDER BY CASE WHEN nih.nihss IS NULL THEN 0 ELSE 1 END DESC, nih.at ASC) AS nihss_date
INTO nihss_at_admission
FROM
  (SELECT
    m.eid,
    m.at,
    sum(m.value) AS nihss
    FROM measurements m
    WHERE m.term_name LIKE '%NIH%' AND m.term_name <> 'NIH stroke skala'
    GROUP BY m.eid, m.at
    ORDER BY m.at ASC
  ) AS nih;

CREATE UNIQUE INDEX nihss_at_admission_idx ON nihss_at_admission (eid, nihss, nihss_date);


-- Systolic Blood pressure at admission and at discharge
DROP INDEX IF EXISTS systolic_blood_pressures_idx;
DROP TABLE IF EXISTS systolic_blood_pressures;

SELECT DISTINCT ON (m.eid)
  m.eid,
  first_value(m.at) OVER (PARTITION BY m.eid ORDER BY CASE WHEN m.value IS NULL THEN 0 ELSE 1 END DESC, m.at ASC) AS systolic_bp_admission_date,
  first_value(m.value) OVER (PARTITION BY m.eid ORDER BY CASE WHEN m.value IS NULL THEN 0 ELSE 1 END DESC, m.at ASC) AS systolic_bp_admission,
  first_value(m.at) OVER (PARTITION BY m.eid ORDER BY CASE WHEN m.value IS NULL THEN 0 ELSE 1 END DESC, m.at DESC) AS systolic_bp_discharge_date,
  first_value(m.value) OVER (PARTITION BY m.eid ORDER BY CASE WHEN m.value IS NULL THEN 0 ELSE 1 END DESC, m.at DESC) AS systolic_bp_discharge
INTO systolic_blood_pressures
FROM measurements m
WHERE m.term_name = 'Blodtryck systoliskt - övre';

CREATE UNIQUE INDEX systolic_blood_pressures_idx ON systolic_blood_pressures (eid, systolic_bp_admission, systolic_bp_admission_date, systolic_bp_discharge, systolic_bp_discharge_date);


-- Diastolic Blood pressure at admission and at discharge
DROP INDEX IF EXISTS diastolic_blood_pressures_idx;
DROP TABLE IF EXISTS diastolic_blood_pressures;

SELECT DISTINCT ON (m.eid)
  m.eid,
  first_value(m.at) OVER (PARTITION BY m.eid ORDER BY CASE WHEN m.value IS NULL THEN 0 ELSE 1 END DESC, m.at ASC)  AS diastolic_bp_admission_date,
  first_value(m.value) OVER (PARTITION BY m.eid ORDER BY CASE WHEN m.value IS NULL THEN 0 ELSE 1 END DESC, m.at ASC)  AS diastolic_bp_admission,
  first_value(m.at) OVER (PARTITION BY m.eid ORDER BY CASE WHEN m.value IS NULL THEN 0 ELSE 1 END DESC, m.at DESC) AS diastolic_bp_discharge_date,
  first_value(m.value) OVER (PARTITION BY m.eid ORDER BY CASE WHEN m.value IS NULL THEN 0 ELSE 1 END DESC, m.at DESC ) AS diastolic_bp_discharge
INTO diastolic_blood_pressures
FROM measurements m
WHERE m.term_name = 'Blodtryck diastoliskt - nedre';

CREATE UNIQUE INDEX diastolic_blood_pressures_idx ON diastolic_blood_pressures (eid, diastolic_bp_admission, diastolic_bp_admission_date, diastolic_bp_discharge, diastolic_bp_discharge_date);


-- Smoking
DROP INDEX IF EXISTS smoking_idx;
DROP TABLE IF EXISTS smoking;

SELECT DISTINCT ON (j.eid)
  j.eid,
  MAX(j.text) as smoking
INTO smoking
FROM journal j
WHERE j.term_name = 'Tobak'
GROUP BY j.eid;

CREATE UNIQUE INDEX smoking_idx ON smoking (eid, smoking);


-- BMI
DROP INDEX IF EXISTS bmi_idx;
DROP TABLE IF EXISTS bmi;

SELECT
  w.eid,
  w.value / power((h.value / 100), 2) as bmi
INTO bmi
FROM (SELECT m.eid, m.value FROM measurements m WHERE m.term_name = 'Vikt') w
LEFT JOIN (SELECT n.eid, n.value FROM measurements n WHERE n.term_name = 'Längd') h ON w.eid = h.eid;

CREATE INDEX bmi_idx ON bmi (eid, bmi); -- not unique!

-- Vascular events numbered as 1st, 2nd, etc
DROP INDEX IF EXISTS vascular_events_idx;
DROP TABLE IF EXISTS vascular_events;

SELECT
  p.pid,
  p.hiss_id,
  p.gender,
  p.birth_date,
  p.death_date,
  v.eid,
  v.admission_at,
  v.discharge_at,
  v.days,
  d.diagnosis,
  (SELECT date_part('year', age(v.admission_at, p.birth_date))) AS age_at_event,
  (SELECT date_part('day', p.death_date - v.admission_at))  AS days_from_admission_to_death,
  -- see 'window functions' (https://www.postgresql.org/docs/current/static/tutorial-window.html)
  -- to understand this OVER ... PARTITION BY ...
  row_number() OVER (PARTITION BY v.pid ORDER BY v.admission_at ASC) AS vascular_event_number,
  n.nihss as nihss_at_admission,
  (SELECT CASE WHEN exists(SELECT 1 FROM measures a WHERE a.eid = v.eid AND a.zatc1 = 'B01AD02') THEN 1 ELSE 0 END) AS thrombolysis_administered
INTO vascular_events
FROM events v
  LEFT JOIN (SELECT vd.eid, vd.diagnosis FROM diagnoses vd WHERE vd.main_diagnosis is TRUE) d ON v.eid = d.eid
  LEFT JOIN patients p ON p.pid = v.pid
  LEFT JOIN nihss_at_admission n ON n.eid = v.eid
WHERE diagnosis ~ '(G4[56].*)|(I6.*)|(I725.*)';
CREATE UNIQUE INDEX vascular_events_idx ON vascular_events (pid, gender, age_at_event, eid, diagnosis, admission_at, discharge_at);


DROP INDEX IF EXISTS stroke_events_idx;
DROP TABLE IF EXISTS stroke_events;

SELECT
  v.*,
  -- the index of this I63 (stroke) event among other I63 events for this patient
  row_number() OVER (PARTITION BY v.pid ORDER BY v.admission_at ASC) AS stroke_number
  -- row offset of the first stroke event among all other vascular events (including bleeds, tia, etc).
  -- Recurrent vascular events will have a vascular_event_number greater than this offset.
  INTO stroke_events
FROM vascular_events v
WHERE v.diagnosis ~ 'I63.*';
CREATE UNIQUE INDEX stroke_events_idx ON stroke_events (eid, diagnosis, admission_at, discharge_at);


-- First I63.* diagnosis ever (in our material)
DROP INDEX IF EXISTS first_stroke_idx;
DROP TABLE IF EXISTS first_stroke;

SELECT
  s.*
INTO first_stroke
FROM stroke_events s
WHERE s.stroke_number = 1;

CREATE UNIQUE INDEX first_stroke_idx ON first_stroke (pid, discharge_at, admission_at, eid);


-- All recurrent vascular events (not necessarily stroke) after the first I63.* stroke diagnosis.
DROP INDEX IF EXISTS recurrent_vascular_events_after_first_stroke_idx;
DROP TABLE IF EXISTS recurrent_vascular_events_after_first_stroke;

-- At the time of first writing SELECT max(vascular_events_number) FROM vascular_events is equal to 6 so we include the first 5 recurrent events
WITH second_events AS (
    SELECT
      v.pid,
      v.eid AS eid_2,
      v.diagnosis AS diagnosis_2,
      v.admission_at AS admission_at_2,
      v.nihss_at_admission AS nihss_at_admission_2
    FROM vascular_events v
    LEFT JOIN first_stroke f ON f.pid = v.pid
    WHERE v.vascular_event_number = f.vascular_event_number + 1
  ),
    third_events AS (
      SELECT
        v.pid,
        v.eid AS eid_3,
        v.diagnosis AS diagnosis_3,
        v.admission_at AS admission_at_3,
        v.nihss_at_admission AS nihss_at_admission_3
      FROM vascular_events v
        LEFT JOIN first_stroke f ON f.pid = v.pid
      WHERE v.vascular_event_number = f.vascular_event_number + 2
  ),
    fourth_events AS (
      SELECT
        v.pid,
        v.eid AS eid_4,
        v.diagnosis AS diagnosis_4,
        v.admission_at AS admission_at_4,
        v.nihss_at_admission AS nihss_at_admission_4
      FROM vascular_events v
        LEFT JOIN first_stroke f ON f.pid = v.pid
      WHERE v.vascular_event_number = f.vascular_event_number + 3
  ),
    fifth_events AS (
      SELECT
        v.pid,
        v.eid AS eid_5,
        v.diagnosis AS diagnosis_5,
        v.admission_at AS admission_at_5,
        v.nihss_at_admission AS nihss_at_admission_5
      FROM vascular_events v
        LEFT JOIN first_stroke f ON f.pid = v.pid
      WHERE v.vascular_event_number = f.vascular_event_number + 4
  ),
    total_events AS (
      SELECT
        v.pid,
        MAX(v.vascular_event_number) AS total_number_of_vascular_events
      FROM vascular_events v
      GROUP BY v.pid
  )

SELECT
  e1.pid,
  e1.eid AS eid_1,
  e1.vascular_event_number AS fist_stroke_vascular_event_number,
  e1.diagnosis AS diagnosis_1,
  e1.stroke_number AS stroke_number, -- always 1
  e1.nihss_at_admission,
  t.total_number_of_vascular_events, -- including first stroke and any vascular events before it
  (SELECT t.total_number_of_vascular_events - e1.vascular_event_number) AS number_of_recurrent_events_after_first_stroke,
  e2.eid_2,
  e2.admission_at_2,
  e2.nihss_at_admission_2,
  e2.diagnosis_2,
  (SELECT date_part('day', age(e2.admission_at_2, e1.admission_at))) AS days_between_events_1_2,
  e3.eid_3,
  e3.admission_at_3,
  e3.nihss_at_admission_3,
  e3.diagnosis_3,
  (SELECT date_part('day', age(e3.admission_at_3, e2.admission_at_2))) AS days_between_events_2_3,
  e4.eid_4,
  e4.admission_at_4,
  e4.nihss_at_admission_4,
  e4.diagnosis_4,
  (SELECT date_part('day', age(e4.admission_at_4, e3.admission_at_3))) AS days_between_events_3_4,
  e5.eid_5,
  e5.admission_at_5,
  e5.nihss_at_admission_5,
  e5.diagnosis_5,
  (SELECT date_part('day', age(e5.admission_at_5, e4.admission_at_4))) AS days_between_events_4_5

INTO recurrent_vascular_events_after_first_stroke
FROM first_stroke e1
  LEFT JOIN total_events t on t.pid = e1.pid
  LEFT JOIN second_events e2 on e2.pid = e1.pid
  LEFT JOIN third_events e3 on e3.pid = e1.pid
  LEFT JOIN fourth_events e4 on e4.pid = e1.pid
  LEFT JOIN fifth_events e5 on e5.pid = e1.pid;

CREATE UNIQUE INDEX recurrent_vascular_events_after_first_stroke_idx ON recurrent_vascular_events_after_first_stroke (pid, eid_1, eid_2, eid_3, eid_4, eid_5);


-- Antithrombotics
DROP INDEX IF EXISTS antithrombotics_idx;
DROP TABLE IF EXISTS antithrombotics;

SELECT * INTO antithrombotics FROM drugs l WHERE l.atc ~ 'B01AC.*';
CREATE INDEX antithrombotics_idx ON antithrombotics(pid, eid, atc, ordered_at, administered_at, first_dose_at, last_dose_at);

-- Anticoagulants
DROP INDEX IF EXISTS anticoagulants_idx;
DROP TABLE IF EXISTS anticoagulants;

SELECT * INTO anticoagulants FROM drugs l WHERE l.atc ~ 'B01A[^C].*';
CREATE INDEX anticoagulants_idx ON anticoagulants(pid, eid, atc, ordered_at, administered_at, first_dose_at, last_dose_at);

-- Statins
DROP INDEX IF EXISTS statins_idx;
DROP TABLE IF EXISTS statins;

SELECT * INTO statins FROM drugs l WHERE l.atc ~ 'C10AA.*';
CREATE INDEX statins_idx ON statins(pid, eid, atc, ordered_at, administered_at, first_dose_at, last_dose_at);
