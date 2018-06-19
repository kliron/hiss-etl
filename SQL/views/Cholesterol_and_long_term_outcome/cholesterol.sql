-- Patients: First ischemic stroke registered in the database, I63 diagnos at discharge.
-- Total cholesterol and LDL cholesterol values at admission.
-- Time from admission until death,
-- Systolic Blood pressure at admission and at discharge
-- Diastolic Blood pressure at admission and at discharge
-- NIHHS at admission
-- BMI
-- Age
-- Gender
-- Atrial fibrillation
-- Hypertension
-- Diabetes mellitus
-- Hyperlipidemia
-- Carotid stenosis
-- Heart failure
-- Previous PCI/CABG
-- History of MI/angina
-- Intermittent claudication
-- History of cancer
-- Asthma
-- COPD
-- DVT/LE
-- Depression
-- Dementia
-- Migraine
-- Ever smoker
-- Biochemical parameters at admission:
-- glucose,
-- HbA1c,
-- sodium,
-- potassium,
-- creatinine,
-- white blood cell count,
-- haemoglobin,
-- erythrocytes,
-- thrombocytes,
-- APTT,
-- INR,
-- cholesterol,
-- LDL,
-- HDL,
-- triglycerides
-- ASA treatment at admission (aspirin, clopidogrel)
-- ASA treatment at discharge (aspirin, clopidogrel)
-- Anticoagulants at admission (LMWH, heparin, warfarin, NOACs)
-- Anticoagulants at discharge (LMWH, heparin, warfarin, NOACs)
-- Statins at admission
-- Statins at discharge


-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS cholesterol_data;

SELECT DISTINCT ON (f.pid)
  f.pid,
  k.gender,
  k.birth_date,
  k.death_date,
  f.eid,
  f.age_at_event,
  f.days_from_admission_to_death,
  f.admission_at,
  f.discharge_at,
  f.days,
  f.diagnosis,
  sys.systolic_bp_admission,
  sys.systolic_bp_discharge,
  dia.diastolic_bp_admission,
  dia.diastolic_bp_discharge,
  f.nihss_at_admission,
  l.cholesterol,
  l.ldl,
  l.hdl,
  l.triglycerides,
  l.glucose,
  l.hba1c,
  l.hba1c_ifcc,
  l.sodium,
  l.potassium,
  l.creatinine,
  l.hemoglobin,
  l.wbc,
  l.platelets,
  l.erythrocytes,
  l.aptt,
  l.inr,
  j.smoking,
  b.bmi,
  -- The following 2 steps (diagnoses and drugs) need to do correlated subqueries over very large row sets (over one million each).
  -- They can take up to 1-2 minutes to calculate
  -- DIAGNOSES --
  (SELECT COALESCE(MAX(w.atrial_fibrillation), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS atrial_fibrillation,
  (SELECT COALESCE(MAX(w.diabetes), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS diabetes,
  (SELECT COALESCE(MAX(w.hypertension), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS hypertension,
  (SELECT COALESCE(MAX(w.hyperlipidemia), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS hyperlipidemia,
  (SELECT COALESCE(MAX(w.carotid_stenosis), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS carotid_stenosis,
  (SELECT COALESCE(MAX(w.heart_failure), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS heart_failure,
  (SELECT COALESCE(MAX(w.claudication), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS claudication,
  (SELECT COALESCE(MAX(w.cabg_or_pci), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS cabg_or_pci,
  (SELECT COALESCE(MAX(w.mi_or_angina), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS mi_or_angina,
  (SELECT COALESCE(MAX(w.cancer), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS cancer,
  (SELECT COALESCE(MAX(w.asthma), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS asthma,
  (SELECT COALESCE(MAX(w.copd), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS copd,
  (SELECT COALESCE(MAX(w.dvt_or_le), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS dvt_or_le,
  (SELECT COALESCE(MAX(w.depression), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS depression,
  (SELECT COALESCE(MAX(w.dementia), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS dementia,
  (SELECT COALESCE(MAX(w.migraine), 0) FROM diagnoses_wide w WHERE w.pid = f.pid AND w.discharge_at <= f.discharge_at) AS migraine,

  -- DRUGS --
  -- See above about IGNORE NULLS emulation with CASE ... WHEN ...
  (SELECT CASE WHEN (SELECT t.pid FROM antithrombotics t WHERE t.pid = f.pid AND t.first_dose_at <= f.admission_at AND (t.last_dose_at IS NULL OR t.last_dose_at >= f.admission_at) LIMIT 1) IS NULL THEN 0 ELSE 1 END) AS antithrombotics_admission,
  (SELECT CASE WHEN (SELECT t.pid FROM antithrombotics t WHERE t.pid = f.pid AND t.first_dose_at <= f.discharge_at AND (t.last_dose_at IS NULL OR t.last_dose_at <= f.discharge_at) LIMIT 1) IS NULL THEN 0 ELSE 1 END) AS antithrombotics_discharge,
  (SELECT CASE WHEN (SELECT t.pid FROM anticoagulants t WHERE t.pid = f.pid AND t.first_dose_at <= f.admission_at AND (t.last_dose_at IS NULL OR t.last_dose_at >= f.admission_at) LIMIT 1) IS NULL THEN 0 ELSE 1 END) AS anticoagulants_admission,
  (SELECT CASE WHEN (SELECT t.pid FROM anticoagulants t WHERE t.pid = f.pid AND t.first_dose_at <= f.discharge_at AND (t.last_dose_at IS NULL OR t.last_dose_at <= f.discharge_at) LIMIT 1) IS NULL THEN 0 ELSE 1 END) AS anticoagulants_discharge,
  (SELECT CASE WHEN (SELECT t.pid FROM statins t WHERE t.pid = f.pid AND t.first_dose_at <= f.admission_at AND (t.last_dose_at IS NULL OR t.last_dose_at >= f.admission_at) LIMIT 1) IS NULL THEN 0 ELSE 1 END) AS statins_admission,
  (SELECT CASE WHEN (SELECT t.pid FROM statins t WHERE t.pid = f.pid AND t.first_dose_at <= f.discharge_at AND (t.last_dose_at IS NULL OR t.last_dose_at <= f.discharge_at) LIMIT 1) IS NULL THEN 0 ELSE 1 END) AS statins_discharge

INTO cholesterol_data
FROM first_stroke f
  LEFT JOIN patients k ON k.pid = f.pid
  LEFT JOIN systolic_blood_pressures sys ON sys.eid = f.eid
  LEFT JOIN diastolic_blood_pressures dia ON dia.eid = f.eid
  LEFT JOIN lab_wide l ON l.eid = f.eid
  LEFT JOIN smoking j ON j.eid = f.eid
  LEFT JOIN bmi b ON b.eid = f.eid
  LEFT JOIN diagnoses_wide w ON w.eid = f.eid
ORDER BY f.pid ASC;


-- Done. Now extract in CSV
-- COPY cholesterol_data TO '/Users/kliron/Projects/HISS/Karda/clean/cholesterol.csv' WITH DELIMITER '|' CSV HEADER;

-- NOTES:
-- Drugs are calculated by taking the first and last dates a patient was EVER on ANY of each drugs for every group
-- (anticoagulants, antithrombotic, statins)


-- PROBLEMS:
-- 1. Smoking is encoded as free text and there are only 357 (from 3753) patients with non-null smoking values
-- 2. The first lab date is 2007-12-20 08:14:00 meaning there are no lab data from 2005-01-01 until that date.
-- 3. The first drug administration date is 2007-02-27 20:00 meaning there are no drug data from 2005-01-01 until that date.
