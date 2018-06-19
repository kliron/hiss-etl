-- patienter: First ischemic stroke registered in the database, I63 diagnosis at discharge.
-- Recurrent vascular events: I21, I24, I25, I26, I46, I49, I50, I51, I61, I62, I63, I64, I66, I67, I69, I70, I71, I97.
-- Homocysteine value at admission.
-- Time from admission until death,
-- Time from admission until recurrent vascular event
-- Systolic Blood pressure at admission and at discharge
-- Diastolic Blood pressure at admission and at discharge
-- NIHHS at admission
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
-- triglycerides.
-- B vitamin supplementation at admission.
-- B vitamin supplementation at discharge.

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS homocysteine_data;

SELECT DISTINCT ON (f.pid)
  f.pid,
  f.gender,
  f.birth_date,
  f.death_date,
  j.smoking,
  b.bmi,
  f.eid AS eid_1,
  f.age_at_event,
  f.admission_at AS admitted_at_1,
  f.discharge_at AS discharged_at_1,
  f.days AS days_1,
  f.diagnosis AS diagnosis_1,
  sys.systolic_bp_admission AS systolic_bp_admission_1,
  dia.diastolic_bp_admission AS diastolic_bp_admission_1,
  sys.systolic_bp_discharge AS systolic_bp_discharge_1,
  dia.diastolic_bp_discharge AS diastolic_bp_discharge_1,
  r.nihss_at_admission,
  l.cholesterol AS cholesterol_1,
  l.ldl AS ldl_1,
  l.hdl AS hdl_1,
  l.triglycerides AS tgl_1,
  l.glucose AS glu_1,
  l.hba1c AS hba1c_1,
  l.hba1c_ifcc AS hba1c_ifcc_1,
  l.sodium AS sodium_1,
  l.potassium AS potassium_1,
  l.creatinine AS creatinine_1,
  l.hemoglobin AS hemoglobin_1,
  l.wbc AS wbc_1,
  l.platelets AS plt_1,
  l.erythrocytes AS ery_1,
  l.aptt AS aptt_1,
  l.inr AS inr_1,
  f.days_from_admission_to_death,
  sys2.systolic_bp_admission AS systolic_bp_admission_2,
  dia2.diastolic_bp_admission AS diastolic_bp_admission_2,
  sys2.systolic_bp_discharge AS systolic_bp_discharge_2,
  dia2.diastolic_bp_discharge AS diastolic_bp_discharge_2,
  r.nihss_at_admission_2,
  l2.cholesterol AS cholesterol_2,
  l2.ldl AS ldl_2,
  l2.hdl AS hdl_2,
  l2.triglycerides AS tgl_2,
  l2.glucose AS glu_2,
  l2.hba1c AS hba1c_2,
  l2.hba1c_ifcc AS hba1c_ifcc_2,
  l2.sodium AS sodium_2,
  l2.potassium AS potassium_2,
  l2.creatinine AS creatinine_2,
  l2.hemoglobin AS hemoglobin_2,
  l2.wbc AS wbc_2,
  l2.platelets AS plt_2,
  l2.erythrocytes AS ery_2,
  l2.aptt AS aptt_2,
  l2.inr AS inr_2,
  r.eid_2,
  r.admission_at_2,
  r.diagnosis_2,
  r.days_between_events_1_2,
  r.eid_3,
  r.admission_at_3,
  r.diagnosis_3,
  r.days_between_events_2_3,
  r.eid_4,
  r.admission_at_4,
  r.diagnosis_4,
  r.days_between_events_3_4,
  r.eid_5,
  r.admission_at_5,
  r.diagnosis_5,
  r.days_between_events_4_5,
  r.total_number_of_vascular_events,
  r.number_of_recurrent_events_after_first_stroke,

  -- The following 2 steps (diagnoses and drugs) need to do correlated subqueries over very large row sets (over one million each).
  -- They can take up to 1-2 minutes to calculate.

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

INTO homocysteine_data
FROM first_stroke f
  LEFT JOIN recurrent_vascular_events_after_first_stroke r ON r.pid = f.pid
  LEFT JOIN systolic_blood_pressures sys ON sys.eid = f.eid
  LEFT JOIN diastolic_blood_pressures dia ON dia.eid = f.eid
  LEFT JOIN systolic_blood_pressures sys2 ON sys2.eid = r.eid_2
  LEFT JOIN diastolic_blood_pressures dia2 ON dia2.eid = r.eid_2
  LEFT JOIN lab_wide l ON l.eid = f.eid
  LEFT JOIN lab_wide l2 ON l2.eid = r.eid_2
  LEFT JOIN smoking j ON j.eid = f.eid
  LEFT JOIN bmi b ON b.eid = f.eid
  LEFT JOIN diagnoses_wide w ON w.eid = f.eid
ORDER BY f.pid ASC;



-- Done. Now extract in CSV
-- COPY homocysteine_data TO '/Users/kliron/Downloads/homocysteine.csv' WITH DELIMITER '|' CSV HEADER;

-- NOTES:

-- Diagnoses (true/false) are up to the FIRST admission.
-- Drugs are calculated by taking the first and last dates a patient was EVER on ANY of each drugs for every group.


-- PROBLEMS:

-- 1. Smoking is encoded as free text and there are only 357 (from 3753) patients with non-null smoking values
-- 2. The first lab date is 2007-12-20 08:14:00 meaning there are no lab data from 2005-01-01 until that date.
-- 3. The first drug administration date is 2007-02-27 20:00 meaning there are no drug data from 2005-01-01 until that date.

