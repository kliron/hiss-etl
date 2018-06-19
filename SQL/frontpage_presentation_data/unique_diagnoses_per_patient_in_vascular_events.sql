-- Calculate the first occurence of each diagnosis code and the age of the patient at that occurence
drop table if exists unique_diagnoses_per_patient;
drop index if exists unique_diagnoses_per_patient_idx;
with partitioned_diagnoses as (
  select
    d.pid,
    p.gender,
    date_part('year', age(d.admission_at, p.birth_date)) as age_at_diagnosis,
    d.diagnosis,
    d.admission_at as at,
    row_number() over (partition by d.pid, d.diagnosis order by d.admission_at asc) as rownum
  from all_diagnoses d
  left join patients p on p.pid = d.pid
  where p.pid in (select distinct(pid) from vascular_events)
)
  select
    diagnosis,
    pid,
    gender,
    age_at_diagnosis,
    (SELECT CASE WHEN age_at_diagnosis < 60 THEN 1 ELSE 0 END) as under_sixty,
    at
  into unique_diagnoses_per_patient
  from partitioned_diagnoses
  where rownum = 1 order by pid asc, diagnosis asc;
create unique index unique_diagnoses_per_patient_idx on unique_diagnoses_per_patient(pid, diagnosis, at);
copy unique_diagnoses_per_patient to '/Users/kliron/Projects/hiss/karda/clean/unique_diagnoses_per_patient.csv' with (delimiter '|', header true, format csv);


-- Now we can calculate nice summary statistics per diagnosis grouped by gender, age, etc
drop table if exists diagnosis_metrics;
select
  count (d.diagnosis) as count,
  d.diagnosis,
  i.description,
  d.gender,
  d.under_sixty
into diagnosis_metrics
from unique_diagnoses_per_patient d
left join icd10 i on i.diagnosis = d.diagnosis
where under_sixty = 1
group by diagnosis, description, gender, under_sixty
order by count desc, under_sixty desc;

-- here are the codes we are missing:
-- select distinct(diagnosis) from stroke_alla_diagnoser where diagnosis not in (select distinct(code) from icd10) and diagnosis !~ '^[0-9].*'  -- ignore non-icd10 codes
