-- actilyse
-- in some rare cases there may be double actilyse administration records for the same pas_vardhandelse_id with
-- different administered_at. we keep only the earliest.
-- a lot of thrombolysis cases are completely missing actilyse order - we only know about them from the zatc1 code. we
-- include these cases but they are worthless in counting door-to-needle times

-- initial time point for door-to-needle is taken as either akut_ankomsttidpunkt or inskrivningstidpunkt but these times
-- may not represent the real time of arrival! in some cases administered_at is earlier than either of these
-- times resulting in a worthless (negative) door_to_needle time.
drop table if exists thrombolysis_metrics;
with actilyse as (
  select act.eid, act.administered_at
  from (
    select
      l.eid,
      l.administered_at,
      row_number()
      over (
        partition by l.eid
        order by l.administered_at asc) nrow
    from drugs l
    where l.atc = 'B01AD02'
  ) act
  where nrow = 1
)
select
  m.eid,
  p.pid,
  p.gender,
  p.death_date,
  date_part('year', age(t.administered_at, p.birth_date)) as age,
  t.administered_at as at,
  (select extract (epoch from t.administered_at - coalesce(e.acute_arrival_at, e.admission_at)) / 60.0) as door_to_needle_min
into thrombolysis_metrics
from events e
  inner join measures m on m.eid = e.eid
  left join actilyse t on t.eid = e.eid
  left join patients p on p.pid = e.pid
where m.zatc1 = 'B01AD02'
order by at ASC, pid ASC;
copy thrombolysis_metrics to '/Users/kliron/Projects/hiss/karda/clean/thrombolysis_metrics.csv' with (delimiter '|', header true, format csv);


