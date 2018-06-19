drop table if exists vascular_events_metrics;
select
  v.eid,
  p.pid,
  p.gender,
  v.death_date,
  date_part('year', age(v.admission_at, p.birth_date)) as age,
  v.days,
  v.diagnosis,
  v.vascular_event_number,
  v.admission_at as at
into vascular_events_metrics
from vascular_events v
left join patients p on p.pid = v.pid
order by admission_at ASC, p.pid ASC;

copy vascular_events_metrics to '/Users/kliron/Projects/hiss/karda/clean/vascular_events_metrics.csv' with (delimiter '|', header true, format csv);

select count(distinct(pid)) from vascular_events_metrics where gender = 'M' and age < 60; -- 761
select count(distinct(pid)) from vascular_events_metrics where gender = 'F' and age < 60; -- 488

select count(distinct(pid)) from vascular_events_metrics where gender = 'M' and age >= 60; -- 2843
select count(distinct(pid)) from vascular_events_metrics where gender = 'F' and age >= 60; -- 2671

