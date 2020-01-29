with
small_table as (
  select distinct
    ch.code
    , jb.schedule
    , jb.category_id
    , jb.id
    , quantity
    , cp.field_exec_code
  from base_gogo.jobs_job_batches jb

  join base_gogo.core_zones cz
    on cz.id = jb.zone_id
  join base_gogo.core_parties cp
    on cp.id = jb.party_id
  join base_gogo.core_users cu
    on cu.party_id = cp.id
  join base_gogo.core_hubs ch
    on ch.id = cz.hub_id
  where jb.deleted_at is null
    and jb.status in ('picked up', 'runsheet released', 'reserved')
    and ch.code = 'MAKATI'
)

, hub_cat_date as (
  select distinct *
  from (
  select distinct
    code
    , category_id
  from small_table
  ) t, `dale.date_list`
)

, base as (
  select distinct
    d.code as hub_code
    , d.day as schedule
    , case
      when d.category_id = 1 then 'Delivery'
      when d.category_id = 2 then 'Pickup'
    end as job_category
    , count(distinct id) as riders
    , sum(coalesce(quantity,0)) as parcels

  from small_table s
  right join hub_cat_date d
    on d.day = s.schedule
      and s.code = d.code
      and s.category_id = d.category_id

  where day <= date_add(current_date(), interval 1 day)
    and day >= date_sub(current_date(), interval 2 year)

  group by 1,2,3
  order by 1,2 desc,3
)

, count_table as (
  select
    b.hub_code
    , b.schedule
    , b.job_category
    , b.riders -- y

    -- raw count of riders
    , lead(riders) over (partition by hub_code, job_category order by schedule desc) as rider_yesterday
    , lead(riders, 7) over (partition by hub_code, job_category order by schedule desc) as rider_lastweek
    , lead(riders, 14) over (partition by hub_code, job_category order by schedule desc) as rider_2week
    , lead(riders, 21) over (partition by hub_code, job_category order by schedule desc) as rider_3week
    , lead(riders, 28) over (partition by hub_code, job_category order by schedule desc) as rider_4week
    , lead(riders, 365) over (partition by hub_code, job_category order by schedule desc) as rider_lastyear
    , lead(riders, 364) over (partition by hub_code, job_category order by schedule desc) as rider_52week
    -- similar riders

    -- volume
    , avg(parcels) over (partition by hub_code, job_category order by schedule desc
                        rows between 1 following and 7 following) as volume_last7day
    --, avg(parcels) over (partition by hub_code, job_category order by schedule desc
    --                    rows between 1 following and 3 following) as volume_last3day
    -- using this is too problematic
    , avg(parcels) over (partition by hub_code, job_category order by schedule desc
                        rows between 1 following and 14 following) as volume_last14day
    , avg(parcels) over (partition by hub_code, job_category order by schedule desc
                        rows between 1 following and 21 following) as volume_last21day
    , avg(parcels) over (partition by hub_code, job_category order by schedule desc
                        rows between 1 following and 28 following) as volume_last28day
    -- dow and quarter
    , case
      when extract(quarter from schedule)=1 then 'Q1'
      when extract(quarter from schedule)=2 then 'Q2'
      when extract(quarter from schedule)=3 then 'Q3'
      when extract(quarter from schedule)=4 then 'Q4'
    end as quarter
    , case
      when extract(dayofweek from schedule)=1 then 'sunday'
      when extract(dayofweek from schedule)=2 then 'monday'
      when extract(dayofweek from schedule)=3 then 'tuesday'
      when extract(dayofweek from schedule)=4 then 'wednesday'
      when extract(dayofweek from schedule)=5 then 'thursday'
      when extract(dayofweek from schedule)=6 then 'friday'
      when extract(dayofweek from schedule)=7 then 'saturday'
    end as day_of_week
    , case
      when mod(extract(dayofweek from schedule), 6)=1 then "weekend"
      when mod(extract(dayofweek from schedule), 6)!=1 then "weekday"
    end as is_weekend
    , extract(month from schedule) as month
    , extract(day from schedule) as day

    -- holiday or sale
    , case
      when h.base_event is null then "not_holiday"
      else "holiday"
    end as is_holiday
    , case
      when schedule in (date('2019-09-10'), date('2019-10-11'), date('2019-11-12'), date('2019-12-13')) then '1day_after_sale'
      when schedule in (date('2019-09-11'), date('2019-10-12'), date('2019-11-13'), date('2019-12-14')) then '2day_after_sale'
      when schedule in (date('2019-09-12'), date('2019-10-13'), date('2019-11-14'), date('2019-12-15')) then '3day_after_sale'
      when schedule in (date('2019-09-13'), date('2019-10-14'), date('2019-11-15'), date('2019-12-16')) then '4day_after_sale'
      when schedule in (date('2019-09-14'), date('2019-10-15'), date('2019-11-16'), date('2019-12-17')) then '5day_after_sale'
      else 'not_after_sale'
    end as is_after_sale

  from base b

  left join `dale.holiday_date` h
    on b.schedule = h.holiday_date
)



select * from count_table
where schedule >= date_sub(current_date(), interval 457 day)


