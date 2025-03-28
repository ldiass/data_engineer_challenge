
{{ config(
    materialized='materialized_view',
) }}

with source_data as (

    select
        battalion
        , date_trunc('month', incident_date::date) AS incident_date_month
        , count(1) as incidents_count 
        , avg(EXTRACT(epoch FROM (alarm_dttm::timestamp - arrival_dttm::timestamp)) / 60) as avg_minutes_to_arrival
        , avg(EXTRACT(epoch FROM (alarm_dttm::timestamp - close_dttm::timestamp)) / 60) as avg_minutes_to_close
        , avg(suppression_units) as avg_suppression_units
        , avg(suppression_personnel) as avg_suppression_personnel
        , avg(ems_units) as avg_ems_units
        , avg(ems_personnel) as avg_ems_personnel
        , avg(other_units) as avg_other_units
        , avg(other_personnel) as avg_other_personnel
        , avg(estimated_property_loss) as avg_estimated_property_loss
        , avg(estimated_contents_loss) as avg_estimated_contents_loss
        , avg(fire_fatalities) as avg_fire_fatalities
        , avg(fire_injuries) as avg_fire_injuries
        , avg(civilian_fatalities) as avg_civilian_fatalities
        , avg(civilian_injuries) as avg_civilian_injuries
        , avg(number_of_alarms) as avg_number_of_alarms
        , sum(CASE 
            WHEN detectors_present='1 Present' or detectors_present='1 -Present' 
            THEN 1
        END) as count_detectors_present
        , avg(number_of_floors_with_minimum_damage) as avg_number_of_floors_with_minimum_damage
        , avg(number_of_floors_with_significant_damage) as avg_number_of_floors_with_significant_damage
        , avg(number_of_floors_with_heavy_damage) as avg_number_of_floors_with_heavy_damage
        , avg(number_of_floors_with_extreme_damage) as avg_number_of_floors_with_extreme_damage
        , sum(CASE 
            WHEN automatic_extinguishing_system_present='1 Present' 
                OR automatic_extinguishing_system_present='1 -Present'
                OR automatic_extinguishing_system_present='2 Partial system present'
                OR automatic_extinguishing_system_present='2 -Partial system present' 
            THEN 1
        END) as count_automatic_extinguishing_system_present
    from {{ref('fire_incidents')}}
    group by battalion, date_trunc('month', incident_date)
)
select *
from source_data