
{{ config(
    materialized='materialized_view',
    on_schema_change='sync_all_columns'
) }}

with source_data as (

    select
        battalion
        , date_trunc('month', incident_date::date) AS incident_date_month
        , count(1) as incidents_count 
        , sum(EXTRACT(epoch FROM (arrival_dttm::timestamp - alarm_dttm::timestamp)) / 60) as sum_minutes_to_arrival
        , sum(EXTRACT(epoch FROM (close_dttm::timestamp - alarm_dttm::timestamp)) / 60) as sum_minutes_to_close
        , sum(suppression_units) as sum_suppression_units
        , sum(suppression_personnel) as sum_suppression_personnel
        , sum(ems_units) as sum_ems_units
        , sum(ems_personnel) as sum_ems_personnel
        , sum(other_units) as sum_other_units
        , sum(other_personnel) as sum_other_personnel
        , sum(estimated_property_loss) as sum_estimated_property_loss
        , sum(estimated_contents_loss) as sum_estimated_contents_loss
        , sum(fire_fatalities) as sum_fire_fatalities
        , sum(fire_injuries) as sum_fire_injuries
        , sum(civilian_fatalities) as sum_civilian_fatalities
        , sum(civilian_injuries) as sum_civilian_injuries
        , sum(number_of_alarms) as sum_number_of_alarms
        , sum(CASE 
            WHEN detectors_present='1 Present' or detectors_present='1 -Present' 
            THEN 1
        END) as count_detectors_present
        , sum(number_of_floors_with_minimum_damage) as sum_number_of_floors_with_minimum_damage
        , sum(number_of_floors_with_significant_damage) as sum_number_of_floors_with_significant_damage
        , sum(number_of_floors_with_heavy_damage) as sum_number_of_floors_with_heavy_damage
        , sum(number_of_floors_with_extreme_damage) as sum_number_of_floors_with_extreme_damage
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