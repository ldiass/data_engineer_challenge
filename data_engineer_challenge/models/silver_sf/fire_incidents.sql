
{{ config(
    materialized='incremental',
    unique_key='incident_number',
    on_schema_change='sync_all_columns'
) }}

with source_data as (

    select incident_number
        , exposure_number::integer
        , id
        , address
        , incident_date::date
        , call_number
        , alarm_dttm::timestamp
        , arrival_dttm::timestamp
        , close_dttm::timestamp
        , city
        , zipcode
        , battalion
        , station_area
        , box
        , suppression_units::integer
        , suppression_personnel::integer
        , ems_units::integer
        , ems_personnel::integer
        , other_units::integer
        , other_personnel::integer
        , first_unit_on_scene
        , estimated_property_loss::float
        , estimated_contents_loss::float
        , fire_fatalities::integer
        , fire_injuries::integer
        , civilian_fatalities::integer
        , civilian_injuries::integer
        , number_of_alarms::integer
        , primary_situation
        , mutual_aid
        , action_taken_primary
        , action_taken_secondary
        , action_taken_other
        , detector_alerted_occupants
        , property_use
        , area_of_fire_origin
        , ignition_cause
        , ignition_factor_primary
        , ignition_factor_secondary
        , heat_source
        , item_first_ignited
        , human_factors_associated_with_ignition
        , structure_type
        , structure_status
        , floor_of_fire_origin::integer
        , fire_spread
        , no_flame_spread
        , number_of_floors_with_minimum_damage::integer
        , number_of_floors_with_significant_damage::integer
        , number_of_floors_with_heavy_damage::integer
        , number_of_floors_with_extreme_damage::integer
        , detectors_present
        , detector_type
        , detector_operation
        , detector_effectiveness
        , detector_failure_reason
        , automatic_extinguishing_system_present
        , automatic_extinguishing_system_type
        , automatic_extinguishing_system_performance
        , automatic_extinguishing_system_failure_reason
        , number_of_sprinkler_heads_operating::integer
        , supervisor_district
        , neighborhood_district
        , point
        , data_as_of::timestamp
        , data_loaded_at::timestamp
        , row_number() over(partition by incident_number order by id desc) as row_num
    from {{source('bronze_sf', 'fire_incidents')}}
    {% if is_incremental() %}
    where data_as_of >= (select coalesce(max(data_as_of),'1900-01-01')::timestamp from {{ this }} )
    {% endif %}
)
select incident_number
    , exposure_number
    , id
    , address
    , incident_date
    , call_number
    , alarm_dttm
    , arrival_dttm
    , close_dttm
    , city
    , zipcode
    , battalion
    , station_area
    , box
    , suppression_units
    , suppression_personnel
    , ems_units
    , ems_personnel
    , other_units
    , other_personnel
    , first_unit_on_scene
    , CASE WHEN estimated_property_loss < 0 THEN NULL ELSE estimated_property_loss END as estimated_property_loss
    , CASE WHEN estimated_contents_loss < 0 THEN NULL ELSE estimated_contents_loss END as estimated_contents_loss
    , fire_fatalities
    , fire_injuries
    , civilian_fatalities
    , civilian_injuries
    , number_of_alarms
    , primary_situation
    , mutual_aid
    , action_taken_primary
    , action_taken_secondary
    , action_taken_other
    , detector_alerted_occupants
    , property_use
    , area_of_fire_origin
    , ignition_cause
    , ignition_factor_primary
    , ignition_factor_secondary
    , heat_source
    , item_first_ignited
    , human_factors_associated_with_ignition
    , structure_type
    , structure_status
    , floor_of_fire_origin
    , fire_spread
    , no_flame_spread
    , number_of_floors_with_minimum_damage
    , number_of_floors_with_significant_damage
    , number_of_floors_with_heavy_damage
    , number_of_floors_with_extreme_damage
    , detectors_present
    , detector_type
    , detector_operation
    , detector_effectiveness
    , detector_failure_reason
    , automatic_extinguishing_system_present
    , automatic_extinguishing_system_type
    , automatic_extinguishing_system_performance
    , automatic_extinguishing_system_failure_reason
    , number_of_sprinkler_heads_operating
    , supervisor_district
    , neighborhood_district
    , point
    , data_as_of
    , data_loaded_at
from source_data
where row_num=1