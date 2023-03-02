--  create  table "postgres".quarantine."covid_epidemiology_f11__dbt_tmp"
--   as (

with raw_form_responses as (
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    jsonb_extract_path_text(_airbyte_data, 'Day') as Day,
    jsonb_extract_path_text(_airbyte_data, 'Email') as email,
    jsonb_extract_path_text(_airbyte_data, 'Shift') as shift,
    jsonb_extract_path_text(_airbyte_data, 'Timestamp') as "Timestamp",
    jsonb_extract_path_text(_airbyte_data, 'Employee_Name') as employee_name,
    jsonb_extract_path_text(_airbyte_data, 'Food_Coupon_ID') as food_coupon_id,
    jsonb_extract_path_text(_airbyte_data, 'Sub_Department') as sub_department,
    jsonb_extract_path_text(_airbyte_data, 'Main_Department') as main_department,
    jsonb_extract_path_text(_airbyte_data, 'Drop_Off_Required') as drop_off_required
from "raw".transformed_raw._airbyte_raw_test_form_responses
-- covid_epidemiology
)
select
    Day,
    email,
    shift,
    "Timestamp",
    UPPER(employee_name),
    food_coupon_id,
    UPPER(sub_department),
    main_department,
    drop_off_required
from raw_form_respones 
