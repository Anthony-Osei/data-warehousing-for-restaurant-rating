--create the database
CREATE DATABASE UDACITY_PROJECT;

--create the staging environment(schema)
CREATE SCHEMA "UDACITY_PROJECT"."STAGING";

--create tables in the staging area
CREATE TABLE BUSINESS_STG (BIZ_INFO VARIANT);

CREATE TABLE REVIEW_STG (REVIEW_DETAILS VARIANT);

CREATE TABLE COVID_STG (COVID_DETAILS VARIANT);

CREATE TABLE USER_STG (USER_DETAILS VARIANT);

CREATE TABLE CHECKIN_STG (CHECKIN_DETAILS VARIANT);

CREATE TABLE TIP_STG (TIP_DETAILS VARIANT);

CREATE TABLE TEMPERATURE_STG 
             (DATE STRING,
              MIN_TEMP STRING, 
              MAX_TEMP STRING, 
              NORMAL_MIN STRING, 
              NORMAL_MAX STRING);

CREATE TABLE PRECIPITATION 
             (DATE STRING, 
              PRECIPITATION STRING, 
              PRECIPITATION_NORMAL STRING);

--copy files into staging tables

COPY INTO PRECIPITATION FROM @/ui1647837847712 
          FILE_FORMAT = CSVFILEFORMAT 
          ON_ERROR = ABORT_STATEMENT 
          PURGE = TRUE;

UPDATE udacity_project.staging.precipitation SET precipitation = 0 WHERE precipitation = 'T';

COPY INTO TEMPERATURE_STG FROM @/ui1647838896322 
          FILE_FORMAT = CSVFILEFORMAT 
          ON_ERROR = ABORT_STATEMENT 
          PURGE = TRUE;

copy into COVID_STG from @tmp_stage/yelp_academic_dataset_covid_features.json.gz file_format=jsonformat;

copy into BUSINESS_STG from @tmp_stage/yelp_academic_dataset_business.json.gz file_format=jsonformat;

copy into CHECKIN_STG from @tmp_stage/yelp_academic_dataset_checkin.json.gz file_format=jsonformat;

copy into TIP_STG from @tmp_stage/yelp_academic_dataset_tip.json.gz file_format=jsonformat;

copy into USER_STG from @tmp_stage/yelp_academic_dataset_user.json.gz file_format=jsonformat;

copy into REVIEW_STG from @tmp_stage/yelp_academic_dataset_review.json.gz file_format=jsonformat;


--Create ODS Schema
CREATE SCHEMA "UDACITY_PROJECT"."ODS";

--Create & populate tables in ODS environment
CREATE TABLE PRECIPITATION (
       "DATE" DATE primary key, 
        PRECIPITATION FLOAT, 
        PRECIPITATION_NORMAL FLOAT);

insert into precipitation(date, precipitation, precipitation_normal)
select to_date(date,'YYYYMMDD'),
       cast(precipitation as float),
       cast(precipitation_normal as float)
from udacity_project.staging.precipitation;


CREATE TABLE TEMPERATURE ("DATE" DATE primary key, 
                           MIN_TEMP INTEGER, 
                           MAX_TEMP INTEGER, 
                           NORMAL_MIN_TEMP FLOAT, 
                           NORMAL_MAX_TEMP FLOAT);


insert into temperature(date, min_temp, max_temp, normal_min_temp, normal_max_temp)
select to_date(date, 'YYYYMMDD'),
       cast(min_temp as integer),
       cast(max_temp as integer),
       cast(normal_min as float),
       cast(normal_max as float)
from udacity_project.staging.temperature_stg;


CREATE TABLE TIP ("BIZ_ID" STRING references business(business_id), 
                   USER_ID STRING references user(user_id), 
                   COMPLIMENT_COUNT INTEGER, 
                   "DATE" DATE, 
                   TEXT STRING);

insert into tip(biz_id, user_id, compliment_count, date, text)
select parse_json($1):business_id,
       parse_json($1):user_id,
       parse_json($1):compliment_count,
       parse_json($1):date,
       parse_json($1):text
from udacity_project.staging.tip_stg;

CREATE TABLE REVIEW (BIZ_ID STRING references business(business_id), 
                     REVIEW_ID STRING primary key, 
                     DATE DATE, 
                     FUNNY INTEGER, 
                     COOL INTEGER, 
                     TEXT STRING, 
                     USEFUL INTEGER,
                     STARS INTEGER, 
                     USER_ID STRING references user(user_id));

insert into REVIEW (BIZ_ID, REVIEW_ID,"DATE", FUNNY, COOL, TEXT,USEFUL, STARS, USER_ID)
select parse_json($1):business_id,
       parse_json($1):review_id,
       parse_json($1):date,
       parse_json($1):funny,
       parse_json($1):cool,
       parse_json($1):text,
       parse_json($1):useful,
       parse_json($1):stars,
       parse_json($1):user_id
from udacity_project.staging.review_stg;

CREATE TABLE CHECKIN ("DATE" STRING, 
                      BIZ_ID STRING references business(business_id));

insert into checkin(date, biz_id)
select parse_json($1):date,
       parse_json($1):business_id
from udacity_project.staging.checkin_stg;

CREATE TABLE covid (biz_id string references business(business_id),
                  Call_To_Action_enabled VARIANT,
	              Covid_Banner VARIANT,
	              Grubhub_enabled	VARIANT,
	              Request_a_Quote_Enabled VARIANT,
	              Temporary_Closed_Until VARIANT,
	              Virtual_Services_Offered VARIANT,
	              delivery_or_takeout VARIANT,
	              highlights VARIANT);

INSERT INTO covid(biz_id,Call_To_Action_enabled,Covid_Banner,Grubhub_enabled,Request_a_Quote_Enabled, Temporary_Closed_Until,
Virtual_Services_Offered,delivery_or_takeout,highlights)
SELECT 
      parse_json($1):"business_id",
	parse_json($1):"Call To Action enabled",
	parse_json($1):"Covid Banner",
	parse_json($1):"Grubhub enabled",
	parse_json($1):"Request a Quote Enabled",
	parse_json($1):"Temporary Closed Until",
	parse_json($1):"Virtual Services Offered",
	parse_json($1):"delivery or takeout",
	parse_json($1):"highlights" FROM udacity_project.staging.covid_stg;


CREATE TABLE USER (USERNAME VARCHAR,
                   USER_ID STRING primary key,
                   USEFUL INTEGER, 
                   AVERAGE_STARS FLOAT, 
                   COMPLIMENT_COOL INTEGER, 
                   COMPLIMENT_CUTE INTEGER, COMPLIMENT_FUNNY INTEGER, 
                   COMPLIMENT_HOT INTEGER, COMPLIMENT_LIST INTEGER, 
                   COMPLIMENT_NOTE INTEGER, 
                   COMPLIMENT_MORE INTEGER, COMPLIMENT_PLAIN INTEGER, 
                   COMPLIMENT_PHOTOS INTEGER,
                   COMPLIMENT_PROFILE INTEGER, COMPLIMENT_WRITER INTEGER, 
                   FANS NUMBER,
                   ELITE STRING, COOL NUMBER,
                   FRIENDS VARIANT, FUNNY NUMBER, 
                   REVIEW_COUNT NUMBER, 
                   YELPING_SINCE STRING);

INSERT INTO USER (USERNAME, USER_ID, USEFUL, AVERAGE_STARS, COMPLIMENT_COOL, COMPLIMENT_CUTE, COMPLIMENT_FUNNY, COMPLIMENT_HOT, COMPLIMENT_LIST, 
                   COMPLIMENT_NOTE, COMPLIMENT_MORE, COMPLIMENT_PLAIN, COMPLIMENT_PHOTOS,COMPLIMENT_PROFILE, COMPLIMENT_WRITER, FANS,
                   ELITE, COOL,FRIENDS, FUNNY, 
                   REVIEW_COUNT, YELPING_SINCE)
select parse_json($1):name,
       parse_json($1):user_id,
       parse_json($1):useful,
       parse_json($1):average_stars,
       parse_json($1):compliment_cool,
       parse_json($1):compliment_cute,
       parse_json($1):compliment_funny,
       parse_json($1):compliment_hot,
       parse_json($1):compliment_list,
       parse_json($1):compliment_note,
       parse_json($1):compliment_more,
       parse_json($1):compliment_plain,
       parse_json($1):compliment_photos,
       parse_json($1):compliment_profile,
       parse_json($1):compliment_writer,
       parse_json($1):fans,
       parse_json($1):elite,
       parse_json($1):cool,
       parse_json($1):friends,
       parse_json($1):funny,
       parse_json($1):review_count,
       parse_json($1):yelping_since
from udacity_project.staging.user_stg;               


CREATE TABLE BUSINESS("BUSINESS_ID" STRING primary key, 
                      "BUSINESS_NAME" VARCHAR, 
                      "ADDRESS" VARCHAR, 
                      "CITY" VARCHAR, 
                      "STATE" VARCHAR, 
                      "POSTAL_CODE" VARCHAR, 
                      "LATITUDE" FLOAT, 
                      "LONGITUDE" FLOAT, 
                      "STARS" FLOAT, 
                      "REVIEW_COUNT" INTEGER, 
                      "IS_OPEN" INTEGER, 
                      "ATTRIBUTE" VARIANT, 
                      "CATEGORIES" STRING, 
                      "HOURS" VARIANT);

insert into business(BUSINESS_ID, BUSINESS_NAME, ADDRESS, CITY, STATE, POSTAL_CODE, LATITUDE, LONGITUDE, STARS, REVIEW_COUNT, IS_OPEN, ATTRIBUTE, CATEGORIES, HOURS)
select parse_json($1):business_id,
       parse_json($1):name,
       parse_json($1):address,
       parse_json($1):city,
       parse_json($1):state,
       parse_json($1):postal_code,
       parse_json($1):latitude,
       parse_json($1):longitude,
       parse_json($1):stars,
       parse_json($1):review_count,
       parse_json($1):is_open,
       parse_json($1):attributes,
       parse_json($1):categories,
       parse_json($1):hours
FROM udacity_project.staging.business_stg;

--Create a schema for the datawarehouse
CREATE SCHEMA "UDACITY_PROJECT"."DWH";

-- create and load data into the dimension and fact tables

create table dim_address(address_id int autoincrement primary key,
                         address_name varchar,
                         city varchar, state varchar,
                         postal_code varchar, latitude float,
                         longitude float);

insert into dim_address(address_name, city, state, postal_code, latitude, longitude)
select distinct address, city, state, postal_code, latitude, longitude
from udacity_project.ods.business;

CREATE TABLE dim_business("BUSINESS_ID" STRING PRIMARY KEY, 
                      "BUSINESS_NAME" VARCHAR, 
                      "STARS" FLOAT, 
                      "REVIEW_COUNT" INTEGER, 
                      "IS_OPEN" INTEGER, 
                      "ATTRIBUTE" VARIANT, 
                      "CATEGORIES" STRING, 
                      "HOURS" VARIANT);
                      
insert into dim_business(BUSINESS_ID, BUSINESS_NAME, 
                     STARS, REVIEW_COUNT, IS_OPEN, ATTRIBUTE, CATEGORIES, HOURS)
select distinct business_id, business_name,
                stars,
                review_count, is_open, attribute,
                categories, hours
FROM udacity_project.ods.business


CREATE TABLE dim_user (USERNAME VARCHAR,USER_ID STRING primary key, USEFUL INTEGER, 
                      AVERAGE_STARS FLOAT, 
                      COMPLIMENT_COOL INTEGER, 
                      COMPLIMENT_CUTE INTEGER, COMPLIMENT_FUNNY INTEGER, 
                      COMPLIMENT_HOT INTEGER, COMPLIMENT_LIST INTEGER, 
                      COMPLIMENT_NOTE INTEGER, 
                      COMPLIMENT_MORE INTEGER, COMPLIMENT_PLAIN INTEGER, 
                      COMPLIMENT_PHOTOS INTEGER,
                      COMPLIMENT_PROFILE INTEGER, COMPLIMENT_WRITER INTEGER, 
                      FANS NUMBER,
                      ELITE STRING, COOL NUMBER,
                      FRIENDS VARIANT, FUNNY NUMBER, 
                      REVIEW_COUNT NUMBER, 
                      YELPING_SINCE STRING);

INSERT INTO dim_user (USERNAME, USER_ID, USEFUL, AVERAGE_STARS, COMPLIMENT_COOL, COMPLIMENT_CUTE, COMPLIMENT_FUNNY, COMPLIMENT_HOT, COMPLIMENT_LIST, 
                      COMPLIMENT_NOTE, COMPLIMENT_MORE, COMPLIMENT_PLAIN, COMPLIMENT_PHOTOS,COMPLIMENT_PROFILE, COMPLIMENT_WRITER, FANS,
                      ELITE, COOL,FRIENDS, FUNNY, 
                      REVIEW_COUNT, YELPING_SINCE)
select distinct username, user_id,
                useful, average_stars,
                compliment_cool, compliment_cute,
                compliment_funny, compliment_hot,
                compliment_list, compliment_note,
                compliment_more, compliment_plain,
                compliment_photos, compliment_profile,
                compliment_writer, fans, elite,
                cool,friends, funny, review_count,
                yelping_since
from udacity_project.ods.user;


CREATE TABLE dim_review (BIZ_ID STRING references dim_business(business_id), 
                         REVIEW_ID STRING primary key, 
                         DATE DATE, 
                         FUNNY INTEGER, 
                         COOL INTEGER, 
                         TEXT STRING, 
                         USEFUL INTEGER,
                         STARS INTEGER, 
                         USER_ID STRING);

insert into dim_review (BIZ_ID, REVIEW_ID,"DATE", FUNNY, COOL, TEXT,USEFUL, STARS, USER_ID)
select distinct biz_id,
       review_id,
       date,
       funny,
       cool,
       text,
       useful,
       stars,
       user_id
from udacity_project.ods.review;


CREATE TABLE dim_temperature ("DATE" DATE primary key, 
                              MIN_TEMP INTEGER, 
                              MAX_TEMP INTEGER, 
                              NORMAL_MIN_TEMP FLOAT, 
                              NORMAL_MAX_TEMP FLOAT);


insert into dim_temperature(date, min_temp, max_temp, normal_min_temp, normal_max_temp)
select distinct date,
                min_temp,
                max_temp,
                normal_min_temp,
                normal_max_temp
from udacity_project.ods.temperature;


CREATE TABLE dim_precipitation (
       "DATE" DATE primary key, 
        PRECIPITATION FLOAT, 
        PRECIPITATION_NORMAL FLOAT);

insert into dim_precipitation(date, precipitation, precipitation_normal)
select distinct date,
                precipitation,
                precipitation_normal
from udacity_project.ods.precipitation;


create table fact_restaurant_review("date" date primary key,
                                    business_id string references dim_business(business_id),
                                    review_id string references dim_review(review_id),
                                    user_id string references dim_user(user_id),
                                   address_id int references dim_address(address_id));
                                    
                                    
                                    
insert into fact_restaurant_review
SELECT r.date, r.biz_id, r.review_id, r.user_id, address_id
FROM UDACITY_PROJECT.ODS.review as r
JOIN UDACITY_PROJECT.ODS.user as u 
ON u.user_id = r.user_id
JOIN UDACITY_PROJECT.ODS.precipitation as p 
ON r.date = p.date
JOIN UDACITY_PROJECT.ODS.temperature as t 
ON r.date = t.date
JOIN UDACITY_PROJECT.ODS.business as b 
ON r.biz_id = b.business_id
JOIN UDACITY_PROJECT.DWH.DIM_ADDRESS as a
ON b.latitude = a.latitude AND b.longitude = a.longitude;



--query to report the data

select b.business_name as "Business Name", r.date as "Date",
       t.min_temp as "Minimum Temperature", t.max_temp as "Maximum Temperature",
       p.precipitation as "Precipitation", p.precipitation_normal as "Normal Precipitation",
       r.stars as "Review Stars"
from fact_restaurant_review as f
left join dim_business as b
on f.business_id = b.business_id
left join dim_review as r
on f.review_id = r.review_id
left join dim_temperature as t
on r.date = t.date
left join dim_precipitation as p
on r.date = p.date