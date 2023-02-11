/****************** Создание таблиц *****************************/
--CREATE TABLE public."Trips"();
--ALTER TABLE IF EXISTS public."Trips" OWNER to postgres;

/*
ALTER TABLE IF EXISTS public."Trips"
	ADD COLUMN VendorID integer,
	ADD COLUMN Trep_pickup_datetime date,
	ADD COLUMN Trep_dropoff_datetime date,
	ADD COLUMN Passanger_count integer,
	ADD COLUMN Trip_distance real,
	ADD COLUMN Ratecodeid integer,
	ADD COLUMN Store_and_fwd_flag varchar(2),
	ADD COLUMN PulocationId integer,
	ADD COLUMN Dolocationid integer,
	ADD COLUMN Payment_type integer,
	ADD COLUMN Fare_amount real,
	ADD COLUMN Extra real,
	ADD COLUMN Mta_tax real,
	ADD COLUMN Tip_amount real,
	ADD COLUMN Tools_amount integer,
	ADD COLUMN Improvement_surchange real,
	ADD COLUMN Total_amount real,
	ADD COLUMN Congestion_surchange real
*/


--CREATE TABLE public."Results"();
--ALTER TABLE IF EXISTS public."Results" OWNER to postgres;

/*
ALTER TABLE IF EXISTS public."Results"
	ADD COLUMN "date" date,
	ADD COLUMN percentage_zero real,
	ADD COLUMN percentage_1p real,
	ADD COLUMN percentage_2p real,
	ADD COLUMN percentage_3p real,
	ADD COLUMN percentage_4p_plus real,
	ADD COLUMN mostexpensive_trip0 real,
	ADD COLUMN cheapest_trip0 real,
	ADD COLUMN mostexpensive_trip1 real,
	ADD COLUMN cheapest_trip1 real,
	ADD COLUMN mostexpensive_trip2 real,
	ADD COLUMN cheapest_trip2 real,
	ADD COLUMN mostexpensive_trip3 real,
	ADD COLUMN cheapest_trip3 real,
	ADD COLUMN mostexpensive_trip4plus real,
	ADD COLUMN cheapest_trip4plus real

*/


--CREATE TABLE public."Analisys"();
--ALTER TABLE IF EXISTS public."Analisys" OWNER to postgres;

/*
ALTER TABLE IF EXISTS public."Analisys"
	ADD COLUMN avgtd double precision,
	ADD COLUMN Passanger_count integer,
	ADD COLUMN maxta double real
*/


/****************** Импорт данных *****************************/
/*
COPY public."Trips"
FROM 'E:\Downloads\yellow_tripdata_2020-01.csv'
DELIMITER ','
CSV HEADER;
*/

--SELECT * FROM public."Trips"
-- 6405008 rows



/****************** Обработка данных *****************************/
/* Необходимо, используя таблицу поездок для каждого дня рассчитать процент поездок по количеству человек в машине 
(без пассажиров, 1, 2,3,4 и более пассажиров). По итогу должна получиться таблица (parquet) с колонками date, percentage_zero, 
percentage_1p, percentage_2p, percentage_3p, percentage_4p_plus. Технологический стек – sql,scala (что-то одно).
Также добавить столбцы к предыдущим результатам с самой дорогой и самой дешевой поездкой для каждой группы*/

WITH 
t1 AS (
    SELECT * FROM public."Trips" WHERE (("total_amount" >= 0) AND (passanger_count IS NOT NULL))
),
t2 AS (SELECT DISTINCT Trep_dropoff_datetime AS "date",
	   --count(Trep_dropoff_datetime) OVER(PARTITION BY Trep_dropoff_datetime), 
	   round( (((count(passanger_count) FILTER (WHERE passanger_count IN (0))*100)/
	   (count(Trep_dropoff_datetime) OVER(PARTITION BY Trep_dropoff_datetime)))) ,2)::real AS percentage_zero,
	    round( (((count(passanger_count) FILTER (WHERE passanger_count IN (1))*100)/
	   (count(Trep_dropoff_datetime) OVER(PARTITION BY Trep_dropoff_datetime)))) ,2)::real AS percentage_1p, 
	    round( (((count(passanger_count) FILTER (WHERE passanger_count IN (2))*100)/
	   (count(Trep_dropoff_datetime) OVER(PARTITION BY Trep_dropoff_datetime)))) ,2)::real AS percentage_2p,
	     round( (((count(passanger_count) FILTER (WHERE passanger_count IN (3))*100)/
	   (count(Trep_dropoff_datetime) OVER(PARTITION BY Trep_dropoff_datetime)))) ,2)::real AS percentage_3p,
	    round( (((count(passanger_count) FILTER (WHERE passanger_count >= 4)*100)/
	   (count(Trep_dropoff_datetime) OVER(PARTITION BY Trep_dropoff_datetime)))) ,2)::real AS percentage_4p_plus,
	   --count(passanger_count) FILTER (WHERE passanger_count IN (0)) OVER(PARTITION BY Trep_dropoff_datetime),
	   --CAST (((count(Trep_dropoff_datetime)/count(passanger_count))*100) AS Real) AS p
	   coalesce(max(Total_amount) FILTER (WHERE passanger_count IN (0)), 0) AS mostexpensive_trip0,
	   coalesce(min(Total_amount) FILTER (WHERE passanger_count IN (0)), 0) AS cheapest_trip0,
	   coalesce(max(Total_amount) FILTER (WHERE passanger_count IN (1)), 0) AS mostexpensive_trip1,
	   coalesce(min(Total_amount) FILTER (WHERE passanger_count IN (1)), 0) AS cheapest_trip1,
	   coalesce(max(Total_amount) FILTER (WHERE passanger_count IN (2)), 0) AS mostexpensive_trip2,
	   coalesce(min(Total_amount) FILTER (WHERE passanger_count IN (2)), 0) AS cheapest_trip2,
	   coalesce(max(Total_amount) FILTER (WHERE passanger_count IN (3)), 0) AS mostexpensive_trip3,
	   coalesce(min(Total_amount) FILTER (WHERE passanger_count IN (3)), 0) AS cheapest_trip3,
	   coalesce(max(Total_amount) FILTER (WHERE passanger_count >= 4  ), 0) AS mostexpensive_trip4plus,
	   coalesce(min(Total_amount) FILTER (WHERE passanger_count >= 4  ), 0) AS cheapest_trip4plus
	 FROM t1
GROUP BY Trep_dropoff_datetime, passanger_count HAVING passanger_count IN (0,1,2,3) OR passanger_count >= 4 --count(passanger_count) = 1
	  )
	  INSERT INTO public."Results"
SELECT * FROM t2;
--SELECT * FROM public."Results";



/****************** Обработка данных *****************************/
/* Провести аналитику на тему “как пройденное расстояние (Trip_distance) и количество пассажиров (Passanger_count) 
влияет на чаевые (Tip_amount)” в любом удобном инструменте.*/

WITH t1 AS (
    SELECT * FROM public."Trips" WHERE (("total_amount" >= 0) AND (passanger_count IS NOT NULL))
),
t2 AS (
	SELECT avg(Trip_distance) AS avgtd, Passanger_count, max(Tip_amount) AS maxta
	FROM t1
GROUP BY Passanger_count HAVING passanger_count IN (1,2,3) OR passanger_count >= 4
)
INSERT INTO public."Analisys"
SELECT * FROM t2 ORDER BY maxta DESC;
--SELECT * FROM public."Analisys";


/****************** Экспорт данных для послед. анализа *****************************/
--COPY public."Analisys" TO 'E:\Analisys.csv' DELIMITER ',' CSV HEADER;
--COPY public."Results" TO 'E:\Results.csv' DELIMITER ',' CSV HEADER;