SELECT
--  CASE
--    WHEN DAYOFWEEK(log_time) == 1 THEN "Sunday"
--    WHEN DAYOFWEEK(log_time) == 2 THEN "Monday"
--    WHEN DAYOFWEEK(log_time) == 3 THEN "Tuesday"
--    WHEN DAYOFWEEK(log_time) == 4 THEN "Wednesday"
--    WHEN DAYOFWEEK(log_time) == 5 THEN "Thursday"
--    WHEN DAYOFWEEK(log_time) == 6 THEN "Friday"
--    WHEN DAYOFWEEK(log_time) == 7 THEN "Saturday"
--    ELSE "Bananas" END as day_of_week,
  CASE 
    WHEN connection_spec.data_direction == 0 THEN "c2s"
    WHEN connection_spec.data_direction == 1 THEN "s2c"
    ELSE "Whoopload" END as label_direction,

    REGEXP_EXTRACT(task_filename, r'gs://.*-(mlab[1-4]-[a-z]{3}[0-9]+)-ndt.*.tgz') AS label_server,
    count(*) as value

FROM
    [measurement-lab:public.ndt]

WHERE
    log_time > TIMESTAMP("2017-10-06")


GROUP BY label_server, label_direction
ORDER BY value
-- GROUP BY day_of_week, server
-- ORDER BY day_of_week, count
