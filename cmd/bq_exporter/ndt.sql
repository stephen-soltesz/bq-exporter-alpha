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
    ELSE "error"
    END as label_direction,

    CONCAT(
        REPLACE(
            REGEXP_EXTRACT(task_filename,
                           r'gs://.*-(mlab[1-4]-[a-z]{3}[0-9]+)-ndt.*.tgz'),
            "-",
            "."),
        ".measurement-lab.org") AS label_machine,

    count(*) as value

FROM
    [measurement-lab:public.ndt]

WHERE
    TIMESTAMP_TO_USEC(log_time) > UTC_USEC_TO_HOUR(TIMESTAMP_TO_USEC(CURRENT_TIMESTAMP())) - (24 * 60 * 60 * 1000000)
AND TIMESTAMP_TO_USEC(log_time) < UTC_USEC_TO_HOUR(TIMESTAMP_TO_USEC(CURRENT_TIMESTAMP())) - (23 * 60 * 60 * 1000000)

GROUP BY label_machine, label_direction
ORDER BY value
