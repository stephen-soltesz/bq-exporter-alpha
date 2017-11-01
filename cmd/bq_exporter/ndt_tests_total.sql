SELECT
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
    TIMESTAMP_TO_USEC(log_time) > ((INTEGER(UNIX_START_TIME) / (REFRESH_RATE_SEC)) * (REFRESH_RATE_SEC) - (24 * 60 * 60)) * 1000000

GROUP BY label_machine, label_direction
ORDER BY value
