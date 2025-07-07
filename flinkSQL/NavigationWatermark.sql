
CREATE TABLE KafkaBoatData3 (
  asset_id STRING,
  `timestamp` STRING,
  source STRING,
  heading_value DOUBLE,
  gnss_position_altitude DOUBLE,
  gnss_position_latitude DOUBLE,
  gnss_position_longitude DOUBLE,
  speed_over_ground_value DOUBLE,
  course_over_ground_value DOUBLE,
  true_wind_direction_value DOUBLE,
  true_wind_speed_value DOUBLE,
  `time` TIMESTAMP_LTZ(3),
  tstamp STRING,
  boat STRING,
  TWA DOUBLE,
  TWAvalue DOUBLE,
  race_course_course_axis DOUBLE,
  CWA DOUBLE,
  CWAvalue DOUBLE,
  VMG DOUBLE,
  VMC DOUBLE,
  X DOUBLE,
  Y DOUBLE,
  Yrolling STRING,
  duration DOUBLE,
  time_to_prev DOUBLE,
  distance_to_prev DOUBLE,
  speed_over_ground_calculated DOUBLE,
  status STRING,
  foiling STRING,
  direction STRING,
  bord STRING,
  class STRING,
  turn STRING,
  tack STRING,
  gybe STRING,
  maneuver STRING,
  distance_to_start DOUBLE,
  distance_derivative DOUBLE,
  time_to_tack DOUBLE,
  time_to_gybe DOUBLE,
  time_to_turn DOUBLE,
  cumulative_VMG DOUBLE,
  leg STRING,
  Ydist DOUBLE,
  race_number STRING,
  opponent STRING,
  processing_time AS PROCTIME(),
  event_time AS TO_TIMESTAMP(`timestamp`),  -- genera il campo di tipo timestamp da stringa
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'boat_data',
  'properties.bootstrap.servers' = 'kafka:19091',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'ISO-8601',
  'properties.group.id' = 'job3'  -- Use a unique group ID for this job
);


CREATE TABLE Navigation (
  boat STRING,
  `timestamp` TIMESTAMP(3),
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_speed DOUBLE,
  avg_vmg DOUBLE,
  avg_heading DOUBLE,
  avg_true_wind_direction DOUBLE,
  avg_true_wind_speed DOUBLE,
  is_foiling BIGINT,
  leg BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'boat_data_navigation',
  'properties.bootstrap.servers' = 'kafka:19091',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'sink.transactional-id-prefix' = 'navigation-sink'
);

INSERT INTO Navigation
SELECT
  boat,
  MAX(event_time) AS `timestamp`,
  TUMBLE_START(event_time, INTERVAL '1' SECOND) AS window_start,
  TUMBLE_END(event_time, INTERVAL '1' SECOND) AS window_end,
  AVG(VMC) AS avg_speed,
  AVG(VMG) AS avg_vmg,
  AVG(heading_value) AS avg_heading,
  AVG(true_wind_direction_value) AS avg_true_wind_direction,
  AVG(true_wind_speed_value) AS avg_true_wind_speed,
  MAX(
    CASE
      WHEN LOWER(foiling) = 'foiling' THEN 1
      ELSE 0
    END
  ) AS is_foiling,
  MAX(
    CASE leg
      WHEN 'prestart' THEN 0
      WHEN 'up1' THEN 1
      WHEN 'down1' THEN 2
      WHEN 'up2' THEN 3
      WHEN 'down2' THEN 4
      WHEN 'up3' THEN 5
      WHEN 'down3' THEN 6
      WHEN 'up4' THEN 7
      WHEN 'down4' THEN 8
      WHEN 'up5' THEN 9
      WHEN 'postrace' THEN 10
      ELSE -1
    END
  ) AS leg_number
FROM KafkaBoatData3
WHERE event_time IS NOT NULL
GROUP BY
  boat,
  TUMBLE(event_time, INTERVAL '1' SECOND);

