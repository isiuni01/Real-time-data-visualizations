
CREATE TABLE KafkaBoatData2 (
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
  processing_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'boat_data',
  'properties.bootstrap.servers' = 'kafka:19091',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'ISO-8601',
  'properties.group.id' = 'job2'
);


CREATE TABLE BoatRaceData (
  boat STRING,
  race_number STRING,
  opponent STRING,
  `timestamp` TIMESTAMP_LTZ(3),
  window_start TIMESTAMP_LTZ(3),
  window_end TIMESTAMP_LTZ(3),
  -- Performance metrics
  avg_speed DOUBLE,
  avg_vmg DOUBLE,
  avg_vmc DOUBLE,
  avg_heading DOUBLE,
  -- Wind data
  avg_true_wind_direction DOUBLE,
  avg_true_wind_speed DOUBLE,
  avg_twa DOUBLE,
  avg_cwa DOUBLE,
  -- Position data for map visualization
  latitude DOUBLE,
  longitude DOUBLE,
  avg_latitude DOUBLE,
  avg_longitude DOUBLE,
  -- Race status
  is_foiling BIGINT,
  leg_number BIGINT,
  leg_name STRING,
  -- Distance and positioning
  distance_to_start DOUBLE,
  distance_covered DOUBLE,
  -- Tactical info
  direction STRING,
  bord STRING,
  maneuver STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'boat_race_data',
  'properties.bootstrap.servers' = 'kafka:19091',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'sink.transactional-id-prefix' = 'boat-race-data-sink'
);

INSERT INTO BoatRaceData
SELECT
  -- Boat and race identification
  boat,
  race_number,
  opponent,
  
  -- Timestamp data
  MAX(`time`) AS `timestamp`,
  TUMBLE_START(processing_time, INTERVAL '1' SECOND) AS window_start,
  TUMBLE_END(processing_time, INTERVAL '1' SECOND) AS window_end,
  
  -- Performance metrics
  AVG(speed_over_ground_value) AS avg_speed,
  AVG(VMG) AS avg_vmg,
  AVG(VMC) AS avg_vmc,
  AVG(heading_value) AS avg_heading,
  
  -- Wind data
  AVG(true_wind_direction_value) AS avg_true_wind_direction,
  AVG(true_wind_speed_value) AS avg_true_wind_speed,
  AVG(COALESCE(TWAvalue, TWA)) AS avg_twa,
  AVG(COALESCE(CWAvalue, CWA)) AS avg_cwa,
  
  -- Position data for map visualization (latest and average)
  LAST_VALUE(gnss_position_latitude) AS latitude,
  LAST_VALUE(gnss_position_longitude) AS longitude,
  AVG(gnss_position_latitude) AS avg_latitude,
  AVG(gnss_position_longitude) AS avg_longitude,
  
  -- Race status
  MAX(
    CASE
      WHEN LOWER(foiling) = 'foiling' THEN 1
      ELSE 0
    END
  ) AS is_foiling,
  
  -- Leg information
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
  ) AS leg_number,
  
  LAST_VALUE(leg) AS leg_name,
  
  -- Distance and positioning
  LAST_VALUE(distance_to_start) AS distance_to_start,
  SUM(COALESCE(distance_to_prev, 0)) AS distance_covered,
  
  -- Tactical information
  LAST_VALUE(direction) AS direction,
  LAST_VALUE(bord) AS bord,
  LAST_VALUE(maneuver) AS maneuver
  
FROM KafkaBoatData
WHERE 
  -- Filter only boats that are in an active race
  race_number IS NOT NULL 
  AND opponent IS NOT NULL
  AND boat IS NOT NULL
  -- Filter out invalid coordinates
  AND gnss_position_latitude IS NOT NULL 
  AND gnss_position_longitude IS NOT NULL
  AND gnss_position_latitude BETWEEN -90 AND 90
  AND gnss_position_longitude BETWEEN -180 AND 180
GROUP BY
  boat,
  race_number,
  opponent,
  TUMBLE(processing_time, INTERVAL '1' SECOND);

