-- Create stream dari topic device-sensors
CREATE STREAM device_readings (
    arrival_time BIGINT,
    creation_time BIGINT,
    device VARCHAR,
    index INT,
    model VARCHAR,
    user VARCHAR,
    gt VARCHAR,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
) WITH (
    KAFKA_TOPIC = 'device-sensors',
    VALUE_FORMAT = 'JSON',
    KEY_FORMAT = 'KAFKA'
);

-- Create table untuk tracking activity counts per device
CREATE TABLE device_activity_counts AS
SELECT
    CONCAT(device, '-', gt) AS device_activity_key,
    COUNT(*) AS activity_count,
    LATEST_BY_OFFSET(model) AS model,
    LATEST_BY_OFFSET(user) AS user
FROM device_readings
GROUP BY CONCAT(device, '-', gt)
EMIT CHANGES;

-- Calculate aggregated sensor stats
CREATE TABLE device_sensor_stats AS
SELECT
    device,
    AVG(x) AS avg_x,
    AVG(y) AS avg_y,
    AVG(z) AS avg_z,
    COUNT(*) AS reading_count,
    LATEST_BY_OFFSET(model) AS model,
    MAX(ABS(x)) AS max_movement_x,
    MAX(ABS(y)) AS max_movement_y,
    MAX(ABS(z)) AS max_movement_z
FROM device_readings
GROUP BY device
EMIT CHANGES;

-- Detect significant movement events (sebagai stream baru)
CREATE STREAM movement_alerts AS
SELECT
    device,
    model,
    gt,
    x, y, z,
    SQRT(x*x + y*y + z*z) AS movement_magnitude,
    arrival_time
FROM device_readings
WHERE SQRT(x*x + y*y + z*z) > 0.05
EMIT CHANGES;

-- Track activity transitions (sebagai stream baru)
CREATE STREAM activity_changes AS
SELECT 
    device,
    model,
    gt AS current_activity,
    LAG(gt, 1) OVER (PARTITION BY device ORDER BY arrival_time) AS previous_activity,
    arrival_time
FROM device_readings
EMIT CHANGES;