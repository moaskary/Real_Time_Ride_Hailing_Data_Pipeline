-- Create a table to hold our final, real-time aggregated data
CREATE TABLE city_metrics (
    city VARCHAR(50) PRIMARY KEY,
    total_trips BIGINT,
    average_fare NUMERIC(10, 2),
    last_updated TIMESTAMP WITH TIME ZONE
);