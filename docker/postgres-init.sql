-- Creates the application database and user
-- Airflow uses the default `airflow` database (created by the image)
-- Our platform data goes into `ai_platform`

CREATE USER platform WITH PASSWORD 'platform123';
CREATE DATABASE ai_platform OWNER platform;
GRANT ALL PRIVILEGES ON DATABASE ai_platform TO platform;
