-- Create a new user for managing food delivery data
CREATE USER food_delivery_user WITH PASSWORD 'SuperSecurePwdHere' CREATEDB;

-- Create a new database for food_delivery_db
CREATE DATABASE food_delivery_db
    WITH 
    OWNER = food_delivery_user
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

-- Create a new user for managing customer data
CREATE USER customer_data_user WITH PASSWORD 'SuperSecurePwdHere' CREATEDB;

-- Create a new database for customer data
CREATE DATABASE customer_data_db
    WITH 
    OWNER = customer_data_user
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;


