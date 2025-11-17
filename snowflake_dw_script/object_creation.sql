create database SF_LANDING_DB;
show databases;
// Data loaded / cdc / scd between schema 1: qa1_test and schema 2 :qa2_test
create schema qa1_test;
create schema qa2_test;

// Source table in qa1_test
create or replace TABLE sf_landing_db.qa1_test.CUSTOMER (
	CUSTOMER_ID NUMBER(38,0) NOT NULL unique,
	FIRST_NAME VARCHAR(50),
	LAST_NAME VARCHAR(50),
	EMAIL VARCHAR(100),
	PHONE_NUMBER VARCHAR(20),
	ADDRESS VARCHAR(16777216),
	CITY VARCHAR(50),
	STATE VARCHAR(50),
	POSTAL_CODE VARCHAR(20),
	COUNTRY VARCHAR(50),
	ETL_RECORD_DELETED BOOLEAN DEFAULT FALSE,
	primary key (CUSTOMER_ID)
);

INSERT INTO sf_landing_db.qa1_test.CUSTOMER 
(CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, ADDRESS, CITY, STATE, POSTAL_CODE, COUNTRY, ETL_RECORD_DELETED)
VALUES
(1, 'Amit', 'Sharma', 'amit.sharma@example.com', '+91-9876543210',
 '123 MG Road', 'Bengaluru', 'Karnataka', '560001', 'India', FALSE),

(2, 'Emily', 'Clark', 'emily.clark@example.com', '+1-202-555-0147',
 '742 Evergreen Terrace', 'Springfield', 'Illinois', '62704', 'USA', FALSE),

(3, 'Raj', 'Verma', 'raj.verma@example.com', '+91-9988776655',
 '22 Park Street', 'Kolkata', 'West Bengal', '700016', 'India', FALSE),

(4, 'Sophia', 'Miller', 'sophia.miller@example.com', '+44-7700-900123',
 '18 Queen’s Way', 'London', 'London', 'SW1A 1AA', 'UK', FALSE),

(5, 'Kenji', 'Tanaka', 'kenji.tanaka@example.com', '+81-90-1234-5678',
 '5 Chome-2-1 Ginza', 'Tokyo', 'Tokyo', '104-0061', 'Japan', FALSE);


select * from sf_landing_db.qa1_test.CUSTOMER;
select * from SF_LANDING_DB.QA1_TEST.CUSTOMER_STREAM_TYPE1;
select * from sf_landing_db.qa2_test.CUSTOMER;

-- update insert
UPDATE sf_landing_db.qa1_test.CUSTOMER
SET PHONE_NUMBER = '+91-7000000001',
    ADDRESS = '456 New MG Road',
    ETL_RECORD_DELETED = 1
WHERE CUSTOMER_ID = 2;
UPDATE sf_landing_db.qa1_test.CUSTOMER
SET EMAIL = 'emily.new@example.com',
    CITY = 'Chicago'
WHERE CUSTOMER_ID = 2;
INSERT INTO sf_landing_db.qa1_test.CUSTOMER 
(CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, ADDRESS, CITY, STATE, POSTAL_CODE, COUNTRY, ETL_RECORD_DELETED)
VALUES
(6, 'Liam', 'Anderson', 'liam.anderson@example.com', '+1-310-555-2211',
 '100 Sunset Blvd', 'Los Angeles', 'California', '90001', 'USA', FALSE),

(7, 'Priya', 'Nair', 'priya.nair@example.com', '+91-9123456780',
 '12 Marine Drive', 'Mumbai', 'Maharashtra', '400020', 'India', FALSE),

(8, 'Carlos', 'Diaz', 'carlos.diaz@example.com', '+34-600-123-456',
 '89 La Rambla', 'Barcelona', 'Catalonia', '08002', 'Spain', FALSE);





// target table in qa2_test with etl columns
create or replace TABLE sf_landing_db.qa2_test.CUSTOMER (
	CUSTOMER_ID NUMBER(38,0) NOT NULL unique,
	FST_NAME VARCHAR(50),

    ETL_RECORD_STATUS_CD varchar(10),
    ETL_RECORD_STATUS_TIME TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);

// First time create stream on source table : not needed
-- create stream qa1_test.customer_STREAM_TYPE1 on table qa1_test.customer SHOW_INITIAL_ROWS =True;
-- show streams in schema qa1_test;
-- drop stream customer_STREAM_TYPE1;


//Create another table:
CREATE OR REPLACE TABLE qa1_test.NATION (
    N_NATIONKEY BIGINT PRIMARY KEY,
    N_NAME VARCHAR(25),
    N_REGIONKEY BIGINT,
    N_COMMENT VARCHAR(152),
    ETL_RECORD_DELETED BOOLEAN DEFAULT FALSE
);

CREATE TABLE qa2_test.NATION (
    N_NATIONKEY BIGINT PRIMARY KEY,
    N_NAME VARCHAR(25),
    N_REGIONKEY BIGINT,
    N_COMMENT VARCHAR(152)
);

desc table qa2_test.NATION;


// testing of insertion 
-- after insertion check if stream cpatured data
-- if stream cpatured the data , data loaded to target?

INSERT INTO qa1_test.NATION (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) VALUES
(1, 'India', 3, 'South Asian nation'),
(2, 'United States', 1, 'North American nation'),
(3, 'Germany', 2, 'Central European nation'),
(4, 'Japan', 4, 'Island country in East Asia'),
(5, 'Brazil', 5, 'Largest country in South America'),
(6, 'Australia', 4, 'Country and continent'),
(7, 'Canada', 1, 'Northernmost country in North America'),
(8, 'France', 2, 'Western European nation'),
(9, 'China', 4, 'Most populous country'),
(10, 'South Africa', 6, 'Southernmost African nation');

SELECT * from SF_LANDING_DB.QA1_TEST.NATION;

SHOW STREAMS;

select * from SF_LANDING_DB.QA1_TEST.NATION_STREAM_TYPE1;

SELECT * from SF_LANDING_DB.QA2_TEST.NATION;
drop table SF_LANDING_DB.QA2_TEST.CUSTOMER;


CREATE OR REPLACE TEMP TABLE SF_LANDING_DB.qa2_test.NATION_changes AS
SELECT
    src.N_NATIONKEY, src.N_REGIONKEY, src.N_COMMENT, src.N_NAME,
    tgt.ETL_RECORD_DELETED,
    CASE
        WHEN tgt.N_NATIONKEY IS NULL THEN 'INSERT'
        WHEN tgt.N_NATIONKEY IS NOT NULL AND (src.N_NATIONKEY != tgt.N_NATIONKEY OR src.N_REGIONKEY != tgt.N_REGIONKEY OR src.N_COMMENT != tgt.N_COMMENT OR src.N_NAME != tgt.N_NAME) THEN 'UPDATE'
        WHEN tgt.N_NATIONKEY IS NOT NULL AND src.N_NATIONKEY IS NULL THEN 'DELETE'
        ELSE 'UNCHANGED'
    END AS change_type
FROM SF_LANDING_DB.qa1_test.NATION src
FULL OUTER JOIN SF_LANDING_DB.qa2_test.NATION tgt
    ON src.N_NATIONKEY = tgt.N_NATIONKEY;


------------------------------------------------------------------------
-- 1. CUSTOMER (qa1_test)
------------------------------------------------------------------------

-- INSERTS
INSERT INTO sf_landing_db.qa1_test.CUSTOMER
(CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, ADDRESS, CITY, STATE, POSTAL_CODE, COUNTRY, ETL_RECORD_DELETED)
VALUES
(101, 'Ravi', 'Kumar', 'ravi.kumar@example.com', '+91-9998887771',
 '12 Residency Road', 'Bengaluru', 'Karnataka', '560025', 'India', FALSE),
(102, 'Laura', 'Smith', 'laura.smith@example.com', '+1-415-555-0101',
 '77 Castro St', 'San Francisco', 'California', '94114', 'USA', FALSE),
(103, 'Wei', 'Chen', 'wei.chen@example.com', '+86-555-443322',
 '88 Nanjing Road', 'Shanghai', 'Shanghai', '200000', 'China', FALSE);

-- UPDATE
UPDATE sf_landing_db.qa1_test.CUSTOMER
SET CITY = 'San Jose',
    PHONE_NUMBER = '+1-408-555-1100'
WHERE CUSTOMER_ID = 102;

-- HARD DELETE
DELETE FROM sf_landing_db.qa1_test.CUSTOMER
WHERE CUSTOMER_ID = 103;



------------------------------------------------------------------------
-- 2. PRODUCT (qa1_test)
------------------------------------------------------------------------

-- INSERTS
INSERT INTO sf_landing_db.qa1_test.PRODUCT
(PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY, PRODUCT_DESCRIPTION, PRICE, CURRENCY,
 STOCK_QUANTITY, SUPPLIER_NAME, SUPPLIER_CONTACT, COUNTRY_OF_ORIGIN, ETL_RECORD_DELETED)
VALUES
(201, 'Google Pixel 8', 'Electronics', 'Android smartphone from Google', 799.00, 'USD',
 250, 'Google LLC', 'support@google.com', 'USA', FALSE),
(202, 'HP Envy Laptop', 'Computers', 'Lightweight HP laptop with Intel CPU', 950.00, 'USD',
 120, 'HP Inc.', 'support@hp.com', 'China', FALSE),
(203, 'JBL Charge 5', 'Audio', 'Portable Bluetooth speaker', 150.00, 'USD',
 300, 'JBL', 'support@jbl.com', 'Vietnam', FALSE);

-- UPDATE
UPDATE sf_landing_db.qa1_test.PRODUCT
SET STOCK_QUANTITY = 500
WHERE PRODUCT_ID = 202;

-- HARD DELETE
DELETE FROM sf_landing_db.qa1_test.PRODUCT
WHERE PRODUCT_ID = 203;



------------------------------------------------------------------------
-- 3. ORDERS (qa1_test)
------------------------------------------------------------------------

-- INSERTS
INSERT INTO sf_landing_db.qa1_test.ORDERS
(ORDER_ID, CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, ETL_RECORD_DELETED)
VALUES
(3001, 101, '2024-12-20 09:30:00', 949.00, FALSE),
(3002, 102, '2024-12-21 15:10:00', 150.00, FALSE),
(3003, 101, '2024-12-22 11:55:00', 799.00, FALSE);

-- UPDATE
UPDATE sf_landing_db.qa1_test.ORDERS
SET TOTAL_AMOUNT = 999.00
WHERE ORDER_ID = 3001;

-- HARD DELETE
DELETE FROM sf_landing_db.qa1_test.ORDERS
WHERE ORDER_ID = 3003;



------------------------------------------------------------------------
-- 4. ORDER_ITEMS (qa1_test)
------------------------------------------------------------------------
select * from sf_landing_db.qa1_test.ORDER_ITEMS;
-- INSERTS
INSERT INTO sf_landing_db.qa1_test.ORDER_ITEMS
(ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE)
VALUES
(1234, 3011, 202, 1, 950.00),
(1235, 3011, 203, 1, 150.00),
(1236, 3012, 203, 1, 150.00),
(1237, 3013, 201, 1, 799.00);

select * from SF_LANDING_DB.QA1_TEST.ORDER_ITEMS_STREAM_TYPE1;

-- UPDATE
UPDATE sf_landing_db.qa1_test.ORDER_ITEMS
SET QUANTITY = 2000
WHERE ORDER_ITEM_ID = 4;

-- HARD DELETE
DELETE FROM sf_landing_db.qa1_test.ORDER_ITEMS
WHERE ORDER_ITEM_ID in (4001,4004);

select * from sf_landing_db.qa2_test.ORDER_ITEMS;




//createing target tables but without audit columns
show streams;
select * from SF_LANDING_DB.QA1_TEST.PRODUCT_STREAM_TYPE1;

//SAMPLE MERGE RUNNING 
-- MERGE INTO SF_LANDING_DB.qa2_test.PRODUCT tgt
--     USING SF_LANDING_DB.qa1_test.PRODUCT_STREAM_TYPE1 src
--     ON tgt.PRODUCT_ID = src.PRODUCT_ID

--     WHEN MATCHED AND src.metadata$action = 'DELETE' AND src.METADATA$ISUPDATE = 'FALSE' THEN        
--         DELETE

--     WHEN MATCHED AND src.metadata$action = 'INSERT' THEN
--         UPDATE SET
--             tgt.PRODUCT_DESCRIPTION = src.PRODUCT_DESCRIPTION, tgt.COUNTRY_OF_ORIGIN = src.COUNTRY_OF_ORIGIN, tgt.STOCK_QUANTITY = src.STOCK_QUANTITY, tgt.PRODUCT_CATEGORY = src.PRODUCT_CATEGORY, tgt.CURRENCY = src.CURRENCY, tgt.PRICE = src.PRICE, tgt.ETL_RECORD_DELETED = src.ETL_RECORD_DELETED, tgt.SUPPLIER_NAME = src.SUPPLIER_NAME, tgt.PRODUCT_ID = src.PRODUCT_ID, tgt.SUPPLIER_CONTACT = src.SUPPLIER_CONTACT, tgt.PRODUCT_NAME = src.PRODUCT_NAME,
--             tgt.ETL_RECORD_PROCESS_TIME = CURRENT_TIMESTAMP(),
--             tgt.ETL_RECORD_CAPTURE_TIME = CURRENT_TIMESTAMP(),
--             tgt.ETL_RECORD_STATUS_CD =
--             CASE
--                 WHEN src.ETL_RECORD_DELETED = 1 THEN 'D'
--                 WHEN src.ETL_RECORD_DELETED = 0 THEN 'A'
--                 ELSE tgt.ETL_RECORD_STATUS_CD
--             END

--     WHEN NOT MATCHED THEN
--         INSERT (PRODUCT_DESCRIPTION, COUNTRY_OF_ORIGIN, STOCK_QUANTITY, PRODUCT_CATEGORY, CURRENCY, PRICE, ETL_RECORD_DELETED, SUPPLIER_NAME, PRODUCT_ID, SUPPLIER_CONTACT, PRODUCT_NAME, ETL_RECORD_PROCESS_TIME, ETL_RECORD_CAPTURE_TIME, ETL_RECORD_STATUS_CD)
--         VALUES (src.PRODUCT_DESCRIPTION, src.COUNTRY_OF_ORIGIN, src.STOCK_QUANTITY, src.PRODUCT_CATEGORY, src.CURRENCY, src.PRICE, src.ETL_RECORD_DELETED, src.SUPPLIER_NAME, src.PRODUCT_ID, src.SUPPLIER_CONTACT, src.PRODUCT_NAME, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'A');

//checking data before cdc load
select * from SF_LANDING_DB.QA2_TEST.ORDERS;
select * from SF_LANDING_DB.QA2_TEST.ORDER_ITEMS;
select * from SF_LANDING_DB.QA2_TEST.PRODUCT;


