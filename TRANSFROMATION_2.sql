CREATE DATABASE DATA_TRANSFORMATION_2
USE DATA_TRANSFORMATION_2

CREATE TABLE RAW_FILE(
Model_Name VARCHAR(30),
Country VARCHAR(30),
Brand VARCHAR(30),
`Source` VARCHAR(30),
Rating INT,
Review_Date DATETIME, 
Feature_1 INT,
Feature_2 INT ,
Feature_3 INT ,
Feature_4 INT );

LOAD DATA INFILE "D:/FSDA/SQL Transformation Task/Raw file.csv"
INTO TABLE RAW_FILE
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
SET  
Feature_1 = CASE WHEN Feature_1 = '' THEN NULL ELSE Feature_1 END,
Feature_2 = CASE WHEN Feature_2 = '' THEN NULL ELSE Feature_2 END,
Feature_3 = CASE WHEN Feature_3 = '' THEN NULL ELSE Feature_3 END,
Feature_4 = CASE WHEN Feature_4 = '' THEN NULL ELSE Feature_4 END;


SELECT * FROM RAW_FILE


CREATE TABLE TRANSFORMATION_2(
Model_Name VARCHAR(30), 
Brand VARCHAR(30),
 `Source` VARCHAR(30), 
 Country VARCHAR(30), 
 Topic VARCHAR(30),
 Negative INT, 
 Neutral INT, 
 Positive INT);
 
 INSERT INTO TRANSFORMATION_2 (Model_Name, Brand, `Source`, Country, Topic, Negative, Neutral, Positive)
SELECT 
    Model_Name,
    Brand,
    `Source`,
    Country,
    'Feature_1' AS Topic,
    SUM(CASE WHEN Feature_1 < 0 THEN 1 ELSE 0 END) AS Negative,
    SUM(CASE WHEN Feature_1 = 0 THEN 1 ELSE 0 END) AS Neutral,
    SUM(CASE WHEN Feature_1 > 0 THEN 1 ELSE 0 END) AS Positive
FROM 
    RAW_FILE
GROUP BY
    Model_Name,
    Brand,
    `Source`,
    Country

UNION ALL
SELECT 
    Model_Name,
    Brand,
    `Source`,
    Country,
    'Feature_2' AS Topic,
    SUM(CASE WHEN Feature_2 < 0 THEN 1 ELSE 0 END) AS Negative,
    SUM(CASE WHEN Feature_2 = 0 THEN 1 ELSE 0 END) AS Neutral,
    SUM(CASE WHEN Feature_2 > 0 THEN 1 ELSE 0 END) AS Positive
FROM 
    RAW_FILE
GROUP BY
    Model_Name,
    Brand,
    `Source`,
    Country

UNION ALL
SELECT 
    Model_Name,
    Brand,
    `Source`,
    Country,
    'Feature_3' AS Topic,
    SUM(CASE WHEN Feature_3 < 0 THEN 1 ELSE 0 END) AS Negative,
    SUM(CASE WHEN Feature_3 = 0 THEN 1 ELSE 0 END) AS Neutral,
    SUM(CASE WHEN Feature_3 > 0 THEN 1 ELSE 0 END) AS Positive
FROM 
    RAW_FILE
GROUP BY
    Model_Name,
    Brand,
    `Source`,
    Country
    
UNION ALL
SELECT 
    Model_Name,
    Brand,
    `Source`,
    Country,
    'Feature_4' AS Topic,
    SUM(CASE WHEN Feature_4 < 0 THEN 1 ELSE 0 END) AS Negative,
    SUM(CASE WHEN Feature_4 = 0 THEN 1 ELSE 0 END) AS Neutral,
    SUM(CASE WHEN Feature_4 > 0 THEN 1 ELSE 0 END) AS Positive
FROM 
    RAW_FILE
GROUP BY
    Model_Name,
    Brand,
    `Source`,
    Country


SELECT * FROM TRANSFORMATION_2