CREATE DATABASE DATA_TRANSFORMATION 
USE DATA_TRANSFORMATION

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

CREATE TABLE TRANSFORMATION_1(
    Model_Name VARCHAR(30),
    Country VARCHAR(30),
    Brand VARCHAR(30),
    `Source` VARCHAR(30),
    Feature1_Negative_Total INT,
    Feature1_Neutral_Total INT,
    Feature1_Positive_Total INT,
    Feature2_Negative_Total INT,
    Feature2_Neutral_Total INT,
    Feature2_Positive_Total INT,
    Feature3_Negative_Total INT,
    Feature3_Neutral_Total INT,
    Feature3_Positive_Total INT,
    Feature4_Negative_Total INT,
    Feature4_Neutral_Total INT,
    Feature4_Positive_Total INT
);

INSERT INTO TRANSFORMATION_1 (
    Model_Name,
    Country,
    Brand,
    `Source`,
    Feature1_Negative_Total,
    Feature1_Neutral_Total,
    Feature1_Positive_Total,
    Feature2_Negative_Total,
    Feature2_Neutral_Total,
    Feature2_Positive_Total,
    Feature3_Negative_Total,
    Feature3_Neutral_Total,
    Feature3_Positive_Total,
    Feature4_Negative_Total,
    Feature4_Neutral_Total,
    Feature4_Positive_Total
)
SELECT
    Model_Name,
    Country,
    Brand,
    `Source`,
    SUM(CASE WHEN Feature_1 < 0 THEN 1 ELSE 0 END) AS Feature1_Negative_Total,
    SUM(CASE WHEN Feature_1 = 0 THEN 1 ELSE 0 END) AS Feature1_Neutral_Total,
    SUM(CASE WHEN Feature_1 > 0 THEN 1 ELSE 0 END) AS Feature1_Positive_Total,
    SUM(CASE WHEN Feature_2 < 0 THEN 1 ELSE 0 END) AS Feature2_Negative_Total,
    SUM(CASE WHEN Feature_2 = 0 THEN 1 ELSE 0 END) AS Feature2_Neutral_Total,
    SUM(CASE WHEN Feature_2 > 0 THEN 1 ELSE 0 END) AS Feature2_Positive_Total,
    SUM(CASE WHEN Feature_3 < 0 THEN 1 ELSE 0 END) AS Feature3_Negative_Total,
    SUM(CASE WHEN Feature_3 = 0 THEN 1 ELSE 0 END) AS Feature3_Neutral_Total,
    SUM(CASE WHEN Feature_3 > 0 THEN 1 ELSE 0 END) AS Feature3_Positive_Total,
    SUM(CASE WHEN Feature_4 < 0 THEN 1 ELSE 0 END) AS Feature4_Negative_Total,
    SUM(CASE WHEN Feature_4 = 0 THEN 1 ELSE 0 END) AS Feature4_Neutral_Total,
    SUM(CASE WHEN Feature_4 > 0 THEN 1 ELSE 0 END) AS Feature4_Positive_Total
FROM
    RAW_FILE
GROUP BY
    Model_Name,
    Country,
    Brand,
    `Source`
ORDER BY Model_Name ;

SELECT * FROM TRANSFORMATION_1;

