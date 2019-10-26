-- AUTHORS: Derek Holsapple, Justin Strelka
-- DATE: 10/26/2019
-- PROJECT: IPPS_Database

-- DESCRIPTION: MySQL database definition 
-- file for ipps dataset. 

-- CREATE DATABASE
CREATE DATABASE IF NOT EXISTS ipps;

-- SELECT DATABASE TO OPERATE ON
USE ipps;

-- CREATE hrr TABLE
CREATE TABLE IF NOT EXISTS hrr (
    referralRegionID INT PRIMARY KEY NOT NULL, 
    referralRegionState CHAR(2) NOT NULL,
    referralRegionDescription VARCHAR(100) NOT NULL,
    UNIQUE KEY (referralRegionID)
    );

-- CREATE providers TABLE
CREATE TABLE IF NOT EXISTS providers (
    providerID INT PRIMARY KEY NOT NULL, 
    providerName VARCHAR(100) NOT NULL, 
    providerStreetAddress VARCHAR(120) NOT NULL,
    providerCity VARCHAR(100) NOT NULL,
    providerState CHAR(2) NOT NULL,
    providerZipCode INT NOT NULL,
    referralRegionID INT NOT NULL,
    FOREIGN KEY (referralRegionID) REFERENCES hrr (referralRegionID),
    UNIQUE KEY (providerID)
    );

-- CREATE drg TABLE
CREATE TABLE IF NOT EXISTS drg (
    dRgKey INT PRIMARY KEY NOT NULL,
    dRgDescription VARCHAR(100) NOT NULL,
    UNIQUE KEY (dRgKey)
    );

-- CREATE proivercondcoverage TABLE
CREATE TABLE IF NOT EXISTS providercondcoverage (
    providerID INT NOT NULL, 
    dRgKey INT NOT NULL,
    totalDischarges INT NOT NULL,
    averageCoveredCharges DECIMAL(9,2) NOT NULL,
    averageTotalPayments DECIMAL(9,2) NOT NULL,
    averageMedicarePayments DECIMAL(9,2) NOT NULL,
    PRIMARY KEY (providerID,dRgKey),
    UNIQUE KEY (providerID,dRgKey)
    );

-- CREATE USERS
CREATE USER IF NOT EXISTS 'ipps' IDENTIFIED BY 'password';

-- GIVE ALL PRIVILEGES TO ipps USER
GRANT ALL PRIVILEGES ON ipps.* TO 'ipps';