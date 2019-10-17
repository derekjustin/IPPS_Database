-- Derek Holsapple
-- Justin Strelka


CREATE DATABASE ipps;

USE ipps;

-- We need to 3rd normalize the 12 csv columns and create tables
CREATE TABLE Providers (
    providerID INT NOT NULL PRIMARY KEY, 
    providerName VARCHAR(100) NOT NULL, 
    providerStreet VARCHAR(120) NOT NULL,
    providerCity VARCHAR(100) NOT NULL,
    providerState CHAR(2) NOT NULL,
    providerZip CHAR(5) NOT NULL,
    totalDischarges INT NOT NULL
     );

-- dRgKey
-- dRgDescription

-- referralRegionState 
-- hospitalReferralRegionDescription'


-- create users
CREATE USER IF NOT EXISTS 'ipps' IDENTIFIED BY '12345';

-- Give total access to ipps
GRANT ALL PRIVILEGES ON ipps.* TO 'ipps';