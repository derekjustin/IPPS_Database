--Derek Holsapple
--Justin Strelka


CREATE DATABASE ipps;

USE ipps;

--We need to 3rd normalize the 12 csv columns and create tables
CREATE TABLE Providers (
    providerID INT NOT NULL PRIMARY KEY, 
    providerName VARCHAR(100) NOT NULL, 
    providerStreet VARCHAR(120),
    providerCity VARCHAR(100),
    providerState CHAR(2),
    providerZip CHAR(5),
    referralState CHAR(2),
    referralCity VARCHAR(100),
    totalDischarges INT
     );

CREATE TABLE After3rdNormal (
 --   id INT(3) NOT NULL PRIMARY KEY, 
 --   name VARCHAR(35) NOT NULL, 
 --   sal INT(6) ); 

--create users
CREATE USER 'ipps' IDENTIFIED BY '12345';

--Give total access to ipps
GRANT ALL ON TABLE Employees TO 'ipps';