--Derek Holsapple
--Justin Strelka


CREATE DATABASE ipps;

USE ipps;

--We need to 3rd normalize the 12 csv columns and create tables
CREATE TABLE DataStuff (
 --   id INT(3) NOT NULL PRIMARY KEY, 
 --   name VARCHAR(35) NOT NULL, 
 --   sal INT(6) );

CREATE TABLE After3rdNormal (
 --   id INT(3) NOT NULL PRIMARY KEY, 
 --   name VARCHAR(35) NOT NULL, 
 --   sal INT(6) ); 

--create users
CREATE USER 'ipps' IDENTIFIED BY '12345';

--Give total access to ipps
GRANT ALL ON TABLE Employees TO 'ipps';