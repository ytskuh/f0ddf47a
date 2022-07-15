DROP DATABASE IF EXISTS biandb;
CREATE DATABASE biandb;
DROP USER IF EXISTS 'bdadmin'@'localhost';
FLUSH PRIVILEGES;
CREATE USER 'bdadmin'@'localhost' IDENTIFIED BY '91f3i';
GRANT ALL ON biandb.* TO 'bdadmin'@'localhost';
