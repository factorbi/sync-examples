/*****************  TABLES  *****************/

CREATE TABLE IF NOT EXISTS customDates(
  id int not null auto_increment unique key,
  tag varchar(30) not null,
  cDate datetime,
  CONSTRAINT PRIMARY KEY(tag)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
-- INSERT INTO customDates (tag,cDate) VALUES ('syncDate','2017-03-29'), ('agingDate','2017-03-29'), ('serviceDate','2017-03-29');

CREATE TABLE IF NOT EXISTS dateInfo(
  id int not null auto_increment unique key,
  tag varchar(50) not null,
  cDate datetime,
  CONSTRAINT PRIMARY KEY(tag)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE IF NOT EXISTS ymInfo(
  id int not null auto_increment unique key,
  tag varchar(50) not null,
  y smallint,
  m smallint,
  CONSTRAINT PRIMARY KEY(tag)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE IF NOT EXISTS syncInfo(
  serviceID     varchar(36) NOT NULL,
  `timestamp`   datetime NULL,
  syncDate      date      NULL,
  timezone      varchar(64) NULL,
  CONSTRAINT PRIMARY KEY(serviceID)
) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
CALL sp_AlterTable('syncInfo', 'serviceDay', 'varchar(10)', 'NULL', 'today');
CALL sp_AlterTable('syncInfo', 'numDays', 'int', 'NULL', '5');
CALL sp_AlterTable('syncInfo', 'lastSyncId', 'int', 'NULL', '');
CALL sp_AlterTable('syncInfo', 'company', 'varchar(10)', 'NULL', '');
CALL sp_AlterTable('syncInfo', 'name', 'varchar(100)', 'NULL', '');
CALL sp_AlterTable('syncInfo', 'group1', 'varchar(100)', 'NULL', '');
CALL sp_AlterTable('syncInfo', 'mergedb', 'varchar(255)', 'NULL', '');
UPDATE syncInfo SET serviceDay = 'today' WHERE serviceDay IS NULL;
-- REPLACE INTO syncInfo (serviceID, timezone, serviceDay, numDays) VALUES ('a1bcd23e-4df5-67a8-bc9a-c0123def4567', 'America/Mexico_City', 'today', 30);

CREATE TABLE IF NOT EXISTS syncInfoStores(
  serviceID     varchar(36) NOT NULL,
  store         varchar(10) NOT NULL,
  `timestamp`   datetime  NULL,
  syncDate      date      NULL,
  timezone      varchar(64) DEFAULT 'America/Mexico_City',
  serviceDay    varchar(10) DEFAULT 'today',
  numDays       int DEFAULT 5,
  CONSTRAINT PRIMARY KEY(serviceID)
) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
CALL sp_AlterTable('syncInfoStores', 'name', 'varchar(100)', 'NULL', '');
CALL sp_AlterTable('syncInfoStores', 'group1', 'varchar(100)', 'NULL', '');
CALL sp_AlterTable('syncInfoStores', 'lastSyncId', 'int', 'NULL', '');
-- INSERT INTO syncInfoStores (serviceID, store, timezone) VALUES ('', '', 'America/Mexico_City');


/*****************  FUNCTIONS  *****************/
DELIMITER $$
DROP FUNCTION IF EXISTS `fnSyncDate`$$
CREATE FUNCTION `fnSyncDate`() RETURNS date
BEGIN
  DECLARE _syncDate, _customDate date;
  DECLARE _serviceID varchar(36);
  DECLARE _timezone varchar(64);

  SELECT IFNULL(cDate,'0000-00-00 00:00:00.0000') INTO _customDate FROM customDates WHERE tag = 'syncDate';
  SELECT serviceID, IFNULL(timezone,'America/Mexico_City') INTO _serviceID, _timezone FROM syncInfo LIMIT 1;
  SELECT CONVERT_TZ(load_timestamp,'UTC',_timezone) INTO _syncDate FROM `mysql`.aurora_s3_load_history WHERE file_name REGEXP _serviceID ORDER BY load_timestamp DESC LIMIT 1;

  IF _customDate <> '0000-00-00 00:00:00.0000' THEN
    SET _syncDate = _customDate;
  END IF;

  RETURN _syncDate;
END$$
DELIMITER ;

DELIMITER $$
DROP FUNCTION IF EXISTS `fnSyncDateTime`$$
CREATE FUNCTION `fnSyncDateTime`() RETURNS datetime
BEGIN
  DECLARE _syncDateTime datetime;
  DECLARE _serviceID varchar(36);
  DECLARE _timezone varchar(64);

  SELECT serviceID, IFNULL(timezone,'America/Mexico_City') INTO _serviceID, _timezone FROM syncInfo LIMIT 1;
  SELECT CONVERT_TZ(load_timestamp,'UTC',_timezone) INTO _syncDateTime FROM `mysql`.aurora_s3_load_history WHERE file_name REGEXP _serviceID ORDER BY load_timestamp DESC LIMIT 1;

  RETURN _syncDateTime;
END$$
DELIMITER ;