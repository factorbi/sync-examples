/*****************  TABLES  *****************/

-- DROP TABLE IF EXISTS businessDay;
CREATE TABLE IF NOT EXISTS businessDay(
  id int not null auto_increment unique key,
  wd smallint not null,
  business bit not null,
  CONSTRAINT PRIMARY KEY(wd)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

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

CREATE TABLE IF NOT EXISTS eqtable(
  id              int NOT NULL AUTO_INCREMENT UNIQUE KEY,
  `type`            varchar(20) NOT NULL,
  `code`            varchar(255) NOT NULL,
  eq1             varchar(255),
  eq2             varchar(255),
  order1           int,
  lastchange      timestamp DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT PRIMARY KEY(`type`, `code`)
) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
CALL sp_AlterTable('eqtable', 'applied', 'tinyint','','0');

/*****************  TABLE DATA  *****************/

DELIMITER $$
DROP PROCEDURE IF EXISTS `spFactorySettings`$$
CREATE PROCEDURE spFactorySettings()
BEGIN

  IF NOT EXISTS(SELECT * FROM businessDay) THEN
    INSERT INTO businessDay (wd, business) VALUES (1,1),(2,1),(3,1),(4,1),(5,1),(6,0),(7,0);
  END IF;

END$$
DELIMITER ;

call spFactorySettings;

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

DELIMITER $$
DROP FUNCTION IF EXISTS `fnServiceDate`$$
CREATE FUNCTION `fnServiceDate`() RETURNS date
BEGIN
  DECLARE _syncDate, _customDate, _serviceDate date;
  DECLARE _serviceDay varchar(10);
  DECLARE _dayAdd int;

  SELECT IFNULL(syncDate,'0000-00-00 00:00:00.0000'), IFNULL(NULLIF(serviceDay,''),'tomorrow')
    INTO _syncDate, _serviceDay
    FROM syncInfo LIMIT 1;

  SELECT IFNULL(cDate,'0000-00-00 00:00:00.0000')
    INTO _customDate
    FROM customDates
    WHERE tag = 'serviceDate';

  SELECT
    CASE WHEN _serviceDay = 'tomorrow' THEN 1
         WHEN 'today' THEN 0
         ELSE 0
    END INTO _dayAdd;

  SELECT date_add(_syncDate, INTERVAL _dayAdd day) INTO _serviceDate;

  IF _customDate <> '0000-00-00 00:00:00.0000' THEN
    SET _serviceDate = _customDate;
  END IF;

  RETURN _serviceDate;
END$$
DELIMITER ;

DELIMITER $$
DROP FUNCTION IF EXISTS fnDateInfo$$
CREATE FUNCTION `fnDateInfo`(_tag varchar(20), _maxFecha datetime) RETURNS date
BEGIN
  DECLARE _workDay, _syncDate, _dayEval date;

  SELECT IFNULL(_maxFecha,'0000-00-00 00:00:00.0000') INTO _maxFecha;

  IF _maxFecha = '0000-00-00 00:00:00.0000' THEN
    SELECT IFNULL(syncDate,'0000-00-00 00:00:00.0000') INTO _syncDate FROM syncInfo LIMIT 1;
    SELECT _syncDate INTO _dayEval;
  ELSE
    SET _dayEval = _maxFecha;
  END IF;

  IF _tag IN ('ayer','anteayer') AND _dayEval <> '0000-00-00 00:00:00.0000' THEN
    SELECT date_add(_dayEval, INTERVAL -1 day) INTO _dayEval;
    ayer: WHILE(1=1) DO

      SELECT t INTO _workDay
      FROM `time` JOIN businessDay b ON `time`.wd = b.wd
      WHERE t = _dayEval AND b.business = 1;

      IF _workDay IS NULL THEN
        SELECT date_add(_dayEval, INTERVAL -1 day) INTO _dayEval;
      ELSE
        LEAVE ayer;
      END IF;

    END WHILE;
  END IF;

  IF _tag = 'anteayer' AND _dayEval <> '0000-00-00 00:00:00.0000' THEN
    SELECT date_add(_workDay, INTERVAL -1 day) INTO _dayEval;
    SET _workDay = NULL;
    anteayer: WHILE(1=1) DO

      SELECT t INTO _workDay
      FROM `time` JOIN businessDay b ON `time`.wd = b.wd
      WHERE t = _dayEval AND b.business = 1;

      IF _workDay IS NULL THEN
        SELECT date_add(_dayEval, INTERVAL -1 day) INTO _dayEval;
      ELSE
        LEAVE anteayer;
      END IF;

    END WHILE;
  END IF;

  RETURN _workDay;
END$$
DELIMITER ;