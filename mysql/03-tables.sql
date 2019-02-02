/*****************  TABLES  *****************/

CREATE TABLE IF NOT EXISTS businessDay(
  id int NOT NULL AUTO_INCREMENT UNIQUE KEY,
  wd smallint NOT NULL,
  business bit NOT NULL,
  CONSTRAINT PRIMARY KEY(wd)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE IF NOT EXISTS customDates(
  id int NOT NULL AUTO_INCREMENT UNIQUE KEY,
  tag varchar(30) NOT NULL,
  cDate datetime,
  CONSTRAINT PRIMARY KEY(tag)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
-- INSERT INTO customDates (tag,cDate) VALUES ('syncDate','2017-03-29'), ('agingDate','2017-03-29'), ('serviceDate','2017-03-29');

CREATE TABLE IF NOT EXISTS dateInfo(
  id int NOT NULL AUTO_INCREMENT UNIQUE KEY,
  tag varchar(50) NOT NULL,
  cDate datetime,
  CONSTRAINT PRIMARY KEY(tag)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE IF NOT EXISTS ymInfo(
  id int NOT NULL AUTO_INCREMENT UNIQUE KEY,
  tag varchar(50) NOT NULL,
  y smallint,
  m smallint,
  CONSTRAINT PRIMARY KEY(tag)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- DROP TABLE IF EXISTS syncInfo;
CREATE TABLE IF NOT EXISTS syncInfo(
  serviceID     varchar(36) NOT NULL,
  `timestamp`   datetime NULL,
  syncDate      date      NULL,
  timezone      varchar(64) NULL,
  serviceDay    varchar(10) DEFAULT 'today',
  numDays       int NULL,
  lastSyncId    int NULL,
  company       varchar(10) NULL,
  name          varchar(100) NULL,
  group1        varchar(100) NULL,
  mergedb       varchar(255) NULL,
  CONSTRAINT PRIMARY KEY(serviceID)
) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
-- REPLACE INTO syncInfo (serviceID, timezone, serviceDay) VALUES ('your-service-numer-bc9a-c0123def4567', 'US/Eastern', 'today');

-- DROP TABLE IF EXISTS syncInfoStores;
CREATE TABLE IF NOT EXISTS syncInfoStores(
  serviceID     varchar(36) NOT NULL,
  store         varchar(10) NOT NULL,
  `timestamp`   datetime  NULL,
  syncDate      date      NULL,
  timezone      varchar(64),
  serviceDay    varchar(10) DEFAULT 'today',
  numDays       int NULL,
  name          varchar(100) NULL,
  group1        varchar(100) NULL,
  lastSyncId    int NULL,
  CONSTRAINT PRIMARY KEY(serviceID)
) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- DROP TABLE IF EXISTS logPostInitial;
CREATE TABLE IF NOT EXISTS logPostInitial(
  id          int NOT NULL AUTO_INCREMENT UNIQUE KEY,
  message     varchar(100) NULL,
  `datetime`  timestamp DEFAULT CURRENT_TIMESTAMP,
  serviceID   varchar(36) NULL,
  syncID      int NULL,
  CONSTRAINT PRIMARY KEY(id)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- DROP TABLE IF EXISTS logPostFinal;
CREATE TABLE IF NOT EXISTS logPostFinal(
  id          int NOT NULL AUTO_INCREMENT UNIQUE KEY,
  message     varchar(100) NULL,
  `datetime`  timestamp DEFAULT CURRENT_TIMESTAMP,
  serviceID   varchar(36) NULL,
  syncID      int NULL,
  CONSTRAINT PRIMARY KEY(id)
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE IF NOT EXISTS eqtable(
  id              int NOT NULL AUTO_INCREMENT UNIQUE KEY,
  `type`            varchar(20) NOT NULL,
  `code`            varchar(255) NOT NULL,
  eq1             varchar(255),
  eq2             varchar(255),
  order1           int,
  lastchange      timestamp DEFAULT CURRENT_TIMESTAMP,
  applied         tinyint DEFAULT 0,
  CONSTRAINT PRIMARY KEY(`type`, `code`)
) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

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
  SELECT serviceID, IFNULL(timezone,'US/Eastern') INTO _serviceID, _timezone FROM syncInfo LIMIT 1;
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

  SELECT serviceID, IFNULL(timezone,'US/Eastern') INTO _serviceID, _timezone FROM syncInfo LIMIT 1;
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
DROP FUNCTION IF EXISTS `fnDateInfo`$$
CREATE FUNCTION `fnDateInfo`(_tag varchar(20), _maxDate datetime) RETURNS date
BEGIN
  DECLARE _workDay, _syncDate, _dayEval date;

  SELECT IFNULL(_maxDate,'0000-00-00 00:00:00.0000') INTO _maxDate;

  IF _maxDate = '0000-00-00 00:00:00.0000' THEN
    SELECT IFNULL(syncDate,'0000-00-00 00:00:00.0000') INTO _syncDate FROM syncInfo LIMIT 1;
    SELECT _syncDate INTO _dayEval;
  ELSE
    SET _dayEval = _maxDate;
  END IF;

  IF _tag IN ('yesterday','day before yesterday') AND _dayEval <> '0000-00-00 00:00:00.0000' THEN
    SELECT date_add(_dayEval, INTERVAL -1 day) INTO _dayEval;
    yesterday: WHILE(1=1) DO

      SELECT t INTO _workDay
      FROM `time` JOIN businessDay b ON `time`.wd = b.wd
      WHERE t = _dayEval AND b.business = 1;

      IF _workDay IS NULL THEN
        SELECT date_add(_dayEval, INTERVAL -1 day) INTO _dayEval;
      ELSE
        LEAVE yesterday;
      END IF;

    END WHILE;
  END IF;

  IF _tag = 'day before yesterday' AND _dayEval <> '0000-00-00 00:00:00.0000' THEN
    SELECT date_add(_workDay, INTERVAL -1 day) INTO _dayEval;
    SET _workDay = NULL;
    daybeforeyesterday: WHILE(1=1) DO

      SELECT t INTO _workDay
      FROM `time` JOIN businessDay b ON `time`.wd = b.wd
      WHERE t = _dayEval AND b.business = 1;

      IF _workDay IS NULL THEN
        SELECT date_add(_dayEval, INTERVAL -1 day) INTO _dayEval;
      ELSE
        LEAVE daybeforeyesterday;
      END IF;

    END WHILE;
  END IF;

  RETURN _workDay;
END$$
DELIMITER ;