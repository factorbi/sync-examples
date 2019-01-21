CREATE TABLE IF NOT EXISTS `T` (
  `n` int(11)
);

CREATE TABLE IF NOT EXISTS `time` (
  `t` 			date 	         	NOT NULL,
  `y` 			smallint DEFAULT 	NULL,
  `m` 			smallint DEFAULT 	NULL,
  `d` 			smallint DEFAULT 	NULL,
  `w` 			smallint DEFAULT 	NULL,
  `q` 			smallint DEFAULT 	NULL,
  `wd` 			smallint DEFAULT 	NULL,
  `mn`  		char(10) DEFAULT 	NULL,
  `smn`  		char(10) DEFAULT 	NULL,
  `dn` 			char(10) DEFAULT 	NULL,
  `sdn` 		char(10) DEFAULT 	NULL,

  PRIMARY KEY (`t`)
);

-- DROP TABLE IF EXISTS ym;
CREATE TABLE IF NOT EXISTS `ym` (
  `id`      int not null auto_increment unique key,
  `y` 			smallint NOT NULL,
  `m` 			smallint NOT NULL,
  `mn`  		char(10) DEFAULT 	NULL,
  `smn`  		char(10) DEFAULT 	NULL,
  `control` varchar(20) DEFAULT NULL,
  CONSTRAINT priym PRIMARY KEY(y,m)
);

-- call sp_AlterTable('ym', 'control', 'varchar(20)', 'NULL', '');

delimiter $$
DROP PROCEDURE IF EXISTS `dowhile`$$
CREATE PROCEDURE dowhile(_d0 datetime, _d1 datetime)
BEGIN
  SET @days = DATEDIFF(_d1, _d0);
  SET @aux  = 1;

  WHILE @aux <=  @days DO
    INSERT T VALUES (@aux);
    SET @aux = @aux + 1;
  END WHILE;
END$$
delimiter ;

delimiter $$
DROP PROCEDURE IF EXISTS `spCreateTime`$$
CREATE PROCEDURE spCreateTime(_d0 datetime, _d1 datetime)
BEGIN
  SET lc_time_names = 'es_MX';

  IF NOT EXISTS(select * from `time`) THEN
    call doWhile(_d0, _d1);

    SET @date = DATE_SUB(_d0, interval 1 day);

    INSERT INTO `time`
    SELECT @date := DATE_ADD(@date, interval 1 day) as date,
		    YEAR(@date),
		    MONTH(@date),
		    DAY(@date),
		    WEEK(@date, 3),
		    QUARTER(@date),
		    WEEKDAY(@date)+1,
        CONCAT(UCASE(SUBSTRING(MONTHNAME(@date), 1, 1)),LOWER(SUBSTRING(MONTHNAME(@date), 2))),
        CONCAT(UCASE(SUBSTRING(SUBSTRING(MONTHNAME(@date), 1, 3), 1, 1)),LOWER(SUBSTRING(SUBSTRING(MONTHNAME(@date), 1, 3), 2))),
        CONCAT(UCASE(SUBSTRING(DAYNAME(@date), 1, 1)),LOWER(SUBSTRING(DAYNAME(@date), 2))),
        CONCAT(UCASE(SUBSTRING(SUBSTRING(DAYNAME(@date), 1, 3), 1, 1)),LOWER(SUBSTRING(SUBSTRING(DAYNAME(@date), 1, 3), 2)))
       FROM T
      WHERE DATE_ADD(@date, interval 1 day) <= _d1
      ORDER BY date;
  END IF;
END$$
delimiter ;

delimiter $$
DROP PROCEDURE IF EXISTS `spCreateYM`$$
CREATE PROCEDURE spCreateYM()
BEGIN

  TRUNCATE TABLE `ym`;

  INSERT INTO `ym` (y, m, mn, smn)
  SELECT DISTINCT y, m, mn, smn
  FROM `time`
  WHERE COALESCE(y,0)<>0 AND COALESCE(m,0)<>0
  ORDER BY y, m;

END$$
delimiter ;
