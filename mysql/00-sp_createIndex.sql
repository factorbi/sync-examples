DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_createIndex`$$
CREATE PROCEDURE `sp_createIndex`(Tabla varchar(50), Indice varchar(255), Campos varchar(255), Unico varchar(10))
createIndex:BEGIN

  SET @Index_cnt = (
  SELECT COUNT(1) cnt
    FROM INFORMATION_SCHEMA.STATISTICS
   WHERE table_name = Tabla
     AND index_name = Indice
   AND table_schema = DATABASE()
  );

  SET @Table_cnt = (
  SELECT COUNT(1) cnt
    FROM INFORMATION_SCHEMA.STATISTICS
   WHERE table_name = Tabla
   AND table_schema = DATABASE()
  );

  IF IFNULL(@Index_cnt,0) = 0 AND IFNULL(@Table_cnt,0) <> 0 THEN
    SET @index_sql = CONCAT('Alter table ',Tabla,' ADD ', Unico, ' INDEX ',Indice,'(',Campos,');');
    PREPARE stmt FROM @index_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
  END IF;

END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_dropIndex`$$
CREATE PROCEDURE `sp_dropIndex`(Tabla varchar(50), Indice varchar(255))
dropIndex:BEGIN

  SET @Index_cnt = (
  SELECT COUNT(1) cnt
    FROM INFORMATION_SCHEMA.STATISTICS
   WHERE table_name = Tabla
     AND index_name = Indice
   AND table_schema = DATABASE()
  );

  SET @Table_cnt = (
  SELECT COUNT(1) cnt
    FROM INFORMATION_SCHEMA.STATISTICS
   WHERE table_name = Tabla
   AND table_schema = DATABASE()
  );

  IF IFNULL(@Index_cnt,0) <> 0 AND IFNULL(@Table_cnt,0) <> 0 THEN
    SET @index_sql = CONCAT('DROP INDEX ', Indice, ' ON ', Tabla, ';');
    PREPARE stmt FROM @index_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
  END IF;

END$$
DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS `sp_dropPrimaryKey`$$
CREATE PROCEDURE `sp_dropPrimaryKey`(Tabla varchar(50))
dropPk:BEGIN

  SET @Index_cnt = (
  SELECT COUNT(1) cnt
    FROM INFORMATION_SCHEMA.STATISTICS
   WHERE table_name = Tabla
     AND index_name = 'PRIMARY'
   AND table_schema = DATABASE()
  );

  SET @Table_cnt = (
  SELECT COUNT(1) cnt
    FROM INFORMATION_SCHEMA.STATISTICS
   WHERE table_name = Tabla
   AND table_schema = DATABASE()
  );

  IF IFNULL(@Index_cnt,0) <> 0 AND IFNULL(@Table_cnt,0) <> 0 THEN
    SET @index_sql = CONCAT('ALTER TABLE ', Tabla, ' DROP PRIMARY KEY;');
    PREPARE stmt FROM @index_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
  END IF;

END$$
DELIMITER ;
