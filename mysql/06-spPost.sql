#***********************  VERSION 115+ ***********************

DELIMITER $$
DROP PROCEDURE IF EXISTS `spPostInitial`$$
CREATE PROCEDURE `spPostInitial`(
  _serviceID varchar(36),
  _syncID int
  )
postinitial:BEGIN

  DECLARE _SQL  longtext;
  DECLARE _store varchar(10);
  DECLARE _timestamp datetime;
  DECLARE _syncDate date;
  DECLARE _timezone varchar(64);
  DECLARE _rid, _prevId, _count_bipost_sync_info, _count_table int;
  DECLARE _numDays decimal(10,0);
  DECLARE _tableName, _comment1, _comment2 varchar(255);

  SET lc_time_names = 'en_US';
  SET group_concat_max_len = 4294967295;
  SET _count_bipost_sync_info = 0;

  SELECT COUNT(*) INTO _count_bipost_sync_info FROM `bipost_system`.bipost_sync_info WHERE id = _syncID AND serviceID = _serviceID;

  IF IFNULL(_count_bipost_sync_info,0) = 0 THEN
    INSERT INTO logPostInitial (message, serviceID, syncID) VALUES ('execution parameters do not match', _serviceID, _syncID);
    SELECT 'execution parameters do not match' AS message;
    LEAVE postinitial;
  END IF;

  SELECT NULLIF(TRIM(timezone),'') INTO _timezone FROM syncInfoStores WHERE serviceID = _serviceID;
  IF _timezone IS NULL THEN
    SELECT IFNULL(NULLIF(TRIM(timezone),''),'US/Eastern') INTO _timezone FROM syncInfo WHERE serviceID = _serviceID;
  END IF;
  SELECT CONVERT_TZ(syncDate, 'UTC', _timezone) INTO _timestamp FROM `bipost_system`.bipost_sync_info WHERE id = _syncID;
  SELECT IFNULL(CAST(_timestamp AS date),'0000-00-00 00:00:00.0000') INTO _syncDate;

  IF _syncDate <> '0000-00-00 00:00:00.0000' THEN

    INSERT INTO logPostInitial (message, serviceID, syncID) VALUES ('ok', _serviceID, _syncID);

    SET _prevId = 0, _count_table = 0;

    table_id: WHILE(1=1) DO
    	SELECT MIN(rid)
        INTO _rid
        FROM `bipost_system`.bipost_sync_table
        WHERE rid > _prevId AND id = _syncID AND comment1 = '-1';

    	IF _rid IS NULL THEN
    	  LEAVE table_id;
    	END IF;

      SELECT tableName, comment1
        INTO _tableName, _comment1
        FROM `bipost_system`.bipost_sync_table
        WHERE rid = _rid;

      SELECT COUNT(*) INTO _count_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name = _tableName AND table_schema = schema();
      IF _count_table > 0 THEN
        SET _SQL = CONCAT(
          'TRUNCATE TABLE `', _tableName, '`;');
        SET @SQL = _SQL; PREPARE stmt3 FROM @SQL; EXECUTE stmt3; DEALLOCATE PREPARE stmt3; SET _SQL = NULL;
      END IF;

      SET _prevId = _rid, _count_table = 0;
    END WHILE;

  #***************** Delete child tables first *****************

    SET _numDays = 0, _count_table = 0, _tableName = 'claves_articulos';
    SELECT CAST(comment1 AS decimal(10,0))*(-1) INTO _numDays FROM `bipost_system`.bipost_sync_table WHERE id = _syncID AND tableName = _tableName;
    SELECT COUNT(*) INTO _count_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name = _tableName AND table_schema = schema();
    IF _numDays < 0 AND _count_table > 0 THEN
      DELETE claves_articulos
        FROM claves_articulos
        JOIN articulos ON claves_articulos.ARTICULO_ID = articulos.ARTICULO_ID
        WHERE ((CAST(articulos.FECHA_HORA_CREACION AS date) BETWEEN date_add(_syncDate, INTERVAL _numDays day) AND _syncDate) OR (CAST(articulos.FECHA_HORA_ULT_MODIF AS date) BETWEEN date_add(_syncDate, INTERVAL _numDays day) AND _syncDate));
    END IF;

    SET _numDays = 0, _count_table = 0, _tableName = 'dirs_clientes';
    SELECT CAST(comment1 AS decimal(10,0))*(-1) INTO _numDays FROM `bipost_system`.bipost_sync_table WHERE id = _syncID AND tableName = _tableName;
    SELECT COUNT(*) INTO _count_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name = _tableName AND table_schema = schema();
    IF _numDays < 0 AND _count_table > 0 THEN
      DELETE dirs_clientes
        FROM dirs_clientes
        JOIN clientes ON dirs_clientes.CLIENTE_ID = clientes.CLIENTE_ID
        WHERE ((CAST(clientes.FECHA_HORA_CREACION AS date) BETWEEN date_add(_syncDate, INTERVAL _numDays day) AND _syncDate) OR (CAST(clientes.FECHA_HORA_ULT_MODIF AS date) BETWEEN date_add(_syncDate, INTERVAL _numDays day) AND _syncDate));
    END IF;

  #***************** Delete header/parent tables *****************

    SET _numDays = 0, _count_table = 0, _tableName = 'articulos';
    SELECT CAST(comment1 AS decimal(10,0))*(-1) INTO _numDays FROM `bipost_system`.bipost_sync_table WHERE id = _syncID AND tableName = _tableName;
    SELECT COUNT(*) INTO _count_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name = _tableName AND table_schema = schema();
    IF _numDays < 0 AND _count_table > 0 THEN
      DELETE FROM articulos
        WHERE ((CAST(FECHA_HORA_CREACION AS date) BETWEEN date_add(_syncDate, INTERVAL _numDays day) AND _syncDate) OR (CAST(FECHA_HORA_ULT_MODIF AS date) BETWEEN date_add(_syncDate, INTERVAL _numDays day) AND _syncDate));
    END IF;

    SET _numDays = 0, _count_table = 0, _tableName = 'clientes';
    SELECT CAST(comment1 AS decimal(10,0))*(-1) INTO _numDays FROM `bipost_system`.bipost_sync_table WHERE id = _syncID AND tableName = _tableName;
    SELECT COUNT(*) INTO _count_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name = _tableName AND table_schema = schema();
    IF _numDays < 0 AND _count_table > 0 THEN
      DELETE FROM clientes
        WHERE ((CAST(FECHA_HORA_CREACION AS date) BETWEEN date_add(_syncDate, INTERVAL _numDays day) AND _syncDate) OR (CAST(FECHA_HORA_ULT_MODIF AS date) BETWEEN date_add(_syncDate, INTERVAL _numDays day) AND _syncDate));
    END IF;

  #***************** Tables with special comments *****************
    SET _numDays = 0, _count_table = 0, _tableName = 'saldos_co';
    SELECT comment2 INTO _comment2 FROM `bipost_system`.bipost_sync_table WHERE id = _syncID AND tableName = _tableName;
    SELECT COUNT(*) INTO _count_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name = _tableName AND table_schema = schema();
    IF _comment2 = 'last year + ytd' AND _count_table > 0 THEN
      DELETE FROM saldos_co
        WHERE ANO >= YEAR(_syncDate)-1;
    END IF;

    SET _numDays = 0, _count_table = 0, _tableName = 'saldos_in';
    SELECT comment2 INTO _comment2 FROM `bipost_system`.bipost_sync_table WHERE id = _syncID AND tableName = _tableName;
    SELECT COUNT(*) INTO _count_table FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name = _tableName AND table_schema = schema();
    IF _comment2 = 'last month + mtd' AND _count_table > 0 THEN
      DELETE FROM saldos_in
        WHERE ANO = YEAR(_syncDate) AND MES IN (MONTH(_syncDate)-1, MONTH(_syncDate));
    END IF;

  END IF;

END$$
DELIMITER ;
-- call spPostInitial('serviceID', syncId);

DELIMITER $$
DROP PROCEDURE IF EXISTS `spPostFinal`$$
CREATE PROCEDURE `spPostFinal`(
  _serviceID varchar(36),
  _syncID int
  )
postfinal:BEGIN

  DECLARE _timestamp datetime;
  DECLARE _syncDate date;
  DECLARE _timezone varchar(64);
  DECLARE _count_businessDay int;

  SET lc_time_names = 'en_US';
  SET group_concat_max_len = 4294967295;

  SELECT COUNT(*) INTO _count_bipost_sync_info FROM `bipost_system`.bipost_sync_info WHERE id = _syncID AND serviceID = _serviceID;

  IF _count_bipost_sync_info = 0 THEN
    INSERT INTO logPostFinal (message, serviceID, syncID) VALUES ('execution parameters do not match', _serviceID, _syncID);
    SELECT 'execution parameters do not match' AS message;
    LEAVE postfinal;
  END IF;

  SELECT NULLIF(TRIM(timezone),'') INTO _timezone FROM syncInfoStores WHERE serviceID = _serviceID;
  IF _timezone IS NULL THEN
    SELECT IFNULL(NULLIF(TRIM(timezone),''),'US/Eastern') INTO _timezone FROM syncInfo WHERE serviceID = _serviceID;
  END IF;
  SELECT CONVERT_TZ(syncDate, 'UTC', _timezone) INTO _timestamp FROM `bipost_system`.bipost_sync_info WHERE id = _syncID;
  SELECT IFNULL(CAST(_timestamp AS date),'0000-00-00 00:00:00.0000') INTO _syncDate;

  UPDATE syncInfo
    SET syncDate = _syncDate, `timestamp` = _timestamp, lastSyncId = _syncID
    WHERE serviceID = _serviceID;

  call spReport1(_serviceID);
  call spReport2(_serviceID);

  INSERT INTO logPostFinal (message, serviceID, syncID) VALUES ('ok', _serviceID, _syncID);
  SELECT 'ok' AS message;

END$$
DELIMITER ;
-- call spPostFinal('serviceID', syncId);
