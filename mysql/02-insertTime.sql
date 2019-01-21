TRUNCATE TABLE `time`;
CALL spCreateTime("2011-01-01", "2022-01-01");
CALL spCreateYM;
CALL sp_createIndex('time', 'time', 't', '');
CALL sp_createIndex('ym', 'ym', 'y,m', '');
