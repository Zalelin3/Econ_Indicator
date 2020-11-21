create TABLE trade_db.economies (
	ecode  VarChar(5) UNIQUE NOT NULL,
    ename VarChar(60) UNIQUE NOT NULL,
    iso3a VarChar(6),
    is_reporter Boolean Not NULL,
    is_partner boolean Not NULL,
    is_group boolean Not NULL,
    is_region boolean Not NULL,
PRIMARY KEY (ecode));

-- DROP TABLE trade_db.economies;

select count(*) from tradevalues;

ALTER TABLE tradevalues MODIFY COLUMN value INT SIGNED NOT NULL;
ALTER TABLE tradevalues MODIFY COLUMN year SMALLINT SIGNED NOT NULL;

desc tradevalues;