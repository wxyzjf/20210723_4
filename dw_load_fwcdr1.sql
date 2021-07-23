alter table h_fw_cdr add if not exists partition (start_date='20200830'); 
alter table h_fw_cdr drop partition (start_date='20200824')
