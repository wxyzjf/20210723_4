alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='11'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_11_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='11');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='11'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_11_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='11');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='11'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_11_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='11');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='3075'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_3075_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='3075');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='3075'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_3075_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='3075');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='3075'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_3075_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='3075');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4104'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4104_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4104');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4104'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4104_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4104');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4104'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4104_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4104');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4110'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4110_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4110');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4110'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4110_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4110');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4110'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4110_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4110');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4112'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4112_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4112');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4112'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4112_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4112');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4112'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4112_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4112');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4114'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4114_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4114');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4114'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4114_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4114');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4114'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4114_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4114');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4120'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4120_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4120');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4120'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4120_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4120');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4120'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4120_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4120');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4121'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4121_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4121');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4121'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4121_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4121');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4121'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4121_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4121');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4125'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4125_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4125');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4125'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4125_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4125');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='4125'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_4125_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='4125');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='5157'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_5157_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='5157');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='5157'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_5157_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='5157');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='5157'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_5157_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='5157');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='5193'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_5193_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='5193');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='5193'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_5193_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='5193');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='5193'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_5193_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='5193');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='8'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_8_p1_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='8');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='8'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_8_p2_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='8');
alter table h_ctr_cdr add if not exists partition (trx_date='20190805',event_id='8'); 
load data LOCAL inpath '/opt/reload//cvtdata/CTR_CDR/CTR_20190805_8_p3_20190813152225.csv.gz' into table h_ctr_cdr partition(trx_date='20190805',event_id='8');