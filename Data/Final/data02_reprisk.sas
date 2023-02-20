*=============================================================================================

	NEW: Merge Green & Feng variables with REPRISK data

=============================================================================================;
*get Green & Feng variables;
DATA temp;
  set '/home/unisg/balzers/data01_green_feng.sas7bdat'; 
RUN; 


*step 1: get isin for each gvkey;
proc sql;
create table gvkey_isin
	  as select gvkey, isin
	  from comp.security
	  where not missing(gvkey) and not missing(isin);
quit;

*step 2: assign reprisk_id to isin;
proc sql;
create table gvkey_reprisk
	as select a.gvkey,r.reprisk_id
	from gvkey_isin a left join reprisk.v2_wrds_company_lookup r
	on a.isin=r.isin
	where not missing(reprisk_id);
quit;
proc sort data=gvkey_reprisk nodupkey;
	by gvkey;
run;

*step 3: add reprisk_id to original data set;										
proc sql;
	create table data_rep
	as select a.*,l.reprisk_id
	from temp a left join gvkey_reprisk l
	on a.gvkey=l.gvkey;
quit;

proc sql;
   create table new as 
     select count(distinct(gvkey)) as gvkeycount, count(distinct(reprisk_id)) as repriskcount, count(gvkey) as total from data_rep;
quit;
proc print;
   title 'Number of distincts for data_rep (missing reprisk_id)'; 
run;

*step 4: delete observations without a reprisk_id;						
data data_rep; set data_rep;
	where not missing(reprisk_id);
run;
proc sort data=data_rep nodupkey;
	by permno date;
run;

proc sql;
   create table new as 
     select count(distinct(gvkey)) as gvkeycount, count(distinct(reprisk_id)) as repriskcount, count(gvkey) as total from data_rep;
quit;
proc print;
   title 'Number of distinct gvkey for data_rep (no missing reprisk_id)'; 
run;


*get a list of all used reprisk_id's;
proc sql;
   create table reprisk_ids as
   select distinct reprisk_id
   from data_rep;
quit;
proc sql;
	create table reprisk_companies
	as select a.reprisk_id,b.name
	from reprisk_ids a left join reprisk.reprisk_company_lookup b
	on a.reprisk_id = b.reprisk_id;
quit;
proc sort data=reprisk_companies nodupkey;
	by reprisk_id;
run;
libname myHome '/home/unisg/balzers';
data myHome.reprisk_companies;
	set reprisk_companies;
run;

*download incident file (only for those reprisk_id's that I need);
proc sql;
	create table incident
	as select a.reprisk_id,story_id,incident_date,unsharp_incident,severity,reach,novelty,environment,social,governance,cross_cutting,ungc_principle_1,
	ungc_principle_2,ungc_principle_3,ungc_principle_4,ungc_principle_5,ungc_principle_6,ungc_principle_7,ungc_principle_8,ungc_principle_9,ungc_principle_10	
	from reprisk.v2_risk_incidents as a, reprisk_ids as b
	where a.reprisk_id = b.reprisk_id;
quit;
*save incident file;
libname myHome '/home/unisg/balzers';
data myHome.reprisk_incidents;
	set incident;
run;


*step 5: get reprisk data;
proc sql;
	create table dreprisk
	as select reprisk_id,date,year(date) as yr,month(date) as month,
	current_rri,trend_rri,peak_rri,reprisk_rating,country_sector_average
	from reprisk.v2_metrics
	group by reprisk_id,year(date),month(date);
quit;
data mreprisk;
	set dreprisk;
run;
proc sort data=mreprisk;
	by reprisk_id yr month descending date;
run;
proc sort data=mreprisk nodupkey;
	by reprisk_id yr month;
run;

*step 6: add reprisk data (match to prior month to use lagged variables to predict returns);
proc sql;
	create table temp8
	as select a.*,current_rri,trend_rri,peak_rri,reprisk_rating,country_sector_average
	from data_rep a left join mreprisk b
	on a.reprisk_id = b.reprisk_id
	and year(intnx('MONTH',a.date,-1))=b.yr and month(intnx('MONTH',a.date,-1))=b.month;
quit;
proc sort data=temp8 nodupkey;
	by permno date;
run;

*step 7: save data (all observations);
libname myHome '/home/unisg/balzers';
data myHome.data02_reprisk_long;
	set temp8;
run;

*step 8: save data (only observations with reprisk data available in respective month);
data temp9;
	set temp8;
run;
proc sort data=temp9 nodupkey;
	where not missing(current_rri);
	by permno date;
run;

libname myHome '/home/unisg/balzers';
data myHome.data02_reprisk_short;
	set temp9;
run;