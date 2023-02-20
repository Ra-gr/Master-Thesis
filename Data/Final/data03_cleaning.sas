*=============================================================================================

	DATA CLEANING (Green & Feng variables and REPRISK)
	primarily we want to limit the influence of extreme outliers which are in a lot of these G&F variables

=============================================================================================;
*get Green & Feng variables and some reprisk data;
DATA temp;
  set '/home/unisg/balzers/data02_reprisk_short.sas7bdat'; 
RUN; 

*this is for all of the continous variables;
%let vars=beta betasq ep mve dy bm lev currat pchcurrat quick pchquick
baspread mom12m depr
pchdepr mom1m mom6m mom36m sgr chempia SP acc turn
pchsale_pchinvt pchsale_pchrect pchcapx_ia pchgm_pchsale pchsale_pchxsga
nincr indmom ps mve_ia cfp_ia bm_ia dolvol std_dolvol std_turn
chinv idiovol
grltnoa cinvest tb cfp roavol lgr egr ill age ms pricedelay
rd_sale rd_mve retvol herf grCAPX zerotrade chmom roic
aeavol agr chcsho chpmia chatoia
ear  rsup stdcf tang hire cashpr roaq
invest absacc stdacc chtx maxret pctacc  cash gma roeq
orgcap  salecash salerec saleinv pchsaleinv cashdebt realestate secured 
	operprof;
*this is for those bounded below at zero but may have large positive outliers;   
%let hitrim=betasq mve dy  lev baspread  depr  SP turn  dolvol std_dolvol std_turn 
			idiovol roavol ill age rd_sale rd_mve retvol zerotrade  stdcf tang absacc stdacc   
			cash orgcap  salecash salerec saleinv pchsaleinv cashdebt realestate  secured;
*this is for those that may have large positive or negative outliers;
%let hilotrim=beta ep mom12m mom1m mom6m mom36m indmom agr maxret bm currat pchcurrat quick pchquick pchdepr sgr chempia acc  
				pchsale_pchinvt pchsale_pchrect pchcapx_ia pchgm_pchsale pchsale_pchxsga mve_ia cfp_ia bm_ia 
				chinv grltnoa cinvest tb cfp lgr egr pricedelay grCAPX chmom roic aeavol 
				chcsho chpmia chatoia ear  rsup hire cashpr roaq roeq invest  chtx pctacc gma operprof;  
*Some of these are not continuous, they are dummy variables so they are excluded 
	from the outlier issue:
	 rd divi divo securedind convind sin;
*----winsorize only positive variables-----;
proc sort data=temp;
   by date;
run; 	
proc means data=temp noprint;
	by date;
	var &hitrim;
  output out=stats p99=/autoname;
run;			
proc sql;
	create table temp2
	as select *
	from temp a left join stats b
	on a.date=b.date;
	quit;
data temp2;
	set temp2;
	array base {*} &hitrim;
	array high {*} betasq_p99--secured_p99;
	do i=1 to dim(base);
		if base(i) ne . and base(i)>(high(i)) then base(i)=(high(i));
		if high(i)=. then base(i)=.;
	end;
	drop _type_ _freq_ betasq_p99--secured_p99;
	run;
*winsorize top and bottom of continuous variables;
proc sort data=temp2;
   by date;
run; 	
proc means data=temp2 noprint;
	by date;
	var &hilotrim;
  output out=stats p1= p99=/autoname;
run;			
proc sql;
	create table temp2
	as select *
	from temp2 a left join stats b
	on a.date=b.date;
	quit;
data temp2;
	set temp2;
	array base {*} &hilotrim;
	array low {*} beta_p1--operprof_p1;
	array high {*} beta_p99--operprof_p99;
	do i=1 to dim(base);
		if base(i) ne . and base(i)<(low(i)) then base(i)=(low(i));
		if base(i) ne . and base(i)>(high(i)) then base(i)=(high(i));
		if low(i)=. then base(i)=.;
	end;
	drop _type_ _freq_ beta_p1--operprof_p1 beta_p99--operprof_p99;
	run;
proc sort data=temp2;
   by date;
run; 		
data temp3;	set temp2;
	drop count i DLRET DLSTCD ewret rsq1 SHROUT;
run;

/* NEW: add FF industry and public date */
data rpsdata_RFS; set temp3;												
  if sic=0 then sic=.;
  if missing(sic)=0 then %FFI49(sic);
  ffi49_desc=upcase(ffi49_desc);
  run;

data rpsdata_RFS; set rpsdata_RFS;
  	cusip = cnum;
  	public_date = intnx('month',date,0,'e');
  	format public_date yymmdd10.;
  	run;

/* ********************************************* */
/*  Specify data set                             */
/* ********************************************* */ 	
%let vars = absacc acc aeavol age agr baspread beta betasq bm bm_ia cash cashdebt cashpr cfp cfp_ia
chatoia chcsho chempia chinv chmom chpmia chtx cinvest convind currat depr divi divo dolvol dy ear
egr ep gma grcapx grltnoa herf hire idiovol ill indmom invest lev lgr maxret mom12m mom1m mom36m
mom6m ms mve mve_ia nincr operprof orgcap pchcapx_ia pchcurrat pchdepr pchgm_pchsale pchquick
pchsale_pchinvt pchsale_pchrect pchsale_pchxsga pchsaleinv pctacc pricedelay ps quick rd rd_mve
rd_sale realestate retvol roaq roavol roeq roic rsup salecash saleinv salerec secured securedind
sgr sin sp std_dolvol std_turn stdacc stdcf tang tb turn zerotrade
current_rri trend_rri peak_rri reprisk_rating country_sector_average;
%let vars_industry = FFI49_desc FFI49;
data da;
	retain public_date permno gvkey sic sic2 cusip exchcd ret mve_m prc vol reprisk_id
  	&vars_industry &vars;
  	set rpsdata_RFS;
  	format public_date Date9.;
run;

libname myHome '/home/unisg/balzers';
data myHome.data03_cleaning;
	set da;
run;