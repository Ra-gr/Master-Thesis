*==============================================================================================================

				Impute missing data

==============================================================================================================;

/* Load data            */
DATA da;
  set '/home/unisg/balzers/data03_cleaning.sas7bdat'; 
RUN; 
proc print data=da(obs=100); run;


/* fill in missing values by FF49 Industry Average at that time */
/* fill in missing values by All Stock at that time Equal Weight Average */
/* ********************************************************************************* */
/* MACRO to fill by Industry Average */
%MACRO FINRATIO_ind_label2 (begdate=, enddate=, label=, avr=, input=, vars=, output=);
%let allvars=&vars;
%let indclass = &label;

data ratios;
set &INPUT;
/*set time frame*/
where "&begdate"d<=public_date<="&enddate"d;
run;

/* average zt */
proc sort data = ratios; by public_date &indclass; run;
/*Computing Industry-level average financial ratios in a given month*/
proc means data=ratios noprint;
  where not missing(&indclass);
    by public_date; class &indclass;
     var &allvars;
  /* weight &weight;   */                    /* value-weight */
    output out=indratios &avr=/autoname;
run;
proc sort data=indratios; by public_date &indclass;run;

data indratios; set indratios;
where not missing(&indclass);
drop _type_ _freq_;
format public_date Date9.;
run;

data &output;
set indratios;
run;

/* clean house */
proc sql; drop table ratios, indratios;
quit;

%mend FINRATIO_ind_label2;

/* ********************************************************************************* */
/* MACRO to fill by All Stocks */
%MACRO FINRATIO_ew_all (begdate=, enddate=, avr=, input=, vars=, output=);

/*List of Ratios to be calculated*/
%let allvars=&vars;

data ratios;
set &INPUT;
/*set time frame*/
where "&begdate"d<=public_date<="&enddate"d;
run;

proc sort data = ratios; by public_date permno; run;
/*Computing average financial ratios in a given month*/
proc means data=ratios noprint;
  where not missing(permno);
    by public_date;
     var &allvars;
    output out=indratios &avr=/autoname;
run;
proc sort data=indratios; by public_date;run;

data &output; set indratios;
drop _type_ _freq_;
format public_date Date9.;
run;

proc sql; drop table ratios, indratios;
quit;

%mend FINRATIO_ew_all;


/* ********************************************* */
/*  Parameters                    */
/* ********************************************* */
%let uni_begdt = 01JAN1970;
%let uni_enddt = 31DEC2022;

%let vars = absacc acc aeavol age agr baspread beta betasq bm bm_ia cash cashdebt cashpr cfp cfp_ia
chatoia chcsho chempia chinv chmom chpmia chtx cinvest convind currat depr divi divo dolvol dy ear
egr ep gma grcapx grltnoa herf hire idiovol ill indmom invest lev lgr maxret mom12m mom1m mom36m
mom6m ms mve mve_ia nincr operprof orgcap pchcapx_ia pchcurrat pchdepr pchgm_pchsale pchquick
pchsale_pchinvt pchsale_pchrect pchsale_pchxsga pchsaleinv pctacc pricedelay ps quick rd rd_mve
rd_sale realestate retvol roaq roavol roeq roic rsup salecash saleinv salerec secured securedind
sgr sin sp std_dolvol std_turn stdacc stdcf tang tb turn zerotrade;

%let vars_industry = FFI49_desc FFI49;


/* ********************************************************************************* */
/* FFI49 Industry Average         */
%FINRATIO_ind_label2  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=FFI49_desc, AVR=median, Input=da, vars=&vars, output=ind_FFI49);

title "FFI49 Industry Weighted Average";
PROC PRINT DATA=ind_FFI49(where=(year(public_date)=1976));RUN;

/* ********************************************************************************* */
/* fill in missing eq zt with FFI49 ind zt         */
proc sql;
create table tmp as
select a.*, b.* from da a left join ind_FFI49 b on
a.public_date = b.public_date and a.FFI49_desc = b.FFI49_desc
order by cusip, public_date;
quit;

title "Merged Table 1";
PROC PRINT DATA=tmp(obs=10);RUN;

data tmp; set tmp;
if missing(absacc) then  absacc=absacc_Median;
if missing(acc) then  acc=acc_Median;
if missing(aeavol) then  aeavol=aeavol_Median;
if missing(age) then  age=age_Median;
if missing(agr) then  agr=agr_Median;
if missing(baspread) then  baspread=baspread_Median;
if missing(beta) then  beta=beta_Median;
if missing(betasq) then  betasq=betasq_Median;
if missing(bm) then  bm=bm_Median;
if missing(bm_ia) then  bm_ia=bm_ia_Median;
if missing(cash) then  cash=cash_Median;
if missing(cashdebt) then  cashdebt=cashdebt_Median;
if missing(cashpr) then  cashpr=cashpr_Median;
if missing(cfp) then  cfp=cfp_Median;
if missing(cfp_ia) then  cfp_ia=cfp_ia_Median;
if missing(chatoia) then  chatoia=chatoia_Median;
if missing(chcsho) then  chcsho=chcsho_Median;
if missing(chempia) then  chempia=chempia_Median;
if missing(chinv) then  chinv=chinv_Median;
if missing(chmom) then  chmom=chmom_Median;
if missing(chpmia) then  chpmia=chpmia_Median;
if missing(chtx) then  chtx=chtx_Median;
if missing(cinvest) then  cinvest=cinvest_Median;
if missing(convind) then  convind=convind_Median;
if missing(currat) then  currat=currat_Median;
if missing(depr) then  depr=depr_Median;
if missing(divi) then  divi=divi_Median;
if missing(divo) then  divo=divo_Median;
if missing(dolvol) then  dolvol=dolvol_Median;
if missing(dy) then  dy=dy_Median;
if missing(ear) then  ear=ear_Median;
if missing(egr) then  egr=egr_Median;
if missing(ep) then  ep=ep_Median;
if missing(gma) then  gma=gma_Median;
if missing(grcapx) then  grcapx=grcapx_Median;
if missing(grltnoa) then  grltnoa=grltnoa_Median;
if missing(herf) then  herf=herf_Median;
if missing(hire) then  hire=hire_Median;
if missing(idiovol) then  idiovol=idiovol_Median;
if missing(ill) then  ill=ill_Median;
if missing(indmom) then  indmom=indmom_Median;
if missing(invest) then  invest=invest_Median;
if missing(lev) then  lev=lev_Median;
if missing(lgr) then  lgr=lgr_Median;
if missing(maxret) then  maxret=maxret_Median;
if missing(mom12m) then  mom12m=mom12m_Median;
if missing(mom1m) then  mom1m=mom1m_Median;
if missing(mom36m) then  mom36m=mom36m_Median;
if missing(mom6m) then  mom6m=mom6m_Median;
if missing(ms) then  ms=ms_Median;
if missing(mve) then  mve=mve_Median;
if missing(mve_ia) then  mve_ia=mve_ia_Median;
if missing(nincr) then  nincr=nincr_Median;
if missing(operprof) then  operprof=operprof_Median;
if missing(orgcap) then  orgcap=orgcap_Median;
if missing(pchcapx_ia) then  pchcapx_ia=pchcapx_ia_Median;
if missing(pchcurrat) then  pchcurrat=pchcurrat_Median;
if missing(pchdepr) then  pchdepr=pchdepr_Median;
if missing(pchgm_pchsale) then  pchgm_pchsale=pchgm_pchsale_Median;
if missing(pchquick) then  pchquick=pchquick_Median;
if missing(pchsale_pchinvt) then  pchsale_pchinvt=pchsale_pchinvt_Median;
if missing(pchsale_pchrect) then  pchsale_pchrect=pchsale_pchrect_Median;
if missing(pchsale_pchxsga) then  pchsale_pchxsga=pchsale_pchxsga_Median;
if missing(pchsaleinv) then  pchsaleinv=pchsaleinv_Median;
if missing(pctacc) then  pctacc=pctacc_Median;
if missing(pricedelay) then  pricedelay=pricedelay_Median;
if missing(ps) then  ps=ps_Median;
if missing(quick) then  quick=quick_Median;
if missing(rd) then  rd=rd_Median;
if missing(rd_mve) then  rd_mve=rd_mve_Median;
if missing(rd_sale) then  rd_sale=rd_sale_Median;
if missing(realestate) then  realestate=realestate_Median;
if missing(retvol) then  retvol=retvol_Median;
if missing(roaq) then  roaq=roaq_Median;
if missing(roavol) then  roavol=roavol_Median;
if missing(roeq) then  roeq=roeq_Median;
if missing(roic) then  roic=roic_Median;
if missing(rsup) then  rsup=rsup_Median;
if missing(salecash) then  salecash=salecash_Median;
if missing(saleinv) then  saleinv=saleinv_Median;
if missing(salerec) then  salerec=salerec_Median;
if missing(secured) then  secured=secured_Median;
if missing(securedind) then  securedind=securedind_Median;
if missing(sgr) then  sgr=sgr_Median;
if missing(sin) then  sin=sin_Median;
if missing(sp) then  sp=sp_Median;
if missing(std_dolvol) then  std_dolvol=std_dolvol_Median;
if missing(std_turn) then  std_turn=std_turn_Median;
if missing(stdacc) then  stdacc=stdacc_Median;
if missing(stdcf) then  stdcf=stdcf_Median;
if missing(tang) then  tang=tang_Median;
if missing(tb) then  tb=tb_Median;
if missing(turn) then  turn=turn_Median;
if missing(zerotrade) then  zerotrade=zerotrade_Median;
run;

data tmp; set tmp;
drop
absacc_Median acc_Median aeavol_Median age_Median agr_Median baspread_Median beta_Median betasq_Median
bm_Median bm_ia_Median cash_Median cashdebt_Median cashpr_Median cfp_Median cfp_ia_Median chatoia_Median
chcsho_Median chempia_Median chinv_Median chmom_Median chpmia_Median chtx_Median cinvest_Median
convind_Median currat_Median depr_Median divi_Median divo_Median dolvol_Median dy_Median ear_Median
egr_Median ep_Median gma_Median grcapx_Median grltnoa_Median herf_Median hire_Median idiovol_Median
ill_Median indmom_Median invest_Median lev_Median lgr_Median maxret_Median mom12m_Median mom1m_Median
mom36m_Median mom6m_Median ms_Median mve_Median mve_ia_Median nincr_Median operprof_Median orgcap_Median
pchcapx_ia_Median pchcurrat_Median pchdepr_Median pchgm_pchsale_Median pchquick_Median pchsale_pchinvt_Median
pchsale_pchrect_Median pchsale_pchxsga_Median pchsaleinv_Median pctacc_Median pricedelay_Median ps_Median
quick_Median rd_Median rd_mve_Median rd_sale_Median realestate_Median retvol_Median roaq_Median roavol_Median
roeq_Median roic_Median rsup_Median salecash_Median saleinv_Median salerec_Median secured_Median
securedind_Median sgr_Median sin_Median sp_Median std_dolvol_Median std_turn_Median stdacc_Median
stdcf_Median tang_Median tb_Median turn_Median zerotrade_Median;
run;

title "Merged Table 2";
PROC PRINT DATA=tmp(obs=10);RUN;

libname myHome '/home/unisg/balzers';
data myHome.data04_imputation_industry;
	set tmp;
run;


/* ********************************************************************************* */
/* All EW Average         */

%FINRATIO_ew_all (begdate=&uni_begdt, enddate=&uni_enddt, avr=median, input=da, vars=&vars, output=ew_all);

title "All Stocks Equal Weighted Average";
PROC PRINT DATA=ew_all(where=(year(public_date)=1976));RUN;

/* ********************************************************************************* */
/* fill in missing eq zt with ew zt         */
proc sql;
create table tmp2 as
select a.*, b.* from tmp a left join ew_all b on
a.public_date = b.public_date
order by cusip, public_date;
quit;

title "Merged Table 3";
PROC PRINT DATA=tmp2(obs=10);RUN;

data tmp2; set tmp2;
if missing(absacc) then  absacc=absacc_Median;
if missing(acc) then  acc=acc_Median;
if missing(aeavol) then  aeavol=aeavol_Median;
if missing(age) then  age=age_Median;
if missing(agr) then  agr=agr_Median;
if missing(baspread) then  baspread=baspread_Median;
if missing(beta) then  beta=beta_Median;
if missing(betasq) then  betasq=betasq_Median;
if missing(bm) then  bm=bm_Median;
if missing(bm_ia) then  bm_ia=bm_ia_Median;
if missing(cash) then  cash=cash_Median;
if missing(cashdebt) then  cashdebt=cashdebt_Median;
if missing(cashpr) then  cashpr=cashpr_Median;
if missing(cfp) then  cfp=cfp_Median;
if missing(cfp_ia) then  cfp_ia=cfp_ia_Median;
if missing(chatoia) then  chatoia=chatoia_Median;
if missing(chcsho) then  chcsho=chcsho_Median;
if missing(chempia) then  chempia=chempia_Median;
if missing(chinv) then  chinv=chinv_Median;
if missing(chmom) then  chmom=chmom_Median;
if missing(chpmia) then  chpmia=chpmia_Median;
if missing(chtx) then  chtx=chtx_Median;
if missing(cinvest) then  cinvest=cinvest_Median;
if missing(convind) then  convind=convind_Median;
if missing(currat) then  currat=currat_Median;
if missing(depr) then  depr=depr_Median;
if missing(divi) then  divi=divi_Median;
if missing(divo) then  divo=divo_Median;
if missing(dolvol) then  dolvol=dolvol_Median;
if missing(dy) then  dy=dy_Median;
if missing(ear) then  ear=ear_Median;
if missing(egr) then  egr=egr_Median;
if missing(ep) then  ep=ep_Median;
if missing(gma) then  gma=gma_Median;
if missing(grcapx) then  grcapx=grcapx_Median;
if missing(grltnoa) then  grltnoa=grltnoa_Median;
if missing(herf) then  herf=herf_Median;
if missing(hire) then  hire=hire_Median;
if missing(idiovol) then  idiovol=idiovol_Median;
if missing(ill) then  ill=ill_Median;
if missing(indmom) then  indmom=indmom_Median;
if missing(invest) then  invest=invest_Median;
if missing(lev) then  lev=lev_Median;
if missing(lgr) then  lgr=lgr_Median;
if missing(maxret) then  maxret=maxret_Median;
if missing(mom12m) then  mom12m=mom12m_Median;
if missing(mom1m) then  mom1m=mom1m_Median;
if missing(mom36m) then  mom36m=mom36m_Median;
if missing(mom6m) then  mom6m=mom6m_Median;
if missing(ms) then  ms=ms_Median;
if missing(mve) then  mve=mve_Median;
if missing(mve_ia) then  mve_ia=mve_ia_Median;
if missing(nincr) then  nincr=nincr_Median;
if missing(operprof) then  operprof=operprof_Median;
if missing(orgcap) then  orgcap=orgcap_Median;
if missing(pchcapx_ia) then  pchcapx_ia=pchcapx_ia_Median;
if missing(pchcurrat) then  pchcurrat=pchcurrat_Median;
if missing(pchdepr) then  pchdepr=pchdepr_Median;
if missing(pchgm_pchsale) then  pchgm_pchsale=pchgm_pchsale_Median;
if missing(pchquick) then  pchquick=pchquick_Median;
if missing(pchsale_pchinvt) then  pchsale_pchinvt=pchsale_pchinvt_Median;
if missing(pchsale_pchrect) then  pchsale_pchrect=pchsale_pchrect_Median;
if missing(pchsale_pchxsga) then  pchsale_pchxsga=pchsale_pchxsga_Median;
if missing(pchsaleinv) then  pchsaleinv=pchsaleinv_Median;
if missing(pctacc) then  pctacc=pctacc_Median;
if missing(pricedelay) then  pricedelay=pricedelay_Median;
if missing(ps) then  ps=ps_Median;
if missing(quick) then  quick=quick_Median;
if missing(rd) then  rd=rd_Median;
if missing(rd_mve) then  rd_mve=rd_mve_Median;
if missing(rd_sale) then  rd_sale=rd_sale_Median;
if missing(realestate) then  realestate=realestate_Median;
if missing(retvol) then  retvol=retvol_Median;
if missing(roaq) then  roaq=roaq_Median;
if missing(roavol) then  roavol=roavol_Median;
if missing(roeq) then  roeq=roeq_Median;
if missing(roic) then  roic=roic_Median;
if missing(rsup) then  rsup=rsup_Median;
if missing(salecash) then  salecash=salecash_Median;
if missing(saleinv) then  saleinv=saleinv_Median;
if missing(salerec) then  salerec=salerec_Median;
if missing(secured) then  secured=secured_Median;
if missing(securedind) then  securedind=securedind_Median;
if missing(sgr) then  sgr=sgr_Median;
if missing(sin) then  sin=sin_Median;
if missing(sp) then  sp=sp_Median;
if missing(std_dolvol) then  std_dolvol=std_dolvol_Median;
if missing(std_turn) then  std_turn=std_turn_Median;
if missing(stdacc) then  stdacc=stdacc_Median;
if missing(stdcf) then  stdcf=stdcf_Median;
if missing(tang) then  tang=tang_Median;
if missing(tb) then  tb=tb_Median;
if missing(turn) then  turn=turn_Median;
if missing(zerotrade) then  zerotrade=zerotrade_Median;
run;

data tmp2; set tmp2;
drop
absacc_Median acc_Median aeavol_Median age_Median agr_Median baspread_Median beta_Median betasq_Median
bm_Median bm_ia_Median cash_Median cashdebt_Median cashpr_Median cfp_Median cfp_ia_Median chatoia_Median
chcsho_Median chempia_Median chinv_Median chmom_Median chpmia_Median chtx_Median cinvest_Median
convind_Median currat_Median depr_Median divi_Median divo_Median dolvol_Median dy_Median ear_Median
egr_Median ep_Median gma_Median grcapx_Median grltnoa_Median herf_Median hire_Median idiovol_Median
ill_Median indmom_Median invest_Median lev_Median lgr_Median maxret_Median mom12m_Median mom1m_Median
mom36m_Median mom6m_Median ms_Median mve_Median mve_ia_Median nincr_Median operprof_Median orgcap_Median
pchcapx_ia_Median pchcurrat_Median pchdepr_Median pchgm_pchsale_Median pchquick_Median pchsale_pchinvt_Median
pchsale_pchrect_Median pchsale_pchxsga_Median pchsaleinv_Median pctacc_Median pricedelay_Median ps_Median
quick_Median rd_Median rd_mve_Median rd_sale_Median realestate_Median retvol_Median roaq_Median roavol_Median
roeq_Median roic_Median rsup_Median salecash_Median saleinv_Median salerec_Median secured_Median
securedind_Median sgr_Median sin_Median sp_Median std_dolvol_Median std_turn_Median stdacc_Median
stdcf_Median tang_Median tb_Median turn_Median zerotrade_Median
cusip cnum sic FFI49_desc;
run;

title "Merged Table 4";
PROC PRINT DATA=tmp2(obs=10);RUN;

/*  Save data                    */
libname myHome '/home/unisg/balzers';
data myHome.data04_imputation_all;
	set tmp2;
run;