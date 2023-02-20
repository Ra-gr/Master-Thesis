*<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

Create signals (RPS) that are aligned in calendar month from firm characteristics pulled
from Compustat and CRSP used to predict monthly cross-sections of stock returns.

This data creation program is adapted from Jeremiah Green's code available on his website.
The original code was the data creation program for the paper Green, Hand, and Zhang (2016).

>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
;
*==============================================================================================================

				start with COMPUSTAT ANNUAL information

==============================================================================================================;
proc sql;
create table data
	  as select 		/*header info*/
				substr(compress(cusip),1,6) as cnum,c.gvkey,datadate,fyear,c.cik,substr(sic,1,2) as sic2,
				sic,naics,

						/*firm variables*/
						/*income statement*/
		sale,revt,cogs,xsga,dp,xrd,xad,ib,ebitda,ebit,nopi,pi,txp,ni,txfed,txfo,txt,xint,

						/*CF statement and others*/
		capx,oancf,dvt,ob,

						/*assets*/
		rect,act,che,ppegt,invt,at,aco,intan,ao,ppent,fatb,fatl,

						/*liabilities*/
		lct,dlc,dltt,lt,dm,dcvt,cshrc,dcpstk,pstk,ap,lco,lo,drc,drlt,txdi,

						/*equity and other*/
		ceq,scstkc,emp,csho,

						/*market*/
		abs(prcc_f) as prcc_f,csho*calculated prcc_f as mve_f
	
	  from comp.company as c, comp.funda as f
	  where f.gvkey = c.gvkey

	  				/*require some reasonable amount of information*/
	  and not missing(at)  and not missing(prcc_f) and not missing(ni) and datadate>='01JAN1990'd

	  				/*get consolidated, standardized, industrial format statements*/
	  and f.indfmt='INDL' and f.datafmt='STD' and f.popsrc='D' and f.consol='C';
	quit;  													
					/*sort and clean up*/	
					proc sort data=data nodupkey;
						by gvkey datadate;
						run;
					/*prep for clean-up and using time series of variables*/
					data data;
						set data;
						retain count;
						by gvkey datadate;
						if first.gvkey then count=1;
						else count+1;
						run;
					data data ;
						set data;
						*do some clean up, several of these variables have lots of missing values;
						if not missing(drc) and not missing(drlt) then dr=drc+drlt;
						if not missing(drc) and missing(drlt) then dr=drc;
						if not missing(drlt) and missing(drc) then dr=drlt;
						
						if missing(dcvt) and not missing(dcpstk) and not missing(pstk) and dcpstk>pstk then dc=dcpstk-pstk;
						if missing(dcvt) and not missing(dcpstk) and missing(pstk) then dc=dcpstk;
						if missing(dc) then dc=dcvt;

						if missing(xint) then xint0=0;
							else xint0=xint;
						if missing(xsga) then xsga0=0;
							else xsga0=0;
						run;
/*
look at how many missing;
data test;
	set data;
	where datadate>='01JAN1980'd;
	array lst{*} xrd nopi xad dvt ob dm dc aco ap intan ao lco lo rect invt drc drlt dr emp dm dcvt fatb fatl che dp lct act xsga at;
	array dms{*} dxrd dnopi dxad ddvt dob ddm ddc daco dap dintan dao dlco dlo drect dinvt ddrc ddrlt ddr dspi dgdwl demp ddm ddcvt dfatb dfatl dche ddp dlct dact dxsga dat;
	do i=1 to dim(lst);
	if lst(i)=. then dms(i)=1; else dms(i)=0;
	end;
	run;
proc means data=test mean sum ;
	var dxrd dnopi dxad ddvt dob ddm ddc daco dap dintan dao dlco dlo drect dinvt ddrc ddrlt ddr dspi dgdwl demp ddm ddcvt dfatb dfatl dche ddp dlct dact dxsga dat;
	run;
endrsubmit;	*/				*xsga also has a fair amount missing...;
					/*--------------------------------------------------------

						more clean-up and create first pass of variables

					----------------------------------------------------------*/
					data data2;
						set data;
					/*create simple-just annual Compustat variables*/
						bm=ceq/mve_f;
						ep=ib/mve_f;
						cashpr=((mve_f+dltt-at)/che);	
						dy=dvt/mve_f;
						lev=lt/mve_f;
						sp=sale/mve_f;
						roic=(ebit-nopi)/(ceq+lt-che);		
						rd_sale=xrd/sale;
						rd_mve=xrd/mve_f;					
						agr= (at/lag(at)) - 1;
						gma=(revt-cogs)/lag(at); 
						chcsho=(csho/lag(csho))-1;
						lgr=(lt/lag(lt))-1;
						acc=(ib-oancf) /  ((at+lag(at))/2);
							if missing(oancf) then acc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/  ((at+lag(at))/2);
						pctacc=(ib-oancf)/abs(ib);
							if ib=0 then pctacc=(ib-oancf)/.01;
						if missing(oancf) then pctacc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/abs(ib);
							if missing(oancf) and ib=0 then pctacc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/.01;
						cfp=(ib-(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) ))/mve_f;
							if not missing(oancf) then cfp=oancf/mve_f;
						absacc=abs(acc);
						age=count;
						chinv=(invt-lag(invt))/((at+lag(at))/2);
						hire=(emp-lag(emp))/lag(emp);
							if missing(emp) or missing(lag(emp)) then hire=0;
						sgr=(sale/lag(sale))-1;
						chpm=(ib/sale)-(lag(ib)/lag(sale));
						chato=(sale/((at+lag(at))/2)) - (lag(sale)/((lag(at)+lag2(at))/2));
						pchsale_pchinvt=((sale-lag(sale))/lag(sale))-((invt-lag(invt))/lag(invt));
						pchsale_pchrect=((sale-lag(sale))/lag(sale))-((rect-lag(rect))/lag(rect));
						pchgm_pchsale=(((sale-cogs)-(lag(sale)-lag(cogs)))/(lag(sale)-lag(cogs)))-((sale-lag(sale))/lag(sale));
						pchsale_pchxsga=( (sale-lag(sale))/lag(sale) )-( (xsga-lag(xsga)) /lag(xsga) );
						depr=dp/ppent;
						pchdepr=((dp/ppent)-(lag(dp)/lag(ppent)))/(lag(dp)/lag(ppent));
						invest=( 	(ppegt-lag(ppegt)) +  (invt-lag(invt))	)	/ lag(at);
						if missing(ppegt) then invest=( 	(ppent-lag(ppent)) +  (invt-lag(invt))	)	/ lag(at);
						egr=( (ceq-lag(ceq))/lag(ceq)  );
							if missing(capx) and count>=2 then capx=ppent-lag(ppent);
						pchcapx=(capx-lag(capx))/lag(capx);
						grcapx=(capx-lag2(capx))/lag2(capx);
						tang=(che+rect*0.715+invt*0.547+ppent*0.535)/at;					
						if (2100<=sic<=2199) or (2080<=sic<=2085) or (naics in ('7132','71312','713210','71329','713290','72112','721120'))
							then sin=1; else sin=0;
							if missing(act) then act=che+rect+invt;
							if missing(lct) then lct=ap;
						currat=act/lct;
						pchcurrat=((act/lct)-(lag(act)/lag(lct)))/(lag(act)/lag(lct));
						quick=(act-invt)/lct;
						pchquick=(		(act-invt)/lct - (lag(act)-lag(invt))/lag(lct)    )/  (   (  lag(act)-lag(invt)  )/lag(lct)   );
						salecash=sale/che;
						salerec=sale/rect;
						saleinv=sale/invt;
						pchsaleinv=( (sale/invt)-(lag(sale)/lag(invt)) ) / (lag(sale)/lag(invt));
						cashdebt=(ib+dp)/((lt+lag(lt))/2);
						realestate=(fatb+fatl)/ppegt;
							if missing(ppegt) then realestate=(fatb+fatl)/ppent;
						if (not missing(dvt) and dvt>0) and (lag(dvt)=0 or missing(lag(dvt))) then divi=1; else divi=0;
						if (missing(dvt) or dvt=0) and (lag(dvt)>0 and not missing(lag(dvt))) then divo=1; else divo=0;
						if not missing(dm) and dm ne 0 then securedind=1; else securedind=0;
						secured=dm/dltt;
						if not missing(dc) and dc ne 0 or (not missing(cshrc) and CSHRC ne 0) then convind=1; else convind=0; 
						grltnoa=  ((rect+invt+ppent+aco+intan+ao-ap-lco-lo)-(lag(rect)+lag(invt)+lag(ppent)+lag(aco)+lag(intan)+lag(ao)-lag(ap)-lag(lco)-lag(lo))
								-( rect-lag(rect)+invt-lag(invt)+aco-lag(aco)-(ap-lag(ap)+lco-lag(lco)) -dp ))/((at+lag(at))/2);
						if ((xrd/at)-(lag(xrd/lag(at))))/(lag(xrd/lag(at))) >.05 then rd=1; else rd=0;
						operprof = (revt-cogs-xsga0-xint0)/lag(ceq);
						ps		= (ni>0)+(oancf>0)+(ni/at > lag(ni)/lag(at))+(oancf>ni)+(dltt/at < lag(dltt)/lag(at))+(act/lct > lag(act)/lag(lct))
								+((sale-cogs)/sale > (lag(sale)-lag(cogs))/lag(sale))+ (sale/at > lag(sale)/lag(at))+ (scstkc=0);
							*-----Lev and Nissim (2004);
							if fyear<=1978 then tr=.48;
							if 1979<=fyear<=1986 then tr=.46;
							if fyear=1987 then tr=.4;
							if 1988<=fyear<=1992 then tr=.34;
							if 1993<=fyear then tr=.35;
							tb_1=((txfo+txfed)/tr)/ib;
							if missing(txfo) or missing(txfed) then tb_1=((txt-txdi)/tr)/ib;  *they rank within industries;
							if (txfo+txfed>0 or txt>txdi) and ib<=0 then tb_1=1;
							*variables that will be used in subsequent steps to get to final RPS;
							*--prep for for Mohanram (2005) score;
							roa=ni/((at+lag(at))/2);
							cfroa=oancf/((at+lag(at))/2);
								if missing(oancf) then cfroa=(ib+dp)/((at+lag(at))/2);
							xrdint=xrd/((at+lag(at))/2);
							capxint=capx/((at+lag(at))/2);
							xadint=xad/((at+lag(at))/2);

						/*clean up for observations that do not have lagged observations to create variables*/
						array req{*} agr invest gma chcsho lgr egr chpm chinv hire acc pctacc absacc sgr 
									pchsale_pchinvt pchsale_pchrect pchgm_pchsale pchsale_pchxsga pchcapx ps roa cfroa xrdint capxint xadint divi divo
									grltnoa rd pchdepr pchcurrat pchquick pchsaleinv operprof;
						if count=1 then do;
							do b=1 to dim(req);
							req(b)=.;
							end;
						end;				
						if count<3 then do;
	 						chato=.;
							grcapx=.;
						end;
						run;
						/*other preparation steps for annual variables: industry adjustments*/
						proc sql;
							create table data2
							as select *,chpm-mean(chpm) as chpmia,chato-mean(chato) as chatoia,
							sum(sale) as indsale,hire-mean(hire) as chempia,bm-mean(bm) as bm_ia,
							pchcapx-mean(pchcapx) as pchcapx_ia,tb_1-mean(tb_1) as tb,
							cfp-mean(cfp) as cfp_ia,mve_f-mean(mve_f) as mve_ia
						from data2
						group by sic2,fyear;
						quit;
						proc sql;
						create table data2
						as select *,sum( (sale/indsale)*(sale/indsale) ) as herf
						from data2
						group by sic2,fyear;
						quit;
						*---industry measures for ms----;
						proc sort data=data2;
						by fyear sic2;
						run;
						proc univariate data=data2 noprint;
							by fyear sic2;
						var roa cfroa  xrdint capxint xadint;
						output out=indmd median=md_roa md_cfroa  md_xrdint md_capxint md_xadint;
						run;
						proc sql;
						create table data2
						as select *
						from data2 a left join indmd b
						on a.fyear=b.fyear and a.sic2=b.sic2;
						quit; 		
						proc sort data=data2 nodupkey;
							by gvkey datadate;
						run;
						data data2;
							set data2;
							*more for Mohanram score;
						if roa>md_roa then m1=1; else m1=0;
						if cfroa>md_cfroa then m2=1; else m2=0;
						if oancf>ni then m3=1; else m3=0;
						if xrdint>md_xrdint then m4=1; else m4=0;
						if capxint>md_capxint then m5=1; else m5=0;
						if xadint>md_xadint then m6=1; else m6=0;
							*still need to add another thing for Mohanram (2005) score;
						run;
						proc sort data=data2 nodupkey;
						by gvkey datadate;
						run;
*consumer price index to create orgcap measure from Bureau of Labor Statistics website;
data cpi;
   infile datalines; 
   input yr 4.0 cpi 10.3;
   datalines;  
2021	270.970
2020	258.811
2019	255.657
2018	251.107
2017	245.120
2016	240.007
2015	237.017
2014	236.736
2013	232.957 
2012	229.594
2011	224.939
2010	218.056
2009	214.537
2008	215.303
2007	207.342
2006	201.6
2005	195.3
2004	188.9
2003	183.96
2002	179.88
2001	177.1
2000	172.2
1999	166.6
1998	163.00
1997	160.5
1996	156.9
1995	152.4
1994	148.2
1993	144.5
1992	140.3
1991	136.2
1990	130.7
1989	124.00
1988	118.3
1987	113.6
1986	109.6
1985	107.6
1984	103.9
1983	99.6
1982	96.5
1981	90.9
1980	82.4
1979	72.6
1978	65.2
1977	60.6
1976	56.9
1975	53.8
1974    49.3
;
run;   
					proc sql;
						create table data2
						as select a.*,b.cpi
						from data2 a left join cpi b
						on a.fyear=b.yr;
						quit;
					proc sort data=data2 nodupkey;
						by gvkey datadate;
						run;
						*finish orgcap measure;
						data data;
							set data2;
						by gvkey datadate;
						retain orgcap_1;
						avgat=((at+lag(at))/2);
						if first.gvkey then orgcap_1=(xsga/cpi)/(.1+.15);
							else orgcap_1=orgcap_1*(1-.15)+xsga/cpi;
						orgcap=orgcap_1/avgat;
						if count=1 then orgcap=.;
						run;
						
*===========================================================

			Now moving past annual Compustat

============================================================;
*===========================================================
		
			Create merge with CRSP

============================================================;
					*======================GET CRSP IDENTIFIER=============================;
					proc sort data=crsp.ccmxpf_linktable out=lnk;
  					where LINKTYPE in ("LU", "LC", "LD", "LF", "LN", "LO", "LS", "LX") and
       					(2021 >= year(LINKDT) or LINKDT = .B) and (1970 <= year(LINKENDDT) or LINKENDDT = .E);
  					by GVKEY LINKDT; run;	    
					proc sql; create table temp as select a.lpermno as permno,b.*
						from lnk a,data b where a.gvkey=b.gvkey 
						and (LINKDT <= b.datadate or LINKDT = .B) and (b.datadate <= LINKENDDT or LINKENDDT = .E) and lpermno ne . and not missing(b.gvkey);
					quit;  
					data temp;
						set temp;
						where not missing(permno);
					run;  	
					*======================================

						Screen on Stock market information: common stocks and major exchanges

					=======================================;
					*----------------------screen for only NYSE, AMEX, NASDAQ, and common stock-------------;
					proc sort data=crsp.mseall(keep=date permno exchcd shrcd siccd) out=mseall nodupkey;
						where exchcd in (1,2,3) or shrcd in (10,11,12);
						by permno exchcd date; run;
					proc sql; create table mseall as 
						select *,min(date) as exchstdt,max(date) as exchedt
						from mseall group by permno,exchcd; quit;    
					proc sort data=mseall nodupkey;
						by permno exchcd; run;
					proc sql; create table temp as select *
						from temp as a left join mseall as b
						on a.permno=b.permno 
						and exchstdt<=datadate<= exchedt; 
					quit; 
					data temp; 
						set temp;
					   	where exchcd in (1,2,3) and shrcd in /*(10,11,12)*/ (10,11) and not missing(permno);
						drop shrcd date siccd exchstdt exchedt;
					run;  			
					proc sort data=temp nodupkey;
						by gvkey datadate;
					run;

*==========================================================================================================

	
				Finalize first Compustat data set
				This is most of the annual compustat variables plus a couple components that still need additional information


==========================================================================================================;	

data temp;
	set temp;
	keep gvkey permno exchcd datadate fyear sic2 sic cnum
	bm cfp ep cashpr dy lev sp roic rd_sale rd_mve agr invest gma
	chcsho lgr egr chinv hire acc pctacc absacc age
	sgr pchsale_pchinvt	pchsale_pchrect	pchgm_pchsale pchsale_pchxsga
	ps  divi divo securedind secured convind grltnoa
	rd chpmia chatoia chempia bm_ia pchcapx_ia tb cfp_ia mve_ia herf
	orgcap m1-m6 
	grcapx depr pchdepr tang 
	sin currat pchcurrat quick pchquick
	salecash salerec saleinv pchsaleinv cashdebt realestate operprof;
	run;	

*========================================================================================================

		Now align the annual Compustat variables in calendar month with the assumption that
		annual information is available with a lag of 6 months (if we had point-in-time we would use that)

=========================================================================================================;
*---------------------------add returns and monthly CRSP data we need later-----------------------------;					
proc sql;
	create table temp2
	as select a.*,b.ret,abs(prc) as prc,shrout,vol,b.date
	from temp a left join crsp.msf b
	on a.permno=b.permno and intnx('MONTH',datadate,7)<=b.date<intnx('MONTH',datadate,20);
	quit;
							*-----------Included delisted returns in the monthly returns--------------------;
							proc sql;
						 	  create table temp2
							      as select a.*,b.dlret,b.dlstcd,b.exchcd
 							     from temp2 a left join crsp.mseall b
							      on a.permno=b.permno and a.date=b.date;
							      quit;	
							data temp2;
								set temp2;
 								if missing(dlret) and (dlstcd=500 or (dlstcd>=520 and dlstcd<=584))
									and exchcd in (1,2) then dlret=-.35;
 								if missing(dlret) and (dlstcd=500 or (dlstcd>=520 and dlstcd<=584))
									and exchcd in (3) then dlret=-.55; *see Johnson and Zhao (2007), Shumway and Warther (1999) etc.;
								if not missing(dlret) and dlret<-1 then dlret=-1;
								if missing(dlret) then dlret=0;
								ret=ret+dlret;
								if missing(ret) and dlret ne 0 then ret=dlret;
								run;
							proc sort data=temp2;
								by permno date descending datadate;
								run;
							proc sort data=temp2 nodupkey;
								by permno date;
							run;	
						*can use monthly market cap and price, but need to lag because it is currently 
						contemporaneous with the returns we want to predict;	
							data temp2;
								set temp2;
								by permno date;
								/*market cap measure*/
								mve_m=abs(lag(prc))*lag(shrout);
								mve=log(mve_m);
								if first.permno then delete;
								run;	
*==============================================================================================================


				Now add in COMPUSTAT QUARTERLY and then add to the monthly aligned dataset


==============================================================================================================;
proc sql;
	create table data
	as select substr(compress(cusip),1,6) as cnum,c.gvkey,fyearq,fqtr,datadate,rdq,substr(sic,1,2) as sic2,

		/*income items*/
			ibq,saleq,txtq,revtq,cogsq,xsgaq,
		/*balance sheet items*/
			atq,actq,cheq,lctq,dlcq,ppentq, 
		/*other*/
	  	abs(prccq)*cshoq as mveq,ceqq,

		seqq,pstkq,atq,ltq,pstkrq

		from comp.company as c, comp.fundq as f
	  where f.gvkey = c.gvkey
	  and f.indfmt='INDL' and f.datafmt='STD' and f.popsrc='D' and f.consol='C'
		and not missing(ibq) and datadate>='01JAN1990'd;
	quit;  	
						proc sort data=data nodupkey;
							by gvkey datadate;
							run;
					
						proc sort data=data ;
							by gvkey datadate;	
							run;										
						*create first set of quarterly compustat variables;
						data data3;
							set data;
							by gvkey datadate;
							retain count;
							if not missing(pstkrq) then pstk=pstkrq;
								else pstk=pstkq;
							scal=seqq;
							if missing(seqq) then scal=ceqq+pstk;
							if missing(seqq) and (missing(ceqq) or missing(pstk)) then scal=atq-ltq;
							chtx=(txtq-lag4(txtq))/lag4(atq);
							roaq=ibq/lag(atq);
							roeq=(ibq)/lag(scal);
							rsup=(saleq-lag4(saleq))/mveq;
							sacc=( (actq-lag(actq) - (cheq-lag(cheq))) - (  (lctq-lag(lctq))-(dlcq-lag(dlcq)) ) ) /saleq; ;
							if saleq<=0 then sacc=( (actq-lag(actq) - (cheq-lag(cheq))) - (  (lctq-lag(lctq))-(dlcq-lag(dlcq)) ) ) /.01;
							stdacc=std(sacc,lag(sacc),lag2(sacc),lag3(sacc),lag4(sacc),lag5(sacc),lag6(sacc),lag7(sacc),
								lag8(sacc),lag9(sacc),lag10(sacc),lag11(sacc),lag12(sacc),lag13(sacc),lag14(sacc),lag15(sacc));
							sgrvol=std(rsup,lag(rsup),lag2(rsup),lag3(rsup),lag4(rsup),lag5(rsup),lag6(rsup),lag7(rsup),
								lag8(rsup),lag9(rsup),lag10(rsup),lag11(rsup),lag12(rsup),lag13(rsup),lag14(rsup));
							roavol=std(roaq,lag(roaq),lag2(roaq),lag3(roaq),lag4(roaq),lag5(roaq),lag6(roaq),lag7(roaq),
								lag8(roaq),lag9(roaq),lag10(roaq),lag11(roaq),lag12(roaq),lag13(roaq),lag14(roaq),lag15(roaq));
							scf=(ibq/saleq)-sacc;
							if saleq<=0 then scf=(ibq/.01)-sacc;
							stdcf=std(scf,lag(scf),lag2(scf),lag3(scf),lag4(scf),lag5(scf),lag6(scf),lag7(scf),
								lag8(scf),lag9(scf),lag10(scf),lag11(scf),lag12(scf),lag13(scf),lag14(scf),lag15(scf));
							cash=cheq/atq;
							cinvest=((ppentq-lag(ppentq))/saleq)-mean(((lag(ppentq)-lag2(ppentq))/lag(saleq)),((lag2(ppentq)-lag3(ppentq))/lag2(saleq)),((lag3(ppentq)-lag4(ppentq))/lag3(saleq)));
								if saleq<=0 then cinvest=((ppentq-lag(ppentq))/.01)-mean(((lag(ppentq)-lag2(ppentq))/(.01)),((lag2(ppentq)-lag3(ppentq))/(.01)),((lag3(ppentq)-lag4(ppentq))/(.01)));
	
							*for nincr;
							nincr	=(  (ibq>lag(ibq))
			+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))
			+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))
			+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))
			+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))*(lag4(ibq)>lag5(ibq))
			+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))*(lag4(ibq)>lag5(ibq))*(lag5(ibq)>lag6(ibq))
			+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))*(lag4(ibq)>lag5(ibq))*(lag5(ibq)>lag6(ibq))*(lag6(ibq)>lag7(ibq))
			+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))*(lag4(ibq)>lag5(ibq))*(lag5(ibq)>lag6(ibq))*(lag6(ibq)>lag7(ibq))*(lag7(ibq)>lag8(ibq))  );

						*clean up;
						if first.gvkey then count=1;
							else count+1;

						if first.gvkey then do;
							roaq=.;
							roeq=.;
						end;
						if count<5 then do;
							chtx=.;
							cinvest=.;
						end;
						if count<17 then do;
							stdacc=.;
							stdcf=.;
							sgrvol=.;
							roavol=.;
						end;

					run;
					*finally finish Mohanram score components;
					proc sort data=data3;
						by fyearq fqtr sic2;
						run;
					proc univariate data=data3 noprint;
						by fyearq fqtr sic2;
						var roavol sgrvol ;
						output out=indmd median=md_roavol md_sgrvol;
					run;
					proc sql;
						create table data3
						as select *
						from data3 a left join indmd b
						on a.fyearq=b.fyearq and a.fqtr=b.fqtr and a.sic2=b.sic2;
					quit;
					proc sort data=data3 nodupkey;
						by gvkey fyearq fqtr;
						run;
					data data3;
						set data3;
						if roavol<md_roavol then m7=1; else m7=0;
						if sgrvol<md_sgrvol then m8=1; else m8=0;
					run;
					
					proc sort data=data3 nodupkey;
						by gvkey datadate;
					run;
					*get permno for CRSP data;
					proc sql; create table data5 as select a.lpermno as permno,b.*
						from lnk a,data3 b where a.gvkey=b.gvkey 
						and (LINKDT <= b.datadate or LINKDT = .B) and (b.datadate <= LINKENDDT or LINKENDDT = .E) and lpermno ne . and not missing(b.gvkey);
					quit;  
					data data5;
						set data5;
						where not missing(permno);
						run;  
data data5;
	set data5;
	where not missing(rdq);  *seems like a reasonable screen at this point to make sure have at least some of this information;
	run;

*=============================================

		Some of the RPS require daily CRSP data in conjunction with Compustat quarterly,
		so add daily CRSP info to create these RPS

==============================================;
*this is for abnormal trading volume and returns around earings announcements;
proc sql;
	create table data5 
	as select a.*,b.vol
	from data5 a left join crsp.dsf b
	on a.permno=b.permno and
     intnx('WEEKDAY',rdq,-30)<=b.date<=intnx('WEEKDAY',rdq,-10);
	quit; 	
						proc sql;
							create table data5
							as select *,mean(vol) as avgvol
							from data5
						group by permno,datadate,rdq;
						quit;
						proc sort data=data5(drop=vol) nodupkey;
						where not missing(rdq);
						by permno datadate rdq;
						run;    									
						proc sql;
						create table data6 
						as select a.*,b.vol,b.ret
						from data5 a left join crsp.dsf b
						on a.permno=b.permno and
   						  intnx('WEEKDAY',rdq,-1)<=b.date<=intnx('WEEKDAY',rdq,1);
						quit;
						proc sql;
						create table data6
						as select *,(mean(vol)-avgvol)/avgvol as aeavol,sum(ret) as ear
						from data6
						group by permno,datadate,rdq;
						quit;
						proc sort data=data6(drop=vol avgvol ret) nodupkey;
						by permno datadate rdq;
						run;
*================================================================================


		First Compustat quarterly data set

================================================================================;
data data6;
	set data6;
	keep gvkey permno datadate rdq
	chtx roaq rsup stdacc stdcf roavol cash cinvest nincr 
	aeavol ear m7 m8 roeq;
	run;
*==============================================================================

	add quarterly compustat data to monthly returns and annual compustat data

===============================================================================;

proc sql;
	alter table temp2
	drop datadate;

	create table temp3
	as select *
	from temp2 a left join data6 b
	on a.permno=b.permno and
     intnx('MONTH',a.date,-10)<=b.datadate<=intnx('MONTH',a.date,-5,'beg');*allow at least four months for quarterly info to become available; *date is the end of the return month;
	quit;		

				proc sort data=temp3;
					by permno date descending datadate;
					run;
				proc sort data=temp3 nodupkey;
					by permno date;
					run;
				*finally finish Mohanram score;
				data temp3;
				set temp3;
					ms=m1+m2+m3+m4+m5+m6+m7+m8;
				drop m1-m8;
				run;		 
*======================================================================================================

				There are some other variables that are based on monthly-CRSP information (already in the dataset from monthly CRSP)
				create those variables plus a couple of others

======================================================================================================;  
data temp4;
	set temp3;
*count to make sure we have enough time series for each firm to create variables;
	where not missing(ret);
	by permno date;
	retain count;
		if first.permno then count=1;
		else count+1;
	run;
proc sql;
	create table temp4
	as select *,mean(ret) as ewret  /*we have used this before, doesn't seem to make a big difference in the variables*/
	from temp4
	group by date;
	quit;

proc sort data=temp4;
	by permno date;
	run;
data temp5;
	set temp4;
	where not missing(ret);
	by permno date;
	retain count;
		if first.permno then count=1;
		else count+1;
	run;
data temp6;
	set temp5;
		mom6m=  (  (1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret)) ) - 1;
		mom12m=  (   (1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret))*
			(1+lag7(ret))*(1+lag8(ret))*(1+lag9(ret))*(1+lag10(ret))*(1+lag11(ret))*(1+lag12(ret))   ) - 1;
		mom36m=(   (1+lag13(ret))*(1+lag14(ret))*(1+lag15(ret))*(1+lag16(ret))*(1+lag17(ret))*(1+lag18(ret))   *
			(1+lag19(ret))*(1+lag20(ret))*(1+lag21(ret))*(1+lag22(ret))*(1+lag23(ret))*(1+lag24(ret))*
			(1+lag25(ret))*(1+lag26(ret))*(1+lag27(ret))*(1+lag28(ret))*(1+lag29(ret))*(1+lag30(ret))     *
			(1+lag31(ret))*(1+lag32(ret))*(1+lag33(ret))*(1+lag34(ret))*(1+lag35(ret))*(1+lag36(ret))  ) - 1;
		mom1m=	lag(ret);
		dolvol=log(lag2(vol)*lag2(prc));
		chmom =(   (1+lag(ret))*(1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret))   ) - 1
			- ((  (1+lag7(ret))*(1+lag8(ret))*(1+lag9(ret))*(1+lag10(ret))*(1+lag11(ret))*(1+lag12(ret))   ) - 1); 
		turn=mean(lag(vol),lag2(vol),lag3(vol))/shrout;

	if count=1 then mom1m=.;
	if count<13 then do;
			mom12m=.;
			chmom=.;
	end;
	if count<7 then mom6m=.;
	if count<37 then mom36m=.;
	if count<3 then dolvol=.;
	if count<4 then turn=.;
	run;
					proc sql;
						create table temp5
						as select *,mean(mom12m) as indmom
						from temp6 
						group by sic2,date;
					quit;				
*=====================================================================================================================


			finally, a few more directly from daily CRSP to create monthly variables


======================================================================================================================;       
proc sql;
	create table dcrsp
	as select permno,year(date) as yr,month(date) as month,max(ret) as maxret,std(ret) as retvol,
			mean((askhi-bidlo)/((askhi+bidlo)/2)) as baspread,
			std(log(abs(prc*vol))) as std_dolvol,std(vol/shrout) as std_turn,
			mean(abs(ret)/(abs(prc)*vol)) as ill,
			sum(vol=0) as countzero,n(permno) as ndays,sum(vol/shrout) as turn
	from crsp.dsf
	group by permno,year(date),month(date)
	having year(date)>=1990;
	quit;
					proc sort data=dcrsp nodupkey;
						by permno yr month;
					run;		
					data dcrsp;
						set dcrsp;
						zerotrade=(countzero+((1/turn)/480000))*21/ndays;
						run;
					*match to prior month to use lagged variables to predict returns;
					proc sql;
						create table temp6
						as select a.*,b.maxret,b.retvol,baspread,std_dolvol,std_turn,ill,zerotrade
						from temp5 a left join dcrsp b
						on a.permno=b.permno and year(intnx('MONTH',date,-1))=b.yr
						and month(intnx('MONTH',date,-1))=b.month;
					quit;
					proc sort data=temp6 nodupkey;
						by permno date;
						run;
					*---create beta from weekly returns---;			
					proc sql;
						create table dcrsp
						as select permno,intnx('WEEK',date,0,'end') as wkdt,
						exp(sum(log(1+(ret))))-1 as wkret
					from crsp.dsf
					group by permno,calculated wkdt;
					quit;
					proc sort data=dcrsp nodupkey;
					where wkdt>='01JAN1990'd;
					by permno wkdt;
					run;				
					proc sql;
						create table dcrsp
						as select *,mean(wkret) as ewret
						from dcrsp
						group by wkdt;
					quit;
					data dcrsp;
						set dcrsp;
						where not missing(wkret) and not missing(ewret);
						run;
					proc sort data=temp6 out=lst(keep=permno date) nodupkey;
						where not missing(permno) and not missing(date);
						by permno date;
					run;					
					proc sql;
					create table betaest
						as select a.*,b.wkret,b.ewret as ewmkt,b.wkdt
					from lst a left join dcrsp b
					on a.permno=b.permno and intnx('MONTH',date,-36)<=wkdt<=intnx('MONTH',date,-1);
					quit;					*3 years of weekly returns;
					proc sql;
					create table betaest
					as select *
					from betaest
					group by permno,date
					having n(wkret)>=52;
					quit;				*require at least 1 year of weekly returns;
					proc sort data=betaest;
						by permno date wkdt;
					run;
					data betaest;
						set betaest;
						by permno date wkdt;
						retain count;
						ewmkt_l1=lag(ewmkt);
						ewmkt_l2=lag2(ewmkt);
						ewmkt_l3=lag3(ewmkt);
						ewmkt_l4=lag4(ewmkt);
						if first.date then count=1;
							else count+1;
						if count<5 then do;
						ewmkt_l1=.;
						ewmkt_l2=.;
						ewmkt_l3=.;
						ewmkt_l4=.;
						end;
						run;
					proc reg data=betaest outest=est noprint;
						by permno date;
						model wkret=ewmkt/adjrsq;
					output out=idiovolest residual=idioret;
					run;			*two different approaches, one typical, the other including lagged market values to use as price delay measure;
					proc reg data=betaest outest=est2 noprint;
						by permno date;
						model wkret=ewmkt ewmkt_l1 ewmkt_l2 ewmkt_l3 ewmkt_l4/adjrsq;
					output out=idiovolest residual=idioret;
					run;
					proc sql;
						create table idiovolest
						as select permno,date,std(idioret) as idiovol
						from idiovolest
						group by permno,date;
						quit;
					proc sort data=idiovolest nodupkey;
						where not missing(idiovol);
						by permno date;
						run;
					data est;
						set est;
						where not missing(permno) and not missing(date);
						beta=ewmkt;
					run;								
					proc sql;
					create table temp7
					as select a.*,b.beta,b.beta*b.beta as betasq,_adjrsq_ as rsq1
					from temp6 a left join est b
					on a.permno=b.permno and a.date=b.date;
					quit;
					proc sql;
					create table temp7
					as select a.*,	1-(	rsq1 / _adjrsq_) as pricedelay
					from temp7 a left join est2 b
					on a.permno=b.permno and a.date=b.date;
					quit;
					proc sql;
					create table temp7
					as select a.*,b.idiovol
					from temp7 a left join idiovolest b
					on a.permno=b.permno and a.date=b.date;
					quit;
					proc sort data=temp7 nodupkey;
						where  year(date)>=1990;
						by permno date;
						run;
*=============================================================================================


	So there we go, save this monster
	
=============================================================================================;
data temp;
	set temp7;
	where not missing(mve) and not missing(mom1m) and not missing(bm);
	run;

libname myHome '/home/unisg/balzers';
data myHome.data01_green_feng_test;
	set temp;
run;