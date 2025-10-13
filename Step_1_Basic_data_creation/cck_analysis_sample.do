* analysis_sample.do - produce analysis dataset 
* David Card, Ana Rute Cardoso, Patrick Kline
* "Bargaining, Sorting, and the Gender Wage Gap: Quantifying the Impact of Firms on the Relative Pay of Women" 
* QJE 22684

set more 1
program drop _all
capture log close
log using ..\logs\analysis_sample.log, replace


*** workers 2002-09

use idtrab ano gender datanasc educ using ..\data\worker_demog02_09.dta, clear
merge idtrab ano using ..\data\worker_hire02_09.dta
recast double idemp
recast double idtrab

* age

gen birthmonth=datanasc-(int(datanasc/100))*100
gen birthyear=int(datanasc/100)
gen birthddate=mdy(birthmonth,01,birthyear)
format birthddate %td
gen age=int((mdy(10,01,ano)-birthddate)/365)

* date hire

gen hiremonth=datehire-(int(datehire/100))*100
gen hireyear=int(datehire/100)
gen hireddate=mdy(hiremonth,01,hireyear)
format hireddate %td

keep     ano idtrab idemp gender age educ hireddate
compress ano              gender age educ hireddate
save ..\data\tmpworker_2002_09.dta, replace


* yearly employment data

foreach yr of numlist 2002(1)2009 {
   use ..\data\trabs`yr'_SelectionVars.dta, clear
   bys idtrab idemp: keep if _n==1
   bys idemp: gen sizeQP=_N
   
   recode hnorm 0=.
   recode wbase 0=.
   gen wh=(wbase+wregul)/hnorm
   
   gen ft=1 if hnorm>=35*4 & hnorm!=.
   replace ft=0 if hnorm<35*4

   gen occup1=int(occup3/100)

   keep     ano idtrab idemp wbase wh hnorm hextra ft occup3 occup1 sizeQP
   compress ano              wbase wh hnorm hextra ft occup3 occup1 sizeQP
   save ..\data\tmpempl`yr'.dta, replace
}

foreach yr of numlist 2002(1)2008 {
   append using ..\data\tmpempl`yr'.dta
}

merge 1:1 idtrab idemp ano using ..\data\tmpworker_2002_09.dta
keep if _merge==3


* deflate w and drop outliers

program define deflateIPC
	gen     lr`1'=log(`1')+0.1518 if ano==2002
	replace lr`1'=log(`1')+0.1193 if ano==2003
	replace lr`1'=log(`1')+0.0956 if ano==2004
	replace lr`1'=log(`1')+0.0729 if ano==2005
	replace lr`1'=log(`1')+0.0423 if ano==2006
	replace lr`1'=log(`1')+0.0176 if ano==2007
	replace lr`1'=log(`1')-0.0080 if ano==2008
	replace lr`1'=log(`1')+0.0000 if ano==2009
	gen r`1'=exp(lr`1')
end
deflateIPC wh

gen tmpdrop=.
foreach yr of numlist 2002(1)2009 {
   qui sum rwh if ano==`yr', detail
   replace tmpdrop=1 if (rwh>3*r(p99) | rwh<0.80*r(p1)) & rwh!=. & ano==`yr' 
}
egen tmpdrop2=sum(tmpdrop), by(idtrab)
drop if tmpdrop2>0
drop tmp*

xtset idtrab ano
gen Dlwage = D.lrwh
gen tmpdrop=1 if Dlwage<-1 | (Dlwage>1 & Dlwage!=.)
egen tmpdrop3=sum(tmpdrop), by(idtrab)
drop if tmpdrop3>0
drop tmp*


* wage-earner aged 19-65, potential experience>=2

keep if age>=19 & age<=65
keep if age-educ-6>1 & age-educ-6!=.
keep if lrwh!=.


* educ 0-4 years lumped together 

recode educ 0=4


* min w

gen wmin=.
replace wmin=348.01 if ano==2002
replace wmin=356.60 if ano==2003
replace wmin=365.60 if ano==2004
replace wmin=374.70 if ano==2005
replace wmin=385.90 if ano==2006
replace wmin=403.00 if ano==2007
replace wmin=426.00 if ano==2008
replace wmin=450.00 if ano==2009

 
* pink / blue occupation

egen sharefem_occup = mean(gender), by(occup3)
egen sharefem_occup_avperson = mean(sharefem_occup), by(idtrab)
quietly sum sharefem_occup, detail
gen pink=1     if sharefem_occup_avperson>=r(p50) & sharefem_occup_avperson!=.
replace pink=0 if sharefem_occup_avperson<r(p50)  & sharefem_occup_avperson!=.


* wage of co-workers

egen wtot = total(lrwh), by(ano idemp)
gen wtot_peers= wtot-lrwh
bys ano idemp: gen Npeers=_N-1
gen lwpeers = wtot_peers / Npeers


* firm size analysis sample

bys idemp ano: gen sizeanalysis=_N 


* labels

ren ano year
ren idtrab idworker
ren idemp idfirm
ren lrwh lwage

label variable idworker    "worker id"
label variable idfirm      "firm id"
label variable gender      "female"
label variable educ        "education completed (yrs)"
label variable hireddate   "date hire (stata format)"
label variable lwage       "(log) real hourly wage"
label variable Dlwage      "change in (log) real hourly wage"
label variable wmin        "monthly min wage"
label variable wbase       "monthly base wage"
label variable hnorm       "normal monthly hours work"
label variable hextra      "overtime monthly hours work"
label variable occup3      "occupation (3 digits)"
label variable occup1      "occupation (1 digit)"
label variable pink        "worker in predomin. female occupation"
label variable lwpeers     "av. (log) wage co-workers"

keep     year idworker idfirm gender age educ wbase wmin lwage Dlwage hnorm hextra ft occup3 occup1 pink lwpeers hireddate sizeQP sizeanalysis
compress year                 gender age educ wbase wmin lwage Dlwage hnorm hextra ft occup3 occup1 pink lwpeers hireddate sizeQP sizeanalysis 
sum
save ..\data\workers_analysis_02_09.dta, replace



*** firms 2002-09

* from worker level data, share females and size

use ..\data\workers_analysis_02_09.dta, clear
collapse sharefemales=gender sizeQP sizeanalysis, by(idfirm year)
save ..\data\tmpfirm_workers.dta, replace


* firm yearly data

forvalues yr=2002(1)2009 {
   use ..\data\empresa`yr'_SelectionVars.dta, clear
   ren idemp idfirm
   ren ano year
   ren caem ind
 
   gen lisbon=1 if nutsm3==171 | nutsm3==172
   gen oporto=1 if nutsm3==114
   recode lisbon .=0
   recode oporto .=0
   label variable lisbon "Lisbon (incl. Setubal)"
   label variable oporto "Oporto" 
 
   recode ind 3401 = 2400 2300 3100 3402 3500 3600 3800 . = 9999

   keep year idfirm vendas lisbon oporto ind
   compress year    vendas lisbon oporto ind
   save ..\data\tmpfirm`yr'.dta, replace
}

foreach yr of numlist 2002(1)2008 {
   append using ..\data\tmpfirm`yr'.dta
}


* sales year t

sort year idfirm
xtset idfirm year

gen sales=vendas/1000
gen salesQP=F.sales


* add share females and size

merge 1:1 idfirm year using ..\data\tmpfirm_workers.dta
drop _merge


* average yearly employment

egen avempl = mean(sizeanalysis), by(idfirm)


* add BvD data

merge 1:1 idfirm year using ..\data\firm_BvD_02_09.dta, keep(match master)


* number of variables in key matching QP-BvD

gen nkeyvarsQP_BvD=5 if strpos(keyvars,"5")!=0
foreach n in 2 3 4 {
	replace nkeyvarsQP_BvD=`n' if strpos(keyvars,"`n'")!=0
}


* deflate sales and VA

recode salesQP 0=.
recode VA 0=.

program define deflateGDPd  
	gen     r`1'=`1'*1.162 if year==2002
	replace r`1'=`1'*1.152 if year==2003
	replace r`1'=`1'*1.127 if year==2004
	replace r`1'=`1'*1.086 if year==2005
	replace r`1'=`1'*1.042 if year==2006
	replace r`1'=`1'*1.014 if year==2007
	replace r`1'=`1'*0.962 if year==2008
	replace r`1'=`1'*1.000 if year==2009
end
deflateGDPd salesQP
deflateGDPd VA


* per capita Sales and VA, trim outliers

gen lsalesQPpe = log(rsalesQP / sizeQP)
gen lVApe      = log(rVA / sizeBvD)

foreach v in lVApe lsalesQPpe {
   forvalues y=2002(1)2009  {
      qui sum `v' if year==`y', detail
      replace `v'=. if (`v'<=r(p1) | `v'>=r(p99)) & year==`y'
   }
}

keep if sizeanalysis!=.

* labels

label variable idfirm      "firm id"
label variable ind          "industry (standard across years)"
label variable lVApe        "(log) real value added per employee, BvD"
label variable lsalesQPpe   "(log) real sales per employee, QP"
label variable sizeQP       "firm size QP"
label variable sizeanalysis "firm size analysis sample"
label variable avempl       "firm av. yearly employment"
label variable sharefemales "share of females in firm (yearly)"
label variable nkeyvars     "nb. vars. matching QP-BvD"

keep      year idfirm lisbon oporto ind sizeQP sizeanalysis avempl sharefemales lVApe lsalesQPpe nkeyvars
compress  year        lisbon oporto ind sizeQP sizeanalysis avempl sharefemales lVApe lsalesQPpe nkeyvars
sum
save ..\data\firms_analysis_02_09.dta, replace



*** export to run AKMs

use idworker idfirm year lwage gender age educ pink sizeanalysis using ..\data\workers_analysis_02_09.dta, clear

ren idworker id
ren idfirm firmid
ren lwage y
ren size fsize_ckc

recode firmid  653075 = 88888888

keep     id year firmid y gender age educ pink fsize_ckc  
order    id year firmid y gender age educ pink fsize_ckc  
compress    year        y gender age educ pink fsize_ckc  
sort id year
save ..\data\tmpPT2002_2009.dta, replace

bys gender: sum

program define toakm
   use ../data/tmpPT2002_2009.dta if gender==`1', clear
   drop gender
   outfile using ../data/PT_2002_2009_`2'_commadelimited, wide comma replace
end

toakm 0 males
toakm 1 females


log close

