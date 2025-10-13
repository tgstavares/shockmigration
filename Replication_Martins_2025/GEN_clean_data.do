clear all
local HOME "/Users/tgst/Library/CloudStorage/Dropbox/PhD_Economics/research/my_projects/3rd_year_project_3rd_year_project/125_LaborIncomeDynamics/Data/Replication_Martins_2025"
cd `HOME'

local bano=2010


**** Load data
use ../DATA_WORKER_EXCTRACT.dta, clear


**** Create new vars
gen totalwage = ganho+rpirg
gen totalhours = nhnor+nhext


**** Basic cleaning
drop if ano < `bano' // no need of data before 2017
*drop if estab_id < 0 // this is done by the author	
capture drop ctcont tipo_contr tipo_contr1 // no need of tipo contrato
capture keep if stpro == 3 // only employees
capture drop stpro
capture drop nuest // ids on different variable
capture drop nuemp // ids on differen


**** Heavy cleaning
{		
	keep if ctrem <=1 // only workers with remuneration
	gen aux = .
	bys ntrab ano: replace aux=_N // this is eliminating workers with two or more jobs
	drop if aux>1
	drop aux
	gen firm_size_derived=.
	forvalues a=`bano'/2023{
		*bys ano ntrab emp_id: drop if _n>1 & ano==`a'
		bys ano emp_id: replace firm_size_derived=_N if ano==`a'
	}

	drop if totalwage == . | totalhours == .

	gen aux1=.
	gen aux2=.
	forvalues a=`bano'/2023{
	   quietly sum totalwage if ano==`a', detail
	   replace aux1=1 if (totalwage>r(p99) | totalwage<r(p1)) & ano==`a' 
	   quietly sum totalhours if ano==`a', detail
	   replace aux2=1 if (totalhours>r(p99) | totalhours<r(p1)) & ano==`a' 
	}
	bys ntrab: egen aux11=sum(aux1)
	bys ntrab: egen aux22=sum(aux2)
	drop if aux11 > 0
	drop if aux22 > 0
	drop aux*		
}


**** Drop inconsistent age observations
sort ntrab ano
by ntrab (ano): gen age_down = (idade < idade[_n-1]) if _n>1 & !missing(idade, idade[_n-1])
by ntrab: egen bad_age_id = max(age_down)
drop if bad_age_id==1
drop age_down bad_age_id


**** Deflate wage
merge m:1 ano using "../DATA_DEFLATORS.dta"
drop _merge
drop if ano<`bano'
gen priceindex=B1GQ_PD20_EUR
replace totalwage = totalwage/priceindex/100 // gdp deflator
label variable totalwage "Total monthly wage income (regular, extended, extraordiary) deflated using 2023 GDP"
label variable totalhours "Total monthly work hours (regular, extended, extraordiary)"
drop D1_CP_MEUR-B1G_PD20_EUR

save DATA_migration_regs.dta,replace


**** Do some merges
use ../DATA_FIRM_EXCTRACT.dta, clear
drop if ano<`bano'
drop ano_const nuemp
save temp_firm.dta, replace
use ../DATA_ESTAB_EXCTRACT.dta, clear
drop if ano<`bano'
drop nuemp nuest
save temp_estab.dta, replace
use DATA_migration_regs.dta,clear

merge m:1 ano emp_id using "temp_firm.dta"
gen aux1=_merge
drop _merge
merge m:1 ano emp_id estab_id using "temp_estab.dta"
gen aux2=_merge
drop _merge
keep if aux1==3 & aux2==3
drop aux*

replace vendas=vendas/priceindex
drop priceindex


**** Cleaning based on previous merges
drop if nuts2_est>19 // no regions nor abroad


**** Fixing some variables: sex, nacionality, education
sort ntrab ano
ren sexo sexo2
ren nacio nacio2
by ntrab: egen sexo = mode(sexo2)
by ntrab: egen nacio = mode(nacio2)
gen aux_sexo=(sexo~=sexo2)
gen aux_nacio=(nacio~=nacio2)
by ntrab: egen aux2_sexo = mean(aux_sexo)
by ntrab: egen aux2_nacio = mean(aux_nacio)
replace aux2_sexo=(aux2_sexo>0)
replace aux2_nacio=(aux2_nacio>0)
sort ntrab ano
by ntrab: replace sexo=sexo2 if sexo==. & sexo2~=. & _n==1
by ntrab: replace nacio=nacio2 if nacio=="" & nacio2~="" & _n==1
sort ntrab ano
by ntrab: replace sexo=sexo[_n-1] if sexo==. & sexo[_n-1]~=.
by ntrab: replace nacio=nacio[_n-1] if nacio=="" & nacio[_n-1]~=""
drop aux*
*drop sexo2 nacio2
clonevar habil2_der = habil2
gsort ntrab -ano
by ntrab: replace habil2_der=habil2_der[_n-1] if (habil2_der==0 | habil2_der==90 | habil2_der==.) & (habil2_der[_n-1]~=0 & habil2_der[_n-1]~=90)
gsort ntrab ano
by ntrab: replace habil2_der=habil2_der[_n-1] if (habil2_der==0 | habil2_der==90 | habil2_der==.) & (habil2_der[_n-1]~=0 & habil2_der[_n-1]~=90)
*by ntrab (ano): gen educ_down = (habil2_der < habil2_der[_n-1]) if _n>1 & !missing(habil2_der, habil2_der[_n-1])
by ntrab: replace habil2_der=habil2_der[_n-1] if habil2_der<habil2_der[_n-1] & habil2_der[_n-1]~=.


**** More merges
merge m:1 nacio using "../Aux_datasets/NACIO_GROUPS.dta" // verify matching and eventually adjust on the NACIO_GROUPS.dta
drop if _merge==2
drop _merge	


**** Apply some codebooks
label define nqual1_lbl ///
    1 "Quadros superiores" ///
    2 "Quadros médios" ///
    3 "Encarregados, Contramestres, Mestres, Chefes de equipa" ///
    4 "Profissionais Altamente Qualificados" ///
    5 "Profissionais Qualificados" ///
    6 "Profissionais Semiqualificados" ///
    7 "Profissionais não qualificados" ///
    8 "Praticantes e Aprendizes" ///
    9 "Ignorado"

label values nqual1 nqual1_lbl
gen nqual2 = 1 if nqual1<=4
replace nqual2 = 2 if nqual1==5
replace nqual2 = 3 if nqual1>5 & nqual1<=7

gen habil2_quant = .
replace habil2_quant= 2 if habil2_der==11
replace habil2_quant= 4 if habil2_der==21
replace habil2_quant= 6 if habil2_der==22
replace habil2_quant= 9 if habil2_der==23
replace habil2_quant=12 if habil2_der==31
replace habil2_quant=13 if habil2_der==40
replace habil2_quant=14 if habil2_der==50
replace habil2_quant=15 if habil2_der==60
replace habil2_quant=17 if habil2_der==70
replace habil2_quant=20 if habil2_der==80


compress _all
save DATA_migration_regs.dta,replace
shell rm temp_*
