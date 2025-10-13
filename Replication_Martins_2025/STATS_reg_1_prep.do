clear all
local HOME "/Users/tgst/Library/CloudStorage/Dropbox/PhD_Economics/research/my_projects/3rd_year_project_3rd_year_project/125_LaborIncomeDynamics/Data/Replication_Martins_2025"
cd `HOME'

use DATA_migration_regs.dta,clear


**** Basic cleaning
keep if ano>2014
drop if nqual2==.
gen migrant = nacio!="" & nacio!="PT"


**** Wage
gen lw = log(totalwage/totalhours)

**** Immigrant share by cell
bysort ano nuts2_est cae2_est nqual2: gen _Ncell = _N
bysort ano nuts2_est cae2_est nqual2: egen immigrants = total(migrant)
gen immigrant_share = immigrants/_Ncell


**** Bartik IV

** market id
egen market = group(nuts2_est cae2_est nqual2), label

** base share
preserve
	keep if ano==2016
	collapse (mean) s2016 = immigrant_share, by(market)
	tempfile base2016
	save `base2016'
restore
merge m:1 market using `base2016', keep(match master) nogen

** total immigrant growth: G_t
bysort ano: egen M_t = total(migrant)
quietly summarize M_t if ano==2016, meanonly
scalar M2016 = r(mean)
gen G_t = M_t / M2016 - 1

** Bartik instrument B_mt = s2016 * G_t
drop if missing(s2016) | s2016==0   // drop markets with zero base share
gen bartik = s2016 * G_t
label var s2016 "Immigrant share in market (base=2016)"
label var G_t   "Total immigrant growth since 2016"
label var bartik "Bartik IV: s2016 * G_t"

** Inspect cell-level series
preserve
	collapse (first) s2016 (first) G_t (first) bartik (mean) immigrant_share, by(ano nuts2_est cae2_est nqual2 market)
	order ano nuts2_est cae2_est nqual2 s2016 G_t bartik immigrant_share
	keep if ano==2017
	list in 1/10, abbrev(20)
restore

** Bartik instrument for 2015
preserve
	keep if ano==2015
	collapse (mean) s2015 = immigrant_share, by(market)
	tempfile base2015
	save `base2015'
restore
merge m:1 market using `base2015', keep(master match) nogen
gen bartik2015 = s2015 * G_t if !missing(s2015) & s2015>0
label var bartik2015 "Bartik IV: s2015 * G_t"


**** Controls
gen age=idade if idade>17 & idade<68
gen age2 = age^2

gen tenure=antig if antig<idade-16
gen tenure2=tenure^2

gen educ=habil2_quant
gen educ2=educ^2

gen sex=sexo
gen byte female = (sex==2) if !missing(sex)

gen log_estemp=log(pest)

gen log_firmrevworker=log(vendas/pemp) if vendas>0

tempvar one emp_rs emp_rs2016
gen `one' = 1
bys ano nuts2_est cae2_est: egen `emp_rs' = total(`one')
preserve
	keep if ano==2016
	bys nuts2_est cae2_est: egen `emp_rs2016' = total(`one')
	keep nuts2_est cae2_est `emp_rs2016'
	duplicates drop
	tempfile base_rs
	save `base_rs'
restore
merge m:1 nuts2_est cae2_est using `base_rs', nogen
gen ldem_growth = (`emp_rs' / `emp_rs2016') - 1 if `emp_rs2016'>0
drop `one' `emp_rs' `emp_rs2016'


**** Save cleaned
order ntrab emp_id estab_id ano market nuts2_est cae2_est nqual2 migrant immigrant_share s2016 G_t bartik bartik2015 age* tenure* educ* sex female log_estemp log_firmrevworker ldem_growth lw
keep ntrab emp_id estab_id ano market nuts2_est cae2_est nqual2 migrant immigrant_share s2016 G_t bartik bartik2015 age* tenure* educ* sex female log_estemp log_firmrevworker ldem_growth lw
save DATA_migration_regs_final.dta, replace
