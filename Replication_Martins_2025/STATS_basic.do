clear all
local HOME "/Users/tgst/Library/CloudStorage/Dropbox/PhD_Economics/research/my_projects/3rd_year_project_3rd_year_project/125_LaborIncomeDynamics/Data/Replication_Martins_2025"
cd `HOME'

use DATA_migration_regs.dta,clear


keep if ano>2015

gen migrant = nacio!="" & nacio!="PT"
bysort ano: gen N_all = _N
bysort ano: egen N_mig = total(migrant)

preserve
keep if ano==2017 & migrant
contract nacio_group, freq(freq2017)
gsort -freq2017
local NN=_N
keep in 1/`NN'
local top
forvalues i=1/`NN'{
	levelsof nacio_group in `i'/`i', local(c) clean
	local top `top' nacio_group=="`c'" |	
}
restore

gen double w_all = 1/N_all
gen double w_mig = cond(migrant, 1/N_mig, .)

**** Migrant quantity
*capture log close
*quietly log using "log1_tables_migration.txt", text replace
table (nacio_group) (ano) if (`top') & nacio!="PT" & ano>2014, statistic(frequency) totals(ano)
table (nacio_group) (ano) if (`top') & nacio!="PT" & ano>2014, statistic(sum w_mig) totals(ano) nformat(%9.3g sum_w_mig)
table (nacio_group) (ano) if (`top') & nacio!="PT" & ano>2014, statistic(sum w_all) totals(ano) nformat(%9.3g sum_w_all)
*quietly log off

**** Migrant quality
bys ano migrant: egen N_ym = count(nqual2)
gen double w_ym = cond(!missing(nqual2) & N_ym>0, 1/N_ym, .)
*quietly log on
table (migrant nqual2) (ano) if !missing(nqual2), statistic(sum w_ym) nototals
*quietly log off

**** Migrant price
gen lw  = log(totalwage/totalhours)
*quietly log on
table (migrant) (ano), statistic(mean lw) statistic(p50 lw) statistic(sd lw) nototals
table (migrant nqual2) (ano), statistic(mean lw) statistic(p50 lw) statistic(sd lw) nototals
*quietly log close
