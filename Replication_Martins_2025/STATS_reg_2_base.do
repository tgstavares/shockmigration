clear all
local HOME "/Users/tgst/Library/CloudStorage/Dropbox/PhD_Economics/research/my_projects/3rd_year_project_3rd_year_project/125_LaborIncomeDynamics/Data/Replication_Martins_2025"
cd `HOME'

local idw ntrab
local ide estab_id
use DATA_migration_regs_final.dta,clear

keep if migrant==0 & ano>=2017
keep if !missing(immigrant_share, bartik, `ide', `idw')

local X age age2 tenure tenure2 educ educ2 female log_estemp log_firmrevworker ldem_growth

// tic
// reghdfe lw immigrant_share `X', absorb(ano) vce(cluster `idw')
// eststo c1
// toc
//
// tic
// reg lw immigrant_share `X' i.ano, vce(cluster `idw')
// eststo c2
// toc
//
// tic
// areg lw immigrant_share `X', absorb(ano) vce(cluster ntrab)
// eststo c3
// toc
//
//
tic
foreach v in lw immigrant_share `X' {
	display "`v'"
    quietly areg `v', absorb(ano)
    predict double R_`v', resid
}
reg R_lw R_immigrant_share R_`X', vce(cluster ntrab) nocons
eststo c4
toc
//
//
// tic
// tab ano, gen(Dano)   // create year dummies
// ivreg2 lw immigrant_share `X' Dano*, cluster(ntrab)
// eststo c5
// toc


// tic
// ivreghdfe lw (immigrant_share = bartik) `X_base', absorb(ano) cluster(`idw') keepsingletons
// eststo c2
// toc

// * Pretty print (edit to taste)
// esttab c1 c2 c3 c4 c5, b(%10.6f) se(%10.6f) star(* 0.10 ** 0.05 *** 0.01) ///
//     stats(N r2, fmt(%12.0fc %10.6f) labels("Observations" "Within R^2")) ///
//     keep(immigrant_share) title("Table 1. Baseline estimates") replace
