clear all
local HOME "/home/tgst/Desktop/Projs/02_Shockmigration/shockmigration/Replication_Martins_2025"
cd `HOME'

local idw ntrab
local ide estab_id
use DATA_migration_regs_final.dta,clear

keep if migrant==0 & ano>=2017
keep if !missing(immigrant_share, bartik, `ide', `idw')

local X age age2 tenure tenure2 educ educ2 female log_estemp log_firmrevworker ldem_growth

**** OLS (equivalent estimators commented out)
timer on 1
reg lw immigrant_share `X' i.ano, vce(cluster `idw')
timer off 1
eststo c1

timer on 2
quietly reg lw `X' i.ano
quietly predict R_y, resid 
quietly reg immigrant_share `X' i.ano
quietly predict R_x, resid
reg R_y R_x, vce(cluster ntrab) nocons
timer off 2
eststo c2

timer on 3
areg lw immigrant_share `X', absorb(ano) vce(cluster ntrab)
timer off 3
eststo c3

timer on 4
reghdfe lw immigrant_share `X', absorb(ano) vce(cluster `idw')
timer off 4
eststo c4

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

* Pretty print (edit to taste)
esttab c1 c2 c3 c4, b(%10.6f) se(%10.6f) star(* 0.10 ** 0.05 *** 0.01) ///
    stats(N r2, fmt(%12.0fc %10.6f) labels("Observations" "Within R^2")) ///
    keep(immigrant_share R_x) title("Table 1. Baseline estimates") replace
    
timer list    
