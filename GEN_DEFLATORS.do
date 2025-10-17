clear all
local HOME "/home/tgst/Desktop/Projs/02_Shockmigration/shockmigration"
cd `HOME'

getTimeSeries EUROSTAT NAMA_10_GDP/A.PD20_EUR+CP_MEUR.D1+B1GQ+B1G+D11.PT "" "" 0 0
gen ano=yearly(DATE,"Y")

local unit CP_MEUR PD20_EUR
local variable D1 B1GQ B1G D11

gen unit =regexs(2) if regexm(TSNAME,"(NAMA_10_GDP.A).([A-Za-z_0-9]+).+")
gen variable  =regexs(3) if regexm(TSNAME,"(NAMA_10_GDP.A).([A-Za-z_0-9]+).([A-Za-z_0-9]+).+")

save temp.dta,replace

local vars

foreach j in `unit'{
	foreach v in `variable'{
		use temp.dta, clear
		capture keep if variable == "`v'" & unit == "`j'"
		capture ren VALUE `v'_`j'
		capture keep ano `v'_`j'
		capture save temp_`v'_`j'.dta, replace
		capture local vars `vars' `v'_`j'
	}
}

clear
gen ano=.
foreach j in `unit'{
	foreach v in `variable'{
		capture merge 1:1 ano using temp_`v'_`j'.dta
		capture drop _merge
	}
}

* Clean and save
drop if ano==2024
quietly summarize B1GQ_PD20_EUR if ano==2023, meanonly
local aux = r(mean)
replace B1GQ_PD20_EUR=B1GQ_PD20_EUR/`aux'
quietly summarize B1G_PD20_EUR if ano==2023, meanonly
local aux = r(mean)
replace B1G_PD20_EUR=B1G_PD20_EUR/`aux'
drop D1_PD20_EUR D11_PD20_EUR

shell rm temp*
save DATA_DEFLATORS.dta, replace


