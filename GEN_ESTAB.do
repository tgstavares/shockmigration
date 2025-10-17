clear all
local HOME "/home/tgst/Desktop/Projs/02_Shockmigration/shockmigration"
cd `HOME'

// Select years 
local anos1 1985/2009
local anos2 2010/2023
*local anos1 0/0
*local anos2 2021/2021

// Select variables
local vars11 ano nuemp nuest n2est pest pestl
local vars21 ano nuemp emp_id n_emp estab_id n_estab nut2_est nut2_est2013 nut2_estab2013 pest pestl pessoal cae2 caest2
local vars22 CAE2_est CAE2_EST

* Extract variables 
forvalues a=`anos1'{
	
	* create locals
	local y = substr("`a'", strlen("`a'")-1, 2)
	local list_vars
	
	* add to import string
	foreach v in `vars11'{
		local list_vars `list_vars' `v'_`y'
		*display "`list_vars'"
	}
	
	* import data
	display "QP_Estabelecimentos_`a'.sav"				
	if fileexists("Spss/QP_Estabelecimentos_`a'.sav") {
		quietly import spss `list_vars' using "Spss/QP_Estabelecimentos_`a'.sav"
	
		* rename variables
		foreach v in `vars11'{
			capture ren `v'_`y' `v'
		}
			
		* save temp
		save "temp_`a'.dta",replace
	}				
	clear
}

forvalues a=`anos2'{
	* create locals
	local y = substr("`a'", strlen("`a'")-1, 2)
	local list_vars
	
	* add to import string
	foreach v in `vars21'{
		local vv = upper("`v'")
		local list_vars `list_vars' `v' `vv'
	}
	
	foreach v in `vars22'{	
		local list_vars `list_vars' `v'
	}
	
	* import data
	display "QP_Estabelecimentos_`a'.sav"				
	if fileexists("Spss/QP_Estabelecimentos_`a'.sav") {
		quietly import spss `list_vars' using "Spss/QP_Estabelecimentos_`a'.sav"
		
		* rename variables
		foreach v in `vars21'{
			local vv = upper("`v'")
			capture ren `vv' `v'
			
			capture confirm numeric variable `v'
			if _rc {
				capture destring `v', replace dpcomma ignore("., ")
			}
		}
		
		foreach v in `vars22'{
			local vv = lower("`v'")
			capture ren `v' `vv' 
			
			capture confirm numeric variable `vv'
			if _rc {
				capture destring `vv', replace dpcomma ignore("., ")
			}
		}
		
		* save temp
		save "temp_`a'.dta",replace
	}
	clear
}
	
*exit	
	
** Merge, clean, and save
use "temp_2022.dta", clear
drop nut2_est // change in format for nuts2
save "temp_2022.dta", replace
clear


forvalues a=`anos1'{
	display `a'
	if fileexists("temp_`a'.dta") {
		append using "temp_`a'.dta"	
	}
}

forvalues a=`anos2'{
	display `a'
	if fileexists("temp_`a'.dta") {		
		append using "temp_`a'.dta"	
	}
}

shell rm temp_*

duplicates drop
clonevar nuts2=n2est
replace nuts2=nut2_est if nuts2==.
replace nuts2=nut2_est2013 if nuts2==.
drop n2est nut2_est nut2_est2013
ren nuts2 nuts2_est
replace pest = pessoal if pest==. & pessoal~=.
drop pessoal
replace emp_id = n_emp if emp_id==. & n_emp~=.
drop n_emp
replace estab_id = n_estab if estab_id==. & n_estab~=.
drop n_estab
gen cae2_est = cae2
replace cae2_est=caest2 if cae2_est==. & caest2~=.
drop cae2 caest2

order ano nuemp emp_id nuest estab_id nuts2_est cae2_est pest pestl

save DATA_ESTAB_EXCTRACT.dta, replace



