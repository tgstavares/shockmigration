clear all
local HOME "/home/tgst/Desktop/Projs/02_Shockmigration/shockmigration"
cd `HOME'

// Select years  - need to breakdown because of dynamic memeory limitations
local anos1 1985/2009
local anos2 2010/2023
*local anos1 0/0
*local anos2 2018/2018

// For now make sure we select a panel of valid workers
local vars11 ano nuemp nuest ntrab sexo stpro ctrem ctcont crtrab ganho rganh rbase rpirg rextr nhnor nhext npnor
local vars21 ano nuemp emp_id nuest estab_id ntrab sexo sitpro ctrem tipo_contr tipo_contr1 reg_dur rganho rbase prest_irreg rextra hnormais hextra pnt nqual1 nacio habil2 antig

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
	local list_vars `list_vars' idade_`y'_TB_COD
	*display "`list_vars'"
	
	* import data
	display "QP_Trabalhadores_`a'.sav"				
	if fileexists("Spss/QP_Trabalhadores_`a'.sav") {
		quietly import spss `list_vars' using "Spss/QP_Trabalhadores_`a'.sav"
	
		* rename variables
		foreach v in `vars11'{
			capture confirm numeric variable `v'_`y'
			if _rc {
				capture destring `v'_`y', replace dpcomma ignore("., ")
			}
			capture ren `v'_`y' `v'
		}
		ren idade_`y'_TB_COD idade
		capture confirm numeric variable idade
		if _rc {
			capture destring idade, replace dpcomma ignore("<>=")
		}
		capture ren rganh ganho
	
		* to compress
		local vars_compress ganho rbase rpirg rextr npnor
		foreach vv in `vars_compress'{
			capture replace `vv'=trunc(`vv'*100)
			capture format `vv' %12.0f
			capture compress `vv'
		}
		duplicates drop
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
	local list_vars `list_vars' idade_Cod IDADE_COD
	
	* import data
	display "QP_Trabalhadores_`a'.sav"				
	if fileexists("Spss/QP_Trabalhadores_`a'.sav") {
		quietly import spss `list_vars' using "Spss/QP_Trabalhadores_`a'.sav"
	
		* rename variables / some manually but must be done to manage memory usage
		foreach v in `vars21'{
			local vv = upper("`v'")
			capture ren `vv' `v'
			capture confirm numeric variable `v'
			if _rc {
				capture destring `v', replace dpcomma ignore("., ")
			}
		}
		capture ren idade_Cod idade
		capture ren IDADE_COD idade
		capture confirm numeric variable idade
		if _rc {
			capture destring idade, replace dpcomma ignore("<>=")
		}
		
		* MANUAL RENAMING - POTENTIAL ERRORS
		capture ren reg_dur crtrab
		capture ren rganho ganho
		capture ren prest_irreg rpirg
		capture ren rextra rextr
		capture ren hnormais nhnor
		capture ren hextra nhext
		capture ren pnt npnor
		capture ren sitpro stpro
	
		* to compress
		local vars_compress ganho rbase rpirg rextr npnor
		foreach vv in `vars_compress'{
			capture replace `vv'=trunc(`vv'*100)
			capture format `vv' %12.0f
			capture compress `vv'
		}
		duplicates drop
		save "temp_`a'.dta",replace
	}
	clear
}

*exit

* Merge, clean, and save
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

format nhnor nhext %12.0f
capture gen ctcont=.
order ano nuemp emp_id nuest estab_id ntrab sexo idade ctcont tipo_contr tipo_contr1 stpro crtrab ctrem ganho rbase rextr rpirg nhnor nhext npnor

save DATA_WORKER_EXCTRACT.dta, replace
clear all

shell rm temp_*

