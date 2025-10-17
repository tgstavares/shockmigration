clear all
local HOME "/home/tgst/Desktop/Projs/02_Shockmigration/shockmigration"
cd `HOME'


// Select years 
local anos1 1985/2009
local anos2 2010/2023
*local anos1 0/0
*local anos2 2023/2023

// For now make sure we select a panel of valid workers
local vars11 ano nuemp n2emp natju nest pemp ancon vvend
local vars21 ano nuemp emp_id nut2_emp2013 nut2_emp natju nat_juridica nest n_estab pemp pessoal ancon ano_constituicao ano_const antiguidade vn volume_vendas cae2 cae_rev3_2dig

* Extract variables 
forvalues a=`anos1'{
	
	* create locals
	local y = substr("`a'", strlen("`a'")-1, 2)
	local list_vars
	
	* add to import string
	foreach v in `vars11'{
		local vv = upper("`v'")
		local list_vars `list_vars' `v' `vv' `v'_`y' `vv'_`y'
	}
	
	* import data
	display "QP_Empresas_`a'.sav"				
	if fileexists("Spss/QP_Empresas_`a'.sav") {
		quietly import spss `list_vars' using "Spss/QP_Empresas_`a'.sav"
	
		* rename variables
		foreach v in `vars11'{
			local vv = upper("`v'")			
			capture ren `v'_`y' `v'
			capture ren `vv'_`y' `v'
			capture ren `vv' `v'
			capture confirm numeric variable `v'
			if _rc {
				capture destring `v', replace dpcomma ignore("., ")
			}
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
	
	* import data
	display "QP_Empresas_`a'.sav"				
	if fileexists("Spss/QP_Empresas_`a'.sav") {
		quietly import spss `list_vars' using "Spss/QP_Empresas_`a'.sav"
		
		* rename variables
		foreach v in `vars21'{
			local vv = upper("`v'")
			capture ren `vv' `v'
			capture confirm numeric variable `v'
			if _rc {
				capture destring `v', replace dpcomma ignore("., ")
			}
		}
				
		if `a'>=2022{
			set varabbrev off
			capture drop nut2_emp
			set varabbrev on
		}
		
		* save temp
		save "temp_`a'.dta",replace
	}
	clear	
}

** Merge, clean, and save
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

duplicates drop
clonevar nuts2=nut2_emp2013
replace nuts2=n2emp if nuts2==. & n2emp~=.
replace nuts2=nut2_emp if nuts2==. & nut2_emp~=.
drop nut2_emp2013 n2emp nut2_emp
replace ancon=. if ancon==0
replace ano_constituicao = ano_const if ano_constituicao==. & ano_const~=.
drop ano_const
gen ano_const=real( substr(string(ancon, "%20.0f"), 1, 4) )
replace ano_const=real( substr(string(ano_constituicao, "%20.0f"), 1, 4) ) if ano_const==. & ano_constituicao~=.
drop ancon ano_constituicao
gen vendas = vvend
replace vendas = vn if vendas==. & vn~=.
replace vendas = volume_vendas if vendas==. & volume_vendas~=.
drop vvend vn volume_vendas
replace pemp=pessoal if pemp==. & pessoal~=.
drop pessoal
replace natju=nat_juridica if natju==. & nat_juridica~=.
drop nat_juridica
replace nest=n_estab if nest==. & n_estab~=.
drop n_estab
order ano nuemp emp_id ano_const antiguidade nuts2 cae2 nest natju pemp vendas
ren cae2 cae2_emp
replace cae2_emp = cae_rev3_2dig if cae2_emp==. & cae_rev3_2dig~=.
drop cae_rev3_2dig

shell rm temp_*
save DATA_FIRM_EXCTRACT.dta, replace



