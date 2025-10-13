
clear all

local HOME "/Users/tgst/Library/CloudStorage/Dropbox/PhD_Economics/research/my_projects/3rd_year_project_3rd_year_project/125_LaborIncomeDynamics/Data/Aux_datasets"
cd `HOME'

set more off

* Define mutually exclusive groups
local brazil "BR"
local palop  "AO CV GW MZ ST"
local eu     "AT BE BG HR CY CZ DE DK EE ES FI FR GR HU IE IT LT LU LV MT NL PL PT RO SE SI SK"
local west   "GB NO CH IS LI AD MC SM VA GI JE GG IM FO AX"
local east   "AL BA BY MD ME MK RS UA RU XK"
local asia   "AE AF AM AZ BD BH BN BT CN HK ID IN IO IQ IR IL JO JP KG KH KP KR KW KZ LA LB LK MM MN MO MV MY NP OM PH PK PS QA SA SG SY TH TJ TM TR TW UZ VN YE TL GE AP"
local africa "DZ EG LY TN MA EH SD SS ER ET DJ SO KE UG RW BI TZ ZM ZW MW MG KM SC MU NA BW ZA LS SZ NE NG GH CI TG BJ BF ML MR SN GM GN SL LR GQ GA CG CD CM CF TD RE YT SH"
local na     "US CA MX BZ CR GT HN NI PA SV AG AI AW BB BS BQ CW DM DO GD GP HT JM KN KY LC MQ MS PR SX TC TT VC VG BM PM GL BL CU"
local sa     "AR BO CL CO EC GF GY PE PY SR UY VE FK"
local other  "AU NZ FJ PG WS VU TO NR NU TK PF NC PN WF MH FM MP GU UM PW AS CC CK SB CX"

* Add missing ISO-2 codes to the right buckets
local na     `"`na' AN"'          // Netherlands Antilles → Caribbean/NA
local sa     `"`sa' GS"'          // South Georgia & South Sandwich → SA (geo, like FK)
local other  `"`other' AQ BV TF"' // Antarctica, Bouvet, French Southern Territories → Other

* ----- Post rows into a new dataset -----
tempname mem
postfile `mem' str2 nacio str20 nacio_group using NACIO_GROUPS.dta, replace

foreach c of local brazil {  
	post `mem' ("`c'") ("Brazil") 
	}
foreach c of local palop  {  
	post `mem' ("`c'") ("PALOP") 
	}
foreach c of local eu     {  
	post `mem' ("`c'") ("EU") 
	}
foreach c of local west   {  
	post `mem' ("`c'") ("Western Europe") 
	}
foreach c of local east   {  
	post `mem' ("`c'") ("Eastern Europe") 
	}
foreach c of local asia   {  
	post `mem' ("`c'") ("Asia") 
	}
foreach c of local africa {  
	post `mem' ("`c'") ("Africa") 
	}
foreach c of local na     {  
	post `mem' ("`c'") ("North America") 
	}
foreach c of local sa     {  
	post `mem' ("`c'") ("South America") 
	}
foreach c of local other  {  
	post `mem' ("`c'") ("Other") 
	}

postclose `mem'

use NACIO_GROUPS.dta, clear
sort nacio
isid nacio
label data "Mapping: 2-letter nationality (nacio) → nacio_group"
save NACIO_GROUPS.dta, replace   // already saved by postfile
