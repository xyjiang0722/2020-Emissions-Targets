clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




use "${data}/cdp_ghg_change", clear
replace year = year-1  // CDP reporting year -> fiscal year

destring ghg_change_, force replace
drop if (ghg_change_!=0 & ghg_change_!=. & direction=="") | (ghg_change_!=0 & ghg_change_!=. & direction=="No change") | (ghg_change_!=0 & ghg_change_!=. & strpos(direction, "not")>0) | ghg_change_==999
replace ghg_change_ = 0 if direction=="No change" | strpos(direction, "not")>0 | strpos(direction, "Hidden")>0
replace ghg_change_ = -ghg_change_ if direction=="Decreased"
replace category = lower(category)
replace category = "boundary" if strpos(category, "boundary")!=0
replace category = "methodology" if strpos(category, "methodology")!=0
replace category = "output" if strpos(category, "output")!=0
replace category = "physical" if strpos(category, "physical")!=0
replace category = "renewable" if strpos(category, "renewable")!=0
replace category = "ems_reduction" if strpos(category, "reduction")!=0
replace category = "other" if strpos(category, "other")!=0

keep id year category ghg_change_
collapse (sum) ghg_change_, by(id year category)
reshape wide ghg_change_, i(id year) j(category) string
egen ghg_change_real = rowtotal(ghg_change_ems_reduction ghg_change_renewable)

replace ghg_change_real = -ghg_change_real  // GHG reduction rate

keep id year ghg_change_real 
save "${cleaned}/ghgchange", replace
