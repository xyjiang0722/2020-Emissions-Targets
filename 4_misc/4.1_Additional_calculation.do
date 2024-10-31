clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




* additional calculation for how many of the disappeared could have achieved their targets by 2020

use "${cleaned}/all_2020_targets_final_year", clear
merge m:1 id using "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", keepusing(id type)
keep if _merge==3
drop _merge

keep if type == "Disappeared" & cdp_targettype == "absolute" & target_scope!="1+2+3"
bys id (target_scope): keep if _n==1
unique id

preserve
use "${data}/cdp_ghg_all_scopes", clear
replace year = year - 1
keep if year==2020
keep id ghg1 ghg2location ghg2market
tempfile temp
save `temp', replace
restore

merge 1:1 id using `temp'
keep if _merge==3
drop _merge

* calculate final emission in 2020 that's covered by the target
tab target_scope
gen final_emission = .
replace final_emission = ghg1*cdp_targetscope_percent/100 if target_scope=="1"
replace final_emission = ghg2market*cdp_targetscope_percent/100 if target_scope=="2"&strpos(cdp_targetscope,"market")>0
replace final_emission = ghg2location*cdp_targetscope_percent/100 if target_scope=="2"&strpos(cdp_targetscope,"location")>0
replace final_emission = (ghg1+ghg2market)*cdp_targetscope_percent/100 if target_scope=="1+2"&strpos(cdp_targetscope,"market")>0
replace final_emission = (ghg1+ghg2location)*cdp_targetscope_percent/100 if target_scope=="1+2"&strpos(cdp_targetscope,"location")>0

* targeted emissions amount
gen emission_target = cdp_baseyearemission * (1-cdp_targetamount/100)
drop if final_emission==. | emission_target==.

count if emission_target>=final_emission  // 43
count if emission_target<final_emission  // 74
di 74/(74+43)
