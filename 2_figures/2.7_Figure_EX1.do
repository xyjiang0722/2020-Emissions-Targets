clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



**# Extended Data Fig. 1
* target year density plot

use "${data}/cdp_target_clean", replace

* for targets with the same scope, type, base year, target year, keep the main one 
keep if cdp_targetscope_percent>=80
keep if cdp_targetduration>=3
keep if inlist(target_scope, "1", "2", "1+2", "1+2+3")  
bys id year target_scope cdp_targettype cdp_targetduration cdp_targetscope_percent cdp_baseyearemission: keep if _n==_N  
bys id year target_scope cdp_targettype cdp_targetduration cdp_targetscope_percent: keep if _n==_N  
bys id year target_scope cdp_targettype cdp_targetduration: keep if _n==_N  
bys id year target_scope cdp_targettype: keep if _n==_N

hist cdp_targetyear if year==2017, xlabel(2000(10)2060) xtitle("Target Year") title("")
graph export "${figures}/figure_ex1a.png", replace

hist cdp_targetyear if year==2020, xlabel(2010(10)2060) xtitle("Target Year") title("")
graph export "${figures}/figure_ex1b.png", replace
