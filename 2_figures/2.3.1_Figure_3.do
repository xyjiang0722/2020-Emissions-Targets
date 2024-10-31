clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"
global code "$root/code"



**# 1 Fig. 3
* Coefficients plot - Main
clear 
set obs 10
gen depvar = ""
gen b1 = .
gen b2 = .
gen b3 = .
gen se1 = .
gen se2 = .
gen se3 = .


local consequences_var cdp_car_m1_p1 cdp_abnvol_pctlog_day0

local i = 1
foreach var in `consequences_var' {
	preserve
	use "${cleaned}/event_study_panel_2021", clear
	local varlabel : var label `var'

	di "`varlabel'"
	qui reg `var' i.i_type, robust
	local b1 = _b[2.i_type]
	local b2 = _b[3.i_type]
	local b3 = _b[4.i_type]
	local se1 = _se[2.i_type]
	local se2 = _se[3.i_type]
	local se3 = _se[4.i_type]
	restore
	
	replace depvar = "`varlabel'" in `i'
	replace b1 = `b1' in `i'
	replace b2 = `b2' in `i'
	replace b3 = `b3' in `i'
	replace se1 = `se1' in `i'
	replace se2 = `se2' in `i'
	replace se3 = `se3' in `i'
	
	local i = `i'+1
}


local consequences_var count_both count_e_only pulseenvironment insightenvironment enscore emiscore msci_enscore msci_emiscore

local i = 3
foreach var in `consequences_var' {
	preserve
	use "${cleaned}/final_consequences", clear
	local varlabel : var label `var'

	local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment
	di "`varlabel'"
	qui reghdfe `var' i.i_type##i.post `controls', vce(cluster id) absorb(id year)
	local b1 = _b[2.i_type#1.post]
	local b2 = _b[3.i_type#1.post]
	local b3 = _b[4.i_type#1.post]
	local se1 = _se[2.i_type#1.post]
	local se2 = _se[3.i_type#1.post]
	local se3 = _se[4.i_type#1.post]
	restore
	
	replace depvar = "`varlabel'" in `i'
	replace b1 = `b1' in `i'
	replace b2 = `b2' in `i'
	replace b3 = `b3' in `i'
	replace se1 = `se1' in `i'
	replace se2 = `se2' in `i'
	replace se3 = `se3' in `i'
	
	local i = `i'+1
}

gen upper1 =  b1 + 1.96*se1
gen upper2 =  b2 + 1.96*se2
gen upper3 =  b3 + 1.96*se3
gen lower1 =  b1 - 1.96*se1
gen lower2 =  b2 - 1.96*se2
gen lower3 =  b3 - 1.96*se3

drop se*

rename *1 *_Disappeared_leaders
rename *2 *_Disappeared_laggards
rename *3 *_Failed

gen order1 = _n
reshape long b_ lower_ upper_, i(depvar order1) j(indepvar) string

gen order2 = 1 if indepvar=="Failed"
replace order2 = 2 if indepvar=="Disappeared_leaders"
replace order2 = 3 if indepvar=="Disappeared_laggards"

sort order1 order2
export delim "${figures}/regression_coefficients_main.csv", replace



clear
rscript using "code/2_figures/2.3.2_Figure_3.R"




