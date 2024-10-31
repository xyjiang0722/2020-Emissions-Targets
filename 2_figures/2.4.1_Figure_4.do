clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"
global code "$root/code"



* Fig. 4
* Coefficients plots


**# Materiality
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
	keep if emission_industry_high==1
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
	keep if emission_industry_high==1
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
export delim "${figures}/regression_coefficients_materiality.csv", replace



**# Covid
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
	drop if type_covid_industry==1
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
	drop if type_covid_industry==1
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
export delim "${figures}/regression_coefficients_covid.csv", replace



**# Ambitiousness
clear 
set obs 10
gen depvar = ""
gen b1 = .
gen b2 = .
gen se1 = .
gen se2 = .


local consequences_var cdp_car_m1_p1 cdp_abnvol_pctlog_day0

local i = 1
foreach var in `consequences_var' {
	preserve
	use "${cleaned}/event_study_panel_2021", clear
	drop if i_type==2|i_type==3
	local varlabel : var label `var'

	di "`varlabel'"
	qui reg `var' i.failed_high_ambition i.failed_low_ambition, robust
	local b1 = _b[1.failed_high_ambition]
	local b2 = _b[1.failed_low_ambition]
	local se1 = _se[1.failed_high_ambition]
	local se2 = _se[1.failed_low_ambition]
	restore
	
	replace depvar = "`varlabel'" in `i'
	replace b1 = `b1' in `i'
	replace b2 = `b2' in `i'
	replace se1 = `se1' in `i'
	replace se2 = `se2' in `i'
	
	local i = `i'+1
}


local consequences_var count_both count_e_only pulseenvironment insightenvironment enscore emiscore msci_enscore msci_emiscore

local i = 3
foreach var in `consequences_var' {
	preserve
	use "${cleaned}/final_consequences", clear
	drop if i_type==2|i_type==3
	local varlabel : var label `var'

	local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment
	di "`varlabel'"
	qui reghdfe `var' i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
	local b1 = _b[1.failed_high_ambition#1.post]
	local b2 = _b[1.failed_low_ambition#1.post]
	local se1 = _se[1.failed_high_ambition#1.post]
	local se2 = _se[1.failed_low_ambition#1.post]
	restore
	
	replace depvar = "`varlabel'" in `i'
	replace b1 = `b1' in `i'
	replace b2 = `b2' in `i'
	replace se1 = `se1' in `i'
	replace se2 = `se2' in `i'
	
	local i = `i'+1
}

gen upper1 =  b1 + 1.96*se1
gen upper2 =  b2 + 1.96*se2
gen lower1 =  b1 - 1.96*se1
gen lower2 =  b2 - 1.96*se2

drop se*

rename *1 *_Failed_Ambitious
rename *2 *_Failed_Unambitious

gen order1 = _n
reshape long b_ lower_ upper_, i(depvar order1) j(indepvar) string

gen order2 = 1 if indepvar=="Failed_Ambitious"
replace order2 = 2 if indepvar=="Failed_Unambitious"

sort order1 order2
export delim "${figures}/regression_coefficients_ambitiousness.csv", replace



**# Prior Info
clear 
set obs 10
gen depvar = ""
gen b1 = .
gen b2 = .
gen se1 = .
gen se2 = .


local consequences_var cdp_car_m1_p1 cdp_abnvol_pctlog_day0

local i = 1
foreach var in `consequences_var' {
	preserve
	use "${cleaned}/event_study_panel_2018", clear
	local varlabel : var label `var'
	di "`varlabel'"
	qui reg `var' i.lag_behind_2018, robust
	local b1 = _b[1.lag_behind_2018]
	local se1 = _se[1.lag_behind_2018]
	restore
	
	preserve
	use "${cleaned}/event_study_panel_2019", clear
	qui reg `var' i.lag_behind_2019, robust
	local b2 = _b[1.lag_behind_2019]
	local se2 = _se[1.lag_behind_2019]
	restore
	
	replace depvar = "`varlabel'" in `i'
	replace b1 = `b1' in `i'
	replace b2 = `b2' in `i'
	replace se1 = `se1' in `i'
	replace se2 = `se2' in `i'
	
	local i = `i'+1
}


local consequences_var count_both count_e_only pulseenvironment insightenvironment enscore emiscore msci_enscore msci_emiscore

local i = 3
foreach var in `consequences_var' {
	preserve
	use "${cleaned}/final_consequences", clear
	gen post1 = year>=2018
	gen post2 = year>=2019
	local varlabel : var label `var'

	local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment
	di "`varlabel'"
	qui reghdfe `var' i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
	local b1 = _b[1.post1#1.lag_behind_2018]
	local se1 = _se[1.post1#1.lag_behind_2018]
	qui reghdfe `var' i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
	local b2 = _b[1.post2#1.lag_behind_2019]
	local se2 = _se[1.post2#1.lag_behind_2019]
	restore
	
	replace depvar = "`varlabel'" in `i'
	replace b1 = `b1' in `i'
	replace b2 = `b2' in `i'
	replace se1 = `se1' in `i'
	replace se2 = `se2' in `i'
	
	local i = `i'+1
}

gen upper1 =  b1 + 1.96*se1
gen upper2 =  b2 + 1.96*se2
gen lower1 =  b1 - 1.96*se1
gen lower2 =  b2 - 1.96*se2

drop se*

rename *1 *_Lagging_Behind_in_2018
rename *2 *_Lagging_Behind_in_2019

gen order1 = _n
reshape long b_ lower_ upper_, i(depvar order1) j(indepvar) string

gen order2 = 1 if indepvar=="Lagging_Behind_in_2018"
replace order2 = 2 if indepvar=="Lagging_Behind_in_2019"

sort order1 order2
export delim "${figures}/regression_coefficients_prior_info.csv", replace



**# Run R
clear
rscript using "code/2_figures/2.4.2_Figure_4.R"



