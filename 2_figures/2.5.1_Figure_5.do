clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



* figure 5a
* input the numbers to excel and make the figure there
use "${cleaned}/media_final_2020_announcements", clear
unique id if press_release_total>0
unique id if news_article_total>0

use "${cleaned}/media_final_2020_outcomes", clear
unique id if press_release_total>0
unique id if news_article_total>0


* figure 5b
clear 
set obs 10
gen depvar = ""
gen b1 = .
gen se1 = .

local consequences_var targetannounce_car_m1_p1 targetannounce_abnvol_pctlog_day

local i = 1
foreach var in `consequences_var' {
	preserve
	import delim "${cleaned}/panel_returnVolume_TargetAnnounce.csv", clear
	label var targetannounce_car_m1_p1 "Cumulative Abnormal Return [-1,1]"
	label var targetannounce_abnvol_pctlog_day "Abnormal Trading Volume"
	local varlabel : var label `var'

	di "`varlabel'"
	qui ttest `var' == 0
	local b1 = r(mu_1) 
	local se1 = r(se)
	restore
	
	replace depvar = "`varlabel'" in `i'
	replace b1 = `b1' in `i'
	replace se1 = `se1' in `i'
	
	local i = `i'+1
}


local consequences_var count_both count_e_only pulseenvironment insightenvironment enscore emiscore msci_enscore msci_emiscore

local i = 3
foreach var in `consequences_var' {
	preserve
	use "${cleaned}/final_announcements", clear
	local varlabel : var label `var'

	local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment
	di "`varlabel'"
	qui reghdfe `var' i.post `controls', vce(cluster id) absorb(id year)
	local b1 = _b[1.post]
	local se1 = _se[1.post]
	restore
	
	replace depvar = "`varlabel'" in `i'
	replace b1 = `b1' in `i'
	replace se1 = `se1' in `i'
	
	local i = `i'+1
}

gen upper1 =  b1 + 1.96*se1
gen lower1 =  b1 - 1.96*se1
drop se*
rename *1 *
gen indepvar = "Post"
gen order = _n

export delim "${figures}/regression_coefficients_announcement.csv", replace



* Run R
clear
rscript using "code/2_figures/2.5.2_Figure_5.R"
