clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* Pre-trend plots
grstyle init
grstyle set plain, horizontal grid
grstyle set legend 6, nobox
grstyle set symbol

use "${cleaned}/final_consequences", clear

gen t_2017 = (year==2017)*failed
gen t_2018 = (year==2018)*failed
gen t_2019 = (year==2019)*failed
gen t_2020 = 0
gen t_2021 = (year==2021)*failed

gen t1_2017 = (year==2017)*type_high_reduction
gen t1_2018 = (year==2018)*type_high_reduction
gen t1_2019 = (year==2019)*type_high_reduction
gen t1_2021 = (year==2021)*type_high_reduction

gen t2_2017 = (year==2017)*(1-type_high_reduction)
gen t2_2018 = (year==2018)*(1-type_high_reduction)
gen t2_2019 = (year==2019)*(1-type_high_reduction)
gen t2_2021 = (year==2021)*(1-type_high_reduction)

foreach v of varlist t1* t2* {
	replace `v' = 0 if `v'==.
}

local consequences_var count_e_only pulseenvironment enscore msci_enscore  
local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

foreach outcome in `consequences_var' {
	preserve
	local varlabel : var label `outcome'
	qui reghdfe `outcome' t_* t1* t2* `controls', absorb(id year) vce(cluster id) nocon
	foreach v of varlist t_* {
		qui gen b_`v' = _b[`v']
		qui gen se_`v' = _se[`v']
	}
	keep in 1
	keep b_* se_*
	qui gen i=.
	qui reshape long b_ se_, i(i) j(x) s
	drop i
	split x, p("_")
	drop x1 
	rename x2 t
	destring t, replace

	qui gen u = b_ + 1.96*se_
	qui gen l = b_ - 1.96*se_
	
	local bar = 1 
	local gap = `bar'/2
	
	twoway rcap u l t || scatter b_ t, ylabel(-`bar' (`gap') `bar') xline(2020) yline(0) ttitle("Year") legend(off) title("`varlabel'") ytitle("")
	graph export "${figures}/figure_s1_`outcome'.png", replace
	restore
}



