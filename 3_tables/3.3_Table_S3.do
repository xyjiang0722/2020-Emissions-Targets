clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




**# 1 Table S3

use "${cleaned}/final_all_years", clear
drop if year >= 2020
drop if lag_behind==.

merge 1:1 id year using "${cleaned}/ghgchange", update replace
drop if _merge==2
drop _merge

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment num_targets_2020

eststo clear
eststo: reghdfe ghg_change_real i.failed i.disappeared `controls', vce(cluster id) absorb(industry2)
estadd local fe1 "No"
estadd local fe2 "Yes"
eststo: reghdfe ghg_change_real i.failed i.disappeared `controls', vce(cluster id) absorb(industry2 year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment num_targets_2020

eststo: reghdfe lag_behind i.failed i.disappeared `controls', vce(cluster id) absorb(industry2)
estadd local fe1 "No"
estadd local fe2 "Yes"
eststo: reghdfe lag_behind i.failed i.disappeared `controls', vce(cluster id) absorb(industry2 year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"

esttab using "${tables}/table_s3.tex", replace label title(Lagging-Behind Targets) noobs nobaselevels keep(1.failed 1.disappeared `controls') s(N r2_a fe1 fe2, label("N" "Adj. R-squared" "Year FE" "Industry FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)







