clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



**# 1 Fig. 1a
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
unique id  // 1041
unique id if failed == 1  // 88
unique id if achieved == 1  // 633
unique id if disappeared == 1  // 320



**# 2 Fig. 1b
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
keep id failed disappeared achieved

merge 1:m id using "${cleaned}/ghgchange"
drop if _merge==2
drop _merge

gen ghg_change_real_failed = ghg_change_real if failed == 1
gen ghg_change_real_achieved = ghg_change_real if achieved == 1
gen ghg_change_real_disappeared = ghg_change_real if disappeared == 1

keep if year>=2014 & year<=2020
graph bar (mean) ghg_change_real_failed ghg_change_real_achieved ghg_change_real_disappeared, over(year) ytitle(% Reduction in Emissions) legend(row(1) size(small) order(1 "Failed" 2 "Disappeared" 3 "Achieved")) bar(1, fcolor("128 155 200")) bar(3, fcolor("99 193 4")) bar(2, fcolor("255 204 102")) title()
graph export "${figures}/figure_1b.png", replace



**# 3 Fig. 1c
use "${cleaned}/all_2020_targets_all_years", clear
replace year = year - 1
drop achieved achieved_early failed disappeared

merge m:1 id using "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", keepusing(id achieved failed disappeared)
keep if _merge==3
drop _merge

gen progress_imputed = (year - cdp_baseyear) / (cdp_targetyear - cdp_baseyear)
gen lag_behind = cdp_targetprogress < progress_imputed

collapse (max) lag_behind achieved failed disappeared, by(id year)
drop if year >= 2020

gen type = cond(achieved==1,"Achieved",cond(failed==1, "Failed", "Disappeared"))
collapse (mean) lag_behind, by(type year)
reshape wide lag_behind, i(year) j(type) string
	
label var lag_behindAchieved "Achieved"
label var lag_behindDisappeared "Disappeared"
label var lag_behindFailed "Failed"
twoway scatter lag_behindAchieved year, c(l) lc("99 193 4") mc("99 193 4") || scatter lag_behindDisappeared year, c(l) lc("255 204 102") mc("255 204 102") || scatter lag_behindFailed year, c(l) lc("128 155 200") mc("128 155 200") xlab(2010(2)2019) ytitle("%Frims Having Lagging-behind Targets") xtitle("") legend(rows(1)) title()
graph export "${figures}/figure_1c.png", replace



