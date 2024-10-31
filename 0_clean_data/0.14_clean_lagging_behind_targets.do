clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* identify lagging-behind targets using the linear progress assumption

use "${cleaned}/all_2020_targets_all_years", clear
replace year = year - 1

gen progress_imputed = (year - cdp_baseyear) / (cdp_targetyear - cdp_baseyear)
gen lag_behind = cdp_targetprogress<progress_imputed

collapse (max) lag_behind, by(id year)

save "${cleaned}/whether_lagging_behind_all_years", replace

keep if inlist(year, 2018, 2019)
rename lag_behind lag_behind_
reshape wide lag_behind_, i(id) j(year)

save "${cleaned}/whether_lagging_behind_2019_2020", replace

