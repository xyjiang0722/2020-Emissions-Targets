clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* identify firms that missed ambitious targets

use "${cleaned}/all_2020_targets_final_year", clear
keep if failed==1

gen failed_ambition = cdp_ambition 
collapse (mean) failed_ambition, by(id)

qui su failed_ambition, d
gen failed_high_ambition = failed_ambition >= r(p50)

keep id failed_high_ambition
gen failed_low_ambition = cond(failed_high_ambition==0,1,0)

save "${cleaned}/ambition_failed_split", replace
