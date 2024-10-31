clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




**# identify disappeared - leaders/laggards based on GHG reduction rate

use "${cleaned}/ghgchange", clear

keep if year>=2017 & year<=2020
collapse (mean) ghg_change_real, by(id)

merge m:1 id using "${cleaned}/cdp_summary_firm", keepusing(blmg_industrygroupname)
drop if _merge==2
drop _merge
drop if strpos(blmg_industrygroupname,"#")>0 | blmg_industrygroupname==""

* sample median for all CDP firms
bys blmg_industrygroup: egen industry_median = median(ghg_change_real)
gen type_high_reduction = ghg_change_real >= industry_median
keep id type_high_reduction

tempfile ghgchange_firm_split
save `ghgchange_firm_split', replace


use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
keep if disappeared==1

merge 1:1 id using `ghgchange_firm_split'
keep if _merge==3
drop _merge

keep id type_high_reduction
save "${cleaned}/disappeared_split", replace





