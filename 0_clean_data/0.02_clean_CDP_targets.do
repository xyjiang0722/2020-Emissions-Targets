clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




**# 1. Clean CDP emissions targets data

use "${data}/cdp_target", clear 

* clean variables / drop data errors
replace cdp_targetscope_percent=cdp_targetscope_percent*100 if year==2011
replace cdp_timeprogress=cdp_timeprogress*100 if year==2011
replace cdp_targetprogress=cdp_targetprogress*100 if year==2011
replace cdp_targetamount=cdp_targetamount*100 if year==2011
replace cdp_timeprogress = 83 if year==2011 & cdp_timeprogress ==0.83
replace cdp_baseyear = 2011 if cdp_timeprogress<1 & cdp_timeprogress>0 & id=="36584"
drop if year==2012 & cdp_timeprogress==2011
drop if year==2012 & cdp_targetprogress >100 & !missing( cdp_targetprogress )
replace cdp_targetscope_percent=cdp_targetscope_percent*100 if year==2012
replace cdp_targetamount=cdp_targetamount*100 if year==2012

drop if missing( cdp_targetamount) & missing( cdp_targetyear) & missing( cdp_baseyear ) & year==2018
drop if missing( cdp_targetamount) & missing( cdp_targetyear) & missing( cdp_baseyear ) & year==2019
drop if missing( cdp_targetamount) & missing( cdp_targetyear) & missing( cdp_baseyear ) & year==2020
drop if missing( cdp_targetamount) & missing( cdp_targetyear) & missing( cdp_baseyear ) & year==2021
drop if missing( cdp_targetamount) & missing( cdp_targetyear) & missing( cdp_baseyear ) & year==2022

gen cdp_targetduration=cdp_targetyear - cdp_startyear
replace cdp_timeprogress=100*((year-1-cdp_startyear)/cdp_targetduration) if inlist(year,2018,2019,2020,2021,2022)
replace cdp_timeprogress=100 if cdp_timeprogress>100 & !missing(cdp_timeprogress)
drop if cdp_timeprogress<0 & !missing(cdp_timeprogress)

drop if cdp_baseyear == cdp_targetyear

* redefine target progress and duration, define ambition
replace cdp_targetprogress=0 if cdp_targetprogress<0
replace cdp_targetprogress=100 if cdp_targetprogress>100 & !missing(cdp_targetprogress)
replace cdp_targetduration = cdp_targetyear - cdp_baseyear
gen cdp_ambition = cdp_targetamount / cdp_targetduration

* clean scopes
gen target_scope_1 = cond(strpos(cdp_targetscope,"1")!=0, 1, 0)
gen target_scope_2 = cond(strpos(cdp_targetscope,"2")!=0, 1, 0)
gen target_scope_3 = cond(strpos(cdp_targetscope,"3")!=0, 1, 0)
gen target_scope = cond(target_scope_1==1 & target_scope_2==1 & target_scope_3==1, "1+2+3", ///
                   cond(target_scope_1==1 & target_scope_2==1 & target_scope_3==0, "1+2", ///
				   cond(target_scope_1==1 & target_scope_2==0 & target_scope_3==1, "1+3", ///
				   cond(target_scope_1==0 & target_scope_2==1 & target_scope_3==1, "2+3", ///
				   cond(target_scope_1==1 & target_scope_2==0 & target_scope_3==0, "1", ///
				   cond(target_scope_1==0 & target_scope_2==1 & target_scope_3==0, "2", ///
				   cond(target_scope_1==0 & target_scope_2==0 & target_scope_3==1, "3", ///
				   cond(target_scope_1==0 & target_scope_2==0 & target_scope_3==0 & length(cdp_targetscope)!=0, "other",""))))))))  

* clean target status
replace cdp_targetstatus = trim(cdp_targetstatus)
replace cdp_targetstatus = cond(cdp_targetprogress==100, "Achieved", "Underway") if year<2020

save "${data}/cdp_target_clean", replace 




**# 2. Clean 2020 emissions targets data

keep if cdp_targetyear==2020

* for targets with the same scope, type, base year, target year, keep the main one 
keep if cdp_targetscope_percent>=80
keep if cdp_targetduration>=3
keep if inlist(target_scope, "1", "2", "1+2", "1+2+3")
bys id year target_scope cdp_targettype cdp_targetduration cdp_targetscope_percent cdp_baseyearemission: keep if _n==_N
bys id year target_scope cdp_targettype cdp_targetduration cdp_targetscope_percent: keep if _n==_N
bys id year target_scope cdp_targettype cdp_targetduration: keep if _n==_N
bys id year target_scope cdp_targettype: keep if _n==_N

egen target_id = group(id target_scope cdp_targettype)
bys id year: gen num_targets_year = _N

* identify target status
gen achieved_ind = cdp_targetstatus == "Achieved"
bys target_id: egen achieved = max(achieved_ind)

bys target_id: egen last_year = max(year)
gen last_year_before_2020 = last_year <= 2020

gen disappeared = 1 if achieved==0 & last_year_before_2020==1
replace disappeared = 0 if disappeared==.

gen failed = 1 if achieved==0 & last_year_before_2020==0
replace failed = 0 if failed==.

tab achieved disappeared
tab achieved failed
tab disappeared failed

bys target_id: egen min_year = min(cdp_startyear)
replace cdp_startyear = min_year if cdp_startyear==.
drop min_year

* manually check the accuracy of target status for failed
replace cdp_targetstatus = "Achieved" if (id==19376 & cdp_targetstatus=="Retired") | (id==44756 & cdp_targetstatus=="")

preserve
	import excel "${cleaned}/all_2020_targets_final_year_failed_reasons", clear firstrow sheet("stata")
	keep failed_reason_keywords target_id
	unique target_id
	tempfile failed_details
	save `failed_details', replace
restore

merge m:1 target_id using `failed_details'
drop _merge
replace achieved = 1 if failed_reason_keywords=="false positive"
replace failed = 0 if failed_reason_keywords=="false positive"
drop failed_reason_keywords

save "${cleaned}/all_2020_targets_all_years", replace




**# 3. Clean target level data

* target level
use "${cleaned}/all_2020_targets_all_years", clear

sort target_id year
bys target_id (year): keep if _n==_N
drop achieved achieved_early disappeared failed

gen achieved = cdp_targetstatus == "Achieved"
gen disappeared = (achieved==0 & year<2021)
gen failed = (achieved==0 & year>=2021)

save "${cleaned}/all_2020_targets_final_year", replace


* firm level
use "${cleaned}/all_2020_targets_final_year", clear
gen count = 1
gen exist_2021 = year>=2021
bys id: replace country = country[1]
collapse (sum) num_targets_2020 = count (max) achieved disappeared failed max_ambition = cdp_ambition last_year = year (min) min_ambition = cdp_ambition min_year=year cdp_startyear cdp_baseyear (mean) mean_ambition = cdp_ambition, by(id country)

merge 1:1 id using "{data}/cdp_summary_firm", keepusing(isin companyname blmg_sectorname)
drop if isin=="" | strpos(blmg_sectorname,"#")>0 | blmg_sectorname==""
drop if _merge==2
drop _merge

replace cdp_startyear = min_year if cdp_startyear==.
replace achieved=0 if failed==1
replace disappeared=0 if achieved==1 | failed==1

preserve
	use "${data}/cdp_summary", clear
	collapse (max) last_report = year, by(id)
	tempfile exit
	save `exit', replace
restore

merge 1:1 id using `exit'
drop if _merge==2
drop _merge
drop if disappeared==1 & last_year==last_report
drop last_report

gen type = cond(achieved==1,"Achieved",cond(failed==1, "Failed", "Disappeared"))
save "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", replace









