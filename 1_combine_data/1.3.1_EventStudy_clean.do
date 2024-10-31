clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* Run Python code to clean event study data first
clear
python script "code/1_combine_data/1.3.2_EventStudy_clean.py"

* clean a firm-event level panel data for regressions
import delim "${cleaned}/panel_returnVolume_CDPrelease.csv", clear
merge 1:1 id using "${cleaned}/final_firm_level_broader_sample"
keep if _merge==3
drop _merge

preserve
drop cdp18* cdp19*
label var cdp_car_m1_p1 "CAR [-1,1]"
label var cdp_car_m1_p3 "CAR [-1,3]"
label var cdp_abnvol_pctlog_day0 "AbnTradingVolume"
save "${cleaned}/event_study_panel_2021", replace
restore

preserve
drop cdp_* cdp19*
rename cdp18* cdp*
label var cdp_car_m1_p1 "CAR [-1,1]"
label var cdp_car_m1_p3 "CAR [-1,3]"
label var cdp_abnvol_pctlog_day0 "AbnTradingVolume"
save "${cleaned}/event_study_panel_2018", replace
restore

preserve
drop cdp_* cdp18*
rename cdp19* cdp*
label var cdp_car_m1_p1 "CAR [-1,1]"
label var cdp_car_m1_p3 "CAR [-1,3]"
label var cdp_abnvol_pctlog_day0 "AbnTradingVolume"
save "${cleaned}/event_study_panel_2019", replace
restore
