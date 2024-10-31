clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* split announcements by better ESG vs worse
import delim "${cleaned}/panel_returnVolume_TargetAnnounce.csv", clear

preserve
use "${cleaned}/ghgchange", clear
keep if year==2017 
keep id ghg_change_real
tempfile temp
save `temp', replace
restore

merge m:1 id using `temp'
drop if _merge==2
drop _merge

egen decarb_median = median(ghg_change_real)
gen type_high_decarb = ghg_change_real >= decarb_median
tab type_high_decarb

eststo clear
eststo: quietly estpost sum targetannounce_car_m1_p1 targetannounce_car_m1_p3 targetannounce_abnvol_pctlog_day if type_high_decarb==1
eststo: quietly estpost sum targetannounce_car_m1_p1 targetannounce_car_m1_p3 targetannounce_abnvol_pctlog_day if type_high_decarb==0
eststo: estpost ttest targetannounce_car_m1_p1 targetannounce_car_m1_p3 targetannounce_abnvol_pctlog_day, by(type_high_decarb)
esttab using "${tables}/table_s12.tex", replace label title(Summary Statistics) cells("mean(pattern(1 1 0) fmt(3)) sd(pattern(1 1 0) fmt(3)) b(pattern(0 0 1) fmt(3)) p(pattern(0 0 1) fmt(3))") mgroups("High Reduction" "Low Reduction" "Diff" "T-test", pattern(1 1 1 1)) varwidth(30) nonumbers   
