clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



* Panel A
use "${cleaned}/event_study_panel_2021", clear
eststo clear
eststo: reg cdp_car_m1_p1 i.i_type, robust
eststo: reg cdp_car_m1_p3 i.i_type, robust
eststo: reg cdp_abnvol_pctlog_day0 i.i_type, robust
esttab using "${tables}/table_ex1a.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type 2.i_type 3.i_type) s(N r2_a , label("N" "Adj. R-squared") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)

 

* Regression Analysis for market reactions for CDP releases is performed in Stata as above. 


* Panel B, C, D: Market reactions to 2020 target outcomes for CDP releases, sustainability reports and media are calculated in code/1_combine_data/1.3.2_EventStudy_clean.py.



