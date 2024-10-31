clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



* Tabel S7a
use "${cleaned}/event_study_panel_2018", clear

eststo clear
eststo: reg cdp_car_m1_p1 i.lag_behind_2018, robust
eststo: reg cdp_car_m1_p3 i.lag_behind_2018, robust
eststo: reg cdp_abnvol_pctlog_day0 i.lag_behind_2018, robust

use "${cleaned}/event_study_panel_2019", clear
eststo: reg cdp_car_m1_p1 i.lag_behind_2019, robust
eststo: reg cdp_car_m1_p3 i.lag_behind_2019, robust
eststo: reg cdp_abnvol_pctlog_day0 i.lag_behind_2019, robust

esttab using "${tables}/table_s7a.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(1.lag_behind_2018 1.lag_behind_2019) s(N r2_a , label("N" "Adj. R-squared") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)



* Tabel S7b
use "${cleaned}/final_consequences", clear

gen post1 = year>=2018
label var post1 "Post 2018"
gen post2 = year>=2019
label var post2 "Post 2019"

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe count_both i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe count_e_only i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe count_both i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe count_e_only i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s7b.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels keep(1.post1#1.lag_behind_2018 1.post2#1.lag_behind_2019) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S7c
eststo clear
eststo: reghdfe pulseenvironment i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe insightenvironment i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe pulseenvironment i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe insightenvironment i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s7c.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels keep(1.post1#1.lag_behind_2018 1.post2#1.lag_behind_2019) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S7d
eststo clear
eststo: reghdfe enscore i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe emiscore i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe enscore i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe emiscore i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s7d.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels keep(1.post1#1.lag_behind_2018 1.post2#1.lag_behind_2019) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S7e
eststo clear
eststo: reghdfe msci_enscore i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe msci_emiscore i.post1##i.lag_behind_2018 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe msci_enscore i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe msci_emiscore i.post2##i.lag_behind_2019 `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s7e.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels keep(1.post1#1.lag_behind_2018 1.post2#1.lag_behind_2019) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


