clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



* Tabel S8a
use "${cleaned}/event_study_panel_2021", clear
drop if i_type==2|i_type==3  // drop disappeared

eststo clear
eststo: reg cdp_car_m1_p1 i.failed_high_ambition i.failed_low_ambition, robust
eststo: reg cdp_car_m1_p3 i.failed_high_ambition i.failed_low_ambition, robust
eststo: reg cdp_abnvol_pctlog_day0 i.failed_high_ambition i.failed_low_ambition, robust
esttab using "${tables}/table_s8a.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(1.failed_high_ambition 1.failed_low_ambition) s(N r2_a , label("N" "Adj. R-squared") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S8b
use "${cleaned}/final_consequences", clear
drop if disappeared==1

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe count_both i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe count_e_only i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s8b.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(1.failed_high_ambition#1.post 1.failed_low_ambition#1.post) keep(1.failed_high_ambition#1.post 1.failed_low_ambition#1.post) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S8c
eststo clear
eststo: reghdfe pulseenvironment i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe insightenvironment i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s8c.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(1.failed_high_ambition#1.post 1.failed_low_ambition#1.post) keep(1.failed_high_ambition#1.post 1.failed_low_ambition#1.post) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S8d
eststo clear
eststo: reghdfe enscore i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe emiscore i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s8d.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(1.failed_high_ambition#1.post 1.failed_low_ambition#1.post) keep(1.failed_high_ambition#1.post 1.failed_low_ambition#1.post) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S8e
eststo clear
eststo: reghdfe msci_enscore i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe msci_emiscore i.failed_high_ambition##i.post i.failed_low_ambition##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s8e.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(1.failed_high_ambition#1.post 1.failed_low_ambition#1.post) keep(1.failed_high_ambition#1.post 1.failed_low_ambition#1.post) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)



