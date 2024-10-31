clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




* Tabel S9a
use "${cleaned}/event_study_panel_2021", clear
drop if type_covid_industry==1

eststo clear
eststo: reg cdp_car_m1_p1 i.i_type, robust
eststo: reg cdp_car_m1_p3 i.i_type, robust
eststo: reg cdp_abnvol_pctlog_day0 i.i_type, robust
esttab using "${tables}/table_s9a.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type 2.i_type 3.i_type) s(N r2_a , label("N" "Adj. R-squared") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)



* Tabel S9b
use "${cleaned}/final_consequences", clear
drop if type_covid_industry==1

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe count_both i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe count_e_only i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s9b.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post) keep(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S9c
eststo clear
eststo: reghdfe pulseenvironment i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe insightenvironment i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s9c.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post) keep(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S9d
eststo clear
eststo: reghdfe enscore i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe emiscore i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s9d.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post) keep(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


* Tabel S9e
eststo clear
eststo: reghdfe msci_enscore i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe msci_emiscore i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local control "Yes"
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s9e.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post) keep(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post) interaction(" X ") s(N r2_a control fe1 fe2, label("N" "Adj. R-squared" "Controls" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)


