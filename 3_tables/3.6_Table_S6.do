clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




**# 1 Table S6
use "${cleaned}/final_consequences", clear

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe enscore i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe emiscore i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s6a.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post `controls') keep(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post `controls') interaction(" X ") s(N r2_a fe1 fe2, label("N" "Adj. R-squared" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)

eststo clear
eststo: reghdfe msci_enscore i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe msci_emiscore i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s6b.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post `controls') keep(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post `controls') interaction(" X ") s(N r2_a fe1 fe2, label("N" "Adj. R-squared" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)
