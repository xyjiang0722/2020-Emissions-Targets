clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




**# 1 Table S4
use "${cleaned}/final_consequences", clear

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe count_both i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe count_e_only i.i_type##i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s4.tex", replace label title(Consequences of Failing Targets) noobs nobaselevels order(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post `controls') keep(4.i_type#1.post 2.i_type#1.post 3.i_type#1.post `controls') interaction(" X ") s(N r2_a fe1 fe2, label("N" "Adj. R-squared" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)
