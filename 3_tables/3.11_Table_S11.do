clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



* Tabel S11a
* Market tests for target announcements are performed in 1_combine_data/1.3.2_EventStudy_clean.py


* Tabel S11b
use "${cleaned}/final_announcements", clear

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe count_both i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe count_e_only i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s11b.tex", replace label title(Announcements of Targets) noobs nobaselevels order(1.post `controls') keep(1.post `controls') interaction(" X ") s(N r2_a fe1 fe2, label("N" "Adj. R-squared" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)



* Tabel S11c
local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe pulseenvironment i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe insightenvironment i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s11c.tex", replace label title(Announcements of Targets) noobs nobaselevels order(1.post `controls') keep(1.post `controls') interaction(" X ") s(N r2_a fe1 fe2, label("N" "Adj. R-squared" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)



* Tabel S11d
local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe enscore i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe emiscore i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s11d.tex", replace label title(Announcements of Targets) noobs nobaselevels order(1.post `controls') keep(1.post `controls') interaction(" X ") s(N r2_a fe1 fe2, label("N" "Adj. R-squared" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)



* Tabel S11e
local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: reghdfe msci_enscore i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
eststo: reghdfe msci_emiscore i.post `controls', vce(cluster id) absorb(id year)
estadd local fe1 "Yes"
estadd local fe2 "Yes"
esttab using "${tables}/table_s11f.tex", replace label title(Announcements of Targets) noobs nobaselevels order(1.post `controls') keep(1.post `controls') interaction(" X ") s(N r2_a fe1 fe2, label("N" "Adj. R-squared" "Firm FE" "Year FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3)



