clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* determinants of target outcomes

use "${cleaned}/final_determinant", clear

gen i_type = cond(achieved==1, 1, cond(failed==1, 2, 3))

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment num_targets_2020

eststo clear
eststo: mlogit i_type mean_ambition `controls' i.industry2, base(1) vce(cluster id) 
estadd local fe1 "Yes"
eststo: mlogit i_type mean_ambition `controls', base(1) vce(cluster id) 
estadd local fe1 "No"
esttab using "${tables}/table_ex4.tex", replace label title(Determinants) noobs keep(mean_ambition `controls') order(mean_ambition `controls') s(N r2_p fe1, label("N" "Psuedo R-squared" "Industry FE") fmt(0 %9.3f)) star(* .10 ** .05 *** .01) nocon b(3) unstack eqlabels("Achieved vs. Failed" "Achieved vs. Disappeared") collab(none) mlabels(none) drop(1:)
