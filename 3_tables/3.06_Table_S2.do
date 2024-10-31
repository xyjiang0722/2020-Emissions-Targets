clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




**# 1 Table S2
* Summary statistics
use "${cleaned}/final_consequences", clear

local consequences_var count_both count_e_only pulseenvironment insightenvironment enscore emiscore msci_enscore msci_emiscore
local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment

eststo clear
eststo: estpost su achieved failed disappeared disappeared_high_reduction disappeared_low_reduction emission_industry_high failed_high_ambition failed_low_ambition `consequences_var' `controls'  post, d
esttab using "${tables}/table_s2.tex", replace label noobs title(Summary statistics) cells("count(fmt(%9.0fc)) mean(fmt(3)) sd(fmt(3)) p25(fmt(3)) p50(fmt(3)) p75(fmt(3))")  varwidth(30)






