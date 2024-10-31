clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


use "${data}/cdp_incentives", clear
replace year = year-1  // CDP reporting year -> fiscal year

gen management = strpos(position, "board")>0 | strpos(position, "chief")>0 | strpos(position, "executive")>0 | strpos(position, "business unit manager")>0 | strpos(position, "facilities manager")>0 | strpos(position, "management group")>0 | strpos(position, "c-suite")>0 | strpos(position, "president")>0 | strpos(position, "process operation manager")>0 | strpos(position, "procurement manager")>0 | strpos(position, "public affairs manager")>0 | strpos(position, "risk manager")>0
gen monetary = strpos(type, "monetary")>0
gen nonmonetary = strpos(type, "non")>0
gen management_monetary = management==1 & monetary==1
gen management_nonmonetary = management==1 & nonmonetary==1
collapse (max) management_monetary management_nonmonetary, by(id year)

save "${cleaned}/incentives", replace
