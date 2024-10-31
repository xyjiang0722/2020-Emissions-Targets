clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



use "${data}/cdp_initiative", clear
replace year = year-1  // CDP reporting year -> fiscal year

gen cdp_ini_count = 1
collapse (sum) cdp_ini_count cdp_ini_co2 cdp_ini_investmentrequired, by(id year)
rename cdp_ini_co2 cdp_ini_saving
rename cdp_ini_investmentrequired cdp_ini_investment

keep id year cdp_ini_count cdp_ini_saving cdp_ini_investment
save "${cleaned}/initiatives", replace
