clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* identify industries that are impacted by COVID more based on change in revenue in 2020 
use "${cleaned}/datastream_variables", clear
winsor2 revenue, c(1 99) replace
bys isin (year): gen revenue_change = abs(revenue[_n] - revenue[_n-1])/abs(revenue[_n-1])*100
keep if year==2020

merge 1:m isin using "${cleaned}/cdp_summary_firm", keepusing(blmg_industrygroupname)
drop if _merge==2
drop _merge
drop if strpos(blmg_industrygroupname,"#")>0 | blmg_industrygroupname==""

collapse (mean) revenue_change, by(blmg_industrygroupname)
su revenue_change, d
gen type_covid_industry = revenue_change >= r(p50)
keep blmg_industrygroupname type_covid_industry
save "${cleaned}/covid_industry_split", replace
