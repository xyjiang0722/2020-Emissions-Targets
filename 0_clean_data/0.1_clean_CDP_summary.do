clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


**# 1. Clean firm-year level CDP summary data

use "${data}\cdp_summary", clear

format %30s companyname country industry46 industry14
format %10s ticker isin
destring id, replace
replace isin = strltrim(isin)
replace isin = strrtrim(isin)
replace ticker = strltrim(ticker)
replace ticker = strrtrim(ticker)

gsort companyname - year
by companyname: replace isin = isin[_n-1] if missing(isin)
by companyname: replace ticker = ticker[_n-1] if missing(ticker)

*clean countries to make it consistent
replace country="USA" if country=="United States of America"
replace country="United Kingdom" if country=="United Kingdom of Great Britain and Northern Ireland"
replace country="Russia" if country=="Russian Federation"
replace country="Taiwan" if country=="Taiwan, Greater China"
replace country="Taiwan" if country=="Taiwan, China"
replace country="South Korea" if country=="Republic of Korea"
replace country="South Korea" if country=="Korea"
replace country="Hong Kong" if country=="China, Hong Kong Special Administrative Region"
replace country="Czech Republic" if country=="Czechia"
replace country="Vietnam" if country=="Viet Nam"
tab country

replace ticker = "9397Z US" if id == 7675
replace ticker = "GE1 GR" if id == 7158
replace ticker = "1012Z NO" if id == 10216
replace ticker = "1436Z IN" if id == 44070
replace ticker = "1007Z SJ" if id == 41390

replace isin = substr(isin, 1, 12) if length(isin)>12

save "${cleaned}\cdp_summary", replace 




**# 2. Firm-level CDP summary data 

* keep the most recent year; unique at the ISIN level if non-missing
use "${cleaned}\cdp_summary", clear 

gsort id -year
by id: keep if _n == 1

gsort isin -year
by isin: keep if isin=="" | _n==1

preserve
	import excel "${data}/cdp_summary_industry.xlsx", sheet("blmg hardcoded") firstrow clear
	drop isinforsearch
	foreach x of varlist _all {
		replace `x' = "" if strpos(`x',"#")!=0
		rename `x' blmg_`x'
	} 
	rename blmg_isin isin
	duplicates drop
	tempfile blmg_industry
	save `blmg_industry', replace

	import excel "${data}/cdp_summary_industry.xlsx", sheet("refinitiv hardcoded") firstrow clear
	foreach x of varlist _all {
		rename `x' ref_`x'
	} 
	rename ref_isin isin
	duplicates drop
	tempfile ref_industry
	save `ref_industry', replace
restore

merge m:1 isin using `ref_industry'
drop if _merge==2
drop _merge

merge m:1 isin using `blmg_industry'
drop if _merge==2
drop _merge

save "${cleaned}\cdp_summary_firm", replace


