clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



**# 1. Mandatory environmental disclosure 

import excel "${data}/country_regulation_data_E_only.xlsx", clear firstrow case(lower)
replace country="Netherlands" if country=="The Netherlands"
replace country="Taiwan" if country=="Taiwan (China)"
gen regulation_e_only = 1
keep country regulation_e_only
save "${cleaned}/regulation_e_only", replace



**# 2. Legal origin

clear 
set obs 18
gen country = "Australia" in 1
replace country = "Canada" in 2
replace country = "Hong Kong" in 3
replace country = "India" in 4
replace country = "Ireland" in 5
replace country = "Israel" in 6
replace country = "Kenya" in 7
replace country = "Malaysia" in 8
replace country = "New Zealand" in 9
replace country = "Nigeria" in 10
replace country = "Pakistan" in 11
replace country = "Singapore" in 12
replace country = "South Africa" in 13
replace country = "Sri Lanka" in 14
replace country = "Thailand" in 15
replace country = "USA" in 16
replace country = "United Kingdom" in 17
replace country = "Zimbabwe" in 18
gen uk_legal_origin = 1
save "${cleaned}/uk_legal_origin", replace



**# 3. Carbon pricing

import excel "${data}/Carbon pricing/laws_and_policies_10022020.xlsx", clear firstrow case(lower)
gen year = substr(events, 7, 4) 
destring year, replace
drop if year>=2020

keep geography
rename geography country
duplicates drop

set obs `=_N+6'
replace country = "Austria" in 72
replace country = "Belgium" in 73
replace country = "Germany" in 74
replace country = "Greece" in 75
replace country = "Hungary" in 76
replace country = "Poland" in 77
gen carbon_pricing = 1
save "${cleaned}/carbon_pricing", replace




**# 4. Developed vs. developing 

import excel "${data}/country_classification_imf.xlsx", clear firstrow case(lower)
replace country = "South Korea" if country=="Korea"
replace country = "Turkey" if country=="Türkiye"
rename country country_std
save "${cleaned}/country_imf", replace




**# 5. Press freedom

import excel "${data}/FOTP1980-FOTP2017_Public-Data.xlsx", sheet("Data") clear
keep A FY
rename (A FY) (country press_freedom)
destring press_freedom, force replace
drop if press_freedom==.
replace country = "USA" if country=="United States"
save "${cleaned}/press_freedom", replace




**# 6. Materiality
use "${data}/cdp_ghg_all_scopes", clear
replace year = year-1
keep if year>=2017 & year<=2020
gen ghg12 = ghg1 + ghg2location
keep id year ghg12

merge m:1 id using "${data}/cdp_summary_firm", keepusing(blmg_industrygroupname)
drop if _merge==2
drop _merge
drop if strpos(blmg_industrygroupname,"#")>0 | blmg_industrygroupname==""

collapse (mean) ghg12, by(blmg_industrygroupname)
qui su ghg12, d
gen emission_industry_high = ghg12 >= r(p50)
drop ghg12
save "${cleaned}/emission_industry_split", replace



**# 7. Media Volume
save "${cleaned}/insight_and_volume", replace
keep if year >= 2017 & year <= 2019
drop insightenvironment
collapse (mean) volumeghg, by(isin)

merge 1:1 isin using "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap"
keep if _merge==3
drop _merge

egen volumeghg_median = median(volumeghg)
gen high_volumeghg = volumeghg >= volumeghg_median

keep isin high_volumeghg
save "${cleaned}/volume_split", replace




