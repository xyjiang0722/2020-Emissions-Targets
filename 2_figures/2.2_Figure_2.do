clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



**# 1 Fig. 2a
* split by industry
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
encode blmg_sectorname, gen(industry1)

bys industry1: egen ind_count_achieved = sum(achieved)
bys industry1: egen ind_count_failed = sum(failed)
bys industry1: egen ind_count_disappeared = sum(disappeared)
bys industry1: egen ind_count = count(failed)
gen achieved_ratio = ind_count_achieved/ind_count
gen failed_ratio = ind_count_failed/ind_count
gen disappeared_ratio = ind_count_disappeared/ind_count

egen group = group(achieved_ratio blmg_sectorname)
replace group = -group
labmask group, values(blmg_sectorname)
label var group "Industry"
label list group

eststo clear
eststo: quietly estpost tabstat ind_count, by(group) s(count) elabels
eststo: quietly estpost tabstat ind_count if achieved==1, by(group) s(count) elabels
eststo: quietly estpost tabstat achieved_ratio , by(group) s(mean) elabels
eststo: quietly estpost tabstat ind_count if failed==1, by(group) s(count) elabels
eststo: quietly estpost tabstat failed_ratio , by(group) s(mean) elabels
eststo: quietly estpost tabstat ind_count if disappeared==1, by(group) s(count) elabels
eststo: quietly estpost tabstat disappeared_ratio , by(group) s(mean) elabels
esttab est1 est2 est3 est4 est5 est6 est7 using "${figures}/split_by_industry.csv", replace label noobs title(Firms with Failed and Disappeared Targets Summary Statistics by Industry) cells("count(pattern(1 1 0 1 0 1 0))  mean(pattern(0 0 1 0 1 0 1) fmt(2))") mgroups("All Firms" "Firms with Achieved Targets" "Achieved Ratio" "Firms with Failed Targets" "Failed Ratio" "Firms with Disappeared Targets" "Disappeared Ratio", pattern(1 1 1 1 1 1 1)) varlabels(`e(labels)')



**# 2 Fig. 2b
* split by country
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear

tab country
bys country: gen num = _N
bys country: egen num2 = sum(achieved)
replace country = "Other" if num<=3 | num2==0
tab country
drop num num2

bys country: egen ind_count_achieved = sum(achieved)
bys country: egen ind_count_failed = sum(failed)
bys country: egen ind_count_disappeared = sum(disappeared)
bys country: egen ind_count = count(failed)
gen achieved_ratio = ind_count_achieved/ind_count
gen failed_ratio = ind_count_failed/ind_count
gen disappeared_ratio = ind_count_disappeared/ind_count

gen group_order = achieved_ratio
replace group_order=0 if country=="Other" 
egen group = group(group_order country)
replace group = -group
labmask group, values(country)
label var group "Country"
label list group

eststo clear
eststo: quietly estpost tabstat ind_count, by(group) s(count) elabels
eststo: quietly estpost tabstat ind_count if achieved==1, by(group) s(count) elabels
eststo: quietly estpost tabstat achieved_ratio , by(group) s(mean) elabels
eststo: quietly estpost tabstat ind_count if failed==1, by(group) s(count) elabels
eststo: quietly estpost tabstat failed_ratio , by(group) s(mean) elabels
eststo: quietly estpost tabstat ind_count if disappeared==1, by(group) s(count) elabels
eststo: quietly estpost tabstat disappeared_ratio , by(group) s(mean) elabels
esttab est1 est2 est3 est4 est5 est6 est7 using "${figures}/split_by_country.csv", replace label noobs title(Firms with Failed and Disappeared Targets Summary Statistics by Country) cells("count(pattern(1 1 0 1 0 1 0))  mean(pattern(0 0 1 0 1 0 1) fmt(2))") mgroups("All Firms" "Firms with Achieved Targets" "Achieved Ratio" "Firms with Failed Targets" "Failed Ratio" "Firms with Disappeared Targets" "Disappeared Ratio", pattern(1 1 1 1 1 1 1)) varlabels(`e(labels)')



**# 3 Fig. 2c
* split by legal origin
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
merge m:1 country using "${cleaned}/uk_legal_origin"
drop if _merge==2
drop _merge
replace uk_legal_origin = 0 if uk_legal_origin==.

unique id if achieved==1 & uk_legal_origin==0
unique id if achieved==1 & uk_legal_origin==1
unique id if failed==1 & uk_legal_origin==0
unique id if failed==1 & uk_legal_origin==1
unique id if disappeared==1 & uk_legal_origin==0
unique id if disappeared==1 & uk_legal_origin==1



**# 4 Fig. 2d
* EU vs US
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
kountry country, from(other) geo(marc)
]replace GEO="Asia" if GEO==""  // Hong Kong and Taiwan
rename (NAMES_STD GEO) (country_std continent)

gen eu = continent=="Europe"
replace eu = 0 if country_std=="United Kingdom" | country=="Iceland" | country=="Norway" | country=="Russia" | country=="Switzerland"
tab eu

unique id if achieved==1 & eu==1
unique id if achieved==1 & country_std=="United States"
unique id if failed==1 & eu==1
unique id if failed==1 & country_std=="United States"
unique id if disappeared==1 & eu==1
unique id if disappeared==1 & country_std=="United States"



**# 5 Fig. 2e
* split by mandatory E disclosure
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
merge m:1 country using "${cleaned}/regulation_e_only"
drop if _merge==2
replace regulation_e_only = 0 if _merge==1
drop _merge

unique id if achieved==1 & regulation_e_only==0
unique id if achieved==1 & regulation_e_only==1
unique id if failed==1 & regulation_e_only==0
unique id if failed==1 & regulation_e_only==1
unique id if disappeared==1 & regulation_e_only==0
unique id if disappeared==1 & regulation_e_only==1



**# 6 Fig. 2f
* split by carbon procong
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
merge m:1 country using "${cleaned}/carbon_pricing"
drop if _merge==2
drop _merge
replace carbon_pricing = 0 if carbon_pricing==.

unique id if achieved==1 & carbon_pricing==0
unique id if achieved==1 & carbon_pricing==1
unique id if failed==1 & carbon_pricing==0
unique id if failed==1 & carbon_pricing==1
unique id if disappeared==1 & carbon_pricing==0
unique id if disappeared==1 & carbon_pricing==1



**# 7 Fig. 2g
* developing vs. developed countries
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
kountry country, from(other) geo(marc)
tab country if GEO==""
replace GEO="Asia" if GEO==""  // Hong Kong and Taiwan
rename (NAMES_STD GEO) (country_std continent)

merge m:1 country_std using "${cleaned}/country_imf"
drop if _merge==2
drop _merge
replace advanced_economy = 0 if advanced_economy==.

unique id if achieved==1 & advanced_economy==0
unique id if achieved==1 & advanced_economy==1
unique id if failed==1 & advanced_economy==0
unique id if failed==1 & advanced_economy==1
unique id if disappeared==1 & advanced_economy==0
unique id if disappeared==1 & advanced_economy==1



**# 8 Fig. 2h
* split by press freedom
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
merge m:1 country using "${cleaned}/press_freedom"
keep if _merge==3
drop _merge

preserve
	collapse (mean) press_freedom, by(country)
	egen median_freedom = median(press_freedom)
	gen high_freedom = press_freedom <= median_freedom
	tempfile temp
	save `temp'
restore

merge m:1 country using `temp', keepusing(country high_freedom)
tab country if high_freedom==0

unique id if achieved==1 & high_freedom==0
unique id if achieved==1 & high_freedom==1
unique id if failed==1 & high_freedom==0
unique id if failed==1 & high_freedom==1
unique id if disappeared==1 & high_freedom==0
unique id if disappeared==1 & high_freedom==1



**# 9 Fig. 2i
* split by materiality
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
merge 1:1 id using "${cleaned}/cdp_summary_firm", keepusing(country blmg_industrygroupname)
keep if _merge==3
drop _merge
merge m:1 blmg_industrygroupname using "${cleaned}/emission_industry_split"
drop _merge

unique id if achieved==1 & emission_industry_high==0
unique id if achieved==1 & emission_industry_high==1
unique id if failed==1 & emission_industry_high==0
unique id if failed==1 & emission_industry_high==1
unique id if disappeared==1 & emission_industry_high==0
unique id if disappeared==1 & emission_industry_high==1



**# 10 Fig. 2j
* split by environmental media volume
* input the numbers to excel and make the figure there
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
merge m:1 isin using "${cleaned}/volume_split"
keep if _merge==3
drop _merge

unique id if achieved==1 & high_volumeghg==0
unique id if achieved==1 & high_volumeghg==1
unique id if failed==1 & high_volumeghg==0
unique id if failed==1 & high_volumeghg==1
unique id if disappeared==1 & high_volumeghg==0
unique id if disappeared==1 & high_volumeghg==1
