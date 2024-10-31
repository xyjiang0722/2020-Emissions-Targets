clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"




**# 1. Merge data -> firm-year level

use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear

merge 1:1 id using "${cleaned}/cdp_summary_firm", keepusing(country blmg_sectorname blmg_industrygroupname blmg_industryname)
drop if _merge==2
drop _merge
drop if isin=="" | strpos(blmg_industrygroupname,"#")>0 | blmg_industrygroupname==""
encode blmg_sectorname, gen(industry1)
encode blmg_industrygroupname, gen(industry2)
encode blmg_industryname, gen(industry3)

merge 1:m isin using "${cleaned}/datastream_variables"
drop if _merge==2
drop _merge

merge 1:1 isin year using "${cleaned}/msci"
drop if _merge==2
drop _merge

merge 1:1 isin year using "${cleaned}/pulse"
drop if _merge==2
drop _merge

merge 1:1 isin year using "${cleaned}/insight_and_volume"
drop if _merge==2
drop _merge

gen cusip = substr(isin, 3, 9)
merge 1:1 cusip year using "${cleaned}/proposal"
drop if _merge==2
replace count_both = 0 if count_both==. & country=="USA"
replace count_e_only = 0 if count_e_only==. & country=="USA"
drop _merge

merge 1:1 id year using "${cleaned}/initiatives"
replace cdp_ini_count = 0 if _merge==1
replace cdp_ini_saving = 0 if _merge==1
replace cdp_ini_investment = 0 if _merge==1
drop if _merge==2
drop _merge

merge 1:1 id year using "${cleaned}/incentives"
drop if _merge==2
replace management_monetary = 0 if _merge==1
replace management_nonmonetary = 0 if _merge==1
drop _merge

merge 1:1 id year using "${cleaned}/ghgchange"
drop if _merge==2
drop _merge

merge m:1 id using "${cleaned}/disappeared_split"
drop _merge
replace type = "Disappeared High Reduction" if type_high_reduction==1
replace type = "Disappeared Low Reduction" if type_high_reduction==0
replace type = "" if type_high_reduction==. & type=="Disappeared"
drop if type==""
encode type, gen(i_type)
gen disappeared_high_reduction = type_high_reduction==1
gen disappeared_low_reduction = type_high_reduction==0

merge m:1 blmg_industrygroupname using "${cleaned}/emission_industry_split"
drop _merge

merge m:1 blmg_industrygroupname using "${cleaned}/covid_industry_split"
drop _merge

merge 1:1 id year using "${cleaned}/whether_lagging_behind_all_years"
drop if _merge==2
drop _merge

merge m:1 id using "${cleaned}/whether_lagging_behind_2019_2020", keepusing(id lag_behind_2018 lag_behind_2019)
drop if _merge==2
replace lag_behind_2018 = 0 if lag_behind_2018==.
replace lag_behind_2019 = 0 if lag_behind_2019==.
drop _merge






**# 2. Clean data -> firm-year level

winsor2 asset revenue mv cdp_ini_saving cdp_ini_investment ghg_change_real, c(1 99) replace 
gen l_mv = log(mv+1)
gen l_cdp_ini_saving = log(cdp_ini_saving+1)
gen l_cdp_ini_investment = log(cdp_ini_investment+1)

local controls l_mv roa vol salesg ptbv capint management_monetary management_nonmonetary cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment
su `controls'
gen constant_sample = 1
foreach v in `controls' {
	replace constant_sample = 0 if `v'==.
}
keep if constant_sample == 1

foreach var of varlist pulseenvironment insightenvironment enscore emiscore {
	replace `var' = `var'/10
}

label var enscore "Asset4 Environmental Score"
label var emiscore "Asset4 Emission Score"
label var msci_enscore "MSCI Environmental Score"
label var msci_emiscore "MSCI Emissions Score"
label var l_mv "Log(MV)"
label var roa "ROA"
label var vol "Price Volatility"
label var salesg "Sales Growth"
label var ptbv "Price to Book"
label var capint "Capital Intensity"
label var cdp_ini_count "#Initiatives"
label var l_cdp_ini_saving "Log(Total Carbon Savings)"
label var l_cdp_ini_investment "Log(Total Project Investment)"
label var management_monetary "Monetary Management"
label var management_nonmonetary "Non-Monetary Management"
label var pulseenvironment "Pulse Score"
label var insightenvironment "Insight Score"
label var count_both "E&S and E Proposals"
label var count_e_only "E Proposal"
label var ghg_change_real "% Emissions Reduction"

label var failed_high_ambition "Failed Ambitious Targets"
label var failed_low_ambition "Failed Unambitious Targets"

label var type "Type"
label var achieved "Achieved"
label var failed "Failed"
label var disappeared "Disappeared"
label var disappeared_high_reduction "Disappeared - leaders"
label var disappeared_low_reduction "Disappeared - laggards"

label var lag_behind "Lagging Behind Indicator"
label var lag_behind_2018 "Lagging Behind in 2018"
label var lag_behind_2019 "Lagging Behind in 2019"
label var num_targets_2020 "#Targets"

save "${cleaned}/final_all_years", replace


* outcomes: 2017 - 2021
use "${cleaned}/final_all_years", clear
gen post = year>=2021
keep if year>=2017 & year<=2021
sort id year
label var post "Post"
save "${cleaned}/final_consequences", replace


* announcements: start year
use "${cleaned}/final_all_years", clear
gen post = year>=cdp_startyear
bys id: egen all_post = min(post)
drop if all_post == 1
drop if cdp_startyear > 2017 
label var post "Post Announcement"
save "${cleaned}/final_announcements", replace


* Firm level
use "${cleaned}/final_consequences", clear
keep id isin companyname country blmg_sectorname blmg_industrygroupname industry1 industry2 last_year type i_type failed disappeared achieved disappeared_high_reduction disappeared_low_reduction emission_industry_high type_covid_industry failed_high_ambition failed_low_ambition lag_behind_2018 lag_behind_2019
order id isin companyname country blmg_sectorname blmg_industrygroupname industry1 industry2 last_year type i_type failed disappeared achieved disappeared_high_reduction disappeared_low_reduction emission_industry_high type_covid_industry failed_high_ambition failed_low_ambition lag_behind_2018 lag_behind_2019
duplicates drop
unique id
save "${cleaned}/final_firm_level", replace


* broader sample
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
keep id isin achieved disappeared failed type
merge m:1 id using "${cleaned}/disappeared_split"
drop _merge
replace type = "Disappeared High Reduction" if type_high_reduction==1
replace type = "Disappeared Low Reduction" if type_high_reduction==0
replace type = "" if type_high_reduction==. & type=="Disappeared"
tab type
drop if type==""
encode type, gen(i_type)

merge m:1 id using "${cleaned}/cdp_summary_firm", keepusing(blmg_industrygroupname)
keep if _merge==3
drop _merge

merge m:1 blmg_industrygroupname using "${cleaned}/emission_industry_split"
drop _merge

merge m:1 blmg_industrygroupname using "${cleaned}/covid_industry_split"
drop _merge

merge 1:1 id using "${cleaned}/ambition_failed_split"
drop if _merge==2
drop _merge
replace failed_high_ambition = 0 if failed_high_ambition==.
replace failed_low_ambition = 0 if failed_low_ambition==.

merge m:1 id using "${cleaned}/whether_lagging_behind_2019_2020", keepusing(id lag_behind_2018 lag_behind_2019)
drop if _merge==2
drop _merge
replace lag_behind_2018 = 0 if lag_behind_2018==.
replace lag_behind_2019 = 0 if lag_behind_2019==.

tab i_type
save "${cleaned}/final_firm_level_broader_sample", replace




**# 3. Determinant - Firm level
use "${cleaned}/final_all_years", clear
keep if year>=2017 & year<=2020

collapse (mean) l_mv roa vol salesg ptbv capint cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment num_targets_2020 mean_ambition (max) management_monetary management_nonmonetary, by(id isin country blmg_sectorname blmg_industrygroupname industry1 industry2 failed achieved disappeared)

label var l_mv "Log(MV)"
label var roa "ROA"
label var vol "Price Volatility"
label var salesg "Sales Growth"
label var ptbv "Price to Book"
label var capint "Capital Intensity"
label var cdp_ini_count "#Initiatives"
label var l_cdp_ini_saving "Log(Total Carbon Savings)"
label var l_cdp_ini_investment "Log(Total Project Investment)"
label var management_monetary "Monetary Management"
label var management_nonmonetary "Non-Monetary Management"
label var num_targets_2020 "#Targets"
label var mean_ambition "Average Ambition"
label var failed "Failed"
label var achieved "Achieved"
label var disappeared "Disappeared"

save "${cleaned}/final_determinant", replace

