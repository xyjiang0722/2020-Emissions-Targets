clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* whether firms set 2020 targets

use "${cleaned}/cdp_summary", clear
bys id: gen num_years = _N  // #Years Reporting to CDP
keep if year<=2018
keep id num_years
duplicates drop

merge 1:1 id using "${cleaned}/cdp_summary_firm", keepusing(isin blmg_sectorname country companyname)
drop if _merge==2
drop _merge
drop if isin=="" | strpos(blmg_sectorname,"#")>0 | blmg_sectorname==""

merge m:1 country using "${cleaned}/regulation_e_only"
drop if _merge==2
replace regulation_e_only = 0 if _merge==1
drop _merge

merge 1:m isin using "${cleaned}/datastream_variables"
drop if _merge==2
drop _merge

merge 1:1 isin year using "${cleaned}/insight_and_volume"
drop if _merge==2
drop _merge

merge 1:1 isin year using "${cleaned}/msci"
drop if _merge==2
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

merge m:1 id using "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", keepusing(id failed)
drop if _merge==2
gen have_target = _merge==3
drop _merge



**# create firm-level data
keep if year>=2017 & year<=2019
collapse (mean) asset-cdp_ini_investment (max) management_monetary management_nonmonetary have_target, by(id failed isin blmg_sectorname country companyname regulation_e_only num_years)

winsor2 mv cdp_ini_saving cdp_ini_investment, c(1 99) replace 
gen l_mv = log(mv+1)
gen l_cdp_ini_saving = log(cdp_ini_saving+1)
gen l_cdp_ini_investment = log(cdp_ini_investment+1)

foreach var of varlist enscore emiscore {
	replace `var' = `var'/10
}

local controls l_mv roa vol salesg ptbv capint
su `controls'
gen constant_sample = 1
foreach v in `controls' {
	replace constant_sample = 0 if `v'==.
}
keep if constant_sample == 1

label var l_mv "Log(MV)"
label var roa "ROA"
label var vol "Price Volatility"
label var salesg "Sales Growth"
label var ptbv "Price to Book"
label var capint "Capital Intensity"

label var enscore "Asset4 Environmental Score"
label var emiscore "Asset4 Emission Score"
label var msci_enscore "MSCI Environmental Score"
label var msci_emiscore "MSCI Emissions Score"

label var cdp_ini_count "#Initiatives"
label var l_cdp_ini_saving "Log(Total Carbon Savings)"
label var l_cdp_ini_investment "Log(Total Project Investment)"
label var management_monetary "Monetary Management"
label var management_nonmonetary "Non-Monetary Management"

label var volumeenvironment "Environmental Media Volume"
label var num_years "\#Years Reporting to CDP" 
label var regulation_e_only "Mandatory E Disclosure"



**# Bookmark #7 Tables
global financial = "l_mv roa vol salesg ptbv capint"
global initiative = "cdp_ini_count l_cdp_ini_saving l_cdp_ini_investment"
global incentive = "management_monetary management_nonmonetary"
global scores = "enscore emiscore msci_enscore msci_emiscore"
global disclosure = "volumeenvironment num_years regulation_e_only"

** Table Appendix
eststo clear
eststo: quietly estpost sum ${financial} ${initiative} ${incentive} ${scores} ${disclosure} if have_target==1
eststo: quietly estpost sum ${financial} ${initiative} ${incentive} ${scores} ${disclosure} if have_target==0
eststo: estpost ttest ${financial} ${initiative} ${incentive} ${scores} ${disclosure}, by(have_target)
esttab using "${tables}/table_ex2.tex", replace label title(Summary Statistics - Determinants Have vs. Not) cells("mean(pattern(1 1 0) fmt(3)) sd(pattern(1 1 0) fmt(3)) b(pattern(0 0 1) fmt(3)) p(pattern(0 0 1) fmt(3))") mgroups("With 2020 Targets" "Without 2020 Targets" "Diff" "T-test", pattern(1 1 1 1)) varwidth(30) nonumbers   



