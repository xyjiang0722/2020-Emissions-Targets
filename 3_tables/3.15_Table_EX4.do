clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



* taregt setters: early vs. late based on announcemetn years

* firms that set targets due in future years, append them with 2020 target setters
preserve
use "${cleaned}/cdp_target_clean", clear
drop if cdp_baseyear == cdp_targetyear
drop if cdp_targetyear <= 2020  // target level, drop 2020 and prior targets
collapse (min) cdp_startyear, by(id)
tempfile temp
save `temp', replace
restore

use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
merge 1:1 id using `temp'
drop _merge

drop isin
merge 1:1 id using "2 Data/CDP/cdp_summary_firm", keepusing(isin country blmg_sectorname blmg_industrygroupname blmg_industryname)
drop if _merge==2
drop _merge
drop if isin=="" | strpos(blmg_industrygroupname,"#")>0 | blmg_industrygroupname==""

merge 1:m isin using "${cleaned}/datastream_variables"
drop if _merge==2
drop _merge

merge 1:1 id year using "${cleaned}/ghgchange"
drop if _merge==2
drop _merge

foreach var of varlist enscore emiscore {
	replace `var' = `var'/10
}

* demean E scores by year
foreach var of varlist enscore emiscore msci_enscore msci_emiscore {
	bys year: egen mean_`var' = mean(`var')
	gen demeaned_`var' = `var'- mean_`var'
}

* 1-year window prior to announcement year
keep if year==cdp_startyear-1
collapse (mean) ghg_change_real demeaned*, by(id cdp_startyear)

* split announcement years into three groups
gen year_grp = cond(cdp_startyear>=2021,3,cond(cdp_startyear>=2017,2,1))
tab cdp_startyear year_grp
tab year_grp
tabstat ghg_change_real demean*, by(year_grp)

label var ghg_change_real "Decarbonization Rate"
label var demeaned_enscore "Asset4 E Score"
label var demeaned_emiscore "Asset4 Emissions Score"
label var demeaned_msci_enscore "MSCI E Score"
label var demeaned_msci_emiscore "MSCI Emissions Score"

eststo clear
eststo: estpost sum ghg_change_real demeaned* if year_grp==1
eststo: estpost sum ghg_change_real demeaned* if year_grp==2
eststo: estpost sum ghg_change_real demeaned* if year_grp==3
eststo: estpost ttest ghg_change_real demeaned* if year_grp!=2, by(year_grp)
esttab using "${tables}/table_ex4.tex", replace label title(Summary Statistics - Early vs. Late) cells("mean(pattern(1 1 1 0 0) fmt(3)) sd(pattern(1 1 1 0 0) fmt(3)) b(pattern(0 0 0 1 1) fmt(3)) p(pattern(0 0 0 1 1) fmt(3))") mgroups("(1) Before 2017" "(2) 2017 - 2020" "(3) After 2020" "T-test (1)-(2)" "T-test (1)-(3)", pattern(1 1 1 1 1)) varwidth(30) nonumbers

