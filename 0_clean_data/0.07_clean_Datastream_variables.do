clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


local var asset
import excel "${data}/datastream_variables_download.xlsx", sheet("`var'_hardcoded") firstrow clear
drop O
destring B-N, replace force
rename (B-N) (`var'2010 `var'2011 `var'2012 `var'2013 `var'2014 `var'2015 `var'2016 `var'2017 `var'2018 `var'2019 `var'2020 `var'2021 `var'2022)
reshape long `var', i(isin) j(year)
keep isin year `var'
save "${cleaned}/datastream_variables", replace
		
local varlist revenue mv ptbv ni capint vol salesg enscore emiscore
foreach var in `varlist'  {
	preserve
		import excel "${data}/datastream_variables_download.xlsx", sheet("`var'_hardcoded") firstrow clear
		drop O
		destring B-N, replace force
		rename (B-N) (`var'2010 `var'2011 `var'2012 `var'2013 `var'2014 `var'2015 `var'2016 `var'2017 `var'2018 `var'2019 `var'2020 `var'2021 `var'2022)
		reshape long `var', i(isin) j(year)
		keep isin year `var'
		tempfile temp
		save `temp', replace
	restore
	
	merge 1:1 isin year using `temp'
	drop _merge
}

replace capint = 0 if revenue!=0 & revenue!=. & capint==.
gen roa = ni/asset
save "${cleaned}/datastream_variables", replace




