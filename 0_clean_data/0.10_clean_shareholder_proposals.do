clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


preserve
	import excel "${data}/ISS_Code_List_Categories.xlsx", clear firstrow case(l)
	keep if subcategory=="SH - E&S Proposal" | subcategory=="SH - Environmental Proposal"
	keep subcategory code
	tempfile code
	save `code'
restore

import delim "${data}/iss_va_shareholder_06_22.csv", clear
rename issagendaitemid code
merge m:1 code using `code'
keep if _merge==3
drop _merge

rename data_year year
gen count_both = 1
gen count_e_only = subcategory=="SH - Environmental Proposal"
collapse (sum) count_both count_e_only, by(cusip year)

save "${cleaned}/proposal", replace
