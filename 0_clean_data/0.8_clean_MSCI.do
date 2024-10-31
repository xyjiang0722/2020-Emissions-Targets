clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


use "${data}/msci_final", clear
keep if ISSUER_ISIN!=""
keep ISSUER_ISIN ENVIRONMENTAL_PILLAR_SCORE CARBON_EMISSIONS_SCORE year

rename (ISSUER_ISIN ENVIRONMENTAL_PILLAR_SCORE CARBON_EMISSIONS_SCORE) (isin msci_enscore msci_emiscore)

save "${cleaned}/msci", replace
