clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


import delim "${data}/scores_pulse", clear
gen year = real(substr(date, 1, 4))
gen month = real(substr(date, 6, 2))
	
local var pulseghgemissions pulseairquality pulseenergymanagement pulseecologicalimpacts pulsewasteandhazardousmaterialsm pulsewaterandwastewatermanagemen
destring `var', force replace
egen pulseenvironment = rowmean(`var')
keep pulseenvironment isin year month
	
collapse (mean) pulseenvironment, by(isin year)
save "${cleaned}/pulse", replace

import delim "${data}/scores_insight_volume_momentum", clear
gen year = real(substr(date, 1, 4))
gen month = real(substr(date, 6, 2))
	
local insightscore insightairquality insightecologicalimpacts insightenergymanagement insightghgemissions insightwasteandhazardousmaterial insightwaterandwastewatermanagem
destring `insightscore', force replace
egen insightenvironment = rowmean(`insightscore')

local volumescore ttmvolumeairquality ttmvolumeecologicalimpacts ttmvolumeenergymanagement ttmvolumeghgemissions ttmvolumewasteandhazardousmateri ttmvolumewaterandwastewatermanag
destring `volumescore', force replace
egen volumeenvironment = rowmean(`volumescore')
rename ttmvolumeghgemissions volumeghg

keep insightenvironment volumeenvironment volumeghg isin year month
collapse (mean) insightenvironment volumeenvironment volumeghg, by(isin year)
save "${cleaned}/insight_and_volume", replace



