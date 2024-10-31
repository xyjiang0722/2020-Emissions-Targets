clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"



**# 1. clean ravenpack_envTopic_data

import delimited "${data}/ravenpack_envTopic_data.csv", clear

gen headline_lower = lower(headline)
keep rp_entity_id rpa_date_utc rp_story_id news_type headline_lower relevance

* clean duplicates in story ID
duplicates drop
unique rp_story_id rp_entity_id
bys rp_story_id: gen dup = _N
bys rp_story_id: egen mean_rel = mean(relevance)
drop if dup>=3 | (dup==2 & relevance==mean_rel)
drop dup mean_rel

gen date = date(rpa_date_utc, "YMD")
format date %td
keep if relevance >= 90
replace relevance = -relevance
bys rp_story_id (relevance rp_entity_id date): keep if _n==1
drop relevance

* Target keywords
gen target_related = 0
replace target_related = 1 if strpos(headline_lower, "target")!=0
replace target_related = 1 if strpos(headline_lower, "goal")!=0
replace target_related = 1 if strpos(headline_lower, "commit")!=0
replace target_related = 1 if strpos(headline_lower, "aim")!=0
replace target_related = 1 if strpos(headline_lower, "objective")!=0
replace target_related = 1 if strpos(headline_lower, "quota")!=0
replace target_related = 1 if strpos(headline_lower, "pledge")!=0
replace target_related = 1 if strpos(headline_lower, "ambitio")!=0
replace target_related = 1 if strpos(headline_lower, "resolution")!=0
replace target_related = 1 if strpos(headline_lower, "mission")!=0
replace target_related = 1 if strpos(headline_lower, "milestone")!=0
replace target_related = 1 if strpos(headline_lower, "benchmark")!=0
replace target_related = 1 if strpos(headline_lower, "sbt")!=0
replace target_related = 1 if strpos(headline_lower, "science-based")!=0
replace target_related = 1 if strpos(headline_lower, "science based")!=0
replace target_related = 1 if strpos(headline_lower, "net-zero")!=0
replace target_related = 1 if strpos(headline_lower, "net zero")!=0
replace target_related = 1 if strpos(headline_lower, "carbon neutral")!=0
replace target_related = 1 if strpos(headline_lower, "carbon-neutral")!=0
replace target_related = 1 if strpos(headline_lower, "climate neutral")!=0
replace target_related = 1 if strpos(headline_lower, "climate-neutral")!=0
keep if target_related==1

* years mentioned in headlines
gen year_mentioned1 = regexs(0) if(regexm(headline_lower, "[0-9][0-9][0-9][0-9]"))
gen year_mentioned2 = regexs(0) if(regexm(substr(headline_lower,strpos(headline_lower,year_mentioned1)+4,length(headline_lower)-strpos(headline_lower,year_mentioned1)-3), "[0-9][0-9][0-9][0-9]"))
destring year_mentioned1 year_mentioned2, replace
egen year_mentioned = rowmax(year_mentioned1 year_mentioned2)
replace year_mentioned = . if year_mentioned<2015 | year_mentioned>2070
replace year_mentioned = . if year_mentioned<=year(date)

* keywords for achievement
gen achieve = 0
replace achieve = 1 if strpos(headline_lower, "success")!=0
replace achieve = 1 if strpos(headline_lower, "achiev")!=0
replace achieve = 1 if strpos(headline_lower, "reach")!=0
replace achieve = 1 if strpos(headline_lower, "beat")!=0
replace achieve = 1 if strpos(headline_lower, "complet")!=0
replace achieve = 1 if strpos(headline_lower, "fulfill")!=0
replace achieve = 1 if strpos(headline_lower, "exceed")!=0
replace achieve = 1 if strpos(headline_lower, "ahead ")!=0
replace achieve = 1 if strpos(headline_lower, "meet")!=0 & strpos(headline_lower, "meeting")==0 
replace achieve = 1 if strpos(headline_lower, " met ")!=0
replace achieve = 1 if strpos(headline_lower, "attain")!=0
replace achieve = 1 if strpos(headline_lower, "accomplish")!=0
replace achieve = 1 if strpos(headline_lower, "hit")!=0
replace achieve = 1 if strpos(headline_lower, "outperform")!=0
replace achieve = 1 if strpos(headline_lower, "surpass")!=0
replace achieve = 1 if strpos(headline_lower, "tick off")!=0
replace achieve = 1 if strpos(headline_lower, "ticks off")!=0
replace achieve = 1 if strpos(headline_lower, "ticked off")!=0
replace achieve = 1 if strpos(headline_lower, "ticking off")!=0

* keywords for failure
gen fail = 0
replace fail = 1 if strpos(headline_lower, "fail")!=0
replace fail = 1 if strpos(headline_lower, "miss")!=0 & strpos(headline_lower, "mission")==0 & strpos(headline_lower, "misso")==0 & strpos(headline_lower, "missis")==0
replace fail = 1 if strpos(headline_lower, "short of")!=0
replace fail = 1 if strpos(headline_lower, "shy of")!=0
replace fail = 1 if strpos(headline_lower, "behind ")!=0

* 2020 target outcomes should be released by the end of 2021
replace achieve = 0 if date>=date("2022-01-01","YMD") | date<date("2011-01-01","YMD")  // can be acieved early
replace fail = 0 if date>=date("2022-01-01","YMD") | date<date("2020-01-01","YMD")

* clean news that are not about target outcomes 
replace achieve = 0 if year_mentioned>=2021 & year_mentioned!=.
replace achieve = 0 if strpos(headline_lower, "announc")!=0
replace achieve = 0 if strpos(headline_lower, "will")!=0
replace achieve = 0 if strpos(headline_lower, "on track")!=0
replace achieve = 0 if strpos(headline_lower, "progress")!=0
replace achieve = 0 if strpos(headline_lower, "likely")!=0

replace fail = 0 if year_mentioned>=2021 & year_mentioned!=.
replace fail = 0 if strpos(headline_lower, "announc")!=0
replace fail = 0 if strpos(headline_lower, "will")!=0
replace fail = 0 if strpos(headline_lower, "on track")!=0
replace fail = 0 if strpos(headline_lower, "progress")!=0
replace fail = 0 if strpos(headline_lower, "likely")!=0

sort date rp_entity_id
order rp_entity_id date rp_story_id news_type headline_lower achieve fail year_mentioned
keep rp_entity_id date rp_story_id news_type headline_lower achieve fail year_mentioned
export excel using "${cleaned}/ravenpack_envTopic_data_filtered.xlsx", firstrow(variables) replace




**# 2. clean ravenpack_envKeyword_data

import delimited "${data}/ravenpack_envKeyword_data.csv", clear
keep if topic==""  // overlap with ravenpack_envTopic_data

gen headline_lower = lower(headline)
keep rp_entity_id rpa_date_utc rp_story_id news_type headline_lower relevance

* clean duplicates in story ID
duplicates drop
unique rp_story_id rp_entity_id
bys rp_story_id: gen dup = _N
bys rp_story_id: egen mean_rel = mean(relevance)
drop if dup>=3 | (dup==2 & relevance==mean_rel)
drop dup mean_rel

gen date = date(rpa_date_utc, "YMD")
format date %td
keep if relevance >= 90
replace relevance = -relevance
bys rp_story_id (relevance rp_entity_id date): keep if _n==1
drop relevance

* Target keywords
* data too large, export first
gen target_related = 0
replace target_related = 1 if strpos(headline_lower, "target")!=0
replace target_related = 1 if strpos(headline_lower, "goal")!=0
replace target_related = 1 if strpos(headline_lower, "commit")!=0
replace target_related = 1 if strpos(headline_lower, "aim")!=0
replace target_related = 1 if strpos(headline_lower, "objective")!=0
replace target_related = 1 if strpos(headline_lower, "quota")!=0
replace target_related = 1 if strpos(headline_lower, "pledge")!=0
replace target_related = 1 if strpos(headline_lower, "ambitio")!=0
replace target_related = 1 if strpos(headline_lower, "resolution")!=0
replace target_related = 1 if strpos(headline_lower, "mission")!=0
replace target_related = 1 if strpos(headline_lower, "milestone")!=0
replace target_related = 1 if strpos(headline_lower, "benchmark")!=0
replace target_related = 1 if strpos(headline_lower, "sbt")!=0
replace target_related = 1 if strpos(headline_lower, "science-based")!=0
replace target_related = 1 if strpos(headline_lower, "science based")!=0
replace target_related = 1 if strpos(headline_lower, "net-zero")!=0
replace target_related = 1 if strpos(headline_lower, "net zero")!=0
replace target_related = 1 if strpos(headline_lower, "carbon neutral")!=0
replace target_related = 1 if strpos(headline_lower, "carbon-neutral")!=0
replace target_related = 1 if strpos(headline_lower, "climate neutral")!=0
replace target_related = 1 if strpos(headline_lower, "climate-neutral")!=0
keep if target_related==1
export delim using "${cleaned}/ravenpack_envKeyword_data_target_related.csv", replace

import delimited "${cleaned}/ravenpack_envKeyword_data_target_related.csv", clear

drop date
gen date = date(rpa_date_utc, "YMD")
format date %td

* years mentioned in headlines
gen year_mentioned1 = regexs(0) if(regexm(headline_lower, "[0-9][0-9][0-9][0-9]"))
gen year_mentioned2 = regexs(0) if(regexm(substr(headline_lower,strpos(headline_lower,year_mentioned1)+4,length(headline_lower)-strpos(headline_lower,year_mentioned1)-3), "[0-9][0-9][0-9][0-9]"))
destring year_mentioned1 year_mentioned2, replace
egen year_mentioned = rowmax(year_mentioned1 year_mentioned2)
replace year_mentioned = . if year_mentioned<2015 | year_mentioned>2070
replace year_mentioned = . if year_mentioned<=year(date)

* keywords for achievement
gen achieve = 0
replace achieve = 1 if strpos(headline_lower, "success")!=0
replace achieve = 1 if strpos(headline_lower, "achiev")!=0
replace achieve = 1 if strpos(headline_lower, "reach")!=0
replace achieve = 1 if strpos(headline_lower, "beat")!=0
replace achieve = 1 if strpos(headline_lower, "complet")!=0
replace achieve = 1 if strpos(headline_lower, "fulfill")!=0
replace achieve = 1 if strpos(headline_lower, "exceed")!=0
replace achieve = 1 if strpos(headline_lower, "ahead ")!=0
replace achieve = 1 if strpos(headline_lower, "meet")!=0 & strpos(headline_lower, "meeting")==0 
replace achieve = 1 if strpos(headline_lower, " met ")!=0
replace achieve = 1 if strpos(headline_lower, "attain")!=0
replace achieve = 1 if strpos(headline_lower, "accomplish")!=0
replace achieve = 1 if strpos(headline_lower, "hit")!=0
replace achieve = 1 if strpos(headline_lower, "outperform")!=0
replace achieve = 1 if strpos(headline_lower, "surpass")!=0
replace achieve = 1 if strpos(headline_lower, "tick off")!=0
replace achieve = 1 if strpos(headline_lower, "ticks off")!=0
replace achieve = 1 if strpos(headline_lower, "ticked off")!=0
replace achieve = 1 if strpos(headline_lower, "ticking off")!=0

* keywords for failure
gen fail = 0
replace fail = 1 if strpos(headline_lower, "fail")!=0
replace fail = 1 if strpos(headline_lower, "miss")!=0 & strpos(headline_lower, "mission")==0 & strpos(headline_lower, "misso")==0 & strpos(headline_lower, "missis")==0
replace fail = 1 if strpos(headline_lower, "short of")!=0
replace fail = 1 if strpos(headline_lower, "shy of")!=0
replace fail = 1 if strpos(headline_lower, "behind ")!=0

* 2020 target outcomes should be released by the end of 2021
replace achieve = 0 if date>=date("2022-01-01","YMD") | date<date("2011-01-01","YMD")  // can be acieved early
replace fail = 0 if date>=date("2022-01-01","YMD") | date<date("2020-01-01","YMD")

* clean news that are not about target outcomes 
replace achieve = 0 if year_mentioned>=2021 & year_mentioned!=.
replace achieve = 0 if strpos(headline_lower, "announc")!=0
replace achieve = 0 if strpos(headline_lower, "will")!=0
replace achieve = 0 if strpos(headline_lower, "on track")!=0
replace achieve = 0 if strpos(headline_lower, "progress")!=0
replace achieve = 0 if strpos(headline_lower, "likely")!=0

replace fail = 0 if year_mentioned>=2021 & year_mentioned!=.
replace fail = 0 if strpos(headline_lower, "announc")!=0
replace fail = 0 if strpos(headline_lower, "will")!=0
replace fail = 0 if strpos(headline_lower, "on track")!=0
replace fail = 0 if strpos(headline_lower, "progress")!=0
replace fail = 0 if strpos(headline_lower, "likely")!=0

sort date rp_entity_id
order rp_entity_id date rp_story_id news_type headline_lower achieve fail year_mentioned
keep rp_entity_id date rp_story_id news_type headline_lower achieve fail year_mentioned
export excel using "${cleaned}/ravenpack_envKeyword_data_filtered.xlsx", firstrow(variables) replace






**# 3. Ravenpack Mapping: Match ID with ISIN

import delim "${data}/rpa_entity_mappings_033124.csv", clear varnames(1)
keep if data_type=="ISIN" | data_type=="ENTITY_NAME"

* ravenpack ID -> ISIN, in the CDP target outcomes sample
gen isin = data_value if data_type=="ISIN"
replace isin = trim(isin)
merge m:1 isin using "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", keepusing(isin)
drop if _merge==2

bys rp_entity_id: egen max_merge = max(_merge)
keep if _merge==3 | (data_type=="ENTITY_NAME" & max_merge==3)
keep rp_entity_id data_type data_value
duplicates drop
drop if data_value=="BRNTCOACNOR5"

reshape wide data_value, i(rp_entity_id) j(data_type) string
rename data_valueENTITY_NAME entity_name
rename data_valueISIN isin
drop if isin==""
compress entity_name isin 

merge m:1 isin using "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", keepusing(id companyname isin achieved failed disappeared)
keep if _merge==3
drop _merge

save "${cleaned}/rpa_entity_isin_InSample", replace


* merge the mapping with news sample (envKeyword and envTopic separately)
import excel "${cleaned}/ravenpack_envKeyword_data_filtered.xlsx", sheet("Sheet1") firstrow clear
merge m:1 rp_entity_id using "${cleaned}/rpa_entity_isin_InSample"
keep if _merge==3
drop _merge

sort id date
keep id companyname achieve fail year_mentioned date headline_lower
order id companyname achieve fail year_mentioned date headline_lower
duplicates drop

export excel using "${cleaned}/ravenpack_envKeyword_data_filtered_with_ISIN_InSample.xlsx", firstrow(variables) replace


import excel "${cleaned}/ravenpack_envTopic_data_filtered.xlsx", sheet("Sheet1") firstrow clear
merge m:1 rp_entity_id using "${cleaned}/rpa_entity_isin_InSample"
keep if _merge==3
drop _merge

sort id date
keep id companyname achieve fail year_mentioned date headline_lower
order id companyname achieve fail year_mentioned date headline_lower
duplicates drop

export excel using "${cleaned}/ravenpack_envTopic_data_filtered_with_ISIN_InSample.xlsx", firstrow(variables) replace




**# 4, clean TruValue

* apply the same cleaning procedure to TruValue news data
import delimited "${data}/spotlight.csv", clear bindquote(strict)
keep if inlist(category,"GHGEmissions","EnergyManagement","WaterAndWastewaterManagement","WasteAndHazardousMaterialsManagement","EcologicalImpacts")
gen headline_lower = lower(headline)

gen date = date(triggerdate, "YMD")
format date %td
keep spotlightid isin headline_lower bullets date 
bys spotlightid (date isin): keep if _n==1

gen target_related = 0
replace target_related = 1 if strpos(headline_lower, "target")!=0
replace target_related = 1 if strpos(headline_lower, "goal")!=0
replace target_related = 1 if strpos(headline_lower, "commit")!=0
replace target_related = 1 if strpos(headline_lower, "aim")!=0
replace target_related = 1 if strpos(headline_lower, "objective")!=0
replace target_related = 1 if strpos(headline_lower, "quota")!=0
replace target_related = 1 if strpos(headline_lower, "pledge")!=0
replace target_related = 1 if strpos(headline_lower, "ambitio")!=0
replace target_related = 1 if strpos(headline_lower, "resolution")!=0
replace target_related = 1 if strpos(headline_lower, "mission")!=0
replace target_related = 1 if strpos(headline_lower, "milestone")!=0
replace target_related = 1 if strpos(headline_lower, "benchmark")!=0
replace target_related = 1 if strpos(headline_lower, "sbt")!=0
replace target_related = 1 if strpos(headline_lower, "science-based")!=0
replace target_related = 1 if strpos(headline_lower, "science based")!=0
replace target_related = 1 if strpos(headline_lower, "net-zero")!=0
replace target_related = 1 if strpos(headline_lower, "net zero")!=0
replace target_related = 1 if strpos(headline_lower, "carbon neutral")!=0
replace target_related = 1 if strpos(headline_lower, "carbon-neutral")!=0
replace target_related = 1 if strpos(headline_lower, "climate neutral")!=0
replace target_related = 1 if strpos(headline_lower, "climate-neutral")!=0
keep if target_related==1

gen year_mentioned1 = regexs(0) if(regexm(headline_lower, "[0-9][0-9][0-9][0-9]"))
gen year_mentioned2 = regexs(0) if(regexm(substr(headline_lower,strpos(headline_lower,year_mentioned1)+4,length(headline_lower)-strpos(headline_lower,year_mentioned1)-3), "[0-9][0-9][0-9][0-9]"))
destring year_mentioned1 year_mentioned2, replace
egen year_mentioned = rowmax(year_mentioned1 year_mentioned2)
replace year_mentioned = . if year_mentioned<2015 | year_mentioned>2070
replace year_mentioned = . if year_mentioned<=year(date)

gen achieve = 0
replace achieve = 1 if strpos(headline_lower, "success")!=0
replace achieve = 1 if strpos(headline_lower, "achiev")!=0
replace achieve = 1 if strpos(headline_lower, "reach")!=0
replace achieve = 1 if strpos(headline_lower, "beat")!=0
replace achieve = 1 if strpos(headline_lower, "complet")!=0
replace achieve = 1 if strpos(headline_lower, "fulfill")!=0
replace achieve = 1 if strpos(headline_lower, "exceed")!=0
replace achieve = 1 if strpos(headline_lower, "ahead ")!=0
replace achieve = 1 if strpos(headline_lower, "meet")!=0 & strpos(headline_lower, "meeting")==0 
replace achieve = 1 if strpos(headline_lower, " met ")!=0
replace achieve = 1 if strpos(headline_lower, "attain")!=0
replace achieve = 1 if strpos(headline_lower, "accomplish")!=0
replace achieve = 1 if strpos(headline_lower, "hit")!=0
replace achieve = 1 if strpos(headline_lower, "outperform")!=0
replace achieve = 1 if strpos(headline_lower, "surpass")!=0
replace achieve = 1 if strpos(headline_lower, "tick off")!=0
replace achieve = 1 if strpos(headline_lower, "ticks off")!=0
replace achieve = 1 if strpos(headline_lower, "ticked off")!=0
replace achieve = 1 if strpos(headline_lower, "ticking off")!=0

gen fail = 0
replace fail = 1 if strpos(headline_lower, "fail")!=0
replace fail = 1 if strpos(headline_lower, "miss")!=0 & strpos(headline_lower, "mission")==0 & strpos(headline_lower, "misso")==0 & strpos(headline_lower, "missis")==0
replace fail = 1 if strpos(headline_lower, "short of")!=0
replace fail = 1 if strpos(headline_lower, "shy of")!=0
replace fail = 1 if strpos(headline_lower, "behind ")!=0

replace achieve = 0 if date>=date("2022-01-01","YMD") | date<date("2011-01-01","YMD")
replace fail = 0 if date>=date("2022-01-01","YMD") | date<date("2020-01-01","YMD")

replace achieve = 0 if year_mentioned>=2021 & year_mentioned!=.
replace achieve = 0 if strpos(headline_lower, "announc")!=0
replace achieve = 0 if strpos(headline_lower, "will")!=0
replace achieve = 0 if strpos(headline_lower, "on track")!=0
replace achieve = 0 if strpos(headline_lower, "progress")!=0
replace achieve = 0 if strpos(headline_lower, "likely")!=0

replace fail = 0 if year_mentioned>=2021 & year_mentioned!=.
replace fail = 0 if strpos(headline_lower, "announc")!=0
replace fail = 0 if strpos(headline_lower, "will")!=0
replace fail = 0 if strpos(headline_lower, "on track")!=0
replace fail = 0 if strpos(headline_lower, "progress")!=0
replace fail = 0 if strpos(headline_lower, "likely")!=0

* merge with main sample
merge m:1 isin using "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", keepusing(id companyname isin achieved failed disappeared)
keep if _merge==3
drop _merge

sort isin date
drop target_related year_mentioned1 year_mentioned2 bullets
gen news_type = "truvalue"

export excel using "${cleaned}/spotlight_filtered_with_ISIN_InSample.xlsx", firstrow(variables) replace




**# 5. compile - 2020 outcomes

* manually check headlines in ravenpack_envKeyword_data_filtered_with_ISIN_InSample, ravenpack_envTopic_data_filtered_with_ISIN_InSample, and spotlight_filtered_with_ISIN_InSample, see whether they are revelant

* 2020 outcomes from ravenpack keyword data
* export at the headline level and merge
import excel "${cleaned}/ravenpack_envKeyword_data_filtered_with_ISIN_InSample_check.xlsx", sheet("Sheet1") firstrow clear
drop achieve fail
rename (achieve_check fail_check) (achieve_news fail_news)
rename year_mentioned_check announcement_2020 

keep if achieve_news==1 | fail_news==1 | announcement_2020==1
replace achieve_news = . if fail_news==1
replace fail_news = . if achieve_news==1

keep headline_lower achieve_news fail_news announcement_2020
duplicates drop
save "${cleaned}/ravenpack_envKeyword_headlines", replace

* raw news data, merge with ISIN and headline-level information
import excel "${cleaned}/ravenpack_envKeyword_data_filtered.xlsx", sheet("Sheet1") firstrow clear
merge m:1 rp_entity_id using "${cleaned}/rpa_entity_isin_InSample"
keep if _merge==3
drop _merge

merge m:1 headline_lower using "${cleaned}/ravenpack_envKeyword_headlines"
keep if _merge==3
drop _merge

drop achieve fail year_mentioned announcement_2020 rp_entity_id entity_name
rename rp_story_id story_id

keep if achieve_news==1 | fail_news==1
replace achieve_news = . if fail_news==1
replace fail_news = . if achieve_news==1

sort id date
order id isin achieved disappeared failed companyname date story_id news_type headline_lower achieve_news fail_news

save "${cleaned}/ravenpack_envKeyword_outcomes", replace


* 2020 outcomes from truvalue data
import excel "${cleaned}/spotlight_filtered_with_ISIN_InSample_check.xlsx", sheet("Sheet1") firstrow clear
drop achieve fail year_mentioned year_mentioned_check
rename (achieve_check fail_check) (achieve_news fail_news)
rename spotlightid story_id

keep if achieve_news==1 | fail_news==1
replace achieve_news = . if fail_news==1
replace fail_news = . if achieve_news==1

sort id date
order id isin achieved disappeared failed companyname date story_id news_type headline_lower achieve_news fail_news

save "${cleaned}/spotlight_outcomes", replace


* append -> final list of news on 2020 outcomes
import excel "${cleaned}/failed_outcome_additional.xlsx", sheet("Sheet1") firstrow clear
append using "${cleaned}/ravenpack_envKeyword_outcomes"
append using "${cleaned}/spotlight_outcomes"
sort id date

gen press_release = news_type=="PRESS-RELEASE"
gen news_article = news_type!="PRESS-RELEASE"

drop if achieve_news>0 & achieve_news!=. & achieved==0

count if press_release>0
count if news_article>0
count


* daily level -> 15-day window as an event, starting from the first news event date
collapse (sum) achieve_news fail_news press_release news_article, by(id isin companyname achieved disappeared failed date)

unique id if press_release>0
unique id if news_article>0
unique id if press_release>0 & news_article>0
unique id 

bys id (date): gen start_date = date[1] 
gen days_since_start = date - start_date
tab days_since_start

gen window1 = days_since_start>14 & days_since_start!=.
drop start_date days_since_start
bys id window1 (date): gen start_date = date[1] if window1==1
gen days_since_start = date - start_date
tab days_since_start

egen window_id = group(id window1)
drop start_date days_since_start window1

* window level -> keep the date with the most news articles
gen daily_total = press_release + news_article
gsort id window_id -daily_total date

bys id window_id: egen achieve_news_total = sum(achieve_news)
bys id window_id: egen fail_news_total = sum(fail_news)
bys id window_id: egen press_release_total = sum(press_release)
bys id window_id: egen news_article_total = sum(news_article)
drop achieve_news fail_news press_release news_article

bys id window_id: keep if _n==1
drop daily_total

unique id if achieve_news_total>0 & press_release_total>0
unique id if achieve_news_total>0 & news_article_total>0
unique id if achieve_news_total>0 & news_article_total>0 & press_release_total>0
unique id if fail_news_total>0 & press_release_total>0
unique id if fail_news_total>0 & news_article_total>0
unique id if fail_news_total>0 & news_article_total>0 & press_release_total>0

merge m:1 isin using "${cleaned}/final_firm_level_broader_sample"
keep if _merge==3
drop _merge

sort id date
order id-failed type-lag_behind_2019


save "${cleaned}/media_final_2020_outcomes", replace




**# 6. compile - 2020 announcement

* 2020 announcement from ravenpack
import excel "${cleaned}/ravenpack_envKeyword_data_filtered.xlsx", sheet("Sheet1") firstrow clear
merge m:1 rp_entity_id using "${cleaned}/rpa_entity_isin_InSample"
keep if _merge==3
drop _merge

merge m:1 headline_lower using "${cleaned}/ravenpack_envKeyword_headlines"
keep if _merge==3
drop _merge

keep if announcement_2020==1
keep if date<=date("2018-12-31","YMD")

drop achieve fail achieve_news fail_news year_mentioned rp_entity_id entity_name announcement_2020
rename rp_story_id story_id

sort id date
order id isin achieved disappeared failed companyname date story_id news_type headline_lower

save "${cleaned}/ravenpack_envKeyword_2020announcements", replace


* 2020 announcement from truvalue
import excel "${cleaned}/spotlight_filtered_with_ISIN_InSample_check.xlsx", sheet("Sheet1") firstrow clear

keep if year_mentioned_check==1
keep if date<=date("2018-12-31","YMD")

drop achieve fail achieve_check fail_check achieve_check fail_check year_mentioned year_mentioned_check
rename spotlightid story_id

sort id date
order id isin achieved disappeared failed companyname date story_id news_type headline_lower

save "${cleaned}/spotlight_2020announcements", replace


* append -> final list of news on 2020 announcements
use "${cleaned}/ravenpack_envKeyword_2020announcements", clear
append using "${cleaned}/spotlight_2020announcements"
sort id date

gen press_release = news_type=="PRESS-RELEASE"
gen news_article = news_type!="PRESS-RELEASE"

count if press_release>0
count if news_article>0
count

* daily level -> 16-day window as an event
collapse (sum) press_release news_article, by(id isin companyname achieved disappeared failed date)

unique id if press_release>0
unique id if news_article>0
unique id if press_release>0 & news_article>0
unique id 

bys id (date): gen start_date = date[1] 
gen days_since_start = date - start_date
tab days_since_start

* same procedure as for 2020 outcomes
local min = 16
local gap = 15
local c = 0

while `min'>=`gap' & `min'!=. {
	local c = `c' + 1
	gen window`c' = days_since_start>=`gap'  & days_since_start!=.
	drop start_date days_since_start
	bys id window`c' (date): gen start_date = date[1] if window`c'==1
	gen days_since_start = date - start_date
	su days_since_start if days_since_start>=`gap'
	local min = r(min)
}

egen window_id = group(id window*)
drop start_date days_since_start window1-window`c'

* window level -> keep the date with the most news articles
gen daily_total = press_release + news_article
gsort id window_id -daily_total date

bys id window_id: egen press_release_total = sum(press_release)
bys id window_id: egen news_article_total = sum(news_article)
drop press_release news_article

bys id window_id: keep if _n==1
drop daily_total

merge m:1 isin using "${cleaned}/final_firm_level_broader_sample"
keep if _merge==3
drop _merge

sort id date
order id-failed type-failed_low_ambition

save "${cleaned}/media_final_2020_announcements", replace




**# 7. check ravenpack coverage for non-English countries
use "${cleaned}/final_firm_level_broader_sample", clear

merge 1:1 id using "${cleaned}/cdp_summary_firm", keepusing(companyname country)
drop if _merge==2
drop _merge

count if inlist(country,"USA","United Kingdom","Canada","Ireland","Singapore","Australia","New Zealand","South Africa")
di 401/1021
drop if inlist(country,"USA","United Kingdom","Canada","Ireland","Singapore","Australia","New Zealand","South Africa")
tab country

* merge in asset
preserve
use "${cleaned}/datastream_variables", clear
keep if year==2021
keep isin asset
tempfile temp
save `temp', replace
restore

merge m:1 isin using `temp'
drop if _merge==2
drop _merge
gsort -asset

* select 50 largest firms by asset and 50 random firms from the rest
set seed 12345

preserve
keep if _n>50
sample 50, count
tempfile temp
save `temp', replace
restore

keep if _n<=50
append using `temp'
keep id isin type companyname country
tab country

* manually check news on 2020 target outcomes for those 100 firms
export delim using "${cleaned}/media_final_2020_announcements_non_English_100firms.csv", replace


