clear all
set more off

global root "."
global figures "$root/figures"
global tables "$root/tables"
global data "$root/raw data"
global cleaned "$root/cleaned files"


* input the numbers to Latex directly

* CDP sample
use "${cleaned}/all_2020_targets_final_year_firmlevel_no_overlap", clear
unique id if achieved == 1  // 633
unique id if failed == 1  // 88
unique id if disappeared == 1  // 320


* corporate disclosure
import excel "${cleaned}/companies_with_failed_targets_CSRreport_sentences_all.xlsx", clear firstrow sheet("Sheet1")
unique id if acknowledged==1
unique id if explicit==1
di 26/78
di 16/78

use "${cleaned}/media_final_2020_outcomes", clear
unique id if achieved == 1 & press_release_total>0  // 12
unique id if failed == 1 & press_release_total>0  // 0


* external coverage
use "${cleaned}/media_final_2020_outcomes", clear
unique id if achieved == 1 & news_article_total>0  // 48
unique id if failed == 1 & news_article_total>0  // 3
unique id if achieved == 1 & press_release_total>0 & news_article_total>0  // 12
unique id if failed == 1 & press_release_total>0 & news_article_total>0  // 0

tab id if failed == 1  // firm ID: 6287, 7344, 58857, they all have corporate disclosure through sustainability reports
