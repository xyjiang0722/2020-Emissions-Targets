net install ftools, from("https://raw.githubusercontent.com/sergiocorreia/ftools/master/src/") 
ssc install reghdfe
ssc install winsor2

set more off
set maxvar 32767
clear all

* To initialize, define these environment variables:
* Environment variables.
global root "." 
global figures "$root/figures/"
global tables "$root/tables/"
global data "$root/raw data"
global cleaned "$root/cleaned files"
cd "."


// These files clean the raw data
do code/0_clean_data/0.01_clean_CDP_summary.do
do code/0_clean_data/0.02_clean_CDP_targets.do
do code/0_clean_data/0.03_clean_CDP_initiatives.do
do code/0_clean_data/0.04_clean_CDP_ghgreduction.do
do code/0_clean_data/0.05_clean_CDP_incentives.do
do code/0_clean_data/0.06_clean_CDP_ghgreduction_split.do
do code/0_clean_data/0.07_clean_Datastream_variables.do
do code/0_clean_data/0.08_clean_MSCI.do
do code/0_clean_data/0.09_clean_TruValue.do
do code/0_clean_data/0.10_clean_shareholder_proposals.do
do code/0_clean_data/0.11_clean_institutional_variations.do
do code/0_clean_data/0.12_clean_target_ambition_split.do
do code/0_clean_data/0.13_clean_covid_industries.do
do code/0_clean_data/0.14_clean_lagging_behind_targets.do


// These files merge the data to construct final datasets and clean additional data based on the merged data
do code/1_combine_data/1.1_combine_data.do
do code/1_combine_data/1.2_clean_news.do
do code/1_combine_data/1.3.1_EventStudy_clean.do


// These files produce the figures and plot the coefficients from regression models
do code/2_figures/2.1_Figure_1.do
do code/2_figures/2.2_Figure_2.do
do code/2_figures/2.3.1_Figure_3.do
do code/2_figures/2.4.1_Figure_4.do
do code/2_figures/2.5.1_Figure_5.do
do code/2_figures/2.6_Figure_EX1.do
do code/2_figures/2.7_Figure_S1.do


// These files produce the tables and output regression results
do code/3_tables/3.01_Table_1.do
do code/3_tables/3.02_Table_EX1.do
do code/3_tables/3.03_Table_EX2.do
do code/3_tables/3.04_Table_EX3.do
do code/3_tables/3.05_Table_EX4.do
do code/3_tables/3.06_Table_S2.do
do code/3_tables/3.07_Table_S3.do
do code/3_tables/3.08_Table_S4.do
do code/3_tables/3.09_Table_S5.do
do code/3_tables/3.10_Table_S6.do
do code/3_tables/3.11_Table_S7.do
do code/3_tables/3.12_Table_S8.do
do code/3_tables/3.13_Table_S9.do
do code/3_tables/3.14_Table_S10.do
do code/3_tables/3.15_Table_S11.do
do code/3_tables/3.16_Table_S12.do



// This file produces additional calculations
do code/4_misc/4.1_Additional_calculation.do
