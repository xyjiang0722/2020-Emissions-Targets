#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This code conducts market tests (returns and volume) around information events.

Input data
- WRDS-Datastream Daily Stock File from wrds (acquired through web quary)

"""

import pandas as pd
import numpy as np
import datetime as dt
import pandasql as ps
import statsmodels.api as sm
from linearmodels import PanelOLS
from datetime import timedelta
import seaborn as sns
import matplotlib.pyplot as plt
import re

# Directories
data_directory = "/raw data/"
output_directory = "/cleaned files/"


''' Read in WRDS-Datastream Daily Stock File '''
# This contains daily stock information for different samples

# Media coverage sample
ds_dsf_mediaCoverage = pd.read_csv(data_directory+"tr_ds_equities_media_v3.csv")
ds_dsf_mediaCoverage = ds_dsf_mediaCoverage.sort_values(by=['ISIN','MarketDate'])

ds_dsf_mediaCoverage['MarketDate'] = pd.to_datetime(ds_dsf_mediaCoverage['MarketDate'])

# CSR report sample
ds_dsf_csrReport = pd.read_csv(data_directory+"tr_ds_equities_csrReport_v2.csv")
ds_dsf_csrReport = ds_dsf_csrReport.sort_values(by=['ISIN','MarketDate'])

ds_dsf_csrReport['MarketDate'] = pd.to_datetime(ds_dsf_csrReport['MarketDate'])

# All CDP 2020 target sample
all_2020_targets_all_years = pd.read_stata(output_directory+"all_2020_targets_all_years.dta")
isin_list = pd.DataFrame(all_2020_targets_all_years['isin'].unique())
isin_list.to_csv(data_directory+"all_cdp_isin_list.txt",header=None, index=None)

ds_dsf_CDPrelease = pd.read_csv(data_directory+"tr_ds_equities_allCDP.csv")
ds_dsf_CDPrelease = ds_dsf_CDPrelease.sort_values(by=['ISIN','MarketDate'])

ds_dsf_CDPrelease['MarketDate'] = pd.to_datetime(ds_dsf_CDPrelease['MarketDate'])

# 2019 CDP release sample (contains target outcomes by 2018)
ds_dsf_CDPrelease19 = pd.read_csv(data_directory+"tr_ds_equities_allCDP.csv")
ds_dsf_CDPrelease19 = ds_dsf_CDPrelease19.sort_values(by=['ISIN','MarketDate'])

ds_dsf_CDPrelease19['MarketDate'] = pd.to_datetime(ds_dsf_CDPrelease19['MarketDate'])

# 2020 CDP release sample (contains target outcomes by 2019)
ds_dsf_CDPrelease20 = pd.read_csv(data_directory+"tr_ds_equities_allCDP.csv")
ds_dsf_CDPrelease20 = ds_dsf_CDPrelease20.sort_values(by=['ISIN','MarketDate'])

ds_dsf_CDPrelease20['MarketDate'] = pd.to_datetime(ds_dsf_CDPrelease20['MarketDate'])


# 2020 target announcement sample
ds_dsf_targetAnnounce = pd.read_csv(data_directory+"tr_ds_equities_media_v3.csv")
ds_dsf_targetAnnounce = ds_dsf_targetAnnounce.sort_values(by=['ISIN','MarketDate'])

ds_dsf_targetAnnounce['MarketDate'] = pd.to_datetime(ds_dsf_targetAnnounce['MarketDate'])


''' Number of days relative to the event date'''
# Media coverage dates
media_dates = pd.read_stata(output_directory+"media_final_2020_outcomes.dta")
# CSR report dates
csrReport_date = pd.read_excel(data_directory+"companies_with_failed_targets_CSRreportdate_withdates.xlsx")

# Clean media coverage dates data
media_dates.rename(columns={'date':'EventDate','isin':'ISIN'},inplace=True)
media_dates['EventDate'] = pd.to_datetime(media_dates['EventDate'])
# For media, keep all occurence
media_dates = media_dates.sort_values(by=['ISIN','EventDate'],ascending=True)

# Clean CSR report dates data
csrReport_date.rename(columns={'isin':'ISIN','Dates released':'EventDate'},inplace=True)
csrReport_date['EventDate'] = pd.to_datetime(csrReport_date['EventDate'])
# Only keep the first occurrence for each firm
csrReport_date = csrReport_date.sort_values(by=['ISIN','EventDate'],ascending=True)
csrReport_date = csrReport_date.drop_duplicates(subset=['ISIN'],keep='first')

# Merge event dates to daily returns data
ds_dsf_mediaCoverage = pd.merge(ds_dsf_mediaCoverage,media_dates,on=['ISIN'],how='left')
ds_dsf_csrReport = pd.merge(ds_dsf_csrReport,csrReport_date,on=['ISIN'],how='left')

# Clean CDP release date - set event date to 2021-10-11 (CDP 2021 release date)
ds_dsf_CDPrelease['EventDate'] = pd.to_datetime('2021-10-11')
# Get information on achieved, failed, disappeared status for 2020 target
final_firm_level_broader_sample = pd.read_stata(output_directory+"final_firm_level_broader_sample.dta")
final_firm_level_broader_sample.rename(columns={'isin':'ISIN'},inplace=True)
ds_dsf_CDPrelease = pd.merge(ds_dsf_CDPrelease,final_firm_level_broader_sample,on='ISIN',how='inner')



# Number of business days relative to the event date
ds_dsf_mediaCoverage = ds_dsf_mediaCoverage.dropna(subset=['EventDate','MarketDate']) # require that we have market date and event date
ds_dsf_csrReport = ds_dsf_csrReport.dropna(subset=['EventDate','MarketDate']) # require that we have market date and event date
ds_dsf_CDPrelease = ds_dsf_CDPrelease.dropna(subset=['EventDate','MarketDate']) # require that we have market date and event date


A = [d.date() for d in ds_dsf_mediaCoverage['EventDate']]
B = [d.date() for d in ds_dsf_mediaCoverage['MarketDate']]
ds_dsf_mediaCoverage['BDaysRelativeToEvent'] = np.busday_count(A, B)

A = [d.date() for d in ds_dsf_csrReport['EventDate']]
B = [d.date() for d in ds_dsf_csrReport['MarketDate']]
ds_dsf_csrReport['BDaysRelativeToEvent'] = np.busday_count(A, B)

A = [d.date() for d in ds_dsf_CDPrelease['EventDate']]
B = [d.date() for d in ds_dsf_CDPrelease['MarketDate']]
ds_dsf_CDPrelease['BDaysRelativeToEvent'] = np.busday_count(A, B)

# Save
# Only keep daily data within 365 days of event date
ds_dsf_mediaCoverage = ds_dsf_mediaCoverage[(ds_dsf_mediaCoverage['BDaysRelativeToEvent']>=-365)&(ds_dsf_mediaCoverage['BDaysRelativeToEvent']<=365)]
ds_dsf_csrReport = ds_dsf_csrReport[(ds_dsf_csrReport['BDaysRelativeToEvent']>=-365)&(ds_dsf_csrReport['BDaysRelativeToEvent']<=365)]
ds_dsf_CDPrelease = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['BDaysRelativeToEvent']>=-365)&(ds_dsf_CDPrelease['BDaysRelativeToEvent']<=365)]


# Drop duplicates
ds_dsf_mediaCoverage = ds_dsf_mediaCoverage.drop_duplicates(subset = ['InfoCode', 'dscode','MarketDate','EventDate','close', 'adjclose', 'close_usd', 'open',
                                                                'high', 'low', 'bid', 'ask', 'vwap', 'mosttrdprc', 'RI', 'ret',
                                                                'ri_usd', 'ret_usd','ISIN','id'])
ds_dsf_mediaCoverage.to_csv(output_directory+"ds_dsf_mediaCoverage.csv",index=False)

ds_dsf_csrReport.to_csv(output_directory+"ds_dsf_csrReport.csv",index=False)

# Drop duplicates
ds_dsf_CDPrelease = ds_dsf_CDPrelease.drop_duplicates(subset = ['InfoCode', 'dscode','MarketDate','close', 'adjclose', 'close_usd', 'open',
                                                                'high', 'low', 'bid', 'ask', 'vwap', 'mosttrdprc', 'RI', 'ret',
                                                                'ri_usd', 'ret_usd','ISIN','id','type'])
ds_dsf_CDPrelease.to_csv(output_directory+"ds_dsf_CDPrelease.csv",index=False)


##### 2019 CDP release
# Set event date to 2019-10-31 (CDP 2019 release date)
ds_dsf_CDPrelease19['EventDate'] = pd.to_datetime('2019-10-31')
# Get information on achieved, failed, disappeared status for 2020 target
final_firm_level_broader_sample = pd.read_stata(output_directory+"final_firm_level_broader_sample.dta")
final_firm_level_broader_sample.rename(columns={'isin':'ISIN'},inplace=True)
ds_dsf_CDPrelease19 = pd.merge(ds_dsf_CDPrelease19,final_firm_level_broader_sample,on='ISIN',how='inner')

ds_dsf_CDPrelease19 = ds_dsf_CDPrelease19.dropna(subset=['EventDate','MarketDate']) # require that we have market date and event date

A = [d.date() for d in ds_dsf_CDPrelease19['EventDate']]
B = [d.date() for d in ds_dsf_CDPrelease19['MarketDate']]
ds_dsf_CDPrelease19['BDaysRelativeToEvent'] = np.busday_count(A, B)

# Save
# Only keep daily data within 365 days of event date
ds_dsf_CDPrelease19 = ds_dsf_CDPrelease19[(ds_dsf_CDPrelease19['BDaysRelativeToEvent']>=-365)&(ds_dsf_CDPrelease19['BDaysRelativeToEvent']<=365)]

# Drop duplicates
ds_dsf_CDPrelease19 = ds_dsf_CDPrelease19.drop_duplicates(subset = ['InfoCode', 'dscode','MarketDate','close', 'adjclose', 'close_usd', 'open',
                                                                'high', 'low', 'bid', 'ask', 'vwap', 'mosttrdprc', 'RI', 'ret',
                                                                'ri_usd', 'ret_usd','ISIN','id','type'])
ds_dsf_CDPrelease19.to_csv(output_directory+"ds_dsf_CDPrelease19.csv",index=False)

##### 2020 CDP release
# Set event date to 2020-10-12 (CDP 2020 release date)
ds_dsf_CDPrelease20['EventDate'] = pd.to_datetime('2020-10-12')

# Get information on achieved, failed, disappeared status for 2020 target
final_firm_level_broader_sample = pd.read_stata(output_directory+"final_firm_level_broader_sample.dta")
final_firm_level_broader_sample.rename(columns={'isin':'ISIN'},inplace=True)
ds_dsf_CDPrelease20 = pd.merge(ds_dsf_CDPrelease20,final_firm_level_broader_sample,on='ISIN',how='inner')

ds_dsf_CDPrelease20 = ds_dsf_CDPrelease20.dropna(subset=['EventDate','MarketDate']) # require that we have market date and event date

A = [d.date() for d in ds_dsf_CDPrelease20['EventDate']]
B = [d.date() for d in ds_dsf_CDPrelease20['MarketDate']]
ds_dsf_CDPrelease20['BDaysRelativeToEvent'] = np.busday_count(A, B)

# Save
# Only keep daily data within 365 days of event date
ds_dsf_CDPrelease20 = ds_dsf_CDPrelease20[(ds_dsf_CDPrelease20['BDaysRelativeToEvent']>=-365)&(ds_dsf_CDPrelease20['BDaysRelativeToEvent']<=365)]

# Drop duplicates
ds_dsf_CDPrelease20 = ds_dsf_CDPrelease20.drop_duplicates(subset = ['InfoCode', 'dscode','MarketDate','close', 'adjclose', 'close_usd', 'open',
                                                                'high', 'low', 'bid', 'ask', 'vwap', 'mosttrdprc', 'RI', 'ret',
                                                                'ri_usd', 'ret_usd','ISIN','id','type'])
ds_dsf_CDPrelease20.to_csv(output_directory+"ds_dsf_CDPrelease20.csv",index=False)


##### Announcement/Media coverage of 2020 targets
# Data on target announcement dates
target_announce_dates = pd.read_stata(output_directory+"media_final_2020_announcements.dta")

# Clean dates data
target_announce_dates.rename(columns={'date':'EventDate','isin':'ISIN'},inplace=True)
target_announce_dates['EventDate'] = pd.to_datetime(target_announce_dates['EventDate'])
# For target announcement coverage, keep all dates
target_announce_dates = target_announce_dates.sort_values(by=['ISIN','EventDate'],ascending=True)
# Merge to daily returns data
ds_dsf_targetAnnounce = pd.merge(ds_dsf_targetAnnounce,target_announce_dates,on=['ISIN'],how='inner')  # inner merge to keep only daily return data of companies with target announcement dates

# Number of business days relative to the event date
ds_dsf_targetAnnounce = ds_dsf_targetAnnounce.dropna(subset=['EventDate','MarketDate']) # require that we have market date and event date

A = [d.date() for d in ds_dsf_targetAnnounce['EventDate']]
B = [d.date() for d in ds_dsf_targetAnnounce['MarketDate']]
ds_dsf_targetAnnounce['BDaysRelativeToEvent'] = np.busday_count(A, B)

# Save
# Only keep daily data within 365 days of event date
ds_dsf_targetAnnounce = ds_dsf_targetAnnounce[(ds_dsf_targetAnnounce['BDaysRelativeToEvent']>=-365)&(ds_dsf_targetAnnounce['BDaysRelativeToEvent']<=365)]
# Drop duplicates
ds_dsf_targetAnnounce = ds_dsf_targetAnnounce.drop_duplicates(subset = ['InfoCode', 'dscode','MarketDate','EventDate','close', 'adjclose', 'close_usd', 'open',
                                                                'high', 'low', 'bid', 'ask', 'vwap', 'mosttrdprc', 'RI', 'ret',
                                                                'ri_usd', 'ret_usd','ISIN','id'])
ds_dsf_targetAnnounce.to_csv(output_directory+"ds_dsf_targetAnnounce.csv",index=False)





''' Merge in market returns (by country, use WRDS World Indices)'''
import wrds
db = wrds.Connection()

# This data does not contain info for US/CA. 
windices_daily = db.get_table('wrdsapps_windices','dwcountryreturns')

# Merge ex-US market index returns
windices_daily_clean = windices_daily[['date','portret','fic']]
windices_daily_clean.columns=['MarketDate','MarketReturn','fic']
windices_daily_clean['MarketDate'] = pd.to_datetime(windices_daily_clean['MarketDate'])
# Read in country code link table
country_code = pd.read_csv(data_directory+"country_codes_alpha2_3.csv")
windices_daily_clean = pd.merge(windices_daily_clean,country_code,on='fic',how='left')

# US index return data from CRSP
us_index = pd.read_csv(data_directory+"crsp_index_daily_2010_2022.csv")
us_index_clean = us_index[['caldt','vwretd']]
us_index_clean.columns = ['MarketDate','MarketReturn']
us_index_clean['MarketDate'] = pd.to_datetime(us_index_clean['MarketDate'])

# merge - media coverage exUS and US separately
ds_dsf_mediaCoverage_exUS = ds_dsf_mediaCoverage[(ds_dsf_mediaCoverage['Region']!='US')&(ds_dsf_mediaCoverage['Region']!='CA')]
ds_dsf_mediaCoverage_exUS = pd.merge(ds_dsf_mediaCoverage_exUS,windices_daily_clean,on=['MarketDate','Region'],how='left')
ds_dsf_mediaCoverage_exUS = ds_dsf_mediaCoverage_exUS.drop(columns=['fic','Country'])

ds_dsf_mediaCoverage_US = ds_dsf_mediaCoverage[(ds_dsf_mediaCoverage['Region']=='US')|(ds_dsf_mediaCoverage['Region']=='CA')]
ds_dsf_mediaCoverage_US = pd.merge(ds_dsf_mediaCoverage_US,us_index_clean,on=['MarketDate'],how='left')

# Concat ex-US and US merged data
ds_dsf_mediaCoverage = pd.concat([ds_dsf_mediaCoverage_exUS,ds_dsf_mediaCoverage_US])
# Save
ds_dsf_mediaCoverage.to_csv(output_directory+"ds_dsf_mediaCoverage.csv",index=False)


# Same for CSR report event dataset
ds_dsf_csrReport_exUS = ds_dsf_csrReport[(ds_dsf_csrReport['Region']!='US')&(ds_dsf_csrReport['Region']!='CA')]
ds_dsf_csrReport_exUS = pd.merge(ds_dsf_csrReport_exUS,windices_daily_clean,on=['MarketDate','Region'],how='left')
ds_dsf_csrReport_exUS = ds_dsf_csrReport_exUS.drop(columns=['fic','Country'])

ds_dsf_csrReport_US = ds_dsf_csrReport[(ds_dsf_csrReport['Region']=='US')|(ds_dsf_csrReport['Region']=='CA')]
ds_dsf_csrReport_US = pd.merge(ds_dsf_csrReport_US,us_index_clean,on=['MarketDate'],how='left')

# Concat ex-US and US merged data
ds_dsf_csrReport = pd.concat([ds_dsf_csrReport_exUS,ds_dsf_csrReport_US])
# Save
ds_dsf_csrReport.to_csv(output_directory+"ds_dsf_csrReport.csv",index=False)


# Same for 2021 CDP release Dates
ds_dsf_CDPrelease_exUS = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['Region']!='US')&(ds_dsf_CDPrelease['Region']!='CA')]
ds_dsf_CDPrelease_exUS = pd.merge(ds_dsf_CDPrelease_exUS,windices_daily_clean,on=['MarketDate','Region'],how='left')
ds_dsf_CDPrelease_exUS = ds_dsf_CDPrelease_exUS.drop(columns=['fic','Country'])

ds_dsf_CDPrelease_US = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['Region']=='US')|(ds_dsf_CDPrelease['Region']=='CA')]
ds_dsf_CDPrelease_US = pd.merge(ds_dsf_CDPrelease_US,us_index_clean,on=['MarketDate'],how='left')

# Concat ex-US and US merged data
ds_dsf_CDPrelease = pd.concat([ds_dsf_CDPrelease_exUS,ds_dsf_CDPrelease_US])
# Save
ds_dsf_CDPrelease.to_csv(output_directory+"ds_dsf_CDPrelease.csv",index=False)


# Same for 2019 CDP release Dates
ds_dsf_CDPrelease19_exUS = ds_dsf_CDPrelease19[(ds_dsf_CDPrelease19['Region']!='US')&(ds_dsf_CDPrelease19['Region']!='CA')]
ds_dsf_CDPrelease19_exUS = pd.merge(ds_dsf_CDPrelease19_exUS,windices_daily_clean,on=['MarketDate','Region'],how='left')
ds_dsf_CDPrelease19_exUS = ds_dsf_CDPrelease19_exUS.drop(columns=['fic','Country'])

ds_dsf_CDPrelease19_US = ds_dsf_CDPrelease19[(ds_dsf_CDPrelease19['Region']=='US')|(ds_dsf_CDPrelease19['Region']=='CA')]
ds_dsf_CDPrelease19_US = pd.merge(ds_dsf_CDPrelease19_US,us_index_clean,on=['MarketDate'],how='left')

# Concat ex-US and US merged data
ds_dsf_CDPrelease19 = pd.concat([ds_dsf_CDPrelease19_exUS,ds_dsf_CDPrelease19_US])
# Save
ds_dsf_CDPrelease19.to_csv(output_directory+"ds_dsf_CDPrelease19.csv",index=False)


# Same for 2020 CDP release Dates
ds_dsf_CDPrelease20_exUS = ds_dsf_CDPrelease20[(ds_dsf_CDPrelease20['Region']!='US')&(ds_dsf_CDPrelease20['Region']!='CA')]
ds_dsf_CDPrelease20_exUS = pd.merge(ds_dsf_CDPrelease20_exUS,windices_daily_clean,on=['MarketDate','Region'],how='left')
ds_dsf_CDPrelease20_exUS = ds_dsf_CDPrelease20_exUS.drop(columns=['fic','Country'])

ds_dsf_CDPrelease20_US = ds_dsf_CDPrelease20[(ds_dsf_CDPrelease20['Region']=='US')|(ds_dsf_CDPrelease20['Region']=='CA')]
ds_dsf_CDPrelease20_US = pd.merge(ds_dsf_CDPrelease20_US,us_index_clean,on=['MarketDate'],how='left')

# Concat ex-US and US merged data
ds_dsf_CDPrelease20 = pd.concat([ds_dsf_CDPrelease20_exUS,ds_dsf_CDPrelease20_US])
# Save
ds_dsf_CDPrelease20.to_csv(output_directory+"ds_dsf_CDPrelease20.csv",index=False)


# Announcement/media on 2020 targets
ds_dsf_targetAnnounce_exUS = ds_dsf_targetAnnounce[(ds_dsf_targetAnnounce['Region']!='US')&(ds_dsf_targetAnnounce['Region']!='CA')]
ds_dsf_targetAnnounce_exUS = pd.merge(ds_dsf_targetAnnounce_exUS,windices_daily_clean,on=['MarketDate','Region'],how='left')
ds_dsf_targetAnnounce_exUS = ds_dsf_targetAnnounce_exUS.drop(columns=['fic','Country'])

ds_dsf_targetAnnounce_US = ds_dsf_targetAnnounce[(ds_dsf_targetAnnounce['Region']=='US')|(ds_dsf_targetAnnounce['Region']=='CA')]
ds_dsf_targetAnnounce_US = pd.merge(ds_dsf_targetAnnounce_US,us_index_clean,on=['MarketDate'],how='left')

# Concat ex-US and US merged data
ds_dsf_targetAnnounce = pd.concat([ds_dsf_targetAnnounce_exUS,ds_dsf_targetAnnounce_US])
# Save
ds_dsf_targetAnnounce.to_csv(output_directory+"ds_dsf_targetAnnounce.csv",index=False)


''' Calculate market adjusted returns (in percentages) '''
ds_dsf_mediaCoverage['MarketReturn_pct'] = ds_dsf_mediaCoverage['MarketReturn']*100
ds_dsf_mediaCoverage['ret_pct'] = pd.to_numeric(ds_dsf_mediaCoverage['ret'].str[0:-1])
ds_dsf_mediaCoverage['ret'] = ds_dsf_mediaCoverage['ret_pct']/100

ds_dsf_mediaCoverage['MAReturn'] = ds_dsf_mediaCoverage['ret'] - ds_dsf_mediaCoverage['MarketReturn']
ds_dsf_mediaCoverage['MAReturn_pct'] = ds_dsf_mediaCoverage['ret_pct'] - ds_dsf_mediaCoverage['MarketReturn_pct']

# Log returns
ds_dsf_mediaCoverage['MAReturn_log'] = np.log(1+ds_dsf_mediaCoverage['MAReturn'])
ds_dsf_mediaCoverage['MAReturn_logPct'] = ds_dsf_mediaCoverage['MAReturn_log']*100


ds_dsf_csrReport['MarketReturn_pct'] = ds_dsf_csrReport['MarketReturn']*100
ds_dsf_csrReport['ret_pct'] = pd.to_numeric(ds_dsf_csrReport['ret'].str[0:-1])
ds_dsf_csrReport['ret'] = ds_dsf_csrReport['ret_pct']/100

ds_dsf_csrReport['MAReturn'] = ds_dsf_csrReport['ret'] - ds_dsf_csrReport['MarketReturn']
ds_dsf_csrReport['MAReturn_pct'] = ds_dsf_csrReport['ret_pct'] - ds_dsf_csrReport['MarketReturn_pct']

# Log returns
ds_dsf_csrReport['MAReturn_log'] = np.log(1+ds_dsf_csrReport['MAReturn'])
ds_dsf_csrReport['MAReturn_logPct'] = ds_dsf_csrReport['MAReturn_log']*100


ds_dsf_CDPrelease['MarketReturn_pct'] = ds_dsf_CDPrelease['MarketReturn']*100
ds_dsf_CDPrelease['ret_pct'] = pd.to_numeric(ds_dsf_CDPrelease['ret'].str[0:-1])
ds_dsf_CDPrelease['ret'] = ds_dsf_CDPrelease['ret_pct']/100

ds_dsf_CDPrelease['MAReturn'] = ds_dsf_CDPrelease['ret'] - ds_dsf_CDPrelease['MarketReturn']
ds_dsf_CDPrelease['MAReturn_pct'] = ds_dsf_CDPrelease['ret_pct'] - ds_dsf_CDPrelease['MarketReturn_pct']

# Log returns
ds_dsf_CDPrelease['MAReturn_log'] = np.log(1+ds_dsf_CDPrelease['MAReturn'])
ds_dsf_CDPrelease['MAReturn_logPct'] = ds_dsf_CDPrelease['MAReturn_log']*100

ds_dsf_CDPrelease19['MarketReturn_pct'] = ds_dsf_CDPrelease19['MarketReturn']*100
ds_dsf_CDPrelease19['ret_pct'] = pd.to_numeric(ds_dsf_CDPrelease19['ret'].str[0:-1])
ds_dsf_CDPrelease19['ret'] = ds_dsf_CDPrelease19['ret_pct']/100

ds_dsf_CDPrelease19['MAReturn'] = ds_dsf_CDPrelease19['ret'] - ds_dsf_CDPrelease19['MarketReturn']
ds_dsf_CDPrelease19['MAReturn_pct'] = ds_dsf_CDPrelease19['ret_pct'] - ds_dsf_CDPrelease19['MarketReturn_pct']

# Log returns
ds_dsf_CDPrelease19['MAReturn_log'] = np.log(1+ds_dsf_CDPrelease19['MAReturn'])
ds_dsf_CDPrelease19['MAReturn_logPct'] = ds_dsf_CDPrelease19['MAReturn_log']*100

ds_dsf_CDPrelease20['MarketReturn_pct'] = ds_dsf_CDPrelease20['MarketReturn']*100
ds_dsf_CDPrelease20['ret_pct'] = pd.to_numeric(ds_dsf_CDPrelease20['ret'].str[0:-1])
ds_dsf_CDPrelease20['ret'] = ds_dsf_CDPrelease20['ret_pct']/100

ds_dsf_CDPrelease20['MAReturn'] = ds_dsf_CDPrelease20['ret'] - ds_dsf_CDPrelease20['MarketReturn']
ds_dsf_CDPrelease20['MAReturn_pct'] = ds_dsf_CDPrelease20['ret_pct'] - ds_dsf_CDPrelease20['MarketReturn_pct']

# Log returns
ds_dsf_CDPrelease20['MAReturn_log'] = np.log(1+ds_dsf_CDPrelease20['MAReturn'])
ds_dsf_CDPrelease20['MAReturn_logPct'] = ds_dsf_CDPrelease20['MAReturn_log']*100


ds_dsf_targetAnnounce['MarketReturn_pct'] = ds_dsf_targetAnnounce['MarketReturn']*100
ds_dsf_targetAnnounce['ret_pct'] = pd.to_numeric(ds_dsf_targetAnnounce['ret'].str[0:-1])
ds_dsf_targetAnnounce['ret'] = ds_dsf_targetAnnounce['ret_pct']/100

ds_dsf_targetAnnounce['MAReturn'] = ds_dsf_targetAnnounce['ret'] - ds_dsf_targetAnnounce['MarketReturn']
ds_dsf_targetAnnounce['MAReturn_pct'] = ds_dsf_targetAnnounce['ret_pct'] - ds_dsf_targetAnnounce['MarketReturn_pct']

# Log returns
ds_dsf_targetAnnounce['MAReturn_log'] = np.log(1+ds_dsf_targetAnnounce['MAReturn'])
ds_dsf_targetAnnounce['MAReturn_logPct'] = ds_dsf_targetAnnounce['MAReturn_log']*100




''' Calculate trading volume (in percentages) '''
ds_dsf_mediaCoverage['Volume_pct'] = ds_dsf_mediaCoverage['Volume']/ds_dsf_mediaCoverage['numshrs']*100
ds_dsf_mediaCoverage['Volume_pctlog'] = np.log(ds_dsf_mediaCoverage['Volume_pct']+0.000255) # small number added to prevent log transforming zero

# Save
ds_dsf_mediaCoverage.to_csv(output_directory+"ds_dsf_mediaCoverage.csv",index=False)


ds_dsf_csrReport['Volume_pct'] = ds_dsf_csrReport['Volume']/ds_dsf_csrReport['numshrs']*100
ds_dsf_csrReport['Volume_pctlog'] = np.log(ds_dsf_csrReport['Volume_pct']+0.000255) # small number added to prevent log transforming zero

# Save
ds_dsf_csrReport.to_csv(output_directory+"ds_dsf_csrReport.csv",index=False)



ds_dsf_CDPrelease['Volume_pct'] = ds_dsf_CDPrelease['Volume']/ds_dsf_CDPrelease['numshrs']*100
ds_dsf_CDPrelease['Volume_pctlog'] = np.log(ds_dsf_CDPrelease['Volume_pct']+0.000255) # small number added to prevent log transforming zero

# Save
ds_dsf_CDPrelease.to_csv(output_directory+"ds_dsf_CDPrelease.csv",index=False)


ds_dsf_CDPrelease19['Volume_pct'] = ds_dsf_CDPrelease19['Volume']/ds_dsf_CDPrelease19['numshrs']*100
ds_dsf_CDPrelease19['Volume_pctlog'] = np.log(ds_dsf_CDPrelease19['Volume_pct']+0.000255) # small number added to prevent log transforming zero

# Save
ds_dsf_CDPrelease19.to_csv(output_directory+"ds_dsf_CDPrelease19.csv",index=False)


ds_dsf_CDPrelease20['Volume_pct'] = ds_dsf_CDPrelease20['Volume']/ds_dsf_CDPrelease20['numshrs']*100
ds_dsf_CDPrelease20['Volume_pctlog'] = np.log(ds_dsf_CDPrelease20['Volume_pct']+0.000255) # small number added to prevent log transforming zero

# Save
ds_dsf_CDPrelease20.to_csv(output_directory+"ds_dsf_CDPrelease20.csv",index=False)


ds_dsf_targetAnnounce['Volume_pct'] = ds_dsf_targetAnnounce['Volume']/ds_dsf_targetAnnounce['numshrs']*100
ds_dsf_targetAnnounce['Volume_pctlog'] = np.log(ds_dsf_targetAnnounce['Volume_pct']+0.000255) # small number added to prevent log transforming zero

# Save
ds_dsf_targetAnnounce.to_csv(output_directory+"ds_dsf_targetAnnounce.csv",index=False)



''' Calculate Market Model Abnormal Returns '''
ds_dsf_mediaCoverage = pd.read_csv(output_directory+"ds_dsf_mediaCoverage.csv")
ds_dsf_csrReport = pd.read_csv(output_directory+"ds_dsf_csrReport.csv")
ds_dsf_CDPrelease = pd.read_csv(output_directory+"ds_dsf_CDPrelease.csv")
ds_dsf_CDPrelease19 = pd.read_csv(output_directory+"ds_dsf_CDPrelease19.csv")
ds_dsf_CDPrelease20 = pd.read_csv(output_directory+"ds_dsf_CDPrelease20.csv")
ds_dsf_targetAnnounce = pd.read_csv(output_directory+"ds_dsf_targetAnnounce.csv")

### Media Coverage data - there can be multiple media coverage event per firm
# List of companies
marketModel_summary =  ds_dsf_mediaCoverage[['ISIN','EventDate']].dropna().drop_duplicates()
# Loop through list of companies and get alpha and beta
for row in marketModel_summary.itertuples():
    reg_data = ds_dsf_mediaCoverage[(ds_dsf_mediaCoverage['ISIN']==row.ISIN)&(ds_dsf_mediaCoverage['EventDate']==row.EventDate)]
    # Estimation window [-130,-30)
    reg_data = reg_data[(reg_data['BDaysRelativeToEvent']>=-130)&(reg_data['BDaysRelativeToEvent']<-30)]
    reg_data = reg_data.dropna(subset=['ret','MarketReturn'])
    if len(reg_data)<60:
        continue
    Y = reg_data['ret']
    X = reg_data['MarketReturn']
    X = sm.add_constant(X)

    mod = sm.OLS(Y,X)
    res = mod.fit()
    # Save alpha and beta
    marketModel_summary.loc[row.Index,'alpha'] = res.params[0]
    marketModel_summary.loc[row.Index,'beta'] = res.params[1]


# Merge in alpha and beta
ds_dsf_mediaCoverage = pd.merge(ds_dsf_mediaCoverage,marketModel_summary,on=['ISIN','EventDate'],how='left')
ds_dsf_mediaCoverage['ret_MarketModel'] = ds_dsf_mediaCoverage['alpha'] + ds_dsf_mediaCoverage['beta']*ds_dsf_mediaCoverage['MarketReturn']
ds_dsf_mediaCoverage['adjRet_MarketModel'] = ds_dsf_mediaCoverage['ret'] - ds_dsf_mediaCoverage['ret_MarketModel']


### CSR Report
# List of companies
marketModel_summary =  ds_dsf_csrReport[['ISIN']].dropna().drop_duplicates()
# Loop through list of companies and get alpha and beta
for row in marketModel_summary.itertuples():
    reg_data = ds_dsf_csrReport[ds_dsf_csrReport['ISIN']==row.ISIN]
    # Estimation window [-130,-30)
    reg_data = reg_data[(reg_data['BDaysRelativeToEvent']>=-130)&(reg_data['BDaysRelativeToEvent']<-30)]
    reg_data = reg_data.dropna(subset=['ret','MarketReturn'])    
    if len(reg_data)<60:
        continue
    Y = reg_data['ret']
    X = reg_data['MarketReturn']
    X = sm.add_constant(X)

    mod = sm.OLS(Y,X)
    res = mod.fit()
    # Save alpha and beta
    marketModel_summary.loc[row.Index,'alpha'] = res.params[0]
    marketModel_summary.loc[row.Index,'beta'] = res.params[1]


# Merge in alpha and beta
ds_dsf_csrReport = pd.merge(ds_dsf_csrReport,marketModel_summary,on=['ISIN'],how='left')
ds_dsf_csrReport['ret_MarketModel'] = ds_dsf_csrReport['alpha'] + ds_dsf_csrReport['beta']*ds_dsf_csrReport['MarketReturn']
ds_dsf_csrReport['adjRet_MarketModel'] = ds_dsf_csrReport['ret'] - ds_dsf_csrReport['ret_MarketModel']


### CDP release
# List of companies
marketModel_summary =  ds_dsf_CDPrelease[['ISIN']].dropna().drop_duplicates()
# Loop through list of companies and get alpha and beta
for row in marketModel_summary.itertuples():
    reg_data = ds_dsf_CDPrelease[ds_dsf_CDPrelease['ISIN']==row.ISIN]
    # Estimation window [-130,-30)
    reg_data = reg_data[(reg_data['BDaysRelativeToEvent']>=-130)&(reg_data['BDaysRelativeToEvent']<-30)]
    reg_data = reg_data.dropna(subset=['ret','MarketReturn'])
    if len(reg_data)<60:
        continue
    Y = reg_data['ret']
    X = reg_data['MarketReturn']
    X = sm.add_constant(X)

    mod = sm.OLS(Y,X)
    res = mod.fit()
    # Save alpha and beta
    marketModel_summary.loc[row.Index,'alpha'] = res.params[0]
    marketModel_summary.loc[row.Index,'beta'] = res.params[1]


# Merge in alpha and beta
ds_dsf_CDPrelease = pd.merge(ds_dsf_CDPrelease,marketModel_summary,on=['ISIN'],how='left')
ds_dsf_CDPrelease['ret_MarketModel'] = ds_dsf_CDPrelease['alpha'] + ds_dsf_CDPrelease['beta']*ds_dsf_CDPrelease['MarketReturn']
ds_dsf_CDPrelease['adjRet_MarketModel'] = ds_dsf_CDPrelease['ret'] - ds_dsf_CDPrelease['ret_MarketModel']


### CDP release 2019
# List of companies
marketModel_summary =  ds_dsf_CDPrelease19[['ISIN']].dropna().drop_duplicates()
# Loop through list of companies and get alpha and beta
for row in marketModel_summary.itertuples():
    reg_data = ds_dsf_CDPrelease19[ds_dsf_CDPrelease19['ISIN']==row.ISIN]
    # Estimation window [-130,-30)
    reg_data = reg_data[(reg_data['BDaysRelativeToEvent']>=-130)&(reg_data['BDaysRelativeToEvent']<-30)]
    reg_data = reg_data.dropna(subset=['ret','MarketReturn'])
    if len(reg_data)<60:
        continue
    Y = reg_data['ret']
    X = reg_data['MarketReturn']
    X = sm.add_constant(X)

    mod = sm.OLS(Y,X)
    res = mod.fit()
    # Save alpha and beta
    marketModel_summary.loc[row.Index,'alpha'] = res.params[0]
    marketModel_summary.loc[row.Index,'beta'] = res.params[1]


# Merge in alpha and beta
ds_dsf_CDPrelease19 = pd.merge(ds_dsf_CDPrelease19,marketModel_summary,on=['ISIN'],how='left')
ds_dsf_CDPrelease19['ret_MarketModel'] = ds_dsf_CDPrelease19['alpha'] + ds_dsf_CDPrelease19['beta']*ds_dsf_CDPrelease19['MarketReturn']
ds_dsf_CDPrelease19['adjRet_MarketModel'] = ds_dsf_CDPrelease19['ret'] - ds_dsf_CDPrelease19['ret_MarketModel']


### CDP release 2020
# List of companies
marketModel_summary =  ds_dsf_CDPrelease20[['ISIN']].dropna().drop_duplicates()
# Loop through list of companies and get alpha and beta
for row in marketModel_summary.itertuples():
    reg_data = ds_dsf_CDPrelease20[ds_dsf_CDPrelease20['ISIN']==row.ISIN]
    # Estimation window [-130,-30)
    reg_data = reg_data[(reg_data['BDaysRelativeToEvent']>=-130)&(reg_data['BDaysRelativeToEvent']<-30)]
    reg_data = reg_data.dropna(subset=['ret','MarketReturn'])
    if len(reg_data)<60:
        continue
    Y = reg_data['ret']
    X = reg_data['MarketReturn']
    X = sm.add_constant(X)

    mod = sm.OLS(Y,X)
    res = mod.fit()
    # Save alpha and beta
    marketModel_summary.loc[row.Index,'alpha'] = res.params[0]
    marketModel_summary.loc[row.Index,'beta'] = res.params[1]


# Merge in alpha and beta
ds_dsf_CDPrelease20 = pd.merge(ds_dsf_CDPrelease20,marketModel_summary,on=['ISIN'],how='left')
ds_dsf_CDPrelease20['ret_MarketModel'] = ds_dsf_CDPrelease20['alpha'] + ds_dsf_CDPrelease20['beta']*ds_dsf_CDPrelease20['MarketReturn']
ds_dsf_CDPrelease20['adjRet_MarketModel'] = ds_dsf_CDPrelease20['ret'] - ds_dsf_CDPrelease20['ret_MarketModel']


### Announcement/Media coverage 2020 targets
# There can be multiple media coverage of target announcement per firm
# List of companies
marketModel_summary =  ds_dsf_targetAnnounce[['ISIN','EventDate']].dropna().drop_duplicates()
# Loop through list of companies and get alpha and beta
for row in marketModel_summary.itertuples():
    reg_data = ds_dsf_targetAnnounce[(ds_dsf_targetAnnounce['ISIN']==row.ISIN)&(ds_dsf_targetAnnounce['EventDate']==row.EventDate)]
    # Estimation window [-130,-30)
    reg_data = reg_data[(reg_data['BDaysRelativeToEvent']>=-130)&(reg_data['BDaysRelativeToEvent']<-30)]
    reg_data = reg_data.dropna(subset=['ret','MarketReturn'])
    if len(reg_data)<60:
        continue
    Y = reg_data['ret']
    X = reg_data['MarketReturn']
    X = sm.add_constant(X)

    mod = sm.OLS(Y,X)
    res = mod.fit()
    # Save alpha and beta
    marketModel_summary.loc[row.Index,'alpha'] = res.params[0]
    marketModel_summary.loc[row.Index,'beta'] = res.params[1]


# Merge in alpha and beta
ds_dsf_targetAnnounce = pd.merge(ds_dsf_targetAnnounce,marketModel_summary,on=['ISIN','EventDate'],how='left')
ds_dsf_targetAnnounce['ret_MarketModel'] = ds_dsf_targetAnnounce['alpha'] + ds_dsf_targetAnnounce['beta']*ds_dsf_targetAnnounce['MarketReturn']
ds_dsf_targetAnnounce['adjRet_MarketModel'] = ds_dsf_targetAnnounce['ret'] - ds_dsf_targetAnnounce['ret_MarketModel']


# Log returns
ds_dsf_mediaCoverage['adjRet_MarketModel_log'] = np.log(1+ds_dsf_mediaCoverage['adjRet_MarketModel'])
ds_dsf_mediaCoverage['adjRet_MarketModel_logPct'] = ds_dsf_mediaCoverage['adjRet_MarketModel_log']*100

ds_dsf_csrReport['adjRet_MarketModel_log'] = np.log(1+ds_dsf_csrReport['adjRet_MarketModel'])
ds_dsf_csrReport['adjRet_MarketModel_logPct'] = ds_dsf_csrReport['adjRet_MarketModel_log']*100

ds_dsf_CDPrelease['adjRet_MarketModel_log'] = np.log(1+ds_dsf_CDPrelease['adjRet_MarketModel'])
ds_dsf_CDPrelease['adjRet_MarketModel_logPct'] = ds_dsf_CDPrelease['adjRet_MarketModel_log']*100

ds_dsf_CDPrelease19['adjRet_MarketModel_log'] = np.log(1+ds_dsf_CDPrelease19['adjRet_MarketModel'])
ds_dsf_CDPrelease19['adjRet_MarketModel_logPct'] = ds_dsf_CDPrelease19['adjRet_MarketModel_log']*100

ds_dsf_CDPrelease20['adjRet_MarketModel_log'] = np.log(1+ds_dsf_CDPrelease20['adjRet_MarketModel'])
ds_dsf_CDPrelease20['adjRet_MarketModel_logPct'] = ds_dsf_CDPrelease20['adjRet_MarketModel_log']*100

ds_dsf_targetAnnounce['adjRet_MarketModel_log'] = np.log(1+ds_dsf_targetAnnounce['adjRet_MarketModel'])
ds_dsf_targetAnnounce['adjRet_MarketModel_logPct'] = ds_dsf_targetAnnounce['adjRet_MarketModel_log']*100


# Save
ds_dsf_mediaCoverage.to_csv(output_directory+"ds_dsf_mediaCoverage.csv",index=False)
ds_dsf_csrReport.to_csv(output_directory+"ds_dsf_csrReport.csv",index=False)
ds_dsf_CDPrelease.to_csv(output_directory+"ds_dsf_CDPrelease.csv",index=False)
ds_dsf_CDPrelease19.to_csv(output_directory+"ds_dsf_CDPrelease19.csv",index=False)
ds_dsf_CDPrelease20.to_csv(output_directory+"ds_dsf_CDPrelease20.csv",index=False)
ds_dsf_targetAnnounce.to_csv(output_directory+"ds_dsf_targetAnnounce.csv",index=False)



''' New Days Relative To Event filling in missing dates '''
ds_dsf_mediaCoverage = pd.read_csv(output_directory+"ds_dsf_mediaCoverage.csv")
ds_dsf_csrReport = pd.read_csv(output_directory+"ds_dsf_csrReport.csv")
ds_dsf_CDPrelease = pd.read_csv(output_directory+"ds_dsf_CDPrelease.csv")
ds_dsf_CDPrelease19 = pd.read_csv(output_directory+"ds_dsf_CDPrelease19.csv")
ds_dsf_CDPrelease20 = pd.read_csv(output_directory+"ds_dsf_CDPrelease20.csv")
ds_dsf_targetAnnounce = pd.read_csv(output_directory+"ds_dsf_targetAnnounce.csv")


# Keep only data in [t-15, t+15] - do not save this
ds_dsf_mediaCoverage_sub = ds_dsf_mediaCoverage[(ds_dsf_mediaCoverage['BDaysRelativeToEvent']>=-15)&(ds_dsf_mediaCoverage['BDaysRelativeToEvent']<=15)]
ds_dsf_csrReport_sub = ds_dsf_csrReport[(ds_dsf_csrReport['BDaysRelativeToEvent']>=-15)&(ds_dsf_csrReport['BDaysRelativeToEvent']<=15)]
ds_dsf_CDPrelease_sub = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['BDaysRelativeToEvent']>=-15)&(ds_dsf_CDPrelease['BDaysRelativeToEvent']<=15)]
ds_dsf_CDPrelease19_sub = ds_dsf_CDPrelease19[(ds_dsf_CDPrelease19['BDaysRelativeToEvent']>=-15)&(ds_dsf_CDPrelease19['BDaysRelativeToEvent']<=15)]
ds_dsf_CDPrelease20_sub = ds_dsf_CDPrelease20[(ds_dsf_CDPrelease20['BDaysRelativeToEvent']>=-15)&(ds_dsf_CDPrelease20['BDaysRelativeToEvent']<=15)]
ds_dsf_targetAnnounce_sub = ds_dsf_targetAnnounce[(ds_dsf_targetAnnounce['BDaysRelativeToEvent']>=-15)&(ds_dsf_targetAnnounce['BDaysRelativeToEvent']<=15)]


# Remove dates with missing market-adjusted returns
ds_dsf_mediaCoverage_sub = ds_dsf_mediaCoverage_sub.dropna(subset=['MAReturn_logPct'])
ds_dsf_csrReport_sub = ds_dsf_csrReport_sub.dropna(subset=['MAReturn_logPct'])
ds_dsf_CDPrelease_sub = ds_dsf_CDPrelease_sub.dropna(subset=['MAReturn_logPct'])
ds_dsf_CDPrelease19_sub = ds_dsf_CDPrelease19_sub.dropna(subset=['MAReturn_logPct'])
ds_dsf_CDPrelease20_sub = ds_dsf_CDPrelease20_sub.dropna(subset=['MAReturn_logPct'])
ds_dsf_targetAnnounce_sub = ds_dsf_targetAnnounce_sub.dropna(subset=['MAReturn_logPct'])

# Remove duplicates
ds_dsf_mediaCoverage_sub = ds_dsf_mediaCoverage_sub.drop_duplicates(subset=['ISIN','MarketDate','EventDate','MAReturn_logPct','BDaysRelativeToEvent'],keep='last')
ds_dsf_csrReport_sub = ds_dsf_csrReport_sub.drop_duplicates(subset=['ISIN','MarketDate','EventDate','MAReturn_logPct','BDaysRelativeToEvent'],keep='last')
ds_dsf_CDPrelease_sub = ds_dsf_CDPrelease_sub.drop_duplicates(subset=['ISIN','MarketDate','EventDate','MAReturn_logPct','BDaysRelativeToEvent'],keep='last')
ds_dsf_CDPrelease19_sub = ds_dsf_CDPrelease19_sub.drop_duplicates(subset=['ISIN','MarketDate','EventDate','MAReturn_logPct','BDaysRelativeToEvent'],keep='last')
ds_dsf_CDPrelease20_sub = ds_dsf_CDPrelease20_sub.drop_duplicates(subset=['ISIN','MarketDate','EventDate','MAReturn_logPct','BDaysRelativeToEvent'],keep='last')
ds_dsf_targetAnnounce_sub = ds_dsf_targetAnnounce_sub.drop_duplicates(subset=['ISIN','MarketDate','EventDate','MAReturn_logPct','BDaysRelativeToEvent'],keep='last')


# Fill in missing BDaysRelativeToEvent
# Need to do it separately for pre-event and post-event data
# Groupby ISIN and rank based on MarketDate and assign NEWDaysRelativeToEvent

# Media coverage - there can be multiple events per firm
mediaCoverage_tempPre = ds_dsf_mediaCoverage_sub[ds_dsf_mediaCoverage_sub['BDaysRelativeToEvent']<0]
mediaCoverage_tempPost = ds_dsf_mediaCoverage_sub[ds_dsf_mediaCoverage_sub['BDaysRelativeToEvent']>=0]

mediaCoverage_tempPre['NEWDaysRelativeToEvent'] = (mediaCoverage_tempPre.groupby(['ISIN','EventDate'])['MarketDate'].rank(ascending=False))*(-1)
mediaCoverage_tempPost['NEWDaysRelativeToEvent'] = (mediaCoverage_tempPost.groupby(['ISIN','EventDate'])['MarketDate'].rank(ascending=True))-1

ds_dsf_mediaCoverage_sub = pd.concat([mediaCoverage_tempPre,mediaCoverage_tempPost])
ds_dsf_mediaCoverage_sub = ds_dsf_mediaCoverage_sub.sort_values(by=['ISIN','EventDate','NEWDaysRelativeToEvent'])

# CSR report
csrReport_tempPre = ds_dsf_csrReport_sub[ds_dsf_csrReport_sub['BDaysRelativeToEvent']<0]
csrReport_tempPost = ds_dsf_csrReport_sub[ds_dsf_csrReport_sub['BDaysRelativeToEvent']>=0]

csrReport_tempPre['NEWDaysRelativeToEvent'] = (csrReport_tempPre.groupby('ISIN')['MarketDate'].rank(ascending=False))*(-1)
csrReport_tempPost['NEWDaysRelativeToEvent'] = (csrReport_tempPost.groupby('ISIN')['MarketDate'].rank(ascending=True))-1

ds_dsf_csrReport_sub = pd.concat([csrReport_tempPre,csrReport_tempPost])
ds_dsf_csrReport_sub = ds_dsf_csrReport_sub.sort_values(by=['ISIN','NEWDaysRelativeToEvent'])

# CDP release date
CDPrelease_tempPre = ds_dsf_CDPrelease_sub[ds_dsf_CDPrelease_sub['BDaysRelativeToEvent']<0]
CDPrelease_tempPost = ds_dsf_CDPrelease_sub[ds_dsf_CDPrelease_sub['BDaysRelativeToEvent']>=0]

CDPrelease_tempPre['NEWDaysRelativeToEvent'] = (CDPrelease_tempPre.groupby('ISIN')['MarketDate'].rank(ascending=False))*(-1)
CDPrelease_tempPost['NEWDaysRelativeToEvent'] = (CDPrelease_tempPost.groupby('ISIN')['MarketDate'].rank(ascending=True))-1

ds_dsf_CDPrelease_sub = pd.concat([CDPrelease_tempPre,CDPrelease_tempPost])
ds_dsf_CDPrelease_sub = ds_dsf_CDPrelease_sub.sort_values(by=['ISIN','NEWDaysRelativeToEvent'])

# 2019 CDP release date
ds_dsf_CDPrelease19_sub = ds_dsf_CDPrelease19_sub.drop_duplicates(subset=['ISIN','MarketDate','BDaysRelativeToEvent'])
CDPrelease19_tempPre = ds_dsf_CDPrelease19_sub[ds_dsf_CDPrelease19_sub['BDaysRelativeToEvent']<0]
CDPrelease19_tempPost = ds_dsf_CDPrelease19_sub[ds_dsf_CDPrelease19_sub['BDaysRelativeToEvent']>=0]

CDPrelease19_tempPre['NEWDaysRelativeToEvent'] = (CDPrelease19_tempPre.groupby('ISIN')['MarketDate'].rank(ascending=False))*(-1)
CDPrelease19_tempPost['NEWDaysRelativeToEvent'] = (CDPrelease19_tempPost.groupby('ISIN')['MarketDate'].rank(ascending=True))-1

ds_dsf_CDPrelease19_sub = pd.concat([CDPrelease19_tempPre,CDPrelease19_tempPost])
ds_dsf_CDPrelease19_sub = ds_dsf_CDPrelease19_sub.sort_values(by=['ISIN','NEWDaysRelativeToEvent'])

# 2020 CDP release date
ds_dsf_CDPrelease20_sub = ds_dsf_CDPrelease20_sub.drop_duplicates(subset=['ISIN','MarketDate','BDaysRelativeToEvent'])
CDPrelease20_tempPre = ds_dsf_CDPrelease20_sub[ds_dsf_CDPrelease20_sub['BDaysRelativeToEvent']<0]
CDPrelease20_tempPost = ds_dsf_CDPrelease20_sub[ds_dsf_CDPrelease20_sub['BDaysRelativeToEvent']>=0]

CDPrelease20_tempPre['NEWDaysRelativeToEvent'] = (CDPrelease20_tempPre.groupby('ISIN')['MarketDate'].rank(ascending=False))*(-1)
CDPrelease20_tempPost['NEWDaysRelativeToEvent'] = (CDPrelease20_tempPost.groupby('ISIN')['MarketDate'].rank(ascending=True))-1

ds_dsf_CDPrelease20_sub = pd.concat([CDPrelease20_tempPre,CDPrelease20_tempPost])
ds_dsf_CDPrelease20_sub = ds_dsf_CDPrelease20_sub.sort_values(by=['ISIN','NEWDaysRelativeToEvent'])

# Announcement/media coverage of 2020 target
ds_dsf_targetAnnounce_sub = ds_dsf_targetAnnounce_sub.drop_duplicates(subset=['ISIN','MarketDate','BDaysRelativeToEvent'])
targetAnnounce_tempPre = ds_dsf_targetAnnounce_sub[ds_dsf_targetAnnounce_sub['BDaysRelativeToEvent']<0]
targetAnnounce_tempPost = ds_dsf_targetAnnounce_sub[ds_dsf_targetAnnounce_sub['BDaysRelativeToEvent']>=0]

targetAnnounce_tempPre['NEWDaysRelativeToEvent'] = (targetAnnounce_tempPre.groupby(['ISIN','EventDate'])['MarketDate'].rank(ascending=False))*(-1)
targetAnnounce_tempPost['NEWDaysRelativeToEvent'] = (targetAnnounce_tempPost.groupby(['ISIN','EventDate'])['MarketDate'].rank(ascending=True))-1

ds_dsf_targetAnnounce_sub = pd.concat([targetAnnounce_tempPre,targetAnnounce_tempPost])
ds_dsf_targetAnnounce_sub = ds_dsf_targetAnnounce_sub.sort_values(by=['ISIN','EventDate','NEWDaysRelativeToEvent'])






####################################################################################
####################################################################################
''' Using Market Model '''
####################################################################################
####################################################################################

'''
####################################################################################
##########   Around Media Coverage    #######################################
###################################################################################
'''
#### There can be multiple media event per firm

''' Returns Around Media Coverage (10 days window; -1 to +10) '''
window_10 = ds_dsf_mediaCoverage_sub[(ds_dsf_mediaCoverage_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_mediaCoverage_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby(['ISIN','EventDate'],as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
remove_list['ISIN_EventDate'] = remove_list['ISIN'] + "_" + remove_list['EventDate'].astype(str)
window_10['ISIN_EventDate'] = window_10['ISIN'] + "_" + window_10['EventDate'].astype(str)

window_10 = window_10[~window_10['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]

achieved_10 = window_10[window_10['achieved']==1]
failed_10 = window_10[window_10['failed']==1]

# Cumulative returns
achieved_10['CAR_logPct'] = achieved_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()
failed_10['CAR_logPct'] = failed_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
achieved_10_summary = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_10_summary = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()

achieved_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

# t-stat - Cross-sectional test
achieved_10 = pd.merge(achieved_10,achieved_10_summary,on=['NEWDaysRelativeToEvent']) # Merge in CAAR
achieved_10['CAR-CAAR'] = achieved_10['CAR_logPct'] - achieved_10['CAAR_logPct']
achieved_10['CAR-CAAR_sq'] = achieved_10['CAR-CAAR']**2

achieved_10_summary['sum_CAR-CAAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']

achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
achieved_10_summary['VAR_CAAR'] = achieved_10_summary['sum_CAR-CAAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_CAAR'] = np.sqrt(achieved_10_summary['VAR_CAAR'])
achieved_10_summary['T_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['CAAR_logPct']/achieved_10_summary['SD_CAAR']

# Get AAR
achieved_10_summary['AAR_logPct'] = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = achieved_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
achieved_10 = pd.merge(achieved_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
achieved_10['MAR-AAR'] = achieved_10['adjRet_MarketModel_logPct'] - achieved_10['AAR_logPct']
achieved_10['MAR-AAR_sq'] = achieved_10['MAR-AAR']**2

achieved_10_summary['sum_MAR-AAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
achieved_10_summary['VAR_AAR'] = achieved_10_summary['sum_MAR-AAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_AAR'] = np.sqrt(achieved_10_summary['VAR_AAR'])

achieved_10_summary['T_MAR_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['AAR_logPct']/achieved_10_summary['SD_AAR']



# Failed  t-stat - Cross-sectional test
failed_10 = pd.merge(failed_10,failed_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_10['CAR-CAAR'] = failed_10['CAR_logPct'] - failed_10['CAAR_logPct']
failed_10['CAR-CAAR_sq'] = failed_10['CAR-CAAR']**2

failed_10_summary['sum_CAR-CAAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']

failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_10_summary['VAR_CAAR'] = failed_10_summary['sum_CAR-CAAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_CAAR'] = np.sqrt(failed_10_summary['VAR_CAAR'])
failed_10_summary['T_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['CAAR_logPct']/failed_10_summary['SD_CAAR']

# Get AAR
failed_10_summary['AAR_logPct'] = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_10 = pd.merge(failed_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_10['MAR-AAR'] = failed_10['adjRet_MarketModel_logPct'] - failed_10['AAR_logPct']
failed_10['MAR-AAR_sq'] = failed_10['MAR-AAR']**2

failed_10_summary['sum_MAR-AAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_10_summary['VAR_AAR'] = failed_10_summary['sum_MAR-AAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_AAR'] = np.sqrt(failed_10_summary['VAR_AAR'])

failed_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['AAR_logPct']/failed_10_summary['SD_AAR']





''' Returns Around Media Coverage - ONLY MATERIAL FIRMS (-1 to +10) '''
### Updated V3
window_10 = ds_dsf_mediaCoverage_sub[(ds_dsf_mediaCoverage_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_mediaCoverage_sub['NEWDaysRelativeToEvent']<=10)]


# Drop firms without all 10 days of data
remove_list = window_10.groupby(['ISIN','EventDate'],as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
remove_list['ISIN_EventDate'] = remove_list['ISIN'] + "_" + remove_list['EventDate'].astype(str)
window_10['ISIN_EventDate'] = window_10['ISIN'] + "_" + window_10['EventDate'].astype(str)

window_10 = window_10[~window_10['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]


### Keep only material industries
window_10 = window_10[window_10['emission_industry_high']==1]

achieved_10 = window_10[window_10['achieved']==1]
failed_10 = window_10[window_10['failed']==1]

# Cumulative returns
achieved_10['CAR_logPct'] = achieved_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()
failed_10['CAR_logPct'] = failed_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
achieved_10_summary = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_10_summary = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()

achieved_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

# t-stat - Cross-sectional test
achieved_10 = pd.merge(achieved_10,achieved_10_summary,on=['NEWDaysRelativeToEvent']) # Merge in CAAR
achieved_10['CAR-CAAR'] = achieved_10['CAR_logPct'] - achieved_10['CAAR_logPct']
achieved_10['CAR-CAAR_sq'] = achieved_10['CAR-CAAR']**2

achieved_10_summary['sum_CAR-CAAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
achieved_10_summary['VAR_CAAR'] = achieved_10_summary['sum_CAR-CAAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_CAAR'] = np.sqrt(achieved_10_summary['VAR_CAAR'])

achieved_10_summary['T_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['CAAR_logPct']/achieved_10_summary['SD_CAAR']

# Get AAR
achieved_10_summary['AAR_logPct'] = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = achieved_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
achieved_10 = pd.merge(achieved_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
achieved_10['MAR-AAR'] = achieved_10['adjRet_MarketModel_logPct'] - achieved_10['AAR_logPct']
achieved_10['MAR-AAR_sq'] = achieved_10['MAR-AAR']**2

achieved_10_summary['sum_MAR-AAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
achieved_10_summary['VAR_AAR'] = achieved_10_summary['sum_MAR-AAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_AAR'] = np.sqrt(achieved_10_summary['VAR_AAR'])

achieved_10_summary['T_MAR_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['AAR_logPct']/achieved_10_summary['SD_AAR']


# Control  t-stat - Cross-sectional test
failed_10 = pd.merge(failed_10,failed_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_10['CAR-CAAR'] = failed_10['CAR_logPct'] - failed_10['CAAR_logPct']
failed_10['CAR-CAAR_sq'] = failed_10['CAR-CAAR']**2

failed_10_summary['sum_CAR-CAAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_10_summary['VAR_CAAR'] = failed_10_summary['sum_CAR-CAAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_CAAR'] = np.sqrt(failed_10_summary['VAR_CAAR'])

failed_10_summary['T_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['CAAR_logPct']/failed_10_summary['SD_CAAR']

# Get AAR
failed_10_summary['AAR_logPct'] = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_10 = pd.merge(failed_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_10['MAR-AAR'] = failed_10['adjRet_MarketModel_logPct'] - failed_10['AAR_logPct']
failed_10['MAR-AAR_sq'] = failed_10['MAR-AAR']**2

failed_10_summary['sum_MAR-AAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_10_summary['VAR_AAR'] = failed_10_summary['sum_MAR-AAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_AAR'] = np.sqrt(failed_10_summary['VAR_AAR'])

failed_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['AAR_logPct']/failed_10_summary['SD_AAR']


''' Returns Around Media Coverage - DROP COVID FIRMS (-1 to +10) '''

window_10 = ds_dsf_mediaCoverage_sub[(ds_dsf_mediaCoverage_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_mediaCoverage_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby(['ISIN','EventDate'],as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
remove_list['ISIN_EventDate'] = remove_list['ISIN'] + "_" + remove_list['EventDate'].astype(str)
window_10['ISIN_EventDate'] = window_10['ISIN'] + "_" + window_10['EventDate'].astype(str)

window_10 = window_10[~window_10['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]

### Drop more COVID affected firms
window_10 = window_10[window_10['type_covid_industry']==0]

achieved_10 = window_10[window_10['achieved']==1]
failed_10 = window_10[window_10['failed']==1]

# Cumulative returns
achieved_10['CAR_logPct'] = achieved_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()
failed_10['CAR_logPct'] = failed_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
achieved_10_summary = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_10_summary = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()

achieved_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

# t-stat - Cross-sectional test
achieved_10 = pd.merge(achieved_10,achieved_10_summary,on=['NEWDaysRelativeToEvent']) # Merge in CAAR
achieved_10['CAR-CAAR'] = achieved_10['CAR_logPct'] - achieved_10['CAAR_logPct']
achieved_10['CAR-CAAR_sq'] = achieved_10['CAR-CAAR']**2

achieved_10_summary['sum_CAR-CAAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
achieved_10_summary['VAR_CAAR'] = achieved_10_summary['sum_CAR-CAAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_CAAR'] = np.sqrt(achieved_10_summary['VAR_CAAR'])

achieved_10_summary['T_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['CAAR_logPct']/achieved_10_summary['SD_CAAR']

# Get AAR
achieved_10_summary['AAR_logPct'] = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = achieved_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
achieved_10 = pd.merge(achieved_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
achieved_10['MAR-AAR'] = achieved_10['adjRet_MarketModel_logPct'] - achieved_10['AAR_logPct']
achieved_10['MAR-AAR_sq'] = achieved_10['MAR-AAR']**2

achieved_10_summary['sum_MAR-AAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
achieved_10_summary['VAR_AAR'] = achieved_10_summary['sum_MAR-AAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_AAR'] = np.sqrt(achieved_10_summary['VAR_AAR'])

achieved_10_summary['T_MAR_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['AAR_logPct']/achieved_10_summary['SD_AAR']


# Control  t-stat - Cross-sectional test
failed_10 = pd.merge(failed_10,failed_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_10['CAR-CAAR'] = failed_10['CAR_logPct'] - failed_10['CAAR_logPct']
failed_10['CAR-CAAR_sq'] = failed_10['CAR-CAAR']**2

failed_10_summary['sum_CAR-CAAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_10_summary['VAR_CAAR'] = failed_10_summary['sum_CAR-CAAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_CAAR'] = np.sqrt(failed_10_summary['VAR_CAAR'])

failed_10_summary['T_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['CAAR_logPct']/failed_10_summary['SD_CAAR']

# Get AAR
failed_10_summary['AAR_logPct'] = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_10 = pd.merge(failed_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_10['MAR-AAR'] = failed_10['adjRet_MarketModel_logPct'] - failed_10['AAR_logPct']
failed_10['MAR-AAR_sq'] = failed_10['MAR-AAR']**2

failed_10_summary['sum_MAR-AAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_10_summary['VAR_AAR'] = failed_10_summary['sum_MAR-AAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_AAR'] = np.sqrt(failed_10_summary['VAR_AAR'])

failed_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['AAR_logPct']/failed_10_summary['SD_AAR']




''' Volume Test - 20 day around Media Coverage '''
window_20 = ds_dsf_mediaCoverage_sub[(ds_dsf_mediaCoverage_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_mediaCoverage_sub['NEWDaysRelativeToEvent']<=10)]


# Drop firms without all 20 days of data
remove_list = window_20.groupby(['ISIN','EventDate'],as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
remove_list['ISIN_EventDate'] = remove_list['ISIN'] + "_" + remove_list['EventDate'].astype(str)
window_20['ISIN_EventDate'] = window_20['ISIN'] + "_" + window_20['EventDate'].astype(str)

window_20 = window_20[~window_20['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]



achieved_20 = window_20[window_20['achieved']==1]
failed_20 = window_20[(window_20['failed']==1)]
disappeared_20 = window_20[(window_20['disappeared']==1)]


# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_mediaCoverage[(ds_dsf_mediaCoverage['BDaysRelativeToEvent']>=-140)&(ds_dsf_mediaCoverage['BDaysRelativeToEvent']<=-40)]
vol_estWindow20['ISIN_EventDate'] = vol_estWindow20['ISIN'] + "_" + vol_estWindow20['EventDate'].astype(str)

achieved_estWin_win20 = vol_estWindow20[vol_estWindow20['achieved']==1]
failed_estWin_win20 = vol_estWindow20[(vol_estWindow20['failed']==1)]
disappeared_estWin_win20 = vol_estWindow20[(vol_estWindow20['disappeared']==1)]


# Normal volume
norVol_achieved20 = achieved_estWin_win20.groupby(['ISIN_EventDate'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_achieved20.columns = ['ISIN_EventDate','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_failed20 = failed_estWin_win20.groupby(['ISIN_EventDate'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_failed20.columns = ['ISIN_EventDate','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_disappeared20 = disappeared_estWin_win20.groupby(['ISIN_EventDate'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_disappeared20.columns = ['ISIN_EventDate','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']


# Merge it to window return/volume data
achieved_20 = pd.merge(achieved_20,norVol_achieved20,on=['ISIN_EventDate'],how='left')
failed_20 = pd.merge(failed_20,norVol_failed20,on=['ISIN_EventDate'],how='left')
disappeared_20 = pd.merge(disappeared_20,norVol_disappeared20,on=['ISIN_EventDate'],how='left')

achieved_20['AbnVolume'] = achieved_20['Volume'] - achieved_20['normal_Volume']
achieved_20['AbnVol_pct'] = achieved_20['Volume_pct'] - achieved_20['normal_Volume_pct']
achieved_20['AbnVol_pctlog'] = achieved_20['Volume_pctlog'] - achieved_20['normal_Volume_pctlog']

failed_20['AbnVolume'] = failed_20['Volume'] - failed_20['normal_Volume']
failed_20['AbnVol_pct'] = failed_20['Volume_pct'] - failed_20['normal_Volume_pct']
failed_20['AbnVol_pctlog'] = failed_20['Volume_pctlog'] - failed_20['normal_Volume_pctlog']

disappeared_20['AbnVolume'] = disappeared_20['Volume'] - disappeared_20['normal_Volume']
disappeared_20['AbnVol_pct'] = disappeared_20['Volume_pct'] - disappeared_20['normal_Volume_pct']
disappeared_20['AbnVol_pctlog'] = disappeared_20['Volume_pctlog'] - disappeared_20['normal_Volume_pctlog']


# Merge normal volume to estimation window volume data to get sd
achieved_estWin_win20 = pd.merge(achieved_estWin_win20,norVol_achieved20,on=['ISIN_EventDate'],how='left')
achieved_estWin_win20['AbnVolume'] = achieved_estWin_win20['Volume'] - achieved_estWin_win20['normal_Volume']
achieved_estWin_win20['AbnVol_pct'] = achieved_estWin_win20['Volume_pct'] - achieved_estWin_win20['normal_Volume_pct']
achieved_estWin_win20['AbnVol_pctlog'] = achieved_estWin_win20['Volume_pctlog'] - achieved_estWin_win20['normal_Volume_pctlog']

failed_estWin_win20 = pd.merge(failed_estWin_win20,norVol_failed20,on=['ISIN_EventDate'],how='left')
failed_estWin_win20['AbnVolume'] = failed_estWin_win20['Volume'] - failed_estWin_win20['normal_Volume']
failed_estWin_win20['AbnVol_pct'] = failed_estWin_win20['Volume_pct'] - failed_estWin_win20['normal_Volume_pct']
failed_estWin_win20['AbnVol_pctlog'] = failed_estWin_win20['Volume_pctlog'] - failed_estWin_win20['normal_Volume_pctlog']

disappeared_estWin_win20 = pd.merge(disappeared_estWin_win20,norVol_disappeared20,on=['ISIN_EventDate'],how='left')
disappeared_estWin_win20['AbnVolume'] = disappeared_estWin_win20['Volume'] - disappeared_estWin_win20['normal_Volume']
disappeared_estWin_win20['AbnVol_pct'] = disappeared_estWin_win20['Volume_pct'] - disappeared_estWin_win20['normal_Volume_pct']
disappeared_estWin_win20['AbnVol_pctlog'] = disappeared_estWin_win20['Volume_pctlog'] - disappeared_estWin_win20['normal_Volume_pctlog']


# Event plot
achieved_20_summary = achieved_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
failed_20_summary = failed_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
disappeared_20_summary = disappeared_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

achieved_20_summary['N'] = achieved_20.groupby('NEWDaysRelativeToEvent',as_index=False)['AbnVolume'].count()['AbnVolume']
failed_20_summary['N'] = failed_20.groupby('NEWDaysRelativeToEvent',as_index=False)['AbnVolume'].count()['AbnVolume']
disappeared_20_summary['N'] = disappeared_20.groupby('NEWDaysRelativeToEvent',as_index=False)['AbnVolume'].count()['AbnVolume']

# t-stat
temp_achieved20 = achieved_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_achieved20['AbnVolume_mbar'] = temp_achieved20['AbnVolume'] - temp_achieved20['AbnVolume'].mean()
temp_achieved20['AbnVol_pct_mbar'] = temp_achieved20['AbnVol_pct'] - temp_achieved20['AbnVol_pct'].mean()
temp_achieved20['AbnVol_pctlog_mbar'] = temp_achieved20['AbnVol_pctlog'] - temp_achieved20['AbnVol_pctlog'].mean()

temp_achieved20['AbnVolume_mbar_sq'] = temp_achieved20['AbnVolume_mbar']**2
temp_achieved20['AbnVol_pct_mbar_sq'] = temp_achieved20['AbnVol_pct_mbar']**2
temp_achieved20['AbnVol_pctlog_mbar_sq'] = temp_achieved20['AbnVol_pctlog_mbar']**2

achieved_20_summary['Var_AbnVolume'] = temp_achieved20['AbnVolume_mbar_sq'].mean()
achieved_20_summary['Var_AbnVol_pct'] = temp_achieved20['AbnVol_pct_mbar_sq'].mean()
achieved_20_summary['Var_AbnVol_pctlog'] = temp_achieved20['AbnVol_pctlog_mbar_sq'].mean()


achieved_20_summary['sd_AbnVolume'] = np.sqrt(achieved_20_summary['Var_AbnVolume'])
achieved_20_summary['sd_AbnVol_pct'] = np.sqrt(achieved_20_summary['Var_AbnVol_pct'])
achieved_20_summary['sd_AbnVol_pctlog'] = np.sqrt(achieved_20_summary['Var_AbnVol_pctlog'])

achieved_20_summary['t_AbnVolume'] = achieved_20_summary['AbnVolume']/achieved_20_summary['sd_AbnVolume']
achieved_20_summary['t_AbnVol_pct'] = achieved_20_summary['AbnVol_pct']/achieved_20_summary['sd_AbnVol_pct']
achieved_20_summary['t_AbnVol_pctlog'] = achieved_20_summary['AbnVol_pctlog']/achieved_20_summary['sd_AbnVol_pctlog']

temp_failed20 = failed_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_failed20['AbnVolume_mbar'] = temp_failed20['AbnVolume'] - temp_failed20['AbnVolume'].mean()
temp_failed20['AbnVol_pct_mbar'] = temp_failed20['AbnVol_pct'] - temp_failed20['AbnVol_pct'].mean()
temp_failed20['AbnVol_pctlog_mbar'] = temp_failed20['AbnVol_pctlog'] - temp_failed20['AbnVol_pctlog'].mean()

temp_failed20['AbnVolume_mbar_sq'] = temp_failed20['AbnVolume_mbar']**2
temp_failed20['AbnVol_pct_mbar_sq'] = temp_failed20['AbnVol_pct_mbar']**2
temp_failed20['AbnVol_pctlog_mbar_sq'] = temp_failed20['AbnVol_pctlog_mbar']**2

failed_20_summary['Var_AbnVolume'] = temp_failed20['AbnVolume_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pct'] = temp_failed20['AbnVol_pct_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pctlog'] = temp_failed20['AbnVol_pctlog_mbar_sq'].mean()

failed_20_summary['sd_AbnVolume'] = np.sqrt(failed_20_summary['Var_AbnVolume'])
failed_20_summary['sd_AbnVol_pct'] = np.sqrt(failed_20_summary['Var_AbnVol_pct'])
failed_20_summary['sd_AbnVol_pctlog'] = np.sqrt(failed_20_summary['Var_AbnVol_pctlog'])

failed_20_summary['t_AbnVolume'] = failed_20_summary['AbnVolume']/failed_20_summary['sd_AbnVolume']
failed_20_summary['t_AbnVol_pct'] = failed_20_summary['AbnVol_pct']/failed_20_summary['sd_AbnVol_pct']
failed_20_summary['t_AbnVol_pctlog'] = failed_20_summary['AbnVol_pctlog']/failed_20_summary['sd_AbnVol_pctlog']


temp_disappeared20 = disappeared_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_disappeared20['AbnVolume_mbar'] = temp_disappeared20['AbnVolume'] - temp_disappeared20['AbnVolume'].mean()
temp_disappeared20['AbnVol_pct_mbar'] = temp_disappeared20['AbnVol_pct'] - temp_disappeared20['AbnVol_pct'].mean()
temp_disappeared20['AbnVol_pctlog_mbar'] = temp_disappeared20['AbnVol_pctlog'] - temp_disappeared20['AbnVol_pctlog'].mean()

temp_disappeared20['AbnVolume_mbar_sq'] = temp_disappeared20['AbnVolume_mbar']**2
temp_disappeared20['AbnVol_pct_mbar_sq'] = temp_disappeared20['AbnVol_pct_mbar']**2
temp_disappeared20['AbnVol_pctlog_mbar_sq'] = temp_disappeared20['AbnVol_pctlog_mbar']**2

disappeared_20_summary['Var_AbnVolume'] = temp_disappeared20['AbnVolume_mbar_sq'].mean()
disappeared_20_summary['Var_AbnVol_pct'] = temp_disappeared20['AbnVol_pct_mbar_sq'].mean()
disappeared_20_summary['Var_AbnVol_pctlog'] = temp_disappeared20['AbnVol_pctlog_mbar_sq'].mean()

disappeared_20_summary['sd_AbnVolume'] = np.sqrt(disappeared_20_summary['Var_AbnVolume'])
disappeared_20_summary['sd_AbnVol_pct'] = np.sqrt(disappeared_20_summary['Var_AbnVol_pct'])
disappeared_20_summary['sd_AbnVol_pctlog'] = np.sqrt(disappeared_20_summary['Var_AbnVol_pctlog'])

disappeared_20_summary['t_AbnVolume'] = disappeared_20_summary['AbnVolume']/disappeared_20_summary['sd_AbnVolume']
disappeared_20_summary['t_AbnVol_pct'] = disappeared_20_summary['AbnVol_pct']/disappeared_20_summary['sd_AbnVol_pct']
disappeared_20_summary['t_AbnVol_pctlog'] = disappeared_20_summary['AbnVol_pctlog']/disappeared_20_summary['sd_AbnVol_pctlog']






'''
####################################################################################
##########   Around CSR Report Announcements  #######################################
###################################################################################
'''

''' Returns Around CSR Reports (10 days window; -1 to +10) '''

window_10 = ds_dsf_csrReport_sub[(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

failed_10 = window_10.copy() # There are only failed firms in this sample

# Cumulative returns
failed_10['CAR_logPct'] = failed_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
failed_10_summary = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

# t-stat - Cross-sectional test
failed_10 = pd.merge(failed_10,failed_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_10['CAR-CAAR'] = failed_10['CAR_logPct'] - failed_10['CAAR_logPct']
failed_10['CAR-CAAR_sq'] = failed_10['CAR-CAAR']**2

failed_10_summary['sum_CAR-CAAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_10_summary['VAR_CAAR'] = failed_10_summary['sum_CAR-CAAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_CAAR'] = np.sqrt(failed_10_summary['VAR_CAAR'])

failed_10_summary['T_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['CAAR_logPct']/failed_10_summary['SD_CAAR']

# Get AAR
failed_10_summary['AAR_logPct'] = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_10 = pd.merge(failed_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_10['MAR-AAR'] = failed_10['adjRet_MarketModel_logPct'] - failed_10['AAR_logPct']
failed_10['MAR-AAR_sq'] = failed_10['MAR-AAR']**2

failed_10_summary['sum_MAR-AAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_10_summary['VAR_AAR'] = failed_10_summary['sum_MAR-AAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_AAR'] = np.sqrt(failed_10_summary['VAR_AAR'])

failed_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['AAR_logPct']/failed_10_summary['SD_AAR']


''' Returns Around CSR Reports - MATERIAL FIRMS ONLY ( -1 to +10) '''

cross_list = pd.read_excel(data_directory+'companies_with_failed_targets_CSRreport_sentences_with_dates.xlsx',
                           sheet_name='list of failed with CSR dates')

window_10 = ds_dsf_csrReport_sub[(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

### Keep only material firms
material_list = cross_list[cross_list['emission_industry_high']==1]
window_10 = window_10[window_10['ISIN'].isin(material_list['isin'])]

failed_10 = window_10.copy() # There are only failed firms in this sample

# Cumulative returns
failed_10['CAR_logPct'] = failed_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
failed_10_summary = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

# t-stat - Cross-sectional test
failed_10 = pd.merge(failed_10,failed_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_10['CAR-CAAR'] = failed_10['CAR_logPct'] - failed_10['CAAR_logPct']
failed_10['CAR-CAAR_sq'] = failed_10['CAR-CAAR']**2

failed_10_summary['sum_CAR-CAAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_10_summary['VAR_CAAR'] = failed_10_summary['sum_CAR-CAAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_CAAR'] = np.sqrt(failed_10_summary['VAR_CAAR'])

failed_10_summary['T_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['CAAR_logPct']/failed_10_summary['SD_CAAR']

# Get AAR
failed_10_summary['AAR_logPct'] = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_10 = pd.merge(failed_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_10['MAR-AAR'] = failed_10['adjRet_MarketModel_logPct'] - failed_10['AAR_logPct']
failed_10['MAR-AAR_sq'] = failed_10['MAR-AAR']**2

failed_10_summary['sum_MAR-AAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_10_summary['VAR_AAR'] = failed_10_summary['sum_MAR-AAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_AAR'] = np.sqrt(failed_10_summary['VAR_AAR'])

failed_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['AAR_logPct']/failed_10_summary['SD_AAR']


''' Returns Around CSR Reports - NON-COVID FIRMS ONLY ( -1 to +10) '''

cross_list = pd.read_excel(output_directory+'companies_with_failed_targets_CSRreport_sentences_with_dates.xlsx',
                           sheet_name='list of failed with CSR dates')

window_10 = ds_dsf_csrReport_sub[(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

### Drop covid firms
noncovid_list = cross_list[cross_list['type_covid_industry']==0]
window_10 = window_10[window_10['ISIN'].isin(noncovid_list['isin'])]

failed_10 = window_10.copy() # There are only failed firms in this sample

# Cumulative returns
failed_10['CAR_logPct'] = failed_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
failed_10_summary = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

# t-stat - Cross-sectional test
failed_10 = pd.merge(failed_10,failed_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_10['CAR-CAAR'] = failed_10['CAR_logPct'] - failed_10['CAAR_logPct']
failed_10['CAR-CAAR_sq'] = failed_10['CAR-CAAR']**2

failed_10_summary['sum_CAR-CAAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_10_summary['VAR_CAAR'] = failed_10_summary['sum_CAR-CAAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_CAAR'] = np.sqrt(failed_10_summary['VAR_CAAR'])

failed_10_summary['T_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['CAAR_logPct']/failed_10_summary['SD_CAAR']

# Get AAR
failed_10_summary['AAR_logPct'] = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_10 = pd.merge(failed_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_10['MAR-AAR'] = failed_10['adjRet_MarketModel_logPct'] - failed_10['AAR_logPct']
failed_10['MAR-AAR_sq'] = failed_10['MAR-AAR']**2

failed_10_summary['sum_MAR-AAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_10_summary['VAR_AAR'] = failed_10_summary['sum_MAR-AAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_AAR'] = np.sqrt(failed_10_summary['VAR_AAR'])

failed_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['AAR_logPct']/failed_10_summary['SD_AAR']


''' Returns Around CSR Reports - High vs. Low Ambition ( -1 to +10) '''

cross_list = pd.read_excel(data_directory+'companies_with_failed_targets_CSRreport_sentences_with_dates.xlsx',
                           sheet_name='list of failed with CSR dates')

window_10 = ds_dsf_csrReport_sub[(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

failed_10 = window_10.copy() # There are only failed firms in this sample

high_amb = cross_list[cross_list['failed_high_ambition']==1]
low_amb = cross_list[cross_list['failed_high_ambition']==0]

failed_high_10 = failed_10[failed_10['ISIN'].isin(high_amb['isin'])]
failed_low_10 = failed_10[failed_10['ISIN'].isin(low_amb['isin'])]

# Cumulative returns
failed_high_10['CAR_logPct'] = failed_high_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
failed_low_10['CAR_logPct'] = failed_low_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
failed_high_10_summary = failed_high_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_high_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

failed_low_10_summary = failed_low_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_low_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

# High ambition t-stat - Cross-sectional test
failed_high_10 = pd.merge(failed_high_10,failed_high_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_high_10['CAR-CAAR'] = failed_high_10['CAR_logPct'] - failed_high_10['CAAR_logPct']
failed_high_10['CAR-CAAR_sq'] = failed_high_10['CAR-CAAR']**2

failed_high_10_summary['sum_CAR-CAAR_sq'] = (failed_high_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_high_10_summary['N'] = (failed_high_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_high_10_summary['VAR_CAAR'] = failed_high_10_summary['sum_CAR-CAAR_sq'] / (failed_high_10_summary['N']-1)
failed_high_10_summary['SD_CAAR'] = np.sqrt(failed_high_10_summary['VAR_CAAR'])

failed_high_10_summary['T_CSecT'] = (np.sqrt(failed_high_10_summary['N']))*failed_high_10_summary['CAAR_logPct']/failed_high_10_summary['SD_CAAR']

# Get AAR
failed_high_10_summary['AAR_logPct'] = failed_high_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_high_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_high_10 = pd.merge(failed_high_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_high_10['MAR-AAR'] = failed_high_10['adjRet_MarketModel_logPct'] - failed_high_10['AAR_logPct']
failed_high_10['MAR-AAR_sq'] = failed_high_10['MAR-AAR']**2

failed_high_10_summary['sum_MAR-AAR_sq'] = (failed_high_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_high_10_summary['N'] = (failed_high_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_high_10_summary['VAR_AAR'] = failed_high_10_summary['sum_MAR-AAR_sq'] / (failed_high_10_summary['N']-1)
failed_high_10_summary['SD_AAR'] = np.sqrt(failed_high_10_summary['VAR_AAR'])

failed_high_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_high_10_summary['N']))*failed_high_10_summary['AAR_logPct']/failed_high_10_summary['SD_AAR']

# Low ambition t-stat - Cross-sectional test
failed_low_10 = pd.merge(failed_low_10,failed_low_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_low_10['CAR-CAAR'] = failed_low_10['CAR_logPct'] - failed_low_10['CAAR_logPct']
failed_low_10['CAR-CAAR_sq'] = failed_low_10['CAR-CAAR']**2

failed_low_10_summary['sum_CAR-CAAR_sq'] = (failed_low_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_low_10_summary['N'] = (failed_low_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_low_10_summary['VAR_CAAR'] = failed_low_10_summary['sum_CAR-CAAR_sq'] / (failed_low_10_summary['N']-1)
failed_low_10_summary['SD_CAAR'] = np.sqrt(failed_low_10_summary['VAR_CAAR'])

failed_low_10_summary['T_CSecT'] = (np.sqrt(failed_low_10_summary['N']))*failed_low_10_summary['CAAR_logPct']/failed_low_10_summary['SD_CAAR']

# Get AAR
failed_low_10_summary['AAR_logPct'] = failed_low_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_low_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_low_10 = pd.merge(failed_low_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_low_10['MAR-AAR'] = failed_low_10['adjRet_MarketModel_logPct'] - failed_low_10['AAR_logPct']
failed_low_10['MAR-AAR_sq'] = failed_low_10['MAR-AAR']**2

failed_low_10_summary['sum_MAR-AAR_sq'] = (failed_low_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_low_10_summary['N'] = (failed_low_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_low_10_summary['VAR_AAR'] = failed_low_10_summary['sum_MAR-AAR_sq'] / (failed_low_10_summary['N']-1)
failed_low_10_summary['SD_AAR'] = np.sqrt(failed_low_10_summary['VAR_AAR'])

failed_low_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_low_10_summary['N']))*failed_low_10_summary['AAR_logPct']/failed_low_10_summary['SD_AAR']




''' Volume Test - 20 day around CSR Report Announcements '''
# T stats for abnormal volume are calculated based on the distribution of the abnormal volume during the estimation window (Campbell and Wasley, 1996).

window_20 = ds_dsf_csrReport_sub[(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_csrReport_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 20 days of data
remove_list = window_20.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
window_20 = window_20[~window_20['ISIN'].isin(remove_list.ISIN)]

failed_20 = window_20.copy() # There are only failed firms in this sample


# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_csrReport[(ds_dsf_csrReport['BDaysRelativeToEvent']>=-140)&(ds_dsf_csrReport['BDaysRelativeToEvent']<=-40)]
failed_estWin_win20 = vol_estWindow20.copy() # There are only failed firms in this sample

# Normal volume
norVol_failed20 = failed_estWin_win20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_failed20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

# Merge it to window return data
failed_20 = pd.merge(failed_20,norVol_failed20,on=['ISIN'],how='left')

failed_20['AbnVolume'] = failed_20['Volume'] - failed_20['normal_Volume']
failed_20['AbnVol_pct'] = failed_20['Volume_pct'] - failed_20['normal_Volume_pct']
failed_20['AbnVol_pctlog'] = failed_20['Volume_pctlog'] - failed_20['normal_Volume_pctlog']

# Merge normal volume to estimation window volume data to get sd
failed_estWin_win20 = pd.merge(failed_estWin_win20,norVol_failed20,on=['ISIN'],how='left')
failed_estWin_win20['AbnVolume'] = failed_estWin_win20['Volume'] - failed_estWin_win20['normal_Volume']
failed_estWin_win20['AbnVol_pct'] = failed_estWin_win20['Volume_pct'] - failed_estWin_win20['normal_Volume_pct']
failed_estWin_win20['AbnVol_pctlog'] = failed_estWin_win20['Volume_pctlog'] - failed_estWin_win20['normal_Volume_pctlog']

# Event plot
failed_20_summary = failed_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
failed_20_summary['N'] = (failed_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])

# t-stat
temp_failed20 = failed_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_failed20['AbnVolume_mbar'] = temp_failed20['AbnVolume'] - temp_failed20['AbnVolume'].mean()
temp_failed20['AbnVol_pct_mbar'] = temp_failed20['AbnVol_pct'] - temp_failed20['AbnVol_pct'].mean()
temp_failed20['AbnVol_pctlog_mbar'] = temp_failed20['AbnVol_pctlog'] - temp_failed20['AbnVol_pctlog'].mean()

temp_failed20['AbnVolume_mbar_sq'] = temp_failed20['AbnVolume_mbar']**2
temp_failed20['AbnVol_pct_mbar_sq'] = temp_failed20['AbnVol_pct_mbar']**2
temp_failed20['AbnVol_pctlog_mbar_sq'] = temp_failed20['AbnVol_pctlog_mbar']**2

failed_20_summary['Var_AbnVolume'] = temp_failed20['AbnVolume_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pct'] = temp_failed20['AbnVol_pct_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pctlog'] = temp_failed20['AbnVol_pctlog_mbar_sq'].mean()

failed_20_summary['sd_AbnVolume'] = np.sqrt(failed_20_summary['Var_AbnVolume'])
failed_20_summary['sd_AbnVol_pct'] = np.sqrt(failed_20_summary['Var_AbnVol_pct'])
failed_20_summary['sd_AbnVol_pctlog'] = np.sqrt(failed_20_summary['Var_AbnVol_pctlog'])

failed_20_summary['t_AbnVolume'] = failed_20_summary['AbnVolume']/failed_20_summary['sd_AbnVolume']
failed_20_summary['t_AbnVol_pct'] = failed_20_summary['AbnVol_pct']/failed_20_summary['sd_AbnVol_pct']
failed_20_summary['t_AbnVol_pctlog'] = failed_20_summary['AbnVol_pctlog']/failed_20_summary['sd_AbnVol_pctlog']






'''
############################################################################################################
######   Around CDP Response Release   ###################
############################################################################################################
'''
##############################################################################
''' #### Use this code to remove outliers with other extreme events '''
### Three companies with extreme events:  Amkor Tech, Lenovo, Pearson
remove_list1 = ['US0316521006', 'HK0992009065', 'GB0006776081']
ds_dsf_CDPrelease_sub = ds_dsf_CDPrelease_sub[~ds_dsf_CDPrelease_sub['ISIN'].isin(remove_list1)]

### Additional outliers
### ABB, Baker Hughes, Medtronic, Shawcor, Celestica, Loreal, Thule (achieved exp), 
### Amorepacific, Intl Cons Airl Group (failed unexpected)
remove_list2 = ['CH0012221716','US05722G1004','IE00BTN1Y115','CA8204391079','CA15101Q1081','FR0000120321','SE0006422390','US9497461015',
                'KR7090430000','ES0177542018']
ds_dsf_CDPrelease_sub = ds_dsf_CDPrelease_sub[~ds_dsf_CDPrelease_sub['ISIN'].isin(remove_list2)]
##############################################################################



''' Returns Around CDP Report Release (5 days window; -1 to +3) '''
window_5 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=3)]

# Drop firms without all 5 days of data
remove_list = window_5.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<5] # This is the count of days
window_5 = window_5[~window_5['ISIN'].isin(remove_list.ISIN)]

achieved_5 = window_5[window_5['type']=='Achieved']
failed_5 = window_5[(window_5['type']=='Failed')]

dis_highReduction_5 = window_5[(window_5['type']=='Disappeared High Reduction')]
dis_lowReduction_5 = window_5[(window_5['type']=='Disappeared Low Reduction')]


# Cumulative returns
achieved_5['CAR_logPct'] = achieved_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
failed_5['CAR_logPct'] = failed_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
dis_highReduction_5['CAR_logPct'] = dis_highReduction_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
dis_lowReduction_5['CAR_logPct'] = dis_lowReduction_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
achieved_5_summary = achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_5_summary = failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
dis_highReduction_5_summary = dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
dis_lowReduction_5_summary = dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()

achieved_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

dis_highReduction_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
dis_lowReduction_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']


# t-stat - Cross-sectional test
achieved_5 = pd.merge(achieved_5,achieved_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
achieved_5['CAR-CAAR'] = achieved_5['CAR_logPct'] - achieved_5['CAAR_logPct']
achieved_5['CAR-CAAR_sq'] = achieved_5['CAR-CAAR']**2

achieved_5_summary['sum_CAR-CAAR_sq'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
achieved_5_summary['N'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
achieved_5_summary['VAR_CAAR'] = achieved_5_summary['sum_CAR-CAAR_sq'] / (achieved_5_summary['N']-1)
achieved_5_summary['SD_CAAR'] = np.sqrt(achieved_5_summary['VAR_CAAR'])

achieved_5_summary['T_CSecT'] = (np.sqrt(achieved_5_summary['N']))*achieved_5_summary['CAAR_logPct']/achieved_5_summary['SD_CAAR']

# Get AAR
achieved_5_summary['AAR_logPct'] = achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = achieved_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
achieved_5 = pd.merge(achieved_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
achieved_5['MAR-AAR'] = achieved_5['adjRet_MarketModel_logPct'] - achieved_5['AAR_logPct']
achieved_5['MAR-AAR_sq'] = achieved_5['MAR-AAR']**2

achieved_5_summary['sum_MAR-AAR_sq'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
achieved_5_summary['N'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
achieved_5_summary['VAR_AAR'] = achieved_5_summary['sum_MAR-AAR_sq'] / (achieved_5_summary['N']-1)
achieved_5_summary['SD_AAR'] = np.sqrt(achieved_5_summary['VAR_AAR'])

achieved_5_summary['T_MAR_CSecT'] = (np.sqrt(achieved_5_summary['N']))*achieved_5_summary['AAR_logPct']/achieved_5_summary['SD_AAR']



# Failed  t-stat - Cross-sectional test
failed_5 = pd.merge(failed_5,failed_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_5['CAR-CAAR'] = failed_5['CAR_logPct'] - failed_5['CAAR_logPct']
failed_5['CAR-CAAR_sq'] = failed_5['CAR-CAAR']**2

failed_5_summary['sum_CAR-CAAR_sq'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_5_summary['N'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_5_summary['VAR_CAAR'] = failed_5_summary['sum_CAR-CAAR_sq'] / (failed_5_summary['N']-1)
failed_5_summary['SD_CAAR'] = np.sqrt(failed_5_summary['VAR_CAAR'])

failed_5_summary['T_CSecT'] = (np.sqrt(failed_5_summary['N']))*failed_5_summary['CAAR_logPct']/failed_5_summary['SD_CAAR']

# Get AAR
failed_5_summary['AAR_logPct'] = failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_5 = pd.merge(failed_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_5['MAR-AAR'] = failed_5['adjRet_MarketModel_logPct'] - failed_5['AAR_logPct']
failed_5['MAR-AAR_sq'] = failed_5['MAR-AAR']**2

failed_5_summary['sum_MAR-AAR_sq'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_5_summary['N'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_5_summary['VAR_AAR'] = failed_5_summary['sum_MAR-AAR_sq'] / (failed_5_summary['N']-1)
failed_5_summary['SD_AAR'] = np.sqrt(failed_5_summary['VAR_AAR'])

failed_5_summary['T_MAR_CSecT'] = (np.sqrt(failed_5_summary['N']))*failed_5_summary['AAR_logPct']/failed_5_summary['SD_AAR']


# Disappeared Type3  t-stat - Cross-sectional test
dis_highReduction_5 = pd.merge(dis_highReduction_5,dis_highReduction_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
dis_highReduction_5['CAR-CAAR'] = dis_highReduction_5['CAR_logPct'] - dis_highReduction_5['CAAR_logPct']
dis_highReduction_5['CAR-CAAR_sq'] = dis_highReduction_5['CAR-CAAR']**2

dis_highReduction_5_summary['sum_CAR-CAAR_sq'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
dis_highReduction_5_summary['N'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
dis_highReduction_5_summary['VAR_CAAR'] = dis_highReduction_5_summary['sum_CAR-CAAR_sq'] / (dis_highReduction_5_summary['N']-1)
dis_highReduction_5_summary['SD_CAAR'] = np.sqrt(dis_highReduction_5_summary['VAR_CAAR'])

dis_highReduction_5_summary['T_CSecT'] = (np.sqrt(dis_highReduction_5_summary['N']))*dis_highReduction_5_summary['CAAR_logPct']/dis_highReduction_5_summary['SD_CAAR']

# Get AAR
dis_highReduction_5_summary['AAR_logPct'] = dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = dis_highReduction_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
dis_highReduction_5 = pd.merge(dis_highReduction_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
dis_highReduction_5['MAR-AAR'] = dis_highReduction_5['adjRet_MarketModel_logPct'] - dis_highReduction_5['AAR_logPct']
dis_highReduction_5['MAR-AAR_sq'] = dis_highReduction_5['MAR-AAR']**2

dis_highReduction_5_summary['sum_MAR-AAR_sq'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
dis_highReduction_5_summary['N'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
dis_highReduction_5_summary['VAR_AAR'] = dis_highReduction_5_summary['sum_MAR-AAR_sq'] / (dis_highReduction_5_summary['N']-1)
dis_highReduction_5_summary['SD_AAR'] = np.sqrt(dis_highReduction_5_summary['VAR_AAR'])

dis_highReduction_5_summary['T_MAR_CSecT'] = (np.sqrt(dis_highReduction_5_summary['N']))*dis_highReduction_5_summary['AAR_logPct']/dis_highReduction_5_summary['SD_AAR']

# Disappeared Type4 t-stat - Cross-sectional test
dis_lowReduction_5 = pd.merge(dis_lowReduction_5,dis_lowReduction_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
dis_lowReduction_5['CAR-CAAR'] = dis_lowReduction_5['CAR_logPct'] - dis_lowReduction_5['CAAR_logPct']
dis_lowReduction_5['CAR-CAAR_sq'] = dis_lowReduction_5['CAR-CAAR']**2

dis_lowReduction_5_summary['sum_CAR-CAAR_sq'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
dis_lowReduction_5_summary['N'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
dis_lowReduction_5_summary['VAR_CAAR'] = dis_lowReduction_5_summary['sum_CAR-CAAR_sq'] / (dis_lowReduction_5_summary['N']-1)
dis_lowReduction_5_summary['SD_CAAR'] = np.sqrt(dis_lowReduction_5_summary['VAR_CAAR'])

dis_lowReduction_5_summary['T_CSecT'] = (np.sqrt(dis_lowReduction_5_summary['N']))*dis_lowReduction_5_summary['CAAR_logPct']/dis_lowReduction_5_summary['SD_CAAR']

# Get AAR
dis_lowReduction_5_summary['AAR_logPct'] = dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = dis_lowReduction_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
dis_lowReduction_5 = pd.merge(dis_lowReduction_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
dis_lowReduction_5['MAR-AAR'] = dis_lowReduction_5['adjRet_MarketModel_logPct'] - dis_lowReduction_5['AAR_logPct']
dis_lowReduction_5['MAR-AAR_sq'] = dis_lowReduction_5['MAR-AAR']**2

dis_lowReduction_5_summary['sum_MAR-AAR_sq'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
dis_lowReduction_5_summary['N'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
dis_lowReduction_5_summary['VAR_AAR'] = dis_lowReduction_5_summary['sum_MAR-AAR_sq'] / (dis_lowReduction_5_summary['N']-1)
dis_lowReduction_5_summary['SD_AAR'] = np.sqrt(dis_lowReduction_5_summary['VAR_AAR'])

dis_lowReduction_5_summary['T_MAR_CSecT'] = (np.sqrt(dis_lowReduction_5_summary['N']))*dis_lowReduction_5_summary['AAR_logPct']/dis_lowReduction_5_summary['SD_AAR']



''' Returns Around CDP Report Release (10 days window; -1 to +10) '''
window_10 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

achieved_10 = window_10[window_10['type']=='Achieved']
failed_10 = window_10[(window_10['type']=='Failed')]

dis_highReduction_10 = window_10[(window_10['type']=='Disappeared High Reduction')]
dis_lowReduction_10 = window_10[(window_10['type']=='Disappeared Low Reduction')]


# Cumulative returns
achieved_10['CAR_logPct'] = achieved_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
failed_10['CAR_logPct'] = failed_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

dis_highReduction_10['CAR_logPct'] = dis_highReduction_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
dis_lowReduction_10['CAR_logPct'] = dis_lowReduction_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
achieved_10_summary = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_10_summary = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
dis_highReduction_10_summary = dis_highReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
dis_lowReduction_10_summary = dis_lowReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()


achieved_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
dis_highReduction_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
dis_lowReduction_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']


# t-stat - Cross-sectional test
achieved_10 = pd.merge(achieved_10,achieved_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
achieved_10['CAR-CAAR'] = achieved_10['CAR_logPct'] - achieved_10['CAAR_logPct']
achieved_10['CAR-CAAR_sq'] = achieved_10['CAR-CAAR']**2

achieved_10_summary['sum_CAR-CAAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
achieved_10_summary['VAR_CAAR'] = achieved_10_summary['sum_CAR-CAAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_CAAR'] = np.sqrt(achieved_10_summary['VAR_CAAR'])

achieved_10_summary['T_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['CAAR_logPct']/achieved_10_summary['SD_CAAR']

# Get AAR
achieved_10_summary['AAR_logPct'] = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = achieved_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
achieved_10 = pd.merge(achieved_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
achieved_10['MAR-AAR'] = achieved_10['adjRet_MarketModel_logPct'] - achieved_10['AAR_logPct']
achieved_10['MAR-AAR_sq'] = achieved_10['MAR-AAR']**2

achieved_10_summary['sum_MAR-AAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
achieved_10_summary['VAR_AAR'] = achieved_10_summary['sum_MAR-AAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_AAR'] = np.sqrt(achieved_10_summary['VAR_AAR'])

achieved_10_summary['T_MAR_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['AAR_logPct']/achieved_10_summary['SD_AAR']



# Failed  t-stat - Cross-sectional test
failed_10 = pd.merge(failed_10,failed_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_10['CAR-CAAR'] = failed_10['CAR_logPct'] - failed_10['CAAR_logPct']
failed_10['CAR-CAAR_sq'] = failed_10['CAR-CAAR']**2

failed_10_summary['sum_CAR-CAAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_10_summary['VAR_CAAR'] = failed_10_summary['sum_CAR-CAAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_CAAR'] = np.sqrt(failed_10_summary['VAR_CAAR'])

failed_10_summary['T_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['CAAR_logPct']/failed_10_summary['SD_CAAR']

# Get AAR
failed_10_summary['AAR_logPct'] = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_10 = pd.merge(failed_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_10['MAR-AAR'] = failed_10['adjRet_MarketModel_logPct'] - failed_10['AAR_logPct']
failed_10['MAR-AAR_sq'] = failed_10['MAR-AAR']**2

failed_10_summary['sum_MAR-AAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_10_summary['VAR_AAR'] = failed_10_summary['sum_MAR-AAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_AAR'] = np.sqrt(failed_10_summary['VAR_AAR'])

failed_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['AAR_logPct']/failed_10_summary['SD_AAR']


# Disappeared Type3  t-stat - Cross-sectional test
dis_highReduction_10 = pd.merge(dis_highReduction_10,dis_highReduction_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
dis_highReduction_10['CAR-CAAR'] = dis_highReduction_10['CAR_logPct'] - dis_highReduction_10['CAAR_logPct']
dis_highReduction_10['CAR-CAAR_sq'] = dis_highReduction_10['CAR-CAAR']**2

dis_highReduction_10_summary['sum_CAR-CAAR_sq'] = (dis_highReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
dis_highReduction_10_summary['N'] = (dis_highReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
dis_highReduction_10_summary['VAR_CAAR'] = dis_highReduction_10_summary['sum_CAR-CAAR_sq'] / (dis_highReduction_10_summary['N']-1)
dis_highReduction_10_summary['SD_CAAR'] = np.sqrt(dis_highReduction_10_summary['VAR_CAAR'])

dis_highReduction_10_summary['T_CSecT'] = (np.sqrt(dis_highReduction_10_summary['N']))*dis_highReduction_10_summary['CAAR_logPct']/dis_highReduction_10_summary['SD_CAAR']

# Get AAR
dis_highReduction_10_summary['AAR_logPct'] = dis_highReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = dis_highReduction_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
dis_highReduction_10 = pd.merge(dis_highReduction_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
dis_highReduction_10['MAR-AAR'] = dis_highReduction_10['adjRet_MarketModel_logPct'] - dis_highReduction_10['AAR_logPct']
dis_highReduction_10['MAR-AAR_sq'] = dis_highReduction_10['MAR-AAR']**2

dis_highReduction_10_summary['sum_MAR-AAR_sq'] = (dis_highReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
dis_highReduction_10_summary['N'] = (dis_highReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
dis_highReduction_10_summary['VAR_AAR'] = dis_highReduction_10_summary['sum_MAR-AAR_sq'] / (dis_highReduction_10_summary['N']-1)
dis_highReduction_10_summary['SD_AAR'] = np.sqrt(dis_highReduction_10_summary['VAR_AAR'])

dis_highReduction_10_summary['T_MAR_CSecT'] = (np.sqrt(dis_highReduction_10_summary['N']))*dis_highReduction_10_summary['AAR_logPct']/dis_highReduction_10_summary['SD_AAR']

# Disappeared Type4 t-stat - Cross-sectional test
dis_lowReduction_10 = pd.merge(dis_lowReduction_10,dis_lowReduction_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
dis_lowReduction_10['CAR-CAAR'] = dis_lowReduction_10['CAR_logPct'] - dis_lowReduction_10['CAAR_logPct']
dis_lowReduction_10['CAR-CAAR_sq'] = dis_lowReduction_10['CAR-CAAR']**2

dis_lowReduction_10_summary['sum_CAR-CAAR_sq'] = (dis_lowReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
dis_lowReduction_10_summary['N'] = (dis_lowReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
dis_lowReduction_10_summary['VAR_CAAR'] = dis_lowReduction_10_summary['sum_CAR-CAAR_sq'] / (dis_lowReduction_10_summary['N']-1)
dis_lowReduction_10_summary['SD_CAAR'] = np.sqrt(dis_lowReduction_10_summary['VAR_CAAR'])

dis_lowReduction_10_summary['T_CSecT'] = (np.sqrt(dis_lowReduction_10_summary['N']))*dis_lowReduction_10_summary['CAAR_logPct']/dis_lowReduction_10_summary['SD_CAAR']

# Get AAR
dis_lowReduction_10_summary['AAR_logPct'] = dis_lowReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = dis_lowReduction_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
dis_lowReduction_10 = pd.merge(dis_lowReduction_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
dis_lowReduction_10['MAR-AAR'] = dis_lowReduction_10['adjRet_MarketModel_logPct'] - dis_lowReduction_10['AAR_logPct']
dis_lowReduction_10['MAR-AAR_sq'] = dis_lowReduction_10['MAR-AAR']**2

dis_lowReduction_10_summary['sum_MAR-AAR_sq'] = (dis_lowReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
dis_lowReduction_10_summary['N'] = (dis_lowReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
dis_lowReduction_10_summary['VAR_AAR'] = dis_lowReduction_10_summary['sum_MAR-AAR_sq'] / (dis_lowReduction_10_summary['N']-1)
dis_lowReduction_10_summary['SD_AAR'] = np.sqrt(dis_lowReduction_10_summary['VAR_AAR'])

dis_lowReduction_10_summary['T_MAR_CSecT'] = (np.sqrt(dis_lowReduction_10_summary['N']))*dis_lowReduction_10_summary['AAR_logPct']/dis_lowReduction_10_summary['SD_AAR']





''' Returns Around CDP Report Release - MATERIAL FIRM ONLY (-1 to +3) '''

window_5 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=3)]

# Drop firms without all 5 days of data
remove_list = window_5.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<5] # This is the count of days
window_5 = window_5[~window_5['ISIN'].isin(remove_list.ISIN)]

### Keep only material firms
window_5 = window_5[window_5['emission_industry_high']==1]

achieved_5 = window_5[window_5['type']=='Achieved']
failed_5 = window_5[(window_5['type']=='Failed')]

dis_highReduction_5 = window_5[(window_5['type']=='Disappeared High Reduction')]
dis_lowReduction_5 = window_5[(window_5['type']=='Disappeared Low Reduction')]

# Cumulative returns
achieved_5['CAR_logPct'] = achieved_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
failed_5['CAR_logPct'] = failed_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
dis_highReduction_5['CAR_logPct'] = dis_highReduction_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
dis_lowReduction_5['CAR_logPct'] = dis_lowReduction_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
achieved_5_summary = achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_5_summary = failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
dis_highReduction_5_summary = dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
dis_lowReduction_5_summary = dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()

achieved_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

dis_highReduction_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
dis_lowReduction_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']


# t-stat - Cross-sectional test
achieved_5 = pd.merge(achieved_5,achieved_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
achieved_5['CAR-CAAR'] = achieved_5['CAR_logPct'] - achieved_5['CAAR_logPct']
achieved_5['CAR-CAAR_sq'] = achieved_5['CAR-CAAR']**2

achieved_5_summary['sum_CAR-CAAR_sq'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
achieved_5_summary['N'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
achieved_5_summary['VAR_CAAR'] = achieved_5_summary['sum_CAR-CAAR_sq'] / (achieved_5_summary['N']-1)
achieved_5_summary['SD_CAAR'] = np.sqrt(achieved_5_summary['VAR_CAAR'])

achieved_5_summary['T_CSecT'] = (np.sqrt(achieved_5_summary['N']))*achieved_5_summary['CAAR_logPct']/achieved_5_summary['SD_CAAR']

# Get AAR
achieved_5_summary['AAR_logPct'] = achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = achieved_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
achieved_5 = pd.merge(achieved_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
achieved_5['MAR-AAR'] = achieved_5['adjRet_MarketModel_logPct'] - achieved_5['AAR_logPct']
achieved_5['MAR-AAR_sq'] = achieved_5['MAR-AAR']**2

achieved_5_summary['sum_MAR-AAR_sq'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
achieved_5_summary['N'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
achieved_5_summary['VAR_AAR'] = achieved_5_summary['sum_MAR-AAR_sq'] / (achieved_5_summary['N']-1)
achieved_5_summary['SD_AAR'] = np.sqrt(achieved_5_summary['VAR_AAR'])

achieved_5_summary['T_MAR_CSecT'] = (np.sqrt(achieved_5_summary['N']))*achieved_5_summary['AAR_logPct']/achieved_5_summary['SD_AAR']



# Failed  t-stat - Cross-sectional test
failed_5 = pd.merge(failed_5,failed_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_5['CAR-CAAR'] = failed_5['CAR_logPct'] - failed_5['CAAR_logPct']
failed_5['CAR-CAAR_sq'] = failed_5['CAR-CAAR']**2

failed_5_summary['sum_CAR-CAAR_sq'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_5_summary['N'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_5_summary['VAR_CAAR'] = failed_5_summary['sum_CAR-CAAR_sq'] / (failed_5_summary['N']-1)
failed_5_summary['SD_CAAR'] = np.sqrt(failed_5_summary['VAR_CAAR'])

failed_5_summary['T_CSecT'] = (np.sqrt(failed_5_summary['N']))*failed_5_summary['CAAR_logPct']/failed_5_summary['SD_CAAR']

# Get AAR
failed_5_summary['AAR_logPct'] = failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_5 = pd.merge(failed_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_5['MAR-AAR'] = failed_5['adjRet_MarketModel_logPct'] - failed_5['AAR_logPct']
failed_5['MAR-AAR_sq'] = failed_5['MAR-AAR']**2

failed_5_summary['sum_MAR-AAR_sq'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_5_summary['N'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_5_summary['VAR_AAR'] = failed_5_summary['sum_MAR-AAR_sq'] / (failed_5_summary['N']-1)
failed_5_summary['SD_AAR'] = np.sqrt(failed_5_summary['VAR_AAR'])

failed_5_summary['T_MAR_CSecT'] = (np.sqrt(failed_5_summary['N']))*failed_5_summary['AAR_logPct']/failed_5_summary['SD_AAR']


# Disappeared Type3  t-stat - Cross-sectional test
dis_highReduction_5 = pd.merge(dis_highReduction_5,dis_highReduction_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
dis_highReduction_5['CAR-CAAR'] = dis_highReduction_5['CAR_logPct'] - dis_highReduction_5['CAAR_logPct']
dis_highReduction_5['CAR-CAAR_sq'] = dis_highReduction_5['CAR-CAAR']**2

dis_highReduction_5_summary['sum_CAR-CAAR_sq'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
dis_highReduction_5_summary['N'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
dis_highReduction_5_summary['VAR_CAAR'] = dis_highReduction_5_summary['sum_CAR-CAAR_sq'] / (dis_highReduction_5_summary['N']-1)
dis_highReduction_5_summary['SD_CAAR'] = np.sqrt(dis_highReduction_5_summary['VAR_CAAR'])

dis_highReduction_5_summary['T_CSecT'] = (np.sqrt(dis_highReduction_5_summary['N']))*dis_highReduction_5_summary['CAAR_logPct']/dis_highReduction_5_summary['SD_CAAR']

# Get AAR
dis_highReduction_5_summary['AAR_logPct'] = dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = dis_highReduction_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
dis_highReduction_5 = pd.merge(dis_highReduction_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
dis_highReduction_5['MAR-AAR'] = dis_highReduction_5['adjRet_MarketModel_logPct'] - dis_highReduction_5['AAR_logPct']
dis_highReduction_5['MAR-AAR_sq'] = dis_highReduction_5['MAR-AAR']**2

dis_highReduction_5_summary['sum_MAR-AAR_sq'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
dis_highReduction_5_summary['N'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
dis_highReduction_5_summary['VAR_AAR'] = dis_highReduction_5_summary['sum_MAR-AAR_sq'] / (dis_highReduction_5_summary['N']-1)
dis_highReduction_5_summary['SD_AAR'] = np.sqrt(dis_highReduction_5_summary['VAR_AAR'])

dis_highReduction_5_summary['T_MAR_CSecT'] = (np.sqrt(dis_highReduction_5_summary['N']))*dis_highReduction_5_summary['AAR_logPct']/dis_highReduction_5_summary['SD_AAR']

# Disappeared Type4 t-stat - Cross-sectional test
dis_lowReduction_5 = pd.merge(dis_lowReduction_5,dis_lowReduction_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
dis_lowReduction_5['CAR-CAAR'] = dis_lowReduction_5['CAR_logPct'] - dis_lowReduction_5['CAAR_logPct']
dis_lowReduction_5['CAR-CAAR_sq'] = dis_lowReduction_5['CAR-CAAR']**2

dis_lowReduction_5_summary['sum_CAR-CAAR_sq'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
dis_lowReduction_5_summary['N'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
dis_lowReduction_5_summary['VAR_CAAR'] = dis_lowReduction_5_summary['sum_CAR-CAAR_sq'] / (dis_lowReduction_5_summary['N']-1)
dis_lowReduction_5_summary['SD_CAAR'] = np.sqrt(dis_lowReduction_5_summary['VAR_CAAR'])

dis_lowReduction_5_summary['T_CSecT'] = (np.sqrt(dis_lowReduction_5_summary['N']))*dis_lowReduction_5_summary['CAAR_logPct']/dis_lowReduction_5_summary['SD_CAAR']

# Get AAR
dis_lowReduction_5_summary['AAR_logPct'] = dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = dis_lowReduction_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
dis_lowReduction_5 = pd.merge(dis_lowReduction_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
dis_lowReduction_5['MAR-AAR'] = dis_lowReduction_5['adjRet_MarketModel_logPct'] - dis_lowReduction_5['AAR_logPct']
dis_lowReduction_5['MAR-AAR_sq'] = dis_lowReduction_5['MAR-AAR']**2

dis_lowReduction_5_summary['sum_MAR-AAR_sq'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
dis_lowReduction_5_summary['N'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
dis_lowReduction_5_summary['VAR_AAR'] = dis_lowReduction_5_summary['sum_MAR-AAR_sq'] / (dis_lowReduction_5_summary['N']-1)
dis_lowReduction_5_summary['SD_AAR'] = np.sqrt(dis_lowReduction_5_summary['VAR_AAR'])

dis_lowReduction_5_summary['T_MAR_CSecT'] = (np.sqrt(dis_lowReduction_5_summary['N']))*dis_lowReduction_5_summary['AAR_logPct']/dis_lowReduction_5_summary['SD_AAR']




''' Returns Around CDP Report Release - DROPPING COVID FIRMS  (-1 to +3) '''
window_5 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=3)]

# Drop firms without all 5 days of data
remove_list = window_5.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<5] # This is the count of days
window_5 = window_5[~window_5['ISIN'].isin(remove_list.ISIN)]

### Drop covid affected firms
window_5 = window_5[window_5['type_covid_industry']==0]

achieved_5 = window_5[window_5['type']=='Achieved']
failed_5 = window_5[(window_5['type']=='Failed')]

dis_highReduction_5 = window_5[(window_5['type']=='Disappeared High Reduction')]
dis_lowReduction_5 = window_5[(window_5['type']=='Disappeared Low Reduction')]

# Cumulative returns
achieved_5['CAR_logPct'] = achieved_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
failed_5['CAR_logPct'] = failed_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
dis_highReduction_5['CAR_logPct'] = dis_highReduction_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
dis_lowReduction_5['CAR_logPct'] = dis_lowReduction_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
achieved_5_summary = achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_5_summary = failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
dis_highReduction_5_summary = dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
dis_lowReduction_5_summary = dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()

achieved_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']

dis_highReduction_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
dis_lowReduction_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']


# t-stat - Cross-sectional test
achieved_5 = pd.merge(achieved_5,achieved_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
achieved_5['CAR-CAAR'] = achieved_5['CAR_logPct'] - achieved_5['CAAR_logPct']
achieved_5['CAR-CAAR_sq'] = achieved_5['CAR-CAAR']**2

achieved_5_summary['sum_CAR-CAAR_sq'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
achieved_5_summary['N'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
achieved_5_summary['VAR_CAAR'] = achieved_5_summary['sum_CAR-CAAR_sq'] / (achieved_5_summary['N']-1)
achieved_5_summary['SD_CAAR'] = np.sqrt(achieved_5_summary['VAR_CAAR'])

achieved_5_summary['T_CSecT'] = (np.sqrt(achieved_5_summary['N']))*achieved_5_summary['CAAR_logPct']/achieved_5_summary['SD_CAAR']

# Get AAR
achieved_5_summary['AAR_logPct'] = achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = achieved_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
achieved_5 = pd.merge(achieved_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
achieved_5['MAR-AAR'] = achieved_5['adjRet_MarketModel_logPct'] - achieved_5['AAR_logPct']
achieved_5['MAR-AAR_sq'] = achieved_5['MAR-AAR']**2

achieved_5_summary['sum_MAR-AAR_sq'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
achieved_5_summary['N'] = (achieved_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
achieved_5_summary['VAR_AAR'] = achieved_5_summary['sum_MAR-AAR_sq'] / (achieved_5_summary['N']-1)
achieved_5_summary['SD_AAR'] = np.sqrt(achieved_5_summary['VAR_AAR'])

achieved_5_summary['T_MAR_CSecT'] = (np.sqrt(achieved_5_summary['N']))*achieved_5_summary['AAR_logPct']/achieved_5_summary['SD_AAR']



# Failed  t-stat - Cross-sectional test
failed_5 = pd.merge(failed_5,failed_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_5['CAR-CAAR'] = failed_5['CAR_logPct'] - failed_5['CAAR_logPct']
failed_5['CAR-CAAR_sq'] = failed_5['CAR-CAAR']**2

failed_5_summary['sum_CAR-CAAR_sq'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_5_summary['N'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_5_summary['VAR_CAAR'] = failed_5_summary['sum_CAR-CAAR_sq'] / (failed_5_summary['N']-1)
failed_5_summary['SD_CAAR'] = np.sqrt(failed_5_summary['VAR_CAAR'])

failed_5_summary['T_CSecT'] = (np.sqrt(failed_5_summary['N']))*failed_5_summary['CAAR_logPct']/failed_5_summary['SD_CAAR']

# Get AAR
failed_5_summary['AAR_logPct'] = failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_5 = pd.merge(failed_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_5['MAR-AAR'] = failed_5['adjRet_MarketModel_logPct'] - failed_5['AAR_logPct']
failed_5['MAR-AAR_sq'] = failed_5['MAR-AAR']**2

failed_5_summary['sum_MAR-AAR_sq'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_5_summary['N'] = (failed_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_5_summary['VAR_AAR'] = failed_5_summary['sum_MAR-AAR_sq'] / (failed_5_summary['N']-1)
failed_5_summary['SD_AAR'] = np.sqrt(failed_5_summary['VAR_AAR'])

failed_5_summary['T_MAR_CSecT'] = (np.sqrt(failed_5_summary['N']))*failed_5_summary['AAR_logPct']/failed_5_summary['SD_AAR']


# Disappeared Type3  t-stat - Cross-sectional test
dis_highReduction_5 = pd.merge(dis_highReduction_5,dis_highReduction_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
dis_highReduction_5['CAR-CAAR'] = dis_highReduction_5['CAR_logPct'] - dis_highReduction_5['CAAR_logPct']
dis_highReduction_5['CAR-CAAR_sq'] = dis_highReduction_5['CAR-CAAR']**2

dis_highReduction_5_summary['sum_CAR-CAAR_sq'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
dis_highReduction_5_summary['N'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
dis_highReduction_5_summary['VAR_CAAR'] = dis_highReduction_5_summary['sum_CAR-CAAR_sq'] / (dis_highReduction_5_summary['N']-1)
dis_highReduction_5_summary['SD_CAAR'] = np.sqrt(dis_highReduction_5_summary['VAR_CAAR'])

dis_highReduction_5_summary['T_CSecT'] = (np.sqrt(dis_highReduction_5_summary['N']))*dis_highReduction_5_summary['CAAR_logPct']/dis_highReduction_5_summary['SD_CAAR']

# Get AAR
dis_highReduction_5_summary['AAR_logPct'] = dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = dis_highReduction_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
dis_highReduction_5 = pd.merge(dis_highReduction_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
dis_highReduction_5['MAR-AAR'] = dis_highReduction_5['adjRet_MarketModel_logPct'] - dis_highReduction_5['AAR_logPct']
dis_highReduction_5['MAR-AAR_sq'] = dis_highReduction_5['MAR-AAR']**2

dis_highReduction_5_summary['sum_MAR-AAR_sq'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
dis_highReduction_5_summary['N'] = (dis_highReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
dis_highReduction_5_summary['VAR_AAR'] = dis_highReduction_5_summary['sum_MAR-AAR_sq'] / (dis_highReduction_5_summary['N']-1)
dis_highReduction_5_summary['SD_AAR'] = np.sqrt(dis_highReduction_5_summary['VAR_AAR'])

dis_highReduction_5_summary['T_MAR_CSecT'] = (np.sqrt(dis_highReduction_5_summary['N']))*dis_highReduction_5_summary['AAR_logPct']/dis_highReduction_5_summary['SD_AAR']

# Disappeared Type4 t-stat - Cross-sectional test
dis_lowReduction_5 = pd.merge(dis_lowReduction_5,dis_lowReduction_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
dis_lowReduction_5['CAR-CAAR'] = dis_lowReduction_5['CAR_logPct'] - dis_lowReduction_5['CAAR_logPct']
dis_lowReduction_5['CAR-CAAR_sq'] = dis_lowReduction_5['CAR-CAAR']**2

dis_lowReduction_5_summary['sum_CAR-CAAR_sq'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
dis_lowReduction_5_summary['N'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
dis_lowReduction_5_summary['VAR_CAAR'] = dis_lowReduction_5_summary['sum_CAR-CAAR_sq'] / (dis_lowReduction_5_summary['N']-1)
dis_lowReduction_5_summary['SD_CAAR'] = np.sqrt(dis_lowReduction_5_summary['VAR_CAAR'])

dis_lowReduction_5_summary['T_CSecT'] = (np.sqrt(dis_lowReduction_5_summary['N']))*dis_lowReduction_5_summary['CAAR_logPct']/dis_lowReduction_5_summary['SD_CAAR']

# Get AAR
dis_lowReduction_5_summary['AAR_logPct'] = dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = dis_lowReduction_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
dis_lowReduction_5 = pd.merge(dis_lowReduction_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
dis_lowReduction_5['MAR-AAR'] = dis_lowReduction_5['adjRet_MarketModel_logPct'] - dis_lowReduction_5['AAR_logPct']
dis_lowReduction_5['MAR-AAR_sq'] = dis_lowReduction_5['MAR-AAR']**2

dis_lowReduction_5_summary['sum_MAR-AAR_sq'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
dis_lowReduction_5_summary['N'] = (dis_lowReduction_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
dis_lowReduction_5_summary['VAR_AAR'] = dis_lowReduction_5_summary['sum_MAR-AAR_sq'] / (dis_lowReduction_5_summary['N']-1)
dis_lowReduction_5_summary['SD_AAR'] = np.sqrt(dis_lowReduction_5_summary['VAR_AAR'])

dis_lowReduction_5_summary['T_MAR_CSecT'] = (np.sqrt(dis_lowReduction_5_summary['N']))*dis_lowReduction_5_summary['AAR_logPct']/dis_lowReduction_5_summary['SD_AAR']





''' Returns Around CDP Report Release - Ambitious vs. Unambitious Failed  (-1 to +3) '''
window_5 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=3)]

# Drop firms without all 5 days of data
remove_list = window_5.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<5] # This is the count of days
window_5 = window_5[~window_5['ISIN'].isin(remove_list.ISIN)]

## High/Low target ambition (only applies to failed companies)
failed_amb_5 = window_5[(window_5['failed_high_ambition']==1)]
failed_unamb_5 = window_5[(window_5['failed_low_ambition']==1)]

# Cumulative returns
failed_amb_5['CAR_logPct'] = failed_amb_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
failed_unamb_5['CAR_logPct'] = failed_unamb_5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# Get CAAR
failed_amb_5_summary = failed_amb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_unamb_5_summary = failed_unamb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()

failed_amb_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_unamb_5_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']


# Failed Ambitious  t-stat - Cross-sectional test
failed_amb_5 = pd.merge(failed_amb_5,failed_amb_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_amb_5['CAR-CAAR'] = failed_amb_5['CAR_logPct'] - failed_amb_5['CAAR_logPct']
failed_amb_5['CAR-CAAR_sq'] = failed_amb_5['CAR-CAAR']**2

failed_amb_5_summary['sum_CAR-CAAR_sq'] = (failed_amb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_amb_5_summary['N'] = (failed_amb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_amb_5_summary['VAR_CAAR'] = failed_amb_5_summary['sum_CAR-CAAR_sq'] / (failed_amb_5_summary['N']-1)
failed_amb_5_summary['SD_CAAR'] = np.sqrt(failed_amb_5_summary['VAR_CAAR'])

failed_amb_5_summary['T_CSecT'] = (np.sqrt(failed_amb_5_summary['N']))*failed_amb_5_summary['CAAR_logPct']/failed_amb_5_summary['SD_CAAR']

# Get AAR
failed_amb_5_summary['AAR_logPct'] = failed_amb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_amb_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_amb_5 = pd.merge(failed_amb_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_amb_5['MAR-AAR'] = failed_amb_5['adjRet_MarketModel_logPct'] - failed_amb_5['AAR_logPct']
failed_amb_5['MAR-AAR_sq'] = failed_amb_5['MAR-AAR']**2

failed_amb_5_summary['sum_MAR-AAR_sq'] = (failed_amb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_amb_5_summary['N'] = (failed_amb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_amb_5_summary['VAR_AAR'] = failed_amb_5_summary['sum_MAR-AAR_sq'] / (failed_amb_5_summary['N']-1)
failed_amb_5_summary['SD_AAR'] = np.sqrt(failed_amb_5_summary['VAR_AAR'])

failed_amb_5_summary['T_MAR_CSecT'] = (np.sqrt(failed_amb_5_summary['N']))*failed_amb_5_summary['AAR_logPct']/failed_amb_5_summary['SD_AAR']


# Failed Unambitious  t-stat - Cross-sectional test
failed_unamb_5 = pd.merge(failed_unamb_5,failed_unamb_5_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_unamb_5['CAR-CAAR'] = failed_unamb_5['CAR_logPct'] - failed_unamb_5['CAAR_logPct']
failed_unamb_5['CAR-CAAR_sq'] = failed_unamb_5['CAR-CAAR']**2

failed_unamb_5_summary['sum_CAR-CAAR_sq'] = (failed_unamb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_unamb_5_summary['N'] = (failed_unamb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_unamb_5_summary['VAR_CAAR'] = failed_unamb_5_summary['sum_CAR-CAAR_sq'] / (failed_unamb_5_summary['N']-1)
failed_unamb_5_summary['SD_CAAR'] = np.sqrt(failed_unamb_5_summary['VAR_CAAR'])

failed_unamb_5_summary['T_CSecT'] = (np.sqrt(failed_unamb_5_summary['N']))*failed_unamb_5_summary['CAAR_logPct']/failed_unamb_5_summary['SD_CAAR']

# Get AAR
failed_unamb_5_summary['AAR_logPct'] = failed_unamb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_unamb_5_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_unamb_5 = pd.merge(failed_unamb_5,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_unamb_5['MAR-AAR'] = failed_unamb_5['adjRet_MarketModel_logPct'] - failed_unamb_5['AAR_logPct']
failed_unamb_5['MAR-AAR_sq'] = failed_unamb_5['MAR-AAR']**2

failed_unamb_5_summary['sum_MAR-AAR_sq'] = (failed_unamb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_unamb_5_summary['N'] = (failed_unamb_5.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_unamb_5_summary['VAR_AAR'] = failed_unamb_5_summary['sum_MAR-AAR_sq'] / (failed_unamb_5_summary['N']-1)
failed_unamb_5_summary['SD_AAR'] = np.sqrt(failed_unamb_5_summary['VAR_AAR'])

failed_unamb_5_summary['T_MAR_CSecT'] = (np.sqrt(failed_unamb_5_summary['N']))*failed_unamb_5_summary['AAR_logPct']/failed_unamb_5_summary['SD_AAR']






''' Volume Test - 10 day around CDP report release '''

window_10 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-5)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=5)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<11] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

achieved_10 = window_10[window_10['type']=='Achieved']
failed_10 = window_10[(window_10['type']=='Failed')]
dis_highReduction_10 = window_10[(window_10['type']=='Disappeared High Reduction')]
dis_lowReduction_10 = window_10[(window_10['type']=='Disappeared Low Reduction')]


# Abnormal volume 
# Estimation window = {-135,-35}; 100 days long gap 30 days
vol_estWindow10 = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['BDaysRelativeToEvent']>=-135)&(ds_dsf_CDPrelease['BDaysRelativeToEvent']<=-35)]
achieved_estWin_win10 = vol_estWindow10[vol_estWindow10['type']=='Achieved']
failed_estWin_win10 = vol_estWindow10[(vol_estWindow10['type']=='Failed')]

dis_highReduction_estWin10 = vol_estWindow10[(vol_estWindow10['type']=='Disappeared High Reduction')]
dis_lowReduction_estWin10 = vol_estWindow10[(vol_estWindow10['type']=='Disappeared Low Reduction')]


# Normal volume
norVol_achieved10 = achieved_estWin_win10.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_achieved10.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_failed10 = failed_estWin_win10.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_failed10.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

norVol_dis_highReduction10 = dis_highReduction_estWin10.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_dis_highReduction10.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_dis_lowReduction10 = dis_lowReduction_estWin10.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_dis_lowReduction10.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

# Merge it to window return data
achieved_10 = pd.merge(achieved_10,norVol_achieved10,on=['ISIN'],how='left')
failed_10 = pd.merge(failed_10,norVol_failed10,on=['ISIN'],how='left')

dis_highReduction_10 = pd.merge(dis_highReduction_10,norVol_dis_highReduction10,on=['ISIN'],how='left')
dis_lowReduction_10 = pd.merge(dis_lowReduction_10,norVol_dis_lowReduction10,on=['ISIN'],how='left')


achieved_10['AbnVolume'] = achieved_10['Volume'] - achieved_10['normal_Volume']
achieved_10['AbnVol_pct'] = achieved_10['Volume_pct'] - achieved_10['normal_Volume_pct']
achieved_10['AbnVol_pctlog'] = achieved_10['Volume_pctlog'] - achieved_10['normal_Volume_pctlog']

failed_10['AbnVolume'] = failed_10['Volume'] - failed_10['normal_Volume']
failed_10['AbnVol_pct'] = failed_10['Volume_pct'] - failed_10['normal_Volume_pct']
failed_10['AbnVol_pctlog'] = failed_10['Volume_pctlog'] - failed_10['normal_Volume_pctlog']

dis_highReduction_10['AbnVolume'] = dis_highReduction_10['Volume'] - dis_highReduction_10['normal_Volume']
dis_highReduction_10['AbnVol_pct'] = dis_highReduction_10['Volume_pct'] - dis_highReduction_10['normal_Volume_pct']
dis_highReduction_10['AbnVol_pctlog'] = dis_highReduction_10['Volume_pctlog'] - dis_highReduction_10['normal_Volume_pctlog']
dis_lowReduction_10['AbnVolume'] = dis_lowReduction_10['Volume'] - dis_lowReduction_10['normal_Volume']
dis_lowReduction_10['AbnVol_pct'] = dis_lowReduction_10['Volume_pct'] - dis_lowReduction_10['normal_Volume_pct']
dis_lowReduction_10['AbnVol_pctlog'] = dis_lowReduction_10['Volume_pctlog'] - dis_lowReduction_10['normal_Volume_pctlog']

# Merge normal volume to estimation window volume data to get sd
achieved_estWin_win10 = pd.merge(achieved_estWin_win10,norVol_achieved10,on=['ISIN'],how='left')
achieved_estWin_win10['AbnVolume'] = achieved_estWin_win10['Volume'] - achieved_estWin_win10['normal_Volume']
achieved_estWin_win10['AbnVol_pct'] = achieved_estWin_win10['Volume_pct'] - achieved_estWin_win10['normal_Volume_pct']
achieved_estWin_win10['AbnVol_pctlog'] = achieved_estWin_win10['Volume_pctlog'] - achieved_estWin_win10['normal_Volume_pctlog']

failed_estWin_win10 = pd.merge(failed_estWin_win10,norVol_failed10,on=['ISIN'],how='left')
failed_estWin_win10['AbnVolume'] = failed_estWin_win10['Volume'] - failed_estWin_win10['normal_Volume']
failed_estWin_win10['AbnVol_pct'] = failed_estWin_win10['Volume_pct'] - failed_estWin_win10['normal_Volume_pct']
failed_estWin_win10['AbnVol_pctlog'] = failed_estWin_win10['Volume_pctlog'] - failed_estWin_win10['normal_Volume_pctlog']

dis_highReduction_estWin10 = pd.merge(dis_highReduction_estWin10,norVol_dis_highReduction10,on=['ISIN'],how='left')
dis_highReduction_estWin10['AbnVolume'] = dis_highReduction_estWin10['Volume'] - dis_highReduction_estWin10['normal_Volume']
dis_highReduction_estWin10['AbnVol_pct'] = dis_highReduction_estWin10['Volume_pct'] - dis_highReduction_estWin10['normal_Volume_pct']
dis_highReduction_estWin10['AbnVol_pctlog'] = dis_highReduction_estWin10['Volume_pctlog'] - dis_highReduction_estWin10['normal_Volume_pctlog']

dis_lowReduction_estWin10 = pd.merge(dis_lowReduction_estWin10,norVol_dis_lowReduction10,on=['ISIN'],how='left')
dis_lowReduction_estWin10['AbnVolume'] = dis_lowReduction_estWin10['Volume'] - dis_lowReduction_estWin10['normal_Volume']
dis_lowReduction_estWin10['AbnVol_pct'] = dis_lowReduction_estWin10['Volume_pct'] - dis_lowReduction_estWin10['normal_Volume_pct']
dis_lowReduction_estWin10['AbnVol_pctlog'] = dis_lowReduction_estWin10['Volume_pctlog'] - dis_lowReduction_estWin10['normal_Volume_pctlog']



# Event plot
achieved_10_summary = achieved_10.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
failed_10_summary = failed_10.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

dis_highReduction_10_summary = dis_highReduction_10.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
dis_lowReduction_10_summary = dis_lowReduction_10.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])

dis_highReduction_10_summary['N'] = (dis_highReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])
dis_lowReduction_10_summary['N'] = (dis_lowReduction_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])


# Achieved t-stat
temp_achieved10 = achieved_estWin_win10.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_achieved10['AbnVolume_mbar'] = temp_achieved10['AbnVolume'] - temp_achieved10['AbnVolume'].mean()
temp_achieved10['AbnVol_pct_mbar'] = temp_achieved10['AbnVol_pct'] - temp_achieved10['AbnVol_pct'].mean()
temp_achieved10['AbnVol_pctlog_mbar'] = temp_achieved10['AbnVol_pctlog'] - temp_achieved10['AbnVol_pctlog'].mean()

temp_achieved10['AbnVolume_mbar_sq'] = temp_achieved10['AbnVolume_mbar']**2
temp_achieved10['AbnVol_pct_mbar_sq'] = temp_achieved10['AbnVol_pct_mbar']**2
temp_achieved10['AbnVol_pctlog_mbar_sq'] = temp_achieved10['AbnVol_pctlog_mbar']**2

achieved_10_summary['Var_AbnVolume'] = temp_achieved10['AbnVolume_mbar_sq'].mean()
achieved_10_summary['Var_AbnVol_pct'] = temp_achieved10['AbnVol_pct_mbar_sq'].mean()
achieved_10_summary['Var_AbnVol_pctlog'] = temp_achieved10['AbnVol_pctlog_mbar_sq'].mean()

achieved_10_summary['sd_AbnVolume'] = np.sqrt(achieved_10_summary['Var_AbnVolume'])
achieved_10_summary['sd_AbnVol_pct'] = np.sqrt(achieved_10_summary['Var_AbnVol_pct'])
achieved_10_summary['sd_AbnVol_pctlog'] = np.sqrt(achieved_10_summary['Var_AbnVol_pctlog'])

achieved_10_summary['t_AbnVolume'] = achieved_10_summary['AbnVolume']/achieved_10_summary['sd_AbnVolume']
achieved_10_summary['t_AbnVol_pct'] = achieved_10_summary['AbnVol_pct']/achieved_10_summary['sd_AbnVol_pct']
achieved_10_summary['t_AbnVol_pctlog'] = achieved_10_summary['AbnVol_pctlog']/achieved_10_summary['sd_AbnVol_pctlog']

# Failed t-stat
temp_failed10 = failed_estWin_win10.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_failed10['AbnVolume_mbar'] = temp_failed10['AbnVolume'] - temp_failed10['AbnVolume'].mean()
temp_failed10['AbnVol_pct_mbar'] = temp_failed10['AbnVol_pct'] - temp_failed10['AbnVol_pct'].mean()
temp_failed10['AbnVol_pctlog_mbar'] = temp_failed10['AbnVol_pctlog'] - temp_failed10['AbnVol_pctlog'].mean()

temp_failed10['AbnVolume_mbar_sq'] = temp_failed10['AbnVolume_mbar']**2
temp_failed10['AbnVol_pct_mbar_sq'] = temp_failed10['AbnVol_pct_mbar']**2
temp_failed10['AbnVol_pctlog_mbar_sq'] = temp_failed10['AbnVol_pctlog_mbar']**2

failed_10_summary['Var_AbnVolume'] = temp_failed10['AbnVolume_mbar_sq'].mean()
failed_10_summary['Var_AbnVol_pct'] = temp_failed10['AbnVol_pct_mbar_sq'].mean()
failed_10_summary['Var_AbnVol_pctlog'] = temp_failed10['AbnVol_pctlog_mbar_sq'].mean()

failed_10_summary['sd_AbnVolume'] = np.sqrt(failed_10_summary['Var_AbnVolume'])
failed_10_summary['sd_AbnVol_pct'] = np.sqrt(failed_10_summary['Var_AbnVol_pct'])
failed_10_summary['sd_AbnVol_pctlog'] = np.sqrt(failed_10_summary['Var_AbnVol_pctlog'])

failed_10_summary['t_AbnVolume'] = failed_10_summary['AbnVolume']/failed_10_summary['sd_AbnVolume']
failed_10_summary['t_AbnVol_pct'] = failed_10_summary['AbnVol_pct']/failed_10_summary['sd_AbnVol_pct']
failed_10_summary['t_AbnVol_pctlog'] = failed_10_summary['AbnVol_pctlog']/failed_10_summary['sd_AbnVol_pctlog']


# Disappeared Type3 t-test
temp_dis_highReduction10 = dis_highReduction_estWin10.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_dis_highReduction10['AbnVolume_mbar'] = temp_dis_highReduction10['AbnVolume'] - temp_dis_highReduction10['AbnVolume'].mean()
temp_dis_highReduction10['AbnVol_pct_mbar'] = temp_dis_highReduction10['AbnVol_pct'] - temp_dis_highReduction10['AbnVol_pct'].mean()
temp_dis_highReduction10['AbnVol_pctlog_mbar'] = temp_dis_highReduction10['AbnVol_pctlog'] - temp_dis_highReduction10['AbnVol_pctlog'].mean()

temp_dis_highReduction10['AbnVolume_mbar_sq'] = temp_dis_highReduction10['AbnVolume_mbar']**2
temp_dis_highReduction10['AbnVol_pct_mbar_sq'] = temp_dis_highReduction10['AbnVol_pct_mbar']**2
temp_dis_highReduction10['AbnVol_pctlog_mbar_sq'] = temp_dis_highReduction10['AbnVol_pctlog_mbar']**2

dis_highReduction_10_summary['Var_AbnVolume'] = temp_dis_highReduction10['AbnVolume_mbar_sq'].mean()
dis_highReduction_10_summary['Var_AbnVol_pct'] = temp_dis_highReduction10['AbnVol_pct_mbar_sq'].mean()
dis_highReduction_10_summary['Var_AbnVol_pctlog'] = temp_dis_highReduction10['AbnVol_pctlog_mbar_sq'].mean()

dis_highReduction_10_summary['sd_AbnVolume'] = np.sqrt(dis_highReduction_10_summary['Var_AbnVolume'])
dis_highReduction_10_summary['sd_AbnVol_pct'] = np.sqrt(dis_highReduction_10_summary['Var_AbnVol_pct'])
dis_highReduction_10_summary['sd_AbnVol_pctlog'] = np.sqrt(dis_highReduction_10_summary['Var_AbnVol_pctlog'])

dis_highReduction_10_summary['t_AbnVolume'] = dis_highReduction_10_summary['AbnVolume']/dis_highReduction_10_summary['sd_AbnVolume']
dis_highReduction_10_summary['t_AbnVol_pct'] = dis_highReduction_10_summary['AbnVol_pct']/dis_highReduction_10_summary['sd_AbnVol_pct']
dis_highReduction_10_summary['t_AbnVol_pctlog'] = dis_highReduction_10_summary['AbnVol_pctlog']/dis_highReduction_10_summary['sd_AbnVol_pctlog']

# Disappeared Type4 t-test
temp_dis_lowReduction10 = dis_lowReduction_estWin10.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_dis_lowReduction10['AbnVolume_mbar'] = temp_dis_lowReduction10['AbnVolume'] - temp_dis_lowReduction10['AbnVolume'].mean()
temp_dis_lowReduction10['AbnVol_pct_mbar'] = temp_dis_lowReduction10['AbnVol_pct'] - temp_dis_lowReduction10['AbnVol_pct'].mean()
temp_dis_lowReduction10['AbnVol_pctlog_mbar'] = temp_dis_lowReduction10['AbnVol_pctlog'] - temp_dis_lowReduction10['AbnVol_pctlog'].mean()

temp_dis_lowReduction10['AbnVolume_mbar_sq'] = temp_dis_lowReduction10['AbnVolume_mbar']**2
temp_dis_lowReduction10['AbnVol_pct_mbar_sq'] = temp_dis_lowReduction10['AbnVol_pct_mbar']**2
temp_dis_lowReduction10['AbnVol_pctlog_mbar_sq'] = temp_dis_lowReduction10['AbnVol_pctlog_mbar']**2

dis_lowReduction_10_summary['Var_AbnVolume'] = temp_dis_lowReduction10['AbnVolume_mbar_sq'].mean()
dis_lowReduction_10_summary['Var_AbnVol_pct'] = temp_dis_lowReduction10['AbnVol_pct_mbar_sq'].mean()
dis_lowReduction_10_summary['Var_AbnVol_pctlog'] = temp_dis_lowReduction10['AbnVol_pctlog_mbar_sq'].mean()

dis_lowReduction_10_summary['sd_AbnVolume'] = np.sqrt(dis_lowReduction_10_summary['Var_AbnVolume'])
dis_lowReduction_10_summary['sd_AbnVol_pct'] = np.sqrt(dis_lowReduction_10_summary['Var_AbnVol_pct'])
dis_lowReduction_10_summary['sd_AbnVol_pctlog'] = np.sqrt(dis_lowReduction_10_summary['Var_AbnVol_pctlog'])

dis_lowReduction_10_summary['t_AbnVolume'] = dis_lowReduction_10_summary['AbnVolume']/dis_lowReduction_10_summary['sd_AbnVolume']
dis_lowReduction_10_summary['t_AbnVol_pct'] = dis_lowReduction_10_summary['AbnVol_pct']/dis_lowReduction_10_summary['sd_AbnVol_pct']
dis_lowReduction_10_summary['t_AbnVol_pctlog'] = dis_lowReduction_10_summary['AbnVol_pctlog']/dis_lowReduction_10_summary['sd_AbnVol_pctlog']



''' Volume Test - 20 day around CDP report release - MATERIAL FIRMS ONLY '''
window_20 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_20.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
window_20 = window_20[~window_20['ISIN'].isin(remove_list.ISIN)]

### Keep only material firms
window_20 = window_20[window_20['emission_industry_high']==1]

achieved_20 = window_20[window_20['type']=='Achieved']
failed_20 = window_20[(window_20['type']=='Failed')]
dis_highReduction_20 = window_20[(window_20['type']=='Disappeared High Reduction')]
dis_lowReduction_20 = window_20[(window_20['type']=='Disappeared Low Reduction')]


# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['BDaysRelativeToEvent']>=-140)&(ds_dsf_CDPrelease['BDaysRelativeToEvent']<=-40)]
achieved_estWin_win20 = vol_estWindow20[vol_estWindow20['type']=='Achieved']
failed_estWin_win20 = vol_estWindow20[(vol_estWindow20['type']=='Failed')]

dis_highReduction_estWin20 = vol_estWindow20[(vol_estWindow20['type']=='Disappeared High Reduction')]
dis_lowReduction_estWin20 = vol_estWindow20[(vol_estWindow20['type']=='Disappeared Low Reduction')]


# Normal volume
norVol_achieved20 = achieved_estWin_win20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_achieved20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_failed20 = failed_estWin_win20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_failed20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

norVol_dis_highReduction20 = dis_highReduction_estWin20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_dis_highReduction20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_dis_lowReduction20 = dis_lowReduction_estWin20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_dis_lowReduction20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

# Merge it to window return data
achieved_20 = pd.merge(achieved_20,norVol_achieved20,on=['ISIN'],how='left')
failed_20 = pd.merge(failed_20,norVol_failed20,on=['ISIN'],how='left')

dis_highReduction_20 = pd.merge(dis_highReduction_20,norVol_dis_highReduction20,on=['ISIN'],how='left')
dis_lowReduction_20 = pd.merge(dis_lowReduction_20,norVol_dis_lowReduction20,on=['ISIN'],how='left')


achieved_20['AbnVolume'] = achieved_20['Volume'] - achieved_20['normal_Volume']
achieved_20['AbnVol_pct'] = achieved_20['Volume_pct'] - achieved_20['normal_Volume_pct']
achieved_20['AbnVol_pctlog'] = achieved_20['Volume_pctlog'] - achieved_20['normal_Volume_pctlog']

failed_20['AbnVolume'] = failed_20['Volume'] - failed_20['normal_Volume']
failed_20['AbnVol_pct'] = failed_20['Volume_pct'] - failed_20['normal_Volume_pct']
failed_20['AbnVol_pctlog'] = failed_20['Volume_pctlog'] - failed_20['normal_Volume_pctlog']

dis_highReduction_20['AbnVolume'] = dis_highReduction_20['Volume'] - dis_highReduction_20['normal_Volume']
dis_highReduction_20['AbnVol_pct'] = dis_highReduction_20['Volume_pct'] - dis_highReduction_20['normal_Volume_pct']
dis_highReduction_20['AbnVol_pctlog'] = dis_highReduction_20['Volume_pctlog'] - dis_highReduction_20['normal_Volume_pctlog']
dis_lowReduction_20['AbnVolume'] = dis_lowReduction_20['Volume'] - dis_lowReduction_20['normal_Volume']
dis_lowReduction_20['AbnVol_pct'] = dis_lowReduction_20['Volume_pct'] - dis_lowReduction_20['normal_Volume_pct']
dis_lowReduction_20['AbnVol_pctlog'] = dis_lowReduction_20['Volume_pctlog'] - dis_lowReduction_20['normal_Volume_pctlog']

# Merge normal volume to estimation window volume data to get sd
achieved_estWin_win20 = pd.merge(achieved_estWin_win20,norVol_achieved20,on=['ISIN'],how='left')
achieved_estWin_win20['AbnVolume'] = achieved_estWin_win20['Volume'] - achieved_estWin_win20['normal_Volume']
achieved_estWin_win20['AbnVol_pct'] = achieved_estWin_win20['Volume_pct'] - achieved_estWin_win20['normal_Volume_pct']
achieved_estWin_win20['AbnVol_pctlog'] = achieved_estWin_win20['Volume_pctlog'] - achieved_estWin_win20['normal_Volume_pctlog']

failed_estWin_win20 = pd.merge(failed_estWin_win20,norVol_failed20,on=['ISIN'],how='left')
failed_estWin_win20['AbnVolume'] = failed_estWin_win20['Volume'] - failed_estWin_win20['normal_Volume']
failed_estWin_win20['AbnVol_pct'] = failed_estWin_win20['Volume_pct'] - failed_estWin_win20['normal_Volume_pct']
failed_estWin_win20['AbnVol_pctlog'] = failed_estWin_win20['Volume_pctlog'] - failed_estWin_win20['normal_Volume_pctlog']

dis_highReduction_estWin20 = pd.merge(dis_highReduction_estWin20,norVol_dis_highReduction20,on=['ISIN'],how='left')
dis_highReduction_estWin20['AbnVolume'] = dis_highReduction_estWin20['Volume'] - dis_highReduction_estWin20['normal_Volume']
dis_highReduction_estWin20['AbnVol_pct'] = dis_highReduction_estWin20['Volume_pct'] - dis_highReduction_estWin20['normal_Volume_pct']
dis_highReduction_estWin20['AbnVol_pctlog'] = dis_highReduction_estWin20['Volume_pctlog'] - dis_highReduction_estWin20['normal_Volume_pctlog']

dis_lowReduction_estWin20 = pd.merge(dis_lowReduction_estWin20,norVol_dis_lowReduction20,on=['ISIN'],how='left')
dis_lowReduction_estWin20['AbnVolume'] = dis_lowReduction_estWin20['Volume'] - dis_lowReduction_estWin20['normal_Volume']
dis_lowReduction_estWin20['AbnVol_pct'] = dis_lowReduction_estWin20['Volume_pct'] - dis_lowReduction_estWin20['normal_Volume_pct']
dis_lowReduction_estWin20['AbnVol_pctlog'] = dis_lowReduction_estWin20['Volume_pctlog'] - dis_lowReduction_estWin20['normal_Volume_pctlog']



# Event plot
achieved_20_summary = achieved_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
failed_20_summary = failed_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

dis_highReduction_20_summary = dis_highReduction_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
dis_lowReduction_20_summary = dis_lowReduction_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

achieved_20_summary['N'] = (achieved_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])
failed_20_summary['N'] = (failed_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])

dis_highReduction_20_summary['N'] = (dis_highReduction_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])
dis_lowReduction_20_summary['N'] = (dis_lowReduction_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])


# Achieved t-stat
temp_achieved20 = achieved_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_achieved20['AbnVolume_mbar'] = temp_achieved20['AbnVolume'] - temp_achieved20['AbnVolume'].mean()
temp_achieved20['AbnVol_pct_mbar'] = temp_achieved20['AbnVol_pct'] - temp_achieved20['AbnVol_pct'].mean()
temp_achieved20['AbnVol_pctlog_mbar'] = temp_achieved20['AbnVol_pctlog'] - temp_achieved20['AbnVol_pctlog'].mean()

temp_achieved20['AbnVolume_mbar_sq'] = temp_achieved20['AbnVolume_mbar']**2
temp_achieved20['AbnVol_pct_mbar_sq'] = temp_achieved20['AbnVol_pct_mbar']**2
temp_achieved20['AbnVol_pctlog_mbar_sq'] = temp_achieved20['AbnVol_pctlog_mbar']**2

achieved_20_summary['Var_AbnVolume'] = temp_achieved20['AbnVolume_mbar_sq'].mean()
achieved_20_summary['Var_AbnVol_pct'] = temp_achieved20['AbnVol_pct_mbar_sq'].mean()
achieved_20_summary['Var_AbnVol_pctlog'] = temp_achieved20['AbnVol_pctlog_mbar_sq'].mean()

achieved_20_summary['sd_AbnVolume'] = np.sqrt(achieved_20_summary['Var_AbnVolume'])
achieved_20_summary['sd_AbnVol_pct'] = np.sqrt(achieved_20_summary['Var_AbnVol_pct'])
achieved_20_summary['sd_AbnVol_pctlog'] = np.sqrt(achieved_20_summary['Var_AbnVol_pctlog'])

achieved_20_summary['t_AbnVolume'] = achieved_20_summary['AbnVolume']/achieved_20_summary['sd_AbnVolume']
achieved_20_summary['t_AbnVol_pct'] = achieved_20_summary['AbnVol_pct']/achieved_20_summary['sd_AbnVol_pct']
achieved_20_summary['t_AbnVol_pctlog'] = achieved_20_summary['AbnVol_pctlog']/achieved_20_summary['sd_AbnVol_pctlog']

# Failed t-stat
temp_failed20 = failed_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_failed20['AbnVolume_mbar'] = temp_failed20['AbnVolume'] - temp_failed20['AbnVolume'].mean()
temp_failed20['AbnVol_pct_mbar'] = temp_failed20['AbnVol_pct'] - temp_failed20['AbnVol_pct'].mean()
temp_failed20['AbnVol_pctlog_mbar'] = temp_failed20['AbnVol_pctlog'] - temp_failed20['AbnVol_pctlog'].mean()

temp_failed20['AbnVolume_mbar_sq'] = temp_failed20['AbnVolume_mbar']**2
temp_failed20['AbnVol_pct_mbar_sq'] = temp_failed20['AbnVol_pct_mbar']**2
temp_failed20['AbnVol_pctlog_mbar_sq'] = temp_failed20['AbnVol_pctlog_mbar']**2

failed_20_summary['Var_AbnVolume'] = temp_failed20['AbnVolume_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pct'] = temp_failed20['AbnVol_pct_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pctlog'] = temp_failed20['AbnVol_pctlog_mbar_sq'].mean()

failed_20_summary['sd_AbnVolume'] = np.sqrt(failed_20_summary['Var_AbnVolume'])
failed_20_summary['sd_AbnVol_pct'] = np.sqrt(failed_20_summary['Var_AbnVol_pct'])
failed_20_summary['sd_AbnVol_pctlog'] = np.sqrt(failed_20_summary['Var_AbnVol_pctlog'])

failed_20_summary['t_AbnVolume'] = failed_20_summary['AbnVolume']/failed_20_summary['sd_AbnVolume']
failed_20_summary['t_AbnVol_pct'] = failed_20_summary['AbnVol_pct']/failed_20_summary['sd_AbnVol_pct']
failed_20_summary['t_AbnVol_pctlog'] = failed_20_summary['AbnVol_pctlog']/failed_20_summary['sd_AbnVol_pctlog']

# Disappeared Type3 t-test
temp_dis_highReduction20 = dis_highReduction_estWin20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_dis_highReduction20['AbnVolume_mbar'] = temp_dis_highReduction20['AbnVolume'] - temp_dis_highReduction20['AbnVolume'].mean()
temp_dis_highReduction20['AbnVol_pct_mbar'] = temp_dis_highReduction20['AbnVol_pct'] - temp_dis_highReduction20['AbnVol_pct'].mean()
temp_dis_highReduction20['AbnVol_pctlog_mbar'] = temp_dis_highReduction20['AbnVol_pctlog'] - temp_dis_highReduction20['AbnVol_pctlog'].mean()

temp_dis_highReduction20['AbnVolume_mbar_sq'] = temp_dis_highReduction20['AbnVolume_mbar']**2
temp_dis_highReduction20['AbnVol_pct_mbar_sq'] = temp_dis_highReduction20['AbnVol_pct_mbar']**2
temp_dis_highReduction20['AbnVol_pctlog_mbar_sq'] = temp_dis_highReduction20['AbnVol_pctlog_mbar']**2

dis_highReduction_20_summary['Var_AbnVolume'] = temp_dis_highReduction20['AbnVolume_mbar_sq'].mean()
dis_highReduction_20_summary['Var_AbnVol_pct'] = temp_dis_highReduction20['AbnVol_pct_mbar_sq'].mean()
dis_highReduction_20_summary['Var_AbnVol_pctlog'] = temp_dis_highReduction20['AbnVol_pctlog_mbar_sq'].mean()

dis_highReduction_20_summary['sd_AbnVolume'] = np.sqrt(dis_highReduction_20_summary['Var_AbnVolume'])
dis_highReduction_20_summary['sd_AbnVol_pct'] = np.sqrt(dis_highReduction_20_summary['Var_AbnVol_pct'])
dis_highReduction_20_summary['sd_AbnVol_pctlog'] = np.sqrt(dis_highReduction_20_summary['Var_AbnVol_pctlog'])

dis_highReduction_20_summary['t_AbnVolume'] = dis_highReduction_20_summary['AbnVolume']/dis_highReduction_20_summary['sd_AbnVolume']
dis_highReduction_20_summary['t_AbnVol_pct'] = dis_highReduction_20_summary['AbnVol_pct']/dis_highReduction_20_summary['sd_AbnVol_pct']
dis_highReduction_20_summary['t_AbnVol_pctlog'] = dis_highReduction_20_summary['AbnVol_pctlog']/dis_highReduction_20_summary['sd_AbnVol_pctlog']

# Disappeared Type4 t-test
temp_dis_lowReduction20 = dis_lowReduction_estWin20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_dis_lowReduction20['AbnVolume_mbar'] = temp_dis_lowReduction20['AbnVolume'] - temp_dis_lowReduction20['AbnVolume'].mean()
temp_dis_lowReduction20['AbnVol_pct_mbar'] = temp_dis_lowReduction20['AbnVol_pct'] - temp_dis_lowReduction20['AbnVol_pct'].mean()
temp_dis_lowReduction20['AbnVol_pctlog_mbar'] = temp_dis_lowReduction20['AbnVol_pctlog'] - temp_dis_lowReduction20['AbnVol_pctlog'].mean()

temp_dis_lowReduction20['AbnVolume_mbar_sq'] = temp_dis_lowReduction20['AbnVolume_mbar']**2
temp_dis_lowReduction20['AbnVol_pct_mbar_sq'] = temp_dis_lowReduction20['AbnVol_pct_mbar']**2
temp_dis_lowReduction20['AbnVol_pctlog_mbar_sq'] = temp_dis_lowReduction20['AbnVol_pctlog_mbar']**2

dis_lowReduction_20_summary['Var_AbnVolume'] = temp_dis_lowReduction20['AbnVolume_mbar_sq'].mean()
dis_lowReduction_20_summary['Var_AbnVol_pct'] = temp_dis_lowReduction20['AbnVol_pct_mbar_sq'].mean()
dis_lowReduction_20_summary['Var_AbnVol_pctlog'] = temp_dis_lowReduction20['AbnVol_pctlog_mbar_sq'].mean()

dis_lowReduction_20_summary['sd_AbnVolume'] = np.sqrt(dis_lowReduction_20_summary['Var_AbnVolume'])
dis_lowReduction_20_summary['sd_AbnVol_pct'] = np.sqrt(dis_lowReduction_20_summary['Var_AbnVol_pct'])
dis_lowReduction_20_summary['sd_AbnVol_pctlog'] = np.sqrt(dis_lowReduction_20_summary['Var_AbnVol_pctlog'])

dis_lowReduction_20_summary['t_AbnVolume'] = dis_lowReduction_20_summary['AbnVolume']/dis_lowReduction_20_summary['sd_AbnVolume']
dis_lowReduction_20_summary['t_AbnVol_pct'] = dis_lowReduction_20_summary['AbnVol_pct']/dis_lowReduction_20_summary['sd_AbnVol_pct']
dis_lowReduction_20_summary['t_AbnVol_pctlog'] = dis_lowReduction_20_summary['AbnVol_pctlog']/dis_lowReduction_20_summary['sd_AbnVol_pctlog']



''' Volume Test - 20 day around CDP report release - DROPPING COVID FIRMS '''
window_20 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 20 days of data
remove_list = window_20.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
window_20 = window_20[~window_20['ISIN'].isin(remove_list.ISIN)]

### Drop covid affected firms
window_20 = window_20[window_20['type_covid_industry']==0]

achieved_20 = window_20[window_20['type']=='Achieved']
failed_20 = window_20[(window_20['type']=='Failed')]
dis_highReduction_20 = window_20[(window_20['type']=='Disappeared High Reduction')]
dis_lowReduction_20 = window_20[(window_20['type']=='Disappeared Low Reduction')]


# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['BDaysRelativeToEvent']>=-140)&(ds_dsf_CDPrelease['BDaysRelativeToEvent']<=-40)]
achieved_estWin_win20 = vol_estWindow20[vol_estWindow20['type']=='Achieved']
failed_estWin_win20 = vol_estWindow20[(vol_estWindow20['type']=='Failed')]

dis_highReduction_estWin20 = vol_estWindow20[(vol_estWindow20['type']=='Disappeared High Reduction')]
dis_lowReduction_estWin20 = vol_estWindow20[(vol_estWindow20['type']=='Disappeared Low Reduction')]


# Normal volume
norVol_achieved20 = achieved_estWin_win20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_achieved20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_failed20 = failed_estWin_win20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_failed20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

norVol_dis_highReduction20 = dis_highReduction_estWin20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_dis_highReduction20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_dis_lowReduction20 = dis_lowReduction_estWin20.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_dis_lowReduction20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

# Merge it to window return data
achieved_20 = pd.merge(achieved_20,norVol_achieved20,on=['ISIN'],how='left')
failed_20 = pd.merge(failed_20,norVol_failed20,on=['ISIN'],how='left')

dis_highReduction_20 = pd.merge(dis_highReduction_20,norVol_dis_highReduction20,on=['ISIN'],how='left')
dis_lowReduction_20 = pd.merge(dis_lowReduction_20,norVol_dis_lowReduction20,on=['ISIN'],how='left')


achieved_20['AbnVolume'] = achieved_20['Volume'] - achieved_20['normal_Volume']
achieved_20['AbnVol_pct'] = achieved_20['Volume_pct'] - achieved_20['normal_Volume_pct']
achieved_20['AbnVol_pctlog'] = achieved_20['Volume_pctlog'] - achieved_20['normal_Volume_pctlog']

failed_20['AbnVolume'] = failed_20['Volume'] - failed_20['normal_Volume']
failed_20['AbnVol_pct'] = failed_20['Volume_pct'] - failed_20['normal_Volume_pct']
failed_20['AbnVol_pctlog'] = failed_20['Volume_pctlog'] - failed_20['normal_Volume_pctlog']

dis_highReduction_20['AbnVolume'] = dis_highReduction_20['Volume'] - dis_highReduction_20['normal_Volume']
dis_highReduction_20['AbnVol_pct'] = dis_highReduction_20['Volume_pct'] - dis_highReduction_20['normal_Volume_pct']
dis_highReduction_20['AbnVol_pctlog'] = dis_highReduction_20['Volume_pctlog'] - dis_highReduction_20['normal_Volume_pctlog']
dis_lowReduction_20['AbnVolume'] = dis_lowReduction_20['Volume'] - dis_lowReduction_20['normal_Volume']
dis_lowReduction_20['AbnVol_pct'] = dis_lowReduction_20['Volume_pct'] - dis_lowReduction_20['normal_Volume_pct']
dis_lowReduction_20['AbnVol_pctlog'] = dis_lowReduction_20['Volume_pctlog'] - dis_lowReduction_20['normal_Volume_pctlog']

# Merge normal volume to estimation window volume data to get sd
achieved_estWin_win20 = pd.merge(achieved_estWin_win20,norVol_achieved20,on=['ISIN'],how='left')
achieved_estWin_win20['AbnVolume'] = achieved_estWin_win20['Volume'] - achieved_estWin_win20['normal_Volume']
achieved_estWin_win20['AbnVol_pct'] = achieved_estWin_win20['Volume_pct'] - achieved_estWin_win20['normal_Volume_pct']
achieved_estWin_win20['AbnVol_pctlog'] = achieved_estWin_win20['Volume_pctlog'] - achieved_estWin_win20['normal_Volume_pctlog']

failed_estWin_win20 = pd.merge(failed_estWin_win20,norVol_failed20,on=['ISIN'],how='left')
failed_estWin_win20['AbnVolume'] = failed_estWin_win20['Volume'] - failed_estWin_win20['normal_Volume']
failed_estWin_win20['AbnVol_pct'] = failed_estWin_win20['Volume_pct'] - failed_estWin_win20['normal_Volume_pct']
failed_estWin_win20['AbnVol_pctlog'] = failed_estWin_win20['Volume_pctlog'] - failed_estWin_win20['normal_Volume_pctlog']

dis_highReduction_estWin20 = pd.merge(dis_highReduction_estWin20,norVol_dis_highReduction20,on=['ISIN'],how='left')
dis_highReduction_estWin20['AbnVolume'] = dis_highReduction_estWin20['Volume'] - dis_highReduction_estWin20['normal_Volume']
dis_highReduction_estWin20['AbnVol_pct'] = dis_highReduction_estWin20['Volume_pct'] - dis_highReduction_estWin20['normal_Volume_pct']
dis_highReduction_estWin20['AbnVol_pctlog'] = dis_highReduction_estWin20['Volume_pctlog'] - dis_highReduction_estWin20['normal_Volume_pctlog']

dis_lowReduction_estWin20 = pd.merge(dis_lowReduction_estWin20,norVol_dis_lowReduction20,on=['ISIN'],how='left')
dis_lowReduction_estWin20['AbnVolume'] = dis_lowReduction_estWin20['Volume'] - dis_lowReduction_estWin20['normal_Volume']
dis_lowReduction_estWin20['AbnVol_pct'] = dis_lowReduction_estWin20['Volume_pct'] - dis_lowReduction_estWin20['normal_Volume_pct']
dis_lowReduction_estWin20['AbnVol_pctlog'] = dis_lowReduction_estWin20['Volume_pctlog'] - dis_lowReduction_estWin20['normal_Volume_pctlog']



# Event plot
achieved_20_summary = achieved_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
failed_20_summary = failed_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

dis_highReduction_20_summary = dis_highReduction_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
dis_lowReduction_20_summary = dis_lowReduction_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

achieved_20_summary['N'] = (achieved_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])
failed_20_summary['N'] = (failed_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])

dis_highReduction_20_summary['N'] = (dis_highReduction_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])
dis_lowReduction_20_summary['N'] = (dis_lowReduction_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])


# Achieved t-stat
temp_achieved20 = achieved_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_achieved20['AbnVolume_mbar'] = temp_achieved20['AbnVolume'] - temp_achieved20['AbnVolume'].mean()
temp_achieved20['AbnVol_pct_mbar'] = temp_achieved20['AbnVol_pct'] - temp_achieved20['AbnVol_pct'].mean()
temp_achieved20['AbnVol_pctlog_mbar'] = temp_achieved20['AbnVol_pctlog'] - temp_achieved20['AbnVol_pctlog'].mean()

temp_achieved20['AbnVolume_mbar_sq'] = temp_achieved20['AbnVolume_mbar']**2
temp_achieved20['AbnVol_pct_mbar_sq'] = temp_achieved20['AbnVol_pct_mbar']**2
temp_achieved20['AbnVol_pctlog_mbar_sq'] = temp_achieved20['AbnVol_pctlog_mbar']**2

achieved_20_summary['Var_AbnVolume'] = temp_achieved20['AbnVolume_mbar_sq'].mean()
achieved_20_summary['Var_AbnVol_pct'] = temp_achieved20['AbnVol_pct_mbar_sq'].mean()
achieved_20_summary['Var_AbnVol_pctlog'] = temp_achieved20['AbnVol_pctlog_mbar_sq'].mean()

achieved_20_summary['sd_AbnVolume'] = np.sqrt(achieved_20_summary['Var_AbnVolume'])
achieved_20_summary['sd_AbnVol_pct'] = np.sqrt(achieved_20_summary['Var_AbnVol_pct'])
achieved_20_summary['sd_AbnVol_pctlog'] = np.sqrt(achieved_20_summary['Var_AbnVol_pctlog'])

achieved_20_summary['t_AbnVolume'] = achieved_20_summary['AbnVolume']/achieved_20_summary['sd_AbnVolume']
achieved_20_summary['t_AbnVol_pct'] = achieved_20_summary['AbnVol_pct']/achieved_20_summary['sd_AbnVol_pct']
achieved_20_summary['t_AbnVol_pctlog'] = achieved_20_summary['AbnVol_pctlog']/achieved_20_summary['sd_AbnVol_pctlog']

# Failed t-stat
temp_failed20 = failed_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_failed20['AbnVolume_mbar'] = temp_failed20['AbnVolume'] - temp_failed20['AbnVolume'].mean()
temp_failed20['AbnVol_pct_mbar'] = temp_failed20['AbnVol_pct'] - temp_failed20['AbnVol_pct'].mean()
temp_failed20['AbnVol_pctlog_mbar'] = temp_failed20['AbnVol_pctlog'] - temp_failed20['AbnVol_pctlog'].mean()

temp_failed20['AbnVolume_mbar_sq'] = temp_failed20['AbnVolume_mbar']**2
temp_failed20['AbnVol_pct_mbar_sq'] = temp_failed20['AbnVol_pct_mbar']**2
temp_failed20['AbnVol_pctlog_mbar_sq'] = temp_failed20['AbnVol_pctlog_mbar']**2

failed_20_summary['Var_AbnVolume'] = temp_failed20['AbnVolume_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pct'] = temp_failed20['AbnVol_pct_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pctlog'] = temp_failed20['AbnVol_pctlog_mbar_sq'].mean()

failed_20_summary['sd_AbnVolume'] = np.sqrt(failed_20_summary['Var_AbnVolume'])
failed_20_summary['sd_AbnVol_pct'] = np.sqrt(failed_20_summary['Var_AbnVol_pct'])
failed_20_summary['sd_AbnVol_pctlog'] = np.sqrt(failed_20_summary['Var_AbnVol_pctlog'])

failed_20_summary['t_AbnVolume'] = failed_20_summary['AbnVolume']/failed_20_summary['sd_AbnVolume']
failed_20_summary['t_AbnVol_pct'] = failed_20_summary['AbnVol_pct']/failed_20_summary['sd_AbnVol_pct']
failed_20_summary['t_AbnVol_pctlog'] = failed_20_summary['AbnVol_pctlog']/failed_20_summary['sd_AbnVol_pctlog']


# Disappeared Type3 t-test
temp_dis_highReduction20 = dis_highReduction_estWin20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_dis_highReduction20['AbnVolume_mbar'] = temp_dis_highReduction20['AbnVolume'] - temp_dis_highReduction20['AbnVolume'].mean()
temp_dis_highReduction20['AbnVol_pct_mbar'] = temp_dis_highReduction20['AbnVol_pct'] - temp_dis_highReduction20['AbnVol_pct'].mean()
temp_dis_highReduction20['AbnVol_pctlog_mbar'] = temp_dis_highReduction20['AbnVol_pctlog'] - temp_dis_highReduction20['AbnVol_pctlog'].mean()

temp_dis_highReduction20['AbnVolume_mbar_sq'] = temp_dis_highReduction20['AbnVolume_mbar']**2
temp_dis_highReduction20['AbnVol_pct_mbar_sq'] = temp_dis_highReduction20['AbnVol_pct_mbar']**2
temp_dis_highReduction20['AbnVol_pctlog_mbar_sq'] = temp_dis_highReduction20['AbnVol_pctlog_mbar']**2

dis_highReduction_20_summary['Var_AbnVolume'] = temp_dis_highReduction20['AbnVolume_mbar_sq'].mean()
dis_highReduction_20_summary['Var_AbnVol_pct'] = temp_dis_highReduction20['AbnVol_pct_mbar_sq'].mean()
dis_highReduction_20_summary['Var_AbnVol_pctlog'] = temp_dis_highReduction20['AbnVol_pctlog_mbar_sq'].mean()

dis_highReduction_20_summary['sd_AbnVolume'] = np.sqrt(dis_highReduction_20_summary['Var_AbnVolume'])
dis_highReduction_20_summary['sd_AbnVol_pct'] = np.sqrt(dis_highReduction_20_summary['Var_AbnVol_pct'])
dis_highReduction_20_summary['sd_AbnVol_pctlog'] = np.sqrt(dis_highReduction_20_summary['Var_AbnVol_pctlog'])

dis_highReduction_20_summary['t_AbnVolume'] = dis_highReduction_20_summary['AbnVolume']/dis_highReduction_20_summary['sd_AbnVolume']
dis_highReduction_20_summary['t_AbnVol_pct'] = dis_highReduction_20_summary['AbnVol_pct']/dis_highReduction_20_summary['sd_AbnVol_pct']
dis_highReduction_20_summary['t_AbnVol_pctlog'] = dis_highReduction_20_summary['AbnVol_pctlog']/dis_highReduction_20_summary['sd_AbnVol_pctlog']

# Disappeared Type4 t-test
temp_dis_lowReduction20 = dis_lowReduction_estWin20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_dis_lowReduction20['AbnVolume_mbar'] = temp_dis_lowReduction20['AbnVolume'] - temp_dis_lowReduction20['AbnVolume'].mean()
temp_dis_lowReduction20['AbnVol_pct_mbar'] = temp_dis_lowReduction20['AbnVol_pct'] - temp_dis_lowReduction20['AbnVol_pct'].mean()
temp_dis_lowReduction20['AbnVol_pctlog_mbar'] = temp_dis_lowReduction20['AbnVol_pctlog'] - temp_dis_lowReduction20['AbnVol_pctlog'].mean()

temp_dis_lowReduction20['AbnVolume_mbar_sq'] = temp_dis_lowReduction20['AbnVolume_mbar']**2
temp_dis_lowReduction20['AbnVol_pct_mbar_sq'] = temp_dis_lowReduction20['AbnVol_pct_mbar']**2
temp_dis_lowReduction20['AbnVol_pctlog_mbar_sq'] = temp_dis_lowReduction20['AbnVol_pctlog_mbar']**2

dis_lowReduction_20_summary['Var_AbnVolume'] = temp_dis_lowReduction20['AbnVolume_mbar_sq'].mean()
dis_lowReduction_20_summary['Var_AbnVol_pct'] = temp_dis_lowReduction20['AbnVol_pct_mbar_sq'].mean()
dis_lowReduction_20_summary['Var_AbnVol_pctlog'] = temp_dis_lowReduction20['AbnVol_pctlog_mbar_sq'].mean()

dis_lowReduction_20_summary['sd_AbnVolume'] = np.sqrt(dis_lowReduction_20_summary['Var_AbnVolume'])
dis_lowReduction_20_summary['sd_AbnVol_pct'] = np.sqrt(dis_lowReduction_20_summary['Var_AbnVol_pct'])
dis_lowReduction_20_summary['sd_AbnVol_pctlog'] = np.sqrt(dis_lowReduction_20_summary['Var_AbnVol_pctlog'])

dis_lowReduction_20_summary['t_AbnVolume'] = dis_lowReduction_20_summary['AbnVolume']/dis_lowReduction_20_summary['sd_AbnVolume']
dis_lowReduction_20_summary['t_AbnVol_pct'] = dis_lowReduction_20_summary['AbnVol_pct']/dis_lowReduction_20_summary['sd_AbnVol_pct']
dis_lowReduction_20_summary['t_AbnVol_pctlog'] = dis_lowReduction_20_summary['AbnVol_pctlog']/dis_lowReduction_20_summary['sd_AbnVol_pctlog']



''' Volume Test - 20 day around CDP report release - Ambitious vs. Unambitious Failed '''
window_20 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_20.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
window_20 = window_20[~window_20['ISIN'].isin(remove_list.ISIN)]

## High/Low target ambition (only applies to failed companies)
failed_amb_20 = window_20[window_20['failed_high_ambition']==1]
failed_unamb_20 = window_20[window_20['failed_low_ambition']==1]


# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['BDaysRelativeToEvent']>=-140)&(ds_dsf_CDPrelease['BDaysRelativeToEvent']<=-40)]
failed_amb_estWin = vol_estWindow20[(vol_estWindow20['failed_high_ambition']==1)]
failed_unamb_estWin = vol_estWindow20[(vol_estWindow20['failed_low_ambition']==1)]


# Normal volume
norVol_failed_amb = failed_amb_estWin.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_failed_amb.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_failed_unamb = failed_unamb_estWin.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_failed_unamb.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

# Merge it to window return data
failed_amb_20 = pd.merge(failed_amb_20,norVol_failed_amb,on=['ISIN'],how='left')
failed_unamb_20 = pd.merge(failed_unamb_20,norVol_failed_unamb,on=['ISIN'],how='left')


failed_amb_20['AbnVolume'] = failed_amb_20['Volume'] - failed_amb_20['normal_Volume']
failed_amb_20['AbnVol_pct'] = failed_amb_20['Volume_pct'] - failed_amb_20['normal_Volume_pct']
failed_amb_20['AbnVol_pctlog'] = failed_amb_20['Volume_pctlog'] - failed_amb_20['normal_Volume_pctlog']

failed_unamb_20['AbnVolume'] = failed_unamb_20['Volume'] - failed_unamb_20['normal_Volume']
failed_unamb_20['AbnVol_pct'] = failed_unamb_20['Volume_pct'] - failed_unamb_20['normal_Volume_pct']
failed_unamb_20['AbnVol_pctlog'] = failed_unamb_20['Volume_pctlog'] - failed_unamb_20['normal_Volume_pctlog']

# Merge normal volume to estimation window volume data to get sd
failed_amb_estWin = pd.merge(failed_amb_estWin,norVol_failed_amb,on=['ISIN'],how='left')
failed_amb_estWin['AbnVolume'] = failed_amb_estWin['Volume'] - failed_amb_estWin['normal_Volume']
failed_amb_estWin['AbnVol_pct'] = failed_amb_estWin['Volume_pct'] - failed_amb_estWin['normal_Volume_pct']
failed_amb_estWin['AbnVol_pctlog'] = failed_amb_estWin['Volume_pctlog'] - failed_amb_estWin['normal_Volume_pctlog']

failed_unamb_estWin = pd.merge(failed_unamb_estWin,norVol_failed_unamb,on=['ISIN'],how='left')
failed_unamb_estWin['AbnVolume'] = failed_unamb_estWin['Volume'] - failed_unamb_estWin['normal_Volume']
failed_unamb_estWin['AbnVol_pct'] = failed_unamb_estWin['Volume_pct'] - failed_unamb_estWin['normal_Volume_pct']
failed_unamb_estWin['AbnVol_pctlog'] = failed_unamb_estWin['Volume_pctlog'] - failed_unamb_estWin['normal_Volume_pctlog']


# Event plot
failed_amb_20_summary = failed_amb_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
failed_unamb_20_summary = failed_unamb_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

failed_amb_20_summary['N'] = (failed_amb_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])
failed_unamb_20_summary['N'] = (failed_unamb_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])


# failed_amb t-stat
temp_failed_amb20 = failed_amb_estWin.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_failed_amb20['AbnVolume_mbar'] = temp_failed_amb20['AbnVolume'] - temp_failed_amb20['AbnVolume'].mean()
temp_failed_amb20['AbnVol_pct_mbar'] = temp_failed_amb20['AbnVol_pct'] - temp_failed_amb20['AbnVol_pct'].mean()
temp_failed_amb20['AbnVol_pctlog_mbar'] = temp_failed_amb20['AbnVol_pctlog'] - temp_failed_amb20['AbnVol_pctlog'].mean()

temp_failed_amb20['AbnVolume_mbar_sq'] = temp_failed_amb20['AbnVolume_mbar']**2
temp_failed_amb20['AbnVol_pct_mbar_sq'] = temp_failed_amb20['AbnVol_pct_mbar']**2
temp_failed_amb20['AbnVol_pctlog_mbar_sq'] = temp_failed_amb20['AbnVol_pctlog_mbar']**2

failed_amb_20_summary['Var_AbnVolume'] = temp_failed_amb20['AbnVolume_mbar_sq'].mean()
failed_amb_20_summary['Var_AbnVol_pct'] = temp_failed_amb20['AbnVol_pct_mbar_sq'].mean()
failed_amb_20_summary['Var_AbnVol_pctlog'] = temp_failed_amb20['AbnVol_pctlog_mbar_sq'].mean()

failed_amb_20_summary['sd_AbnVolume'] = np.sqrt(failed_amb_20_summary['Var_AbnVolume'])
failed_amb_20_summary['sd_AbnVol_pct'] = np.sqrt(failed_amb_20_summary['Var_AbnVol_pct'])
failed_amb_20_summary['sd_AbnVol_pctlog'] = np.sqrt(failed_amb_20_summary['Var_AbnVol_pctlog'])

failed_amb_20_summary['t_AbnVolume'] = failed_amb_20_summary['AbnVolume']/failed_amb_20_summary['sd_AbnVolume']
failed_amb_20_summary['t_AbnVol_pct'] = failed_amb_20_summary['AbnVol_pct']/failed_amb_20_summary['sd_AbnVol_pct']
failed_amb_20_summary['t_AbnVol_pctlog'] = failed_amb_20_summary['AbnVol_pctlog']/failed_amb_20_summary['sd_AbnVol_pctlog']

# failed_unamb t-stat
temp_failed_unamb20 = failed_unamb_estWin.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_failed_unamb20['AbnVolume_mbar'] = temp_failed_unamb20['AbnVolume'] - temp_failed_unamb20['AbnVolume'].mean()
temp_failed_unamb20['AbnVol_pct_mbar'] = temp_failed_unamb20['AbnVol_pct'] - temp_failed_unamb20['AbnVol_pct'].mean()
temp_failed_unamb20['AbnVol_pctlog_mbar'] = temp_failed_unamb20['AbnVol_pctlog'] - temp_failed_unamb20['AbnVol_pctlog'].mean()

temp_failed_unamb20['AbnVolume_mbar_sq'] = temp_failed_unamb20['AbnVolume_mbar']**2
temp_failed_unamb20['AbnVol_pct_mbar_sq'] = temp_failed_unamb20['AbnVol_pct_mbar']**2
temp_failed_unamb20['AbnVol_pctlog_mbar_sq'] = temp_failed_unamb20['AbnVol_pctlog_mbar']**2

failed_unamb_20_summary['Var_AbnVolume'] = temp_failed_unamb20['AbnVolume_mbar_sq'].mean()
failed_unamb_20_summary['Var_AbnVol_pct'] = temp_failed_unamb20['AbnVol_pct_mbar_sq'].mean()
failed_unamb_20_summary['Var_AbnVol_pctlog'] = temp_failed_unamb20['AbnVol_pctlog_mbar_sq'].mean()

failed_unamb_20_summary['sd_AbnVolume'] = np.sqrt(failed_unamb_20_summary['Var_AbnVolume'])
failed_unamb_20_summary['sd_AbnVol_pct'] = np.sqrt(failed_unamb_20_summary['Var_AbnVol_pct'])
failed_unamb_20_summary['sd_AbnVol_pctlog'] = np.sqrt(failed_unamb_20_summary['Var_AbnVol_pctlog'])

failed_unamb_20_summary['t_AbnVolume'] = failed_unamb_20_summary['AbnVolume']/failed_unamb_20_summary['sd_AbnVolume']
failed_unamb_20_summary['t_AbnVol_pct'] = failed_unamb_20_summary['AbnVol_pct']/failed_unamb_20_summary['sd_AbnVol_pct']
failed_unamb_20_summary['t_AbnVol_pctlog'] = failed_unamb_20_summary['AbnVol_pctlog']/failed_unamb_20_summary['sd_AbnVol_pctlog']






'''
############################################################################################################
######  Market Response to 2019 Target Completion Info   ###################
############################################################################################################
'''

''' Market Response to 2019 Target Completion Info (12 days window; -1 t0 +10) '''
window_10 = ds_dsf_CDPrelease20_sub[(ds_dsf_CDPrelease20_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease20_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

lagbehind_10 = window_10[window_10['lag_behind_2020']==1]
ontrack_10 = window_10[window_10['lag_behind_2020']==0]

lagbehind_top10_10 = window_10[window_10['lag_top10_2020']==1]
lagbehind_top20_10 = window_10[window_10['lag_top20_2020']==1]
ontrack_top10_10 = window_10[window_10['ontrack_top10_2020']==1]
ontrack_top20_10 = window_10[window_10['ontrack_top20_2020']==1]


# Cumulative returns
lagbehind_10['CAR_logPct'] = lagbehind_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
ontrack_10['CAR_logPct'] = ontrack_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
lagbehind_top10_10['CAR_logPct'] = lagbehind_top10_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
lagbehind_top20_10['CAR_logPct'] = lagbehind_top20_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
ontrack_top10_10['CAR_logPct'] = ontrack_top10_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
ontrack_top20_10['CAR_logPct'] = ontrack_top20_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()


# Get CAAR
lagbehind_10_summary = lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
ontrack_10_summary = ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
lagbehind_top10_10_summary = lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
lagbehind_top20_10_summary = lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
ontrack_top10_10_summary = ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
ontrack_top20_10_summary = ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()


lagbehind_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
ontrack_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
lagbehind_top10_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
lagbehind_top20_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
ontrack_top10_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
ontrack_top20_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']


# Lag Behind t-stat - Cross-sectional test
lagbehind_10 = pd.merge(lagbehind_10,lagbehind_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
lagbehind_10['CAR-CAAR'] = lagbehind_10['CAR_logPct'] - lagbehind_10['CAAR_logPct']
lagbehind_10['CAR-CAAR_sq'] = lagbehind_10['CAR-CAAR']**2

lagbehind_10_summary['sum_CAR-CAAR_sq'] = (lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
lagbehind_10_summary['N'] = (lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
lagbehind_10_summary['VAR_CAAR'] = lagbehind_10_summary['sum_CAR-CAAR_sq'] / (lagbehind_10_summary['N']-1)
lagbehind_10_summary['SD_CAAR'] = np.sqrt(lagbehind_10_summary['VAR_CAAR'])

lagbehind_10_summary['T_CSecT'] = (np.sqrt(lagbehind_10_summary['N']))*lagbehind_10_summary['CAAR_logPct']/lagbehind_10_summary['SD_CAAR']

# Get AAR
lagbehind_10_summary['AAR_logPct'] = lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = lagbehind_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
lagbehind_10 = pd.merge(lagbehind_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
lagbehind_10['MAR-AAR'] = lagbehind_10['adjRet_MarketModel_logPct'] - lagbehind_10['AAR_logPct']
lagbehind_10['MAR-AAR_sq'] = lagbehind_10['MAR-AAR']**2

lagbehind_10_summary['sum_MAR-AAR_sq'] = (lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
lagbehind_10_summary['N'] = (lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
lagbehind_10_summary['VAR_AAR'] = lagbehind_10_summary['sum_MAR-AAR_sq'] / (lagbehind_10_summary['N']-1)
lagbehind_10_summary['SD_AAR'] = np.sqrt(lagbehind_10_summary['VAR_AAR'])

lagbehind_10_summary['T_MAR_CSecT'] = (np.sqrt(lagbehind_10_summary['N']))*lagbehind_10_summary['AAR_logPct']/lagbehind_10_summary['SD_AAR']



# On Track t-stat - Cross-sectional test
ontrack_10 = pd.merge(ontrack_10,ontrack_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
ontrack_10['CAR-CAAR'] = ontrack_10['CAR_logPct'] - ontrack_10['CAAR_logPct']
ontrack_10['CAR-CAAR_sq'] = ontrack_10['CAR-CAAR']**2

ontrack_10_summary['sum_CAR-CAAR_sq'] = (ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
ontrack_10_summary['N'] = (ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
ontrack_10_summary['VAR_CAAR'] = ontrack_10_summary['sum_CAR-CAAR_sq'] / (ontrack_10_summary['N']-1)
ontrack_10_summary['SD_CAAR'] = np.sqrt(ontrack_10_summary['VAR_CAAR'])

ontrack_10_summary['T_CSecT'] = (np.sqrt(ontrack_10_summary['N']))*ontrack_10_summary['CAAR_logPct']/ontrack_10_summary['SD_CAAR']

# Get AAR
ontrack_10_summary['AAR_logPct'] = ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = ontrack_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
ontrack_10 = pd.merge(ontrack_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
ontrack_10['MAR-AAR'] = ontrack_10['adjRet_MarketModel_logPct'] - ontrack_10['AAR_logPct']
ontrack_10['MAR-AAR_sq'] = ontrack_10['MAR-AAR']**2

ontrack_10_summary['sum_MAR-AAR_sq'] = (ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
ontrack_10_summary['N'] = (ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
ontrack_10_summary['VAR_AAR'] = ontrack_10_summary['sum_MAR-AAR_sq'] / (ontrack_10_summary['N']-1)
ontrack_10_summary['SD_AAR'] = np.sqrt(ontrack_10_summary['VAR_AAR'])

ontrack_10_summary['T_MAR_CSecT'] = (np.sqrt(ontrack_10_summary['N']))*ontrack_10_summary['AAR_logPct']/ontrack_10_summary['SD_AAR']


# lagbehind_top10  t-stat - Cross-sectional test
lagbehind_top10_10 = pd.merge(lagbehind_top10_10,lagbehind_top10_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
lagbehind_top10_10['CAR-CAAR'] = lagbehind_top10_10['CAR_logPct'] - lagbehind_top10_10['CAAR_logPct']
lagbehind_top10_10['CAR-CAAR_sq'] = lagbehind_top10_10['CAR-CAAR']**2

lagbehind_top10_10_summary['sum_CAR-CAAR_sq'] = (lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
lagbehind_top10_10_summary['N'] = (lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
lagbehind_top10_10_summary['VAR_CAAR'] = lagbehind_top10_10_summary['sum_CAR-CAAR_sq'] / (lagbehind_top10_10_summary['N']-1)
lagbehind_top10_10_summary['SD_CAAR'] = np.sqrt(lagbehind_top10_10_summary['VAR_CAAR'])

lagbehind_top10_10_summary['T_CSecT'] = (np.sqrt(lagbehind_top10_10_summary['N']))*lagbehind_top10_10_summary['CAAR_logPct']/lagbehind_top10_10_summary['SD_CAAR']

# Get AAR
lagbehind_top10_10_summary['AAR_logPct'] = lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = lagbehind_top10_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
lagbehind_top10_10 = pd.merge(lagbehind_top10_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
lagbehind_top10_10['MAR-AAR'] = lagbehind_top10_10['adjRet_MarketModel_logPct'] - lagbehind_top10_10['AAR_logPct']
lagbehind_top10_10['MAR-AAR_sq'] = lagbehind_top10_10['MAR-AAR']**2

lagbehind_top10_10_summary['sum_MAR-AAR_sq'] = (lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
lagbehind_top10_10_summary['N'] = (lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
lagbehind_top10_10_summary['VAR_AAR'] = lagbehind_top10_10_summary['sum_MAR-AAR_sq'] / (lagbehind_top10_10_summary['N']-1)
lagbehind_top10_10_summary['SD_AAR'] = np.sqrt(lagbehind_top10_10_summary['VAR_AAR'])

lagbehind_top10_10_summary['T_MAR_CSecT'] = (np.sqrt(lagbehind_top10_10_summary['N']))*lagbehind_top10_10_summary['AAR_logPct']/lagbehind_top10_10_summary['SD_AAR']


# lagbehind_top20  t-stat - Cross-sectional test
lagbehind_top20_10 = pd.merge(lagbehind_top20_10,lagbehind_top20_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
lagbehind_top20_10['CAR-CAAR'] = lagbehind_top20_10['CAR_logPct'] - lagbehind_top20_10['CAAR_logPct']
lagbehind_top20_10['CAR-CAAR_sq'] = lagbehind_top20_10['CAR-CAAR']**2

lagbehind_top20_10_summary['sum_CAR-CAAR_sq'] = (lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
lagbehind_top20_10_summary['N'] = (lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
lagbehind_top20_10_summary['VAR_CAAR'] = lagbehind_top20_10_summary['sum_CAR-CAAR_sq'] / (lagbehind_top20_10_summary['N']-1)
lagbehind_top20_10_summary['SD_CAAR'] = np.sqrt(lagbehind_top20_10_summary['VAR_CAAR'])

lagbehind_top20_10_summary['T_CSecT'] = (np.sqrt(lagbehind_top20_10_summary['N']))*lagbehind_top20_10_summary['CAAR_logPct']/lagbehind_top20_10_summary['SD_CAAR']

# Get AAR
lagbehind_top20_10_summary['AAR_logPct'] = lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = lagbehind_top20_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
lagbehind_top20_10 = pd.merge(lagbehind_top20_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
lagbehind_top20_10['MAR-AAR'] = lagbehind_top20_10['adjRet_MarketModel_logPct'] - lagbehind_top20_10['AAR_logPct']
lagbehind_top20_10['MAR-AAR_sq'] = lagbehind_top20_10['MAR-AAR']**2

lagbehind_top20_10_summary['sum_MAR-AAR_sq'] = (lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
lagbehind_top20_10_summary['N'] = (lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
lagbehind_top20_10_summary['VAR_AAR'] = lagbehind_top20_10_summary['sum_MAR-AAR_sq'] / (lagbehind_top20_10_summary['N']-1)
lagbehind_top20_10_summary['SD_AAR'] = np.sqrt(lagbehind_top20_10_summary['VAR_AAR'])

lagbehind_top20_10_summary['T_MAR_CSecT'] = (np.sqrt(lagbehind_top20_10_summary['N']))*lagbehind_top20_10_summary['AAR_logPct']/lagbehind_top20_10_summary['SD_AAR']


# ontrack_top10  t-stat - Cross-sectional test
ontrack_top10_10 = pd.merge(ontrack_top10_10,ontrack_top10_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
ontrack_top10_10['CAR-CAAR'] = ontrack_top10_10['CAR_logPct'] - ontrack_top10_10['CAAR_logPct']
ontrack_top10_10['CAR-CAAR_sq'] = ontrack_top10_10['CAR-CAAR']**2

ontrack_top10_10_summary['sum_CAR-CAAR_sq'] = (ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
ontrack_top10_10_summary['N'] = (ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
ontrack_top10_10_summary['VAR_CAAR'] = ontrack_top10_10_summary['sum_CAR-CAAR_sq'] / (ontrack_top10_10_summary['N']-1)
ontrack_top10_10_summary['SD_CAAR'] = np.sqrt(ontrack_top10_10_summary['VAR_CAAR'])

ontrack_top10_10_summary['T_CSecT'] = (np.sqrt(ontrack_top10_10_summary['N']))*ontrack_top10_10_summary['CAAR_logPct']/ontrack_top10_10_summary['SD_CAAR']

# Get AAR
ontrack_top10_10_summary['AAR_logPct'] = ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = ontrack_top10_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
ontrack_top10_10 = pd.merge(ontrack_top10_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
ontrack_top10_10['MAR-AAR'] = ontrack_top10_10['adjRet_MarketModel_logPct'] - ontrack_top10_10['AAR_logPct']
ontrack_top10_10['MAR-AAR_sq'] = ontrack_top10_10['MAR-AAR']**2

ontrack_top10_10_summary['sum_MAR-AAR_sq'] = (ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
ontrack_top10_10_summary['N'] = (ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
ontrack_top10_10_summary['VAR_AAR'] = ontrack_top10_10_summary['sum_MAR-AAR_sq'] / (ontrack_top10_10_summary['N']-1)
ontrack_top10_10_summary['SD_AAR'] = np.sqrt(ontrack_top10_10_summary['VAR_AAR'])

ontrack_top10_10_summary['T_MAR_CSecT'] = (np.sqrt(ontrack_top10_10_summary['N']))*ontrack_top10_10_summary['AAR_logPct']/ontrack_top10_10_summary['SD_AAR']


# ontrack_top20  t-stat - Cross-sectional test
ontrack_top20_10 = pd.merge(ontrack_top20_10,ontrack_top20_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
ontrack_top20_10['CAR-CAAR'] = ontrack_top20_10['CAR_logPct'] - ontrack_top20_10['CAAR_logPct']
ontrack_top20_10['CAR-CAAR_sq'] = ontrack_top20_10['CAR-CAAR']**2

ontrack_top20_10_summary['sum_CAR-CAAR_sq'] = (ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
ontrack_top20_10_summary['N'] = (ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
ontrack_top20_10_summary['VAR_CAAR'] = ontrack_top20_10_summary['sum_CAR-CAAR_sq'] / (ontrack_top20_10_summary['N']-1)
ontrack_top20_10_summary['SD_CAAR'] = np.sqrt(ontrack_top20_10_summary['VAR_CAAR'])

ontrack_top20_10_summary['T_CSecT'] = (np.sqrt(ontrack_top20_10_summary['N']))*ontrack_top20_10_summary['CAAR_logPct']/ontrack_top20_10_summary['SD_CAAR']

# Get AAR
ontrack_top20_10_summary['AAR_logPct'] = ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = ontrack_top20_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
ontrack_top20_10 = pd.merge(ontrack_top20_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
ontrack_top20_10['MAR-AAR'] = ontrack_top20_10['adjRet_MarketModel_logPct'] - ontrack_top20_10['AAR_logPct']
ontrack_top20_10['MAR-AAR_sq'] = ontrack_top20_10['MAR-AAR']**2

ontrack_top20_10_summary['sum_MAR-AAR_sq'] = (ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
ontrack_top20_10_summary['N'] = (ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
ontrack_top20_10_summary['VAR_AAR'] = ontrack_top20_10_summary['sum_MAR-AAR_sq'] / (ontrack_top20_10_summary['N']-1)
ontrack_top20_10_summary['SD_AAR'] = np.sqrt(ontrack_top20_10_summary['VAR_AAR'])

ontrack_top20_10_summary['T_MAR_CSecT'] = (np.sqrt(ontrack_top20_10_summary['N']))*ontrack_top20_10_summary['AAR_logPct']/ontrack_top20_10_summary['SD_AAR']




''' Volume Test - Around 2019 Target Completion Info '''

window_20 = ds_dsf_CDPrelease20_sub[(ds_dsf_CDPrelease20_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_CDPrelease20_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 20 days of data
remove_list = window_20.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
window_20 = window_20[~window_20['ISIN'].isin(remove_list.ISIN)]

lagbehind_20 = window_20[window_20['lag_behind_2020']==1]

# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_CDPrelease20[(ds_dsf_CDPrelease20['BDaysRelativeToEvent']>=-140)&(ds_dsf_CDPrelease20['BDaysRelativeToEvent']<=-40)]
lagbehind_estWin = vol_estWindow20[vol_estWindow20['lag_behind_2020']==1]

# Normal volume
norVol_lagbehind20 = lagbehind_estWin.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_lagbehind20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

# Merge it to window return data
lagbehind_20 = pd.merge(lagbehind_20,norVol_lagbehind20,on=['ISIN'],how='left')

lagbehind_20['AbnVolume'] = lagbehind_20['Volume'] - lagbehind_20['normal_Volume']
lagbehind_20['AbnVol_pct'] = lagbehind_20['Volume_pct'] - lagbehind_20['normal_Volume_pct']
lagbehind_20['AbnVol_pctlog'] = lagbehind_20['Volume_pctlog'] - lagbehind_20['normal_Volume_pctlog']

# Merge normal volume to estimation window volume data to get sd
lagbehind_estWin = pd.merge(lagbehind_estWin,norVol_lagbehind20,on=['ISIN'],how='left')
lagbehind_estWin['AbnVolume'] = lagbehind_estWin['Volume'] - lagbehind_estWin['normal_Volume']
lagbehind_estWin['AbnVol_pct'] = lagbehind_estWin['Volume_pct'] - lagbehind_estWin['normal_Volume_pct']
lagbehind_estWin['AbnVol_pctlog'] = lagbehind_estWin['Volume_pctlog'] - lagbehind_estWin['normal_Volume_pctlog']

# Event plot
lagbehind_20_summary = lagbehind_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
lagbehind_20_summary['N'] = (lagbehind_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])

# t-stat
temp_lagbehind20 = lagbehind_estWin.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_lagbehind20['AbnVolume_mbar'] = temp_lagbehind20['AbnVolume'] - temp_lagbehind20['AbnVolume'].mean()
temp_lagbehind20['AbnVol_pct_mbar'] = temp_lagbehind20['AbnVol_pct'] - temp_lagbehind20['AbnVol_pct'].mean()
temp_lagbehind20['AbnVol_pctlog_mbar'] = temp_lagbehind20['AbnVol_pctlog'] - temp_lagbehind20['AbnVol_pctlog'].mean()

temp_lagbehind20['AbnVolume_mbar_sq'] = temp_lagbehind20['AbnVolume_mbar']**2
temp_lagbehind20['AbnVol_pct_mbar_sq'] = temp_lagbehind20['AbnVol_pct_mbar']**2
temp_lagbehind20['AbnVol_pctlog_mbar_sq'] = temp_lagbehind20['AbnVol_pctlog_mbar']**2

lagbehind_20_summary['Var_AbnVolume'] = temp_lagbehind20['AbnVolume_mbar_sq'].mean()
lagbehind_20_summary['Var_AbnVol_pct'] = temp_lagbehind20['AbnVol_pct_mbar_sq'].mean()
lagbehind_20_summary['Var_AbnVol_pctlog'] = temp_lagbehind20['AbnVol_pctlog_mbar_sq'].mean()

lagbehind_20_summary['sd_AbnVolume'] = np.sqrt(lagbehind_20_summary['Var_AbnVolume'])
lagbehind_20_summary['sd_AbnVol_pct'] = np.sqrt(lagbehind_20_summary['Var_AbnVol_pct'])
lagbehind_20_summary['sd_AbnVol_pctlog'] = np.sqrt(lagbehind_20_summary['Var_AbnVol_pctlog'])

lagbehind_20_summary['t_AbnVolume'] = lagbehind_20_summary['AbnVolume']/lagbehind_20_summary['sd_AbnVolume']
lagbehind_20_summary['t_AbnVol_pct'] = lagbehind_20_summary['AbnVol_pct']/lagbehind_20_summary['sd_AbnVol_pct']
lagbehind_20_summary['t_AbnVol_pctlog'] = lagbehind_20_summary['AbnVol_pctlog']/lagbehind_20_summary['sd_AbnVol_pctlog']




'''
############################################################################################################
######  Market Response to 2018 Target Completion Info   ###################
############################################################################################################
'''

''' Market Response to 2018 Target Completion Info (10 days window) '''
window_10 = ds_dsf_CDPrelease19_sub[(ds_dsf_CDPrelease19_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease19_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 20 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

lagbehind_10 = window_10[window_10['lag_behind_2019']==1]
ontrack_10 = window_10[window_10['lag_behind_2019']==0]

lagbehind_top10_10 = window_10[window_10['lag_top10_2019']==1]
lagbehind_top20_10 = window_10[window_10['lag_top20_2019']==1]
ontrack_top10_10 = window_10[window_10['ontrack_top10_2019']==1]
ontrack_top20_10 = window_10[window_10['ontrack_top20_2019']==1]


# Cumulative returns
lagbehind_10['CAR_logPct'] = lagbehind_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
ontrack_10['CAR_logPct'] = ontrack_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
lagbehind_top10_10['CAR_logPct'] = lagbehind_top10_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
lagbehind_top20_10['CAR_logPct'] = lagbehind_top20_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
ontrack_top10_10['CAR_logPct'] = ontrack_top10_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()
ontrack_top20_10['CAR_logPct'] = ontrack_top20_10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()


# Get CAAR
lagbehind_10_summary = lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
ontrack_10_summary = ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
lagbehind_top10_10_summary = lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
lagbehind_top20_10_summary = lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
ontrack_top10_10_summary = ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
ontrack_top20_10_summary = ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()


lagbehind_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
ontrack_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
lagbehind_top10_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
lagbehind_top20_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
ontrack_top10_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
ontrack_top20_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']


# Lag Behind t-stat - Cross-sectional test
lagbehind_10 = pd.merge(lagbehind_10,lagbehind_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
lagbehind_10['CAR-CAAR'] = lagbehind_10['CAR_logPct'] - lagbehind_10['CAAR_logPct']
lagbehind_10['CAR-CAAR_sq'] = lagbehind_10['CAR-CAAR']**2

lagbehind_10_summary['sum_CAR-CAAR_sq'] = (lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
lagbehind_10_summary['N'] = (lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
lagbehind_10_summary['VAR_CAAR'] = lagbehind_10_summary['sum_CAR-CAAR_sq'] / (lagbehind_10_summary['N']-1)
lagbehind_10_summary['SD_CAAR'] = np.sqrt(lagbehind_10_summary['VAR_CAAR'])

lagbehind_10_summary['T_CSecT'] = (np.sqrt(lagbehind_10_summary['N']))*lagbehind_10_summary['CAAR_logPct']/lagbehind_10_summary['SD_CAAR']

# Get AAR
lagbehind_10_summary['AAR_logPct'] = lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = lagbehind_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
lagbehind_10 = pd.merge(lagbehind_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
lagbehind_10['MAR-AAR'] = lagbehind_10['adjRet_MarketModel_logPct'] - lagbehind_10['AAR_logPct']
lagbehind_10['MAR-AAR_sq'] = lagbehind_10['MAR-AAR']**2

lagbehind_10_summary['sum_MAR-AAR_sq'] = (lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
lagbehind_10_summary['N'] = (lagbehind_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
lagbehind_10_summary['VAR_AAR'] = lagbehind_10_summary['sum_MAR-AAR_sq'] / (lagbehind_10_summary['N']-1)
lagbehind_10_summary['SD_AAR'] = np.sqrt(lagbehind_10_summary['VAR_AAR'])

lagbehind_10_summary['T_MAR_CSecT'] = (np.sqrt(lagbehind_10_summary['N']))*lagbehind_10_summary['AAR_logPct']/lagbehind_10_summary['SD_AAR']



# On Track t-stat - Cross-sectional test
ontrack_10 = pd.merge(ontrack_10,ontrack_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
ontrack_10['CAR-CAAR'] = ontrack_10['CAR_logPct'] - ontrack_10['CAAR_logPct']
ontrack_10['CAR-CAAR_sq'] = ontrack_10['CAR-CAAR']**2

ontrack_10_summary['sum_CAR-CAAR_sq'] = (ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
ontrack_10_summary['N'] = (ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
ontrack_10_summary['VAR_CAAR'] = ontrack_10_summary['sum_CAR-CAAR_sq'] / (ontrack_10_summary['N']-1)
ontrack_10_summary['SD_CAAR'] = np.sqrt(ontrack_10_summary['VAR_CAAR'])

ontrack_10_summary['T_CSecT'] = (np.sqrt(ontrack_10_summary['N']))*ontrack_10_summary['CAAR_logPct']/ontrack_10_summary['SD_CAAR']

# Get AAR
ontrack_10_summary['AAR_logPct'] = ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = ontrack_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
ontrack_10 = pd.merge(ontrack_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
ontrack_10['MAR-AAR'] = ontrack_10['adjRet_MarketModel_logPct'] - ontrack_10['AAR_logPct']
ontrack_10['MAR-AAR_sq'] = ontrack_10['MAR-AAR']**2

ontrack_10_summary['sum_MAR-AAR_sq'] = (ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
ontrack_10_summary['N'] = (ontrack_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
ontrack_10_summary['VAR_AAR'] = ontrack_10_summary['sum_MAR-AAR_sq'] / (ontrack_10_summary['N']-1)
ontrack_10_summary['SD_AAR'] = np.sqrt(ontrack_10_summary['VAR_AAR'])

ontrack_10_summary['T_MAR_CSecT'] = (np.sqrt(ontrack_10_summary['N']))*ontrack_10_summary['AAR_logPct']/ontrack_10_summary['SD_AAR']


# lagbehind_top10  t-stat - Cross-sectional test
lagbehind_top10_10 = pd.merge(lagbehind_top10_10,lagbehind_top10_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
lagbehind_top10_10['CAR-CAAR'] = lagbehind_top10_10['CAR_logPct'] - lagbehind_top10_10['CAAR_logPct']
lagbehind_top10_10['CAR-CAAR_sq'] = lagbehind_top10_10['CAR-CAAR']**2

lagbehind_top10_10_summary['sum_CAR-CAAR_sq'] = (lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
lagbehind_top10_10_summary['N'] = (lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
lagbehind_top10_10_summary['VAR_CAAR'] = lagbehind_top10_10_summary['sum_CAR-CAAR_sq'] / (lagbehind_top10_10_summary['N']-1)
lagbehind_top10_10_summary['SD_CAAR'] = np.sqrt(lagbehind_top10_10_summary['VAR_CAAR'])

lagbehind_top10_10_summary['T_CSecT'] = (np.sqrt(lagbehind_top10_10_summary['N']))*lagbehind_top10_10_summary['CAAR_logPct']/lagbehind_top10_10_summary['SD_CAAR']

# Get AAR
lagbehind_top10_10_summary['AAR_logPct'] = lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = lagbehind_top10_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
lagbehind_top10_10 = pd.merge(lagbehind_top10_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
lagbehind_top10_10['MAR-AAR'] = lagbehind_top10_10['adjRet_MarketModel_logPct'] - lagbehind_top10_10['AAR_logPct']
lagbehind_top10_10['MAR-AAR_sq'] = lagbehind_top10_10['MAR-AAR']**2

lagbehind_top10_10_summary['sum_MAR-AAR_sq'] = (lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
lagbehind_top10_10_summary['N'] = (lagbehind_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
lagbehind_top10_10_summary['VAR_AAR'] = lagbehind_top10_10_summary['sum_MAR-AAR_sq'] / (lagbehind_top10_10_summary['N']-1)
lagbehind_top10_10_summary['SD_AAR'] = np.sqrt(lagbehind_top10_10_summary['VAR_AAR'])

lagbehind_top10_10_summary['T_MAR_CSecT'] = (np.sqrt(lagbehind_top10_10_summary['N']))*lagbehind_top10_10_summary['AAR_logPct']/lagbehind_top10_10_summary['SD_AAR']


# lagbehind_top20  t-stat - Cross-sectional test
lagbehind_top20_10 = pd.merge(lagbehind_top20_10,lagbehind_top20_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
lagbehind_top20_10['CAR-CAAR'] = lagbehind_top20_10['CAR_logPct'] - lagbehind_top20_10['CAAR_logPct']
lagbehind_top20_10['CAR-CAAR_sq'] = lagbehind_top20_10['CAR-CAAR']**2

lagbehind_top20_10_summary['sum_CAR-CAAR_sq'] = (lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
lagbehind_top20_10_summary['N'] = (lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
lagbehind_top20_10_summary['VAR_CAAR'] = lagbehind_top20_10_summary['sum_CAR-CAAR_sq'] / (lagbehind_top20_10_summary['N']-1)
lagbehind_top20_10_summary['SD_CAAR'] = np.sqrt(lagbehind_top20_10_summary['VAR_CAAR'])

lagbehind_top20_10_summary['T_CSecT'] = (np.sqrt(lagbehind_top20_10_summary['N']))*lagbehind_top20_10_summary['CAAR_logPct']/lagbehind_top20_10_summary['SD_CAAR']

# Get AAR
lagbehind_top20_10_summary['AAR_logPct'] = lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = lagbehind_top20_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
lagbehind_top20_10 = pd.merge(lagbehind_top20_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
lagbehind_top20_10['MAR-AAR'] = lagbehind_top20_10['adjRet_MarketModel_logPct'] - lagbehind_top20_10['AAR_logPct']
lagbehind_top20_10['MAR-AAR_sq'] = lagbehind_top20_10['MAR-AAR']**2

lagbehind_top20_10_summary['sum_MAR-AAR_sq'] = (lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
lagbehind_top20_10_summary['N'] = (lagbehind_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
lagbehind_top20_10_summary['VAR_AAR'] = lagbehind_top20_10_summary['sum_MAR-AAR_sq'] / (lagbehind_top20_10_summary['N']-1)
lagbehind_top20_10_summary['SD_AAR'] = np.sqrt(lagbehind_top20_10_summary['VAR_AAR'])

lagbehind_top20_10_summary['T_MAR_CSecT'] = (np.sqrt(lagbehind_top20_10_summary['N']))*lagbehind_top20_10_summary['AAR_logPct']/lagbehind_top20_10_summary['SD_AAR']


# ontrack_top10  t-stat - Cross-sectional test
ontrack_top10_10 = pd.merge(ontrack_top10_10,ontrack_top10_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
ontrack_top10_10['CAR-CAAR'] = ontrack_top10_10['CAR_logPct'] - ontrack_top10_10['CAAR_logPct']
ontrack_top10_10['CAR-CAAR_sq'] = ontrack_top10_10['CAR-CAAR']**2

ontrack_top10_10_summary['sum_CAR-CAAR_sq'] = (ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
ontrack_top10_10_summary['N'] = (ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
ontrack_top10_10_summary['VAR_CAAR'] = ontrack_top10_10_summary['sum_CAR-CAAR_sq'] / (ontrack_top10_10_summary['N']-1)
ontrack_top10_10_summary['SD_CAAR'] = np.sqrt(ontrack_top10_10_summary['VAR_CAAR'])

ontrack_top10_10_summary['T_CSecT'] = (np.sqrt(ontrack_top10_10_summary['N']))*ontrack_top10_10_summary['CAAR_logPct']/ontrack_top10_10_summary['SD_CAAR']

# Get AAR
ontrack_top10_10_summary['AAR_logPct'] = ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = ontrack_top10_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
ontrack_top10_10 = pd.merge(ontrack_top10_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
ontrack_top10_10['MAR-AAR'] = ontrack_top10_10['adjRet_MarketModel_logPct'] - ontrack_top10_10['AAR_logPct']
ontrack_top10_10['MAR-AAR_sq'] = ontrack_top10_10['MAR-AAR']**2

ontrack_top10_10_summary['sum_MAR-AAR_sq'] = (ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
ontrack_top10_10_summary['N'] = (ontrack_top10_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
ontrack_top10_10_summary['VAR_AAR'] = ontrack_top10_10_summary['sum_MAR-AAR_sq'] / (ontrack_top10_10_summary['N']-1)
ontrack_top10_10_summary['SD_AAR'] = np.sqrt(ontrack_top10_10_summary['VAR_AAR'])

ontrack_top10_10_summary['T_MAR_CSecT'] = (np.sqrt(ontrack_top10_10_summary['N']))*ontrack_top10_10_summary['AAR_logPct']/ontrack_top10_10_summary['SD_AAR']


# ontrack_top20  t-stat - Cross-sectional test
ontrack_top20_10 = pd.merge(ontrack_top20_10,ontrack_top20_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
ontrack_top20_10['CAR-CAAR'] = ontrack_top20_10['CAR_logPct'] - ontrack_top20_10['CAAR_logPct']
ontrack_top20_10['CAR-CAAR_sq'] = ontrack_top20_10['CAR-CAAR']**2

ontrack_top20_10_summary['sum_CAR-CAAR_sq'] = (ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
ontrack_top20_10_summary['N'] = (ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
ontrack_top20_10_summary['VAR_CAAR'] = ontrack_top20_10_summary['sum_CAR-CAAR_sq'] / (ontrack_top20_10_summary['N']-1)
ontrack_top20_10_summary['SD_CAAR'] = np.sqrt(ontrack_top20_10_summary['VAR_CAAR'])

ontrack_top20_10_summary['T_CSecT'] = (np.sqrt(ontrack_top20_10_summary['N']))*ontrack_top20_10_summary['CAAR_logPct']/ontrack_top20_10_summary['SD_CAAR']

# Get AAR
ontrack_top20_10_summary['AAR_logPct'] = ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = ontrack_top20_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
ontrack_top20_10 = pd.merge(ontrack_top20_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
ontrack_top20_10['MAR-AAR'] = ontrack_top20_10['adjRet_MarketModel_logPct'] - ontrack_top20_10['AAR_logPct']
ontrack_top20_10['MAR-AAR_sq'] = ontrack_top20_10['MAR-AAR']**2

ontrack_top20_10_summary['sum_MAR-AAR_sq'] = (ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
ontrack_top20_10_summary['N'] = (ontrack_top20_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
ontrack_top20_10_summary['VAR_AAR'] = ontrack_top20_10_summary['sum_MAR-AAR_sq'] / (ontrack_top20_10_summary['N']-1)
ontrack_top20_10_summary['SD_AAR'] = np.sqrt(ontrack_top20_10_summary['VAR_AAR'])

ontrack_top20_10_summary['T_MAR_CSecT'] = (np.sqrt(ontrack_top20_10_summary['N']))*ontrack_top20_10_summary['AAR_logPct']/ontrack_top20_10_summary['SD_AAR']



''' Volume Test - Around 2018 Target Completion Info '''

window_20 = ds_dsf_CDPrelease19_sub[(ds_dsf_CDPrelease19_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_CDPrelease19_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 20 days of data
remove_list = window_20.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
window_20 = window_20[~window_20['ISIN'].isin(remove_list.ISIN)]

lagbehind_20 = window_20[window_20['lag_behind_2019']==1]

# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_CDPrelease19[(ds_dsf_CDPrelease19['BDaysRelativeToEvent']>=-140)&(ds_dsf_CDPrelease19['BDaysRelativeToEvent']<=-40)]
lagbehind_estWin = vol_estWindow20[vol_estWindow20['lag_behind_2019']==1]

# Normal volume
norVol_lagbehind20 = lagbehind_estWin.groupby(['ISIN'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_lagbehind20.columns = ['ISIN','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']

# Merge it to window return data
lagbehind_20 = pd.merge(lagbehind_20,norVol_lagbehind20,on=['ISIN'],how='left')

lagbehind_20['AbnVolume'] = lagbehind_20['Volume'] - lagbehind_20['normal_Volume']
lagbehind_20['AbnVol_pct'] = lagbehind_20['Volume_pct'] - lagbehind_20['normal_Volume_pct']
lagbehind_20['AbnVol_pctlog'] = lagbehind_20['Volume_pctlog'] - lagbehind_20['normal_Volume_pctlog']

# Merge normal volume to estimation window volume data to get sd
lagbehind_estWin = pd.merge(lagbehind_estWin,norVol_lagbehind20,on=['ISIN'],how='left')
lagbehind_estWin['AbnVolume'] = lagbehind_estWin['Volume'] - lagbehind_estWin['normal_Volume']
lagbehind_estWin['AbnVol_pct'] = lagbehind_estWin['Volume_pct'] - lagbehind_estWin['normal_Volume_pct']
lagbehind_estWin['AbnVol_pctlog'] = lagbehind_estWin['Volume_pctlog'] - lagbehind_estWin['normal_Volume_pctlog']

# Event plot
lagbehind_20_summary = lagbehind_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
lagbehind_20_summary['N'] = (lagbehind_20.groupby(['NEWDaysRelativeToEvent'],as_index=False)['AbnVolume'].count()['AbnVolume'])

# t-stat
temp_lagbehind20 = lagbehind_estWin.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_lagbehind20['AbnVolume_mbar'] = temp_lagbehind20['AbnVolume'] - temp_lagbehind20['AbnVolume'].mean()
temp_lagbehind20['AbnVol_pct_mbar'] = temp_lagbehind20['AbnVol_pct'] - temp_lagbehind20['AbnVol_pct'].mean()
temp_lagbehind20['AbnVol_pctlog_mbar'] = temp_lagbehind20['AbnVol_pctlog'] - temp_lagbehind20['AbnVol_pctlog'].mean()

temp_lagbehind20['AbnVolume_mbar_sq'] = temp_lagbehind20['AbnVolume_mbar']**2
temp_lagbehind20['AbnVol_pct_mbar_sq'] = temp_lagbehind20['AbnVol_pct_mbar']**2
temp_lagbehind20['AbnVol_pctlog_mbar_sq'] = temp_lagbehind20['AbnVol_pctlog_mbar']**2

lagbehind_20_summary['Var_AbnVolume'] = temp_lagbehind20['AbnVolume_mbar_sq'].mean()
lagbehind_20_summary['Var_AbnVol_pct'] = temp_lagbehind20['AbnVol_pct_mbar_sq'].mean()
lagbehind_20_summary['Var_AbnVol_pctlog'] = temp_lagbehind20['AbnVol_pctlog_mbar_sq'].mean()

lagbehind_20_summary['sd_AbnVolume'] = np.sqrt(lagbehind_20_summary['Var_AbnVolume'])
lagbehind_20_summary['sd_AbnVol_pct'] = np.sqrt(lagbehind_20_summary['Var_AbnVol_pct'])
lagbehind_20_summary['sd_AbnVol_pctlog'] = np.sqrt(lagbehind_20_summary['Var_AbnVol_pctlog'])

lagbehind_20_summary['t_AbnVolume'] = lagbehind_20_summary['AbnVolume']/lagbehind_20_summary['sd_AbnVolume']
lagbehind_20_summary['t_AbnVol_pct'] = lagbehind_20_summary['AbnVol_pct']/lagbehind_20_summary['sd_AbnVol_pct']
lagbehind_20_summary['t_AbnVol_pctlog'] = lagbehind_20_summary['AbnVol_pctlog']/lagbehind_20_summary['sd_AbnVol_pctlog']






'''
############################################################################################################
######   Around Announcement/Media coverage of 2020 targets   ###################
############################################################################################################
'''
''' Market Response to 2020 target announcement media coverage (10 days window) '''
window_10 = ds_dsf_targetAnnounce_sub[(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby(['ISIN','EventDate'],as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
remove_list['ISIN_EventDate'] = remove_list['ISIN'] + "_" + remove_list['EventDate'].astype(str)
window_10['ISIN_EventDate'] = window_10['ISIN'] + "_" + window_10['EventDate'].astype(str)

window_10 = window_10[~window_10['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]


all_10 = window_10.copy()
achieved_10 = window_10[window_10['achieved']==1]
failed_10 = window_10[window_10['failed']==1]
disappeared_10 = window_10[window_10['disappeared']==1]


# Cumulative returns
all_10['CAR_logPct'] = all_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()
achieved_10['CAR_logPct'] = achieved_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()
failed_10['CAR_logPct'] = failed_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()
disappeared_10['CAR_logPct'] = disappeared_10.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()


# Get CAAR
all_10_summary = all_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
achieved_10_summary = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
failed_10_summary = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()
disappeared_10_summary = disappeared_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR_logPct'].mean()


all_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
achieved_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
failed_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']
disappeared_10_summary.columns = ['NEWDaysRelativeToEvent','CAAR_logPct']


# All media t-stat - Cross-sectional test
all_10 = pd.merge(all_10,all_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
all_10['CAR-CAAR'] = all_10['CAR_logPct'] - all_10['CAAR_logPct']
all_10['CAR-CAAR_sq'] = all_10['CAR-CAAR']**2

all_10_summary['sum_CAR-CAAR_sq'] = (all_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
all_10_summary['N'] = (all_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
all_10_summary['VAR_CAAR'] = all_10_summary['sum_CAR-CAAR_sq'] / (all_10_summary['N']-1)
all_10_summary['SD_CAAR'] = np.sqrt(all_10_summary['VAR_CAAR'])

all_10_summary['T_CSecT'] = (np.sqrt(all_10_summary['N']))*all_10_summary['CAAR_logPct']/all_10_summary['SD_CAAR']

# Get AAR
all_10_summary['AAR_logPct'] = all_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = all_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
all_10 = pd.merge(all_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
all_10['MAR-AAR'] = all_10['adjRet_MarketModel_logPct'] - all_10['AAR_logPct']
all_10['MAR-AAR_sq'] = all_10['MAR-AAR']**2

all_10_summary['sum_MAR-AAR_sq'] = (all_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
all_10_summary['N'] = (all_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
all_10_summary['VAR_AAR'] = all_10_summary['sum_MAR-AAR_sq'] / (all_10_summary['N']-1)
all_10_summary['SD_AAR'] = np.sqrt(all_10_summary['VAR_AAR'])

all_10_summary['T_MAR_CSecT'] = (np.sqrt(all_10_summary['N']))*all_10_summary['AAR_logPct']/all_10_summary['SD_AAR']


# Achieved announcement/media t-stat - Cross-sectional test
achieved_10 = pd.merge(achieved_10,achieved_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
achieved_10['CAR-CAAR'] = achieved_10['CAR_logPct'] - achieved_10['CAAR_logPct']
achieved_10['CAR-CAAR_sq'] = achieved_10['CAR-CAAR']**2

achieved_10_summary['sum_CAR-CAAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
achieved_10_summary['VAR_CAAR'] = achieved_10_summary['sum_CAR-CAAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_CAAR'] = np.sqrt(achieved_10_summary['VAR_CAAR'])

achieved_10_summary['T_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['CAAR_logPct']/achieved_10_summary['SD_CAAR']

# Get AAR
achieved_10_summary['AAR_logPct'] = achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = achieved_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
achieved_10 = pd.merge(achieved_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
achieved_10['MAR-AAR'] = achieved_10['adjRet_MarketModel_logPct'] - achieved_10['AAR_logPct']
achieved_10['MAR-AAR_sq'] = achieved_10['MAR-AAR']**2

achieved_10_summary['sum_MAR-AAR_sq'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
achieved_10_summary['N'] = (achieved_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
achieved_10_summary['VAR_AAR'] = achieved_10_summary['sum_MAR-AAR_sq'] / (achieved_10_summary['N']-1)
achieved_10_summary['SD_AAR'] = np.sqrt(achieved_10_summary['VAR_AAR'])

achieved_10_summary['T_MAR_CSecT'] = (np.sqrt(achieved_10_summary['N']))*achieved_10_summary['AAR_logPct']/achieved_10_summary['SD_AAR']


# Failed announcement/media t-stat - Cross-sectional test
failed_10 = pd.merge(failed_10,failed_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
failed_10['CAR-CAAR'] = failed_10['CAR_logPct'] - failed_10['CAAR_logPct']
failed_10['CAR-CAAR_sq'] = failed_10['CAR-CAAR']**2

failed_10_summary['sum_CAR-CAAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
failed_10_summary['VAR_CAAR'] = failed_10_summary['sum_CAR-CAAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_CAAR'] = np.sqrt(failed_10_summary['VAR_CAAR'])

failed_10_summary['T_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['CAAR_logPct']/failed_10_summary['SD_CAAR']

# Get AAR
failed_10_summary['AAR_logPct'] = failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = failed_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
failed_10 = pd.merge(failed_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
failed_10['MAR-AAR'] = failed_10['adjRet_MarketModel_logPct'] - failed_10['AAR_logPct']
failed_10['MAR-AAR_sq'] = failed_10['MAR-AAR']**2

failed_10_summary['sum_MAR-AAR_sq'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
failed_10_summary['N'] = (failed_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
failed_10_summary['VAR_AAR'] = failed_10_summary['sum_MAR-AAR_sq'] / (failed_10_summary['N']-1)
failed_10_summary['SD_AAR'] = np.sqrt(failed_10_summary['VAR_AAR'])

failed_10_summary['T_MAR_CSecT'] = (np.sqrt(failed_10_summary['N']))*failed_10_summary['AAR_logPct']/failed_10_summary['SD_AAR']


# Disappeared media t-stat - Cross-sectional test
disappeared_10 = pd.merge(disappeared_10,disappeared_10_summary,on=['NEWDaysRelativeToEvent']) # Get CAAR
disappeared_10['CAR-CAAR'] = disappeared_10['CAR_logPct'] - disappeared_10['CAAR_logPct']
disappeared_10['CAR-CAAR_sq'] = disappeared_10['CAR-CAAR']**2

disappeared_10_summary['sum_CAR-CAAR_sq'] = (disappeared_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].sum())['CAR-CAAR_sq']
disappeared_10_summary['N'] = (disappeared_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['CAR-CAAR_sq'].count()['CAR-CAAR_sq'])
disappeared_10_summary['VAR_CAAR'] = disappeared_10_summary['sum_CAR-CAAR_sq'] / (disappeared_10_summary['N']-1)
disappeared_10_summary['SD_CAAR'] = np.sqrt(disappeared_10_summary['VAR_CAAR'])

disappeared_10_summary['T_CSecT'] = (np.sqrt(disappeared_10_summary['N']))*disappeared_10_summary['CAAR_logPct']/disappeared_10_summary['SD_CAAR']

# Get AAR
disappeared_10_summary['AAR_logPct'] = disappeared_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['adjRet_MarketModel_logPct'].mean()['adjRet_MarketModel_logPct']
temp = disappeared_10_summary[['NEWDaysRelativeToEvent','AAR_logPct']]
disappeared_10 = pd.merge(disappeared_10,temp,on=['NEWDaysRelativeToEvent']) # merge AAR
disappeared_10['MAR-AAR'] = disappeared_10['adjRet_MarketModel_logPct'] - disappeared_10['AAR_logPct']
disappeared_10['MAR-AAR_sq'] = disappeared_10['MAR-AAR']**2

disappeared_10_summary['sum_MAR-AAR_sq'] = (disappeared_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].sum())['MAR-AAR_sq']
disappeared_10_summary['N'] = (disappeared_10.groupby(['NEWDaysRelativeToEvent'],as_index=False)['MAR-AAR_sq'].count()['MAR-AAR_sq'])
disappeared_10_summary['VAR_AAR'] = disappeared_10_summary['sum_MAR-AAR_sq'] / (disappeared_10_summary['N']-1)
disappeared_10_summary['SD_AAR'] = np.sqrt(disappeared_10_summary['VAR_AAR'])

disappeared_10_summary['T_MAR_CSecT'] = (np.sqrt(disappeared_10_summary['N']))*disappeared_10_summary['AAR_logPct']/disappeared_10_summary['SD_AAR']




''' Volume Test - 20 day around 2020 Target announcement media coverage '''

window_20 = ds_dsf_targetAnnounce_sub[(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']<=10)]


# Drop firms without all 20 days of data
remove_list = window_20.groupby(['ISIN','EventDate'],as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
remove_list['ISIN_EventDate'] = remove_list['ISIN'] + "_" + remove_list['EventDate'].astype(str)
window_20['ISIN_EventDate'] = window_20['ISIN'] + "_" + window_20['EventDate'].astype(str)

window_20 = window_20[~window_20['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]


all_20 = window_20.copy()
achieved_20 = window_20[window_20['achieved']==1]
failed_20 = window_20[(window_20['failed']==1)]
disappeared_20 = window_20[(window_20['disappeared']==1)]


# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_targetAnnounce[(ds_dsf_targetAnnounce['BDaysRelativeToEvent']>=-140)&(ds_dsf_targetAnnounce['BDaysRelativeToEvent']<=-40)]
vol_estWindow20['ISIN_EventDate'] = vol_estWindow20['ISIN'] + "_" + vol_estWindow20['EventDate'].astype(str)

all_estWin_win20 = vol_estWindow20.copy()
achieved_estWin_win20 = vol_estWindow20[vol_estWindow20['achieved']==1]
failed_estWin_win20 = vol_estWindow20[(vol_estWindow20['failed']==1)]
disappeared_estWin_win20 = vol_estWindow20[(vol_estWindow20['disappeared']==1)]


# Normal volume
norVol_all20 = all_estWin_win20.groupby(['ISIN_EventDate'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_all20.columns = ['ISIN_EventDate','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_achieved20 = achieved_estWin_win20.groupby(['ISIN_EventDate'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_achieved20.columns = ['ISIN_EventDate','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_failed20 = failed_estWin_win20.groupby(['ISIN_EventDate'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_failed20.columns = ['ISIN_EventDate','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']
norVol_disappeared20 = disappeared_estWin_win20.groupby(['ISIN_EventDate'],as_index=False)[['Volume','Volume_pct','Volume_pctlog']].mean()
norVol_disappeared20.columns = ['ISIN_EventDate','normal_Volume','normal_Volume_pct','normal_Volume_pctlog']


# Merge it to window return/volume data
all_20 = pd.merge(all_20,norVol_all20,on=['ISIN_EventDate'],how='left')
achieved_20 = pd.merge(achieved_20,norVol_achieved20,on=['ISIN_EventDate'],how='left')
failed_20 = pd.merge(failed_20,norVol_failed20,on=['ISIN_EventDate'],how='left')
disappeared_20 = pd.merge(disappeared_20,norVol_disappeared20,on=['ISIN_EventDate'],how='left')

all_20['AbnVolume'] = all_20['Volume'] - all_20['normal_Volume']
all_20['AbnVol_pct'] = all_20['Volume_pct'] - all_20['normal_Volume_pct']
all_20['AbnVol_pctlog'] = all_20['Volume_pctlog'] - all_20['normal_Volume_pctlog']

achieved_20['AbnVolume'] = achieved_20['Volume'] - achieved_20['normal_Volume']
achieved_20['AbnVol_pct'] = achieved_20['Volume_pct'] - achieved_20['normal_Volume_pct']
achieved_20['AbnVol_pctlog'] = achieved_20['Volume_pctlog'] - achieved_20['normal_Volume_pctlog']

failed_20['AbnVolume'] = failed_20['Volume'] - failed_20['normal_Volume']
failed_20['AbnVol_pct'] = failed_20['Volume_pct'] - failed_20['normal_Volume_pct']
failed_20['AbnVol_pctlog'] = failed_20['Volume_pctlog'] - failed_20['normal_Volume_pctlog']

disappeared_20['AbnVolume'] = disappeared_20['Volume'] - disappeared_20['normal_Volume']
disappeared_20['AbnVol_pct'] = disappeared_20['Volume_pct'] - disappeared_20['normal_Volume_pct']
disappeared_20['AbnVol_pctlog'] = disappeared_20['Volume_pctlog'] - disappeared_20['normal_Volume_pctlog']


# Merge normal volume to estimation window volume data to get sd
all_estWin_win20 = pd.merge(all_estWin_win20,norVol_all20,on=['ISIN_EventDate'],how='left')
all_estWin_win20['AbnVolume'] = all_estWin_win20['Volume'] - all_estWin_win20['normal_Volume']
all_estWin_win20['AbnVol_pct'] = all_estWin_win20['Volume_pct'] - all_estWin_win20['normal_Volume_pct']
all_estWin_win20['AbnVol_pctlog'] = all_estWin_win20['Volume_pctlog'] - all_estWin_win20['normal_Volume_pctlog']

achieved_estWin_win20 = pd.merge(achieved_estWin_win20,norVol_achieved20,on=['ISIN_EventDate'],how='left')
achieved_estWin_win20['AbnVolume'] = achieved_estWin_win20['Volume'] - achieved_estWin_win20['normal_Volume']
achieved_estWin_win20['AbnVol_pct'] = achieved_estWin_win20['Volume_pct'] - achieved_estWin_win20['normal_Volume_pct']
achieved_estWin_win20['AbnVol_pctlog'] = achieved_estWin_win20['Volume_pctlog'] - achieved_estWin_win20['normal_Volume_pctlog']

failed_estWin_win20 = pd.merge(failed_estWin_win20,norVol_failed20,on=['ISIN_EventDate'],how='left')
failed_estWin_win20['AbnVolume'] = failed_estWin_win20['Volume'] - failed_estWin_win20['normal_Volume']
failed_estWin_win20['AbnVol_pct'] = failed_estWin_win20['Volume_pct'] - failed_estWin_win20['normal_Volume_pct']
failed_estWin_win20['AbnVol_pctlog'] = failed_estWin_win20['Volume_pctlog'] - failed_estWin_win20['normal_Volume_pctlog']

disappeared_estWin_win20 = pd.merge(disappeared_estWin_win20,norVol_disappeared20,on=['ISIN_EventDate'],how='left')
disappeared_estWin_win20['AbnVolume'] = disappeared_estWin_win20['Volume'] - disappeared_estWin_win20['normal_Volume']
disappeared_estWin_win20['AbnVol_pct'] = disappeared_estWin_win20['Volume_pct'] - disappeared_estWin_win20['normal_Volume_pct']
disappeared_estWin_win20['AbnVol_pctlog'] = disappeared_estWin_win20['Volume_pctlog'] - disappeared_estWin_win20['normal_Volume_pctlog']


# Event plot
all_20_summary = all_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
achieved_20_summary = achieved_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
failed_20_summary = failed_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
disappeared_20_summary = disappeared_20.groupby('NEWDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()

all_20_summary['N'] = all_20.groupby('NEWDaysRelativeToEvent',as_index=False)['AbnVolume'].count()['AbnVolume']
achieved_20_summary['N'] = achieved_20.groupby('NEWDaysRelativeToEvent',as_index=False)['AbnVolume'].count()['AbnVolume']
failed_20_summary['N'] = failed_20.groupby('NEWDaysRelativeToEvent',as_index=False)['AbnVolume'].count()['AbnVolume']
disappeared_20_summary['N'] = disappeared_20.groupby('NEWDaysRelativeToEvent',as_index=False)['AbnVolume'].count()['AbnVolume']

# t-stat
temp_all20 = all_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_all20['AbnVolume_mbar'] = temp_all20['AbnVolume'] - temp_all20['AbnVolume'].mean()
temp_all20['AbnVol_pct_mbar'] = temp_all20['AbnVol_pct'] - temp_all20['AbnVol_pct'].mean()
temp_all20['AbnVol_pctlog_mbar'] = temp_all20['AbnVol_pctlog'] - temp_all20['AbnVol_pctlog'].mean()

temp_all20['AbnVolume_mbar_sq'] = temp_all20['AbnVolume_mbar']**2
temp_all20['AbnVol_pct_mbar_sq'] = temp_all20['AbnVol_pct_mbar']**2
temp_all20['AbnVol_pctlog_mbar_sq'] = temp_all20['AbnVol_pctlog_mbar']**2

all_20_summary['Var_AbnVolume'] = temp_all20['AbnVolume_mbar_sq'].mean()
all_20_summary['Var_AbnVol_pct'] = temp_all20['AbnVol_pct_mbar_sq'].mean()
all_20_summary['Var_AbnVol_pctlog'] = temp_all20['AbnVol_pctlog_mbar_sq'].mean()


all_20_summary['sd_AbnVolume'] = np.sqrt(all_20_summary['Var_AbnVolume'])
all_20_summary['sd_AbnVol_pct'] = np.sqrt(all_20_summary['Var_AbnVol_pct'])
all_20_summary['sd_AbnVol_pctlog'] = np.sqrt(all_20_summary['Var_AbnVol_pctlog'])

all_20_summary['t_AbnVolume'] = all_20_summary['AbnVolume']/all_20_summary['sd_AbnVolume']
all_20_summary['t_AbnVol_pct'] = all_20_summary['AbnVol_pct']/all_20_summary['sd_AbnVol_pct']
all_20_summary['t_AbnVol_pctlog'] = all_20_summary['AbnVol_pctlog']/all_20_summary['sd_AbnVol_pctlog']


temp_achieved20 = achieved_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_achieved20['AbnVolume_mbar'] = temp_achieved20['AbnVolume'] - temp_achieved20['AbnVolume'].mean()
temp_achieved20['AbnVol_pct_mbar'] = temp_achieved20['AbnVol_pct'] - temp_achieved20['AbnVol_pct'].mean()
temp_achieved20['AbnVol_pctlog_mbar'] = temp_achieved20['AbnVol_pctlog'] - temp_achieved20['AbnVol_pctlog'].mean()

temp_achieved20['AbnVolume_mbar_sq'] = temp_achieved20['AbnVolume_mbar']**2
temp_achieved20['AbnVol_pct_mbar_sq'] = temp_achieved20['AbnVol_pct_mbar']**2
temp_achieved20['AbnVol_pctlog_mbar_sq'] = temp_achieved20['AbnVol_pctlog_mbar']**2

achieved_20_summary['Var_AbnVolume'] = temp_achieved20['AbnVolume_mbar_sq'].mean()
achieved_20_summary['Var_AbnVol_pct'] = temp_achieved20['AbnVol_pct_mbar_sq'].mean()
achieved_20_summary['Var_AbnVol_pctlog'] = temp_achieved20['AbnVol_pctlog_mbar_sq'].mean()


achieved_20_summary['sd_AbnVolume'] = np.sqrt(achieved_20_summary['Var_AbnVolume'])
achieved_20_summary['sd_AbnVol_pct'] = np.sqrt(achieved_20_summary['Var_AbnVol_pct'])
achieved_20_summary['sd_AbnVol_pctlog'] = np.sqrt(achieved_20_summary['Var_AbnVol_pctlog'])

achieved_20_summary['t_AbnVolume'] = achieved_20_summary['AbnVolume']/achieved_20_summary['sd_AbnVolume']
achieved_20_summary['t_AbnVol_pct'] = achieved_20_summary['AbnVol_pct']/achieved_20_summary['sd_AbnVol_pct']
achieved_20_summary['t_AbnVol_pctlog'] = achieved_20_summary['AbnVol_pctlog']/achieved_20_summary['sd_AbnVol_pctlog']

temp_failed20 = failed_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_failed20['AbnVolume_mbar'] = temp_failed20['AbnVolume'] - temp_failed20['AbnVolume'].mean()
temp_failed20['AbnVol_pct_mbar'] = temp_failed20['AbnVol_pct'] - temp_failed20['AbnVol_pct'].mean()
temp_failed20['AbnVol_pctlog_mbar'] = temp_failed20['AbnVol_pctlog'] - temp_failed20['AbnVol_pctlog'].mean()

temp_failed20['AbnVolume_mbar_sq'] = temp_failed20['AbnVolume_mbar']**2
temp_failed20['AbnVol_pct_mbar_sq'] = temp_failed20['AbnVol_pct_mbar']**2
temp_failed20['AbnVol_pctlog_mbar_sq'] = temp_failed20['AbnVol_pctlog_mbar']**2

failed_20_summary['Var_AbnVolume'] = temp_failed20['AbnVolume_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pct'] = temp_failed20['AbnVol_pct_mbar_sq'].mean()
failed_20_summary['Var_AbnVol_pctlog'] = temp_failed20['AbnVol_pctlog_mbar_sq'].mean()

failed_20_summary['sd_AbnVolume'] = np.sqrt(failed_20_summary['Var_AbnVolume'])
failed_20_summary['sd_AbnVol_pct'] = np.sqrt(failed_20_summary['Var_AbnVol_pct'])
failed_20_summary['sd_AbnVol_pctlog'] = np.sqrt(failed_20_summary['Var_AbnVol_pctlog'])

failed_20_summary['t_AbnVolume'] = failed_20_summary['AbnVolume']/failed_20_summary['sd_AbnVolume']
failed_20_summary['t_AbnVol_pct'] = failed_20_summary['AbnVol_pct']/failed_20_summary['sd_AbnVol_pct']
failed_20_summary['t_AbnVol_pctlog'] = failed_20_summary['AbnVol_pctlog']/failed_20_summary['sd_AbnVol_pctlog']


temp_disappeared20 = disappeared_estWin_win20.groupby('BDaysRelativeToEvent',as_index=False)[['AbnVolume','AbnVol_pct','AbnVol_pctlog']].mean()
temp_disappeared20['AbnVolume_mbar'] = temp_disappeared20['AbnVolume'] - temp_disappeared20['AbnVolume'].mean()
temp_disappeared20['AbnVol_pct_mbar'] = temp_disappeared20['AbnVol_pct'] - temp_disappeared20['AbnVol_pct'].mean()
temp_disappeared20['AbnVol_pctlog_mbar'] = temp_disappeared20['AbnVol_pctlog'] - temp_disappeared20['AbnVol_pctlog'].mean()

temp_disappeared20['AbnVolume_mbar_sq'] = temp_disappeared20['AbnVolume_mbar']**2
temp_disappeared20['AbnVol_pct_mbar_sq'] = temp_disappeared20['AbnVol_pct_mbar']**2
temp_disappeared20['AbnVol_pctlog_mbar_sq'] = temp_disappeared20['AbnVol_pctlog_mbar']**2

disappeared_20_summary['Var_AbnVolume'] = temp_disappeared20['AbnVolume_mbar_sq'].mean()
disappeared_20_summary['Var_AbnVol_pct'] = temp_disappeared20['AbnVol_pct_mbar_sq'].mean()
disappeared_20_summary['Var_AbnVol_pctlog'] = temp_disappeared20['AbnVol_pctlog_mbar_sq'].mean()

disappeared_20_summary['sd_AbnVolume'] = np.sqrt(disappeared_20_summary['Var_AbnVolume'])
disappeared_20_summary['sd_AbnVol_pct'] = np.sqrt(disappeared_20_summary['Var_AbnVol_pct'])
disappeared_20_summary['sd_AbnVol_pctlog'] = np.sqrt(disappeared_20_summary['Var_AbnVol_pctlog'])

disappeared_20_summary['t_AbnVolume'] = disappeared_20_summary['AbnVolume']/disappeared_20_summary['sd_AbnVolume']
disappeared_20_summary['t_AbnVol_pct'] = disappeared_20_summary['AbnVol_pct']/disappeared_20_summary['sd_AbnVol_pct']
disappeared_20_summary['t_AbnVol_pctlog'] = disappeared_20_summary['AbnVol_pctlog']/disappeared_20_summary['sd_AbnVol_pctlog']






'''
############################################################################################################
###### FIrm-Year Panel data on market response around CDP releases  ###################
############################################################################################################
'''

##############################################################################
''' #### Use this code to remove outliers with other extreme events '''
### Three companies with extreme events:  Amkor Tech, Lenovo, Pearson
remove_list1 = ['US0316521006', 'HK0992009065', 'GB0006776081']
ds_dsf_CDPrelease_sub = ds_dsf_CDPrelease_sub[~ds_dsf_CDPrelease_sub['ISIN'].isin(remove_list1)]

### Additional outliers
### ABB, Baker Hughes, Medtronic, Shawcor, Celestica, Loreal, Thule (achieved exp), 
### Amorepacific, Intl Cons Airl Group (failed unexpected)
remove_list2 = ['CH0012221716','US05722G1004','IE00BTN1Y115','CA8204391079','CA15101Q1081','FR0000120321','SE0006422390','US9497461015',
                'KR7090430000','ES0177542018']
ds_dsf_CDPrelease_sub = ds_dsf_CDPrelease_sub[~ds_dsf_CDPrelease_sub['ISIN'].isin(remove_list2)]
##############################################################################

### Dataset to save panel data
panel_returnVolume_CDPrelease = pd.DataFrame()


''' 2021 CDP release (-1,10) '''
window_m1p10 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 12 days of data
remove_list = window_m1p10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<12] # This is the count of days
window_m1p10 = window_m1p10[~window_m1p10['ISIN'].isin(remove_list.ISIN)]

# Cumulative returns
window_m1p10['CAR_logPct'] = window_m1p10.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# CAR (-1,10)
car_m1p10 = window_m1p10[window_m1p10['NEWDaysRelativeToEvent']==10] # cumulated over 12 days
car_m1p10 = car_m1p10[['ISIN','id','EventDate','CAR_logPct']]
car_m1p10.columns = ['ISIN','id','EventDate_CDP','CDP_CAR_m1_p10'] # rename columns

# Merge
panel_returnVolume_CDPrelease = pd.concat([panel_returnVolume_CDPrelease,car_m1p10])


''' 2021 CDP release (-1,5) '''
window_m1p5 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=5)]

# Drop firms without all 7 days of data
remove_list = window_m1p5.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<7] # This is the count of days
window_m1p5 = window_m1p5[~window_m1p5['ISIN'].isin(remove_list.ISIN)]

# Cumulative returns
window_m1p5['CAR_logPct'] = window_m1p5.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# CAR (-1,5)
car_m1p5 = window_m1p5[window_m1p5['NEWDaysRelativeToEvent']==5] # cumulated over 7 days
car_m1p5 = car_m1p5[['ISIN','id','CAR_logPct']]
car_m1p5.columns = ['ISIN','id','CDP_CAR_m1_p5'] # rename columns

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,car_m1p5,
                                                            on=['ISIN','id'],how='outer')


''' 2021 CDP release (-1,3) & (-1,1)'''
window_m1p3 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=3)]

# Drop firms without all 5 days of data
remove_list = window_m1p3.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<5] # This is the count of days
window_m1p3 = window_m1p3[~window_m1p3['ISIN'].isin(remove_list.ISIN)]

# Cumulative returns
window_m1p3['CAR_logPct'] = window_m1p3.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# CAR (-1,3)
car_m1p3 = window_m1p3[window_m1p3['NEWDaysRelativeToEvent']==3] # cumulated over 5 days
car_m1p3 = car_m1p3[['ISIN','id','CAR_logPct']]
car_m1p3.columns = ['ISIN','id','CDP_CAR_m1_p3'] # rename columns

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,car_m1p3,
                                                            on=['ISIN','id'],how='outer')

# CAR (-1,1)
car_m1p1 = window_m1p3[window_m1p3['NEWDaysRelativeToEvent']==1] # cumulated over 3 days
car_m1p1 = car_m1p1[['ISIN','id','CAR_logPct']]
car_m1p1.columns = ['ISIN','id','CDP_CAR_m1_p1'] # rename columns

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,car_m1p1,
                                                            on=['ISIN','id'],how='outer')




''' 2021 CDP release Volume (-5,5) '''
window_10 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-5)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=5)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<11] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

# Abnormal volume 
# Estimation window = {-135,-35}; 100 days long gap 30 days
vol_estWindow10 = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['BDaysRelativeToEvent']>=-135)&(ds_dsf_CDPrelease['BDaysRelativeToEvent']<=-35)]

# Normal volume
norVol_10 = vol_estWindow10.groupby(['ISIN'],as_index=False)[['Volume_pctlog']].mean()
norVol_10.columns = ['ISIN','normal_Volume_pctlog']

# Merge it to window return data
window_10 = pd.merge(window_10,norVol_10,on=['ISIN'],how='left')

window_10['AbnVol_pctlog'] = window_10['Volume_pctlog'] - window_10['normal_Volume_pctlog']

vol_sum = window_10.groupby(['ISIN','id'],as_index=False)[['AbnVol_pctlog']].mean()
vol_sum.columns = ['ISIN','id','CDP_AbnVol_pctlog_avgm5p5']

# Event date volume (day=0)
temp = window_10[window_10['NEWDaysRelativeToEvent']==0] 
temp = temp[['ISIN','id','AbnVol_pctlog']]
temp.columns = ['ISIN','id','CDP_AbnVol_pctlog_day0']

vol_sum = pd.merge(vol_sum,temp,on=['ISIN','id'])

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,vol_sum,
                                                            on=['ISIN','id'],how='outer')

''' 2021 CDP release Volume (-10,10) '''
window_20 = ds_dsf_CDPrelease_sub[(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']>=-10)&(ds_dsf_CDPrelease_sub['NEWDaysRelativeToEvent']<=10)]

# Drop firms without all 21 days of data
remove_list = window_20.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<21] # This is the count of days
window_20 = window_20[~window_20['ISIN'].isin(remove_list.ISIN)]

# Abnormal volume 
# Estimation window = {-140,-40}; 100 days long gap 30 days
vol_estWindow20 = ds_dsf_CDPrelease[(ds_dsf_CDPrelease['BDaysRelativeToEvent']>=-140)&(ds_dsf_CDPrelease['BDaysRelativeToEvent']<=-40)]

# Normal volume
norVol_20 = vol_estWindow20.groupby(['ISIN'],as_index=False)[['Volume_pctlog']].mean()
norVol_20.columns = ['ISIN','normal_Volume_pctlog']

# Merge it to window return data
window_20 = pd.merge(window_20,norVol_20,on=['ISIN'],how='left')

window_20['AbnVol_pctlog'] = window_20['Volume_pctlog'] - window_20['normal_Volume_pctlog']

vol_sum = window_20.groupby(['ISIN','id'],as_index=False)[['AbnVol_pctlog']].mean()
vol_sum.columns = ['ISIN','id','CDP_AbnVol_pctlog_avgm10p10']


# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,vol_sum,
                                                            on=['ISIN','id'],how='outer')

# Save Panel Data
panel_returnVolume_CDPrelease.to_csv(output_directory+"panel_returnVolume_CDPrelease.csv",index=False)





''' 2020 CDP release (-1,3) & (-1,1) '''
window_m1p3 = ds_dsf_CDPrelease20_sub[(ds_dsf_CDPrelease20_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease20_sub['NEWDaysRelativeToEvent']<=3)]

# Drop firms without all 5 days of data
remove_list = window_m1p3.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<5] # This is the count of days
window_m1p3 = window_m1p3[~window_m1p3['ISIN'].isin(remove_list.ISIN)]

# Cumulative returns
window_m1p3['CAR_logPct'] = window_m1p3.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# CAR (-1,3)
car_m1p3 = window_m1p3[window_m1p3['NEWDaysRelativeToEvent']==3] # cumulated over 5 days
car_m1p3 = car_m1p3[['ISIN','id','CAR_logPct']]
car_m1p3.columns = ['ISIN','id','CDP19_CAR_m1_p3'] # rename columns

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,car_m1p3,
                                                            on=['ISIN','id'],how='outer')

# CAR (-1,1)
car_m1p1 = window_m1p3[window_m1p3['NEWDaysRelativeToEvent']==1] # cumulated over 3 days
car_m1p1 = car_m1p1[['ISIN','id','CAR_logPct']]
car_m1p1.columns = ['ISIN','id','CDP19_CAR_m1_p1'] # rename columns

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,car_m1p1,
                                                            on=['ISIN','id'],how='outer')


''' 2020 CDP release Volume (-5,5) '''
window_10 = ds_dsf_CDPrelease20_sub[(ds_dsf_CDPrelease20_sub['NEWDaysRelativeToEvent']>=-5)&(ds_dsf_CDPrelease20_sub['NEWDaysRelativeToEvent']<=5)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<11] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

# Abnormal volume 
# Estimation window = {-135,-35}; 100 days long gap 30 days
vol_estWindow10 = ds_dsf_CDPrelease20[(ds_dsf_CDPrelease20['BDaysRelativeToEvent']>=-135)&(ds_dsf_CDPrelease20['BDaysRelativeToEvent']<=-35)]

# Normal volume
norVol_10 = vol_estWindow10.groupby(['ISIN'],as_index=False)[['Volume_pctlog']].mean()
norVol_10.columns = ['ISIN','normal_Volume_pctlog']

# Merge it to window return data
window_10 = pd.merge(window_10,norVol_10,on=['ISIN'],how='left')

window_10['AbnVol_pctlog'] = window_10['Volume_pctlog'] - window_10['normal_Volume_pctlog']

vol_sum = window_10.groupby(['ISIN','id'],as_index=False)[['AbnVol_pctlog']].mean()
vol_sum.columns = ['ISIN','id','CDP19_AbnVol_pctlog_avgm5p5']

# Event date volume (day=0)
temp = window_10[window_10['NEWDaysRelativeToEvent']==0] 
temp = temp[['ISIN','id','AbnVol_pctlog']]
temp.columns = ['ISIN','id','CDP19_AbnVol_pctlog_day0']

vol_sum = pd.merge(vol_sum,temp,on=['ISIN','id'])

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,vol_sum,
                                                            on=['ISIN','id'],how='outer')



''' 2019 CDP release (-1,3) & (-1,1) '''
window_m1p3 = ds_dsf_CDPrelease19_sub[(ds_dsf_CDPrelease19_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_CDPrelease19_sub['NEWDaysRelativeToEvent']<=3)]

# Drop firms without all 5 days of data
remove_list = window_m1p3.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<5] # This is the count of days
window_m1p3 = window_m1p3[~window_m1p3['ISIN'].isin(remove_list.ISIN)]

# Cumulative returns
window_m1p3['CAR_logPct'] = window_m1p3.groupby(['ISIN'])['adjRet_MarketModel_logPct'].cumsum()

# CAR (-1,3)
car_m1p3 = window_m1p3[window_m1p3['NEWDaysRelativeToEvent']==3] # cumulated over 5 days
car_m1p3 = car_m1p3[['ISIN','id','CAR_logPct']]
car_m1p3.columns = ['ISIN','id','CDP18_CAR_m1_p3'] # rename columns

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,car_m1p3,
                                                            on=['ISIN','id'],how='outer')

# CAR (-1,1)
car_m1p1 = window_m1p3[window_m1p3['NEWDaysRelativeToEvent']==1] # cumulated over 3 days
car_m1p1 = car_m1p1[['ISIN','id','CAR_logPct']]
car_m1p1.columns = ['ISIN','id','CDP18_CAR_m1_p1'] # rename columns

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,car_m1p1,
                                                            on=['ISIN','id'],how='outer')

''' 2019 CDP release Volume (-5,5) '''
window_10 = ds_dsf_CDPrelease19_sub[(ds_dsf_CDPrelease19_sub['NEWDaysRelativeToEvent']>=-5)&(ds_dsf_CDPrelease19_sub['NEWDaysRelativeToEvent']<=5)]

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<11] # This is the count of days
window_10 = window_10[~window_10['ISIN'].isin(remove_list.ISIN)]

# Abnormal volume 
# Estimation window = {-135,-35}; 100 days long gap 30 days
vol_estWindow10 = ds_dsf_CDPrelease19[(ds_dsf_CDPrelease19['BDaysRelativeToEvent']>=-135)&(ds_dsf_CDPrelease19['BDaysRelativeToEvent']<=-35)]

# Normal volume
norVol_10 = vol_estWindow10.groupby(['ISIN'],as_index=False)[['Volume_pctlog']].mean()
norVol_10.columns = ['ISIN','normal_Volume_pctlog']

# Merge it to window return data
window_10 = pd.merge(window_10,norVol_10,on=['ISIN'],how='left')

window_10['AbnVol_pctlog'] = window_10['Volume_pctlog'] - window_10['normal_Volume_pctlog']

vol_sum = window_10.groupby(['ISIN','id'],as_index=False)[['AbnVol_pctlog']].mean()
vol_sum.columns = ['ISIN','id','CDP18_AbnVol_pctlog_avgm5p5']

# Event date volume (day=0)
temp = window_10[window_10['NEWDaysRelativeToEvent']==0] 
temp = temp[['ISIN','id','AbnVol_pctlog']]
temp.columns = ['ISIN','id','CDP18_AbnVol_pctlog_day0']

vol_sum = pd.merge(vol_sum,temp,on=['ISIN','id'])

# Merge
panel_returnVolume_CDPrelease = pd.merge(panel_returnVolume_CDPrelease,vol_sum,
                                                            on=['ISIN','id'],how='outer')

# Save Panel Data
panel_returnVolume_CDPrelease.to_csv(output_directory+"panel_returnVolume_CDPrelease.csv",index=False)





''' Panel for 2020 Target Announcement Media Coverage'''

panel_returnVolume_TargetAnnounce = pd.DataFrame()

''' 2020 Target Announcement Media (-1,3)  '''
window_m1p3 = ds_dsf_targetAnnounce_sub[(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']<=3)]

# There can be multiple media events for a firm
window_m1p3['ISIN_EventDate'] = window_m1p3['ISIN'] + "_" + window_m1p3['EventDate'].astype(str)

# Drop firms without all 5 days of data
remove_list = window_m1p3.groupby('ISIN_EventDate',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<5] # This is the count of days
window_m1p3 = window_m1p3[~window_m1p3['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]

# Cumulative returns
window_m1p3['CAR_logPct'] = window_m1p3.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()

# CAR (-1,3)
car_m1p3 = window_m1p3[window_m1p3['NEWDaysRelativeToEvent']==3] # cumulated over 5 days
car_m1p3 = car_m1p3[['ISIN','ISIN_EventDate','id','CAR_logPct']]
car_m1p3.columns = ['ISIN','ISIN_EventDate','id','TargetAnnounce_CAR_m1_p3'] # rename columns

# Merge
panel_returnVolume_TargetAnnounce = pd.concat([panel_returnVolume_TargetAnnounce,car_m1p3])


''' 2020 Target Announcement Media (-1,1) '''
window_m1p1 = ds_dsf_targetAnnounce_sub[(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']>=-1)&(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']<=1)]

# There can be multiple media events for a firm
window_m1p1['ISIN_EventDate'] = window_m1p1['ISIN'] + "_" + window_m1p1['EventDate'].astype(str)

# Drop firms without all 3 days of data
remove_list = window_m1p1.groupby('ISIN_EventDate',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<3] # This is the count of days
window_m1p1 = window_m1p1[~window_m1p1['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]

# Cumulative returns
window_m1p1['CAR_logPct'] = window_m1p1.groupby(['ISIN_EventDate'])['adjRet_MarketModel_logPct'].cumsum()

# CAR (-1,1)
car_m1p1 = window_m1p1[window_m1p1['NEWDaysRelativeToEvent']==1] # cumulated over 3 days
car_m1p1 = car_m1p1[['ISIN','ISIN_EventDate','id','CAR_logPct']]
car_m1p1.columns = ['ISIN','ISIN_EventDate','id','TargetAnnounce_CAR_m1_p1'] # rename columns

# Merge
panel_returnVolume_TargetAnnounce = pd.merge(panel_returnVolume_TargetAnnounce,car_m1p1,
                                                            on=['ISIN','ISIN_EventDate','id'],how='outer')

''' 2020 Target Announcement Media Volume (-5,5) '''
window_10 = ds_dsf_targetAnnounce_sub[(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']>=-5)&(ds_dsf_targetAnnounce_sub['NEWDaysRelativeToEvent']<=5)]

# There can be multiple media events for a firm
window_10['ISIN_EventDate'] = window_10['ISIN'] + "_" + window_10['EventDate'].astype(str)

# Drop firms without all 10 days of data
remove_list = window_10.groupby('ISIN_EventDate',as_index=False)['NEWDaysRelativeToEvent'].count()
remove_list = remove_list[remove_list['NEWDaysRelativeToEvent']<11] # This is the count of days
window_10 = window_10[~window_10['ISIN_EventDate'].isin(remove_list.ISIN_EventDate)]

# Abnormal volume 
# Estimation window = {-135,-35}; 100 days long gap 30 days
vol_estWindow10 = ds_dsf_targetAnnounce[(ds_dsf_targetAnnounce['BDaysRelativeToEvent']>=-135)&(ds_dsf_targetAnnounce['BDaysRelativeToEvent']<=-35)]

# Normal volume
vol_estWindow10['ISIN_EventDate'] = vol_estWindow10['ISIN'] + "_" + vol_estWindow10['EventDate'].astype(str)
norVol_10 = vol_estWindow10.groupby(['ISIN_EventDate'],as_index=False)[['Volume_pctlog']].mean()
norVol_10.columns = ['ISIN_EventDate','normal_Volume_pctlog']

# Merge it to window return data
window_10 = pd.merge(window_10,norVol_10,on=['ISIN_EventDate'],how='left')

window_10['AbnVol_pctlog'] = window_10['Volume_pctlog'] - window_10['normal_Volume_pctlog']

vol_sum = window_10.groupby(['ISIN','ISIN_EventDate','id'],as_index=False)[['AbnVol_pctlog']].mean()
vol_sum.columns = ['ISIN','ISIN_EventDate','id','TargetAnnounce_AbnVol_pctlog_avgm5p5']

# Event date volume (day=0)
temp = window_10[window_10['NEWDaysRelativeToEvent']==0] 
temp = temp[['ISIN','ISIN_EventDate','id','AbnVol_pctlog']]
temp.columns = ['ISIN','ISIN_EventDate','id','TargetAnnounce_AbnVol_pctlog_day0']

vol_sum = pd.merge(vol_sum,temp,on=['ISIN','ISIN_EventDate','id'])

# Merge
panel_returnVolume_TargetAnnounce = pd.merge(panel_returnVolume_TargetAnnounce,vol_sum,
                                                            on=['ISIN','ISIN_EventDate','id'],how='outer')

# Save Panel
panel_returnVolume_TargetAnnounce.to_csv(output_directory+"panel_returnVolume_TargetAnnounce.csv",index=False)



