#IMPORTS
import pandas as pd

#read in both raw uncleaned datasets
raw_search_trends_df = pd.read_csv('2020_US_weekly_symptoms_dataset.csv')
raw_hospitalization_df = pd.read_csv('aggregated_cc_by.csv')

#REMOVE ALL LOCATIONS NOT PRESENT IN BOTH DATASETS
st_location_set = set(raw_search_trends_df['open_covid_region_code'].tolist()) 
# print(st_location_set)
hos_loc_filt_df = raw_hospitalization_df[raw_hospitalization_df['open_covid_region_code'].isin(st_location_set)]
hos_loc_filt_df = hos_loc_filt_df.dropna(axis=1, how='all') # drops the columns with all null values
# hos_loc_filt_df.to_csv("hos_loc_filt.csv", index=False)

#MAKE THE DAILY DATA WEEKLY DATA TO MATCH THE SEARCH TREND DATA 
hos_loc_filt_df['date'] = pd.to_datetime(hos_loc_filt_df['date']) - pd.to_timedelta(7, unit='d')
hos_week_loc_new_filt_df = hos_loc_filt_df.groupby(['open_covid_region_code', pd.Grouper(key='date', freq='W-MON')])['hospitalized_new'].sum().reset_index().sort_values(['open_covid_region_code','date'], ascending=[True, True])
# hos_week_loc_new_filt_df.to_csv("hos_week_loc_new_filt.csv", index=False)
# if we want cumulative hospitalization data, we need to calcualte it manually using the new hospitalization data. Easy to do.

#CLEANUP SEARCH TRENDS DATA COLUMNS BEFORE MERGE
raw_search_trends_df['date'] = pd.to_datetime(raw_search_trends_df['date']) - pd.to_timedelta(7, unit='d')
st_cleaned_data = raw_search_trends_df.dropna(axis=1, how='all') #drops any columns that are entirely null values
# st_cleaned_data = raw_search_trends_df.dropna(thresh=0.7*len(raw_search_trends_df) ,axis=1) #drops any columns with more than 70% data as nan if we wanted to
st_cleaned_data = st_cleaned_data.drop(['country_region_code', 'country_region', 'sub_region_1_code', 'sub_region_1'], axis=1)
# st_cleaned_data.to_csv("st_cleaned_data.csv", index=False)

#JOIN THE DATAFRAMES TOGETHER
st_cleaned_data = st_cleaned_data.set_index(['open_covid_region_code', 'date'])
hos_week_loc_new_filt_df = hos_week_loc_new_filt_df.set_index(['open_covid_region_code', 'date'])

combined_df = st_cleaned_data.join(hos_week_loc_new_filt_df, how='outer')
combined_df = combined_df.fillna(0)
print(combined_df)
combined_df.to_csv("combined.csv", index=True)





