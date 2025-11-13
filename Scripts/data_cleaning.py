import numpy as np
import pandas as pd

# Pesticides dataset cleaning

pesticides = pd.read_csv('../Data/pesticides.csv')
pesticides.drop(['Domain', 'Element', 'Unit'], axis=1)
pesticides['Value'].rename('pesticides_tonnes', inplace=True)

pesticides.drop_duplicates(subset=['Area', 'Year'], inplace=True)

pesticides['pesticides_tonnes'].fillna(pesticides['pesticides_tonnes'].median(), inplace=True)

country_counts = pesticides['Area'].value_counts()
valid_countries = country_counts[country_counts >= 100].index
pesticides = pesticides[pesticides['Area'].isin(valid_countries)]

# Rainfall dataset cleaning

rainfall = pd.read_csv('../Data/rainfall.csv')
rainfall = rainfall.dropna(subset=['Year'])
rainfall['Year'] = rainfall['Year'].astype(int)

rainfall.drop_duplicates(subset=['Area', 'Year'], inplace=True)

rainfall['average_rain_fall_mm_per_year'] = (
    rainfall.groupby('Area')['average_rain_fall_mm_per_year']
    .transform(lambda x: x.fillna(x.median()))
)

# Temperature dataset cleaning

temp = pd.read_csv('../Data/temp.csv')
temp = temp.dropna(subset=['year'])
temp['year'] = temp['year'].astype(int)

temp.rename(columns={'country': 'Area', 'year': 'Year'}, inplace=True)
temp = temp[temp['Year'] >= 1990]

temp.drop_duplicates(subset=['Area', 'Year'], inplace=True)

temp['avg_temp'] = (
    temp.groupby('Area')['avg_temp']
    .transform(lambda x: x.fillna(x.median()))
)

# Yield dataset cleaning

yieldd = pd.read_csv('../Data/yield.csv')
yieldd = yieldd.dropna(subset=['Year'])
yieldd['Year'] = yieldd['Year'].astype(int)

yieldd = yieldd[['Area', 'Item', 'Year', 'Value']]

yieldd.rename(columns={'Value': 'hg/ha_yield'}, inplace=True)

yieldd = yieldd[yieldd['Year'] >= 1990]

yieldd.drop_duplicates(subset=['Area', 'Item', 'Year'], inplace=True)

yieldd['hg/ha_yield'] = (
    yieldd.groupby('Item')['hg/ha_yield']
    .transform(lambda x: x.fillna(x.median()))
)

# Combining all the df into a single df

yield_df = yieldd.copy()

yield_df = pd.merge(
    yield_df, 
    rainfall[['Area', 'Year', 'average_rain_fall_mm_per_year']], 
    on=['Area', 'Year'], 
    how='left'
)

yield_df = pd.merge(
    yield_df, 
    pesticides[['Area', 'Year', 'pesticides_tonnes']], 
    on=['Area', 'Year'], 
    how='left'
)

yield_df = pd.merge(
    yield_df, 
    temp[['Area', 'Year', 'avg_temp']], 
    on=['Area', 'Year'], 
    how='left'
)

yield_df.reset_index(inplace=True)
yield_df.rename(columns={'index': 'Unnamed: 0'}, inplace=True)

yield_df.drop("Unnamed: 0", axis=1,inplace=True)

country_counts =yield_df['Area'].value_counts()
countries_to_drop = country_counts[country_counts < 100].index.tolist()
df_filtered = yield_df[~yield_df['Area'].isin(countries_to_drop)]
yield_df = df_filtered.reset_index(drop=True)

yield_df.to_csv('../Data/yield_df.csv', index=False)
