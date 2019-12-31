import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import statsmodels.api as sm
import statsmodels.formula.api as smf
import re

from src.functions.functions import data_import, replace_hardcoded_columnames

folderPath = "~/PycharmProjects/FinanceSeminar"
filePath = folderPath + "/data/initial_data/sdc_america.csv"
filePath = folderPath + "/data/initial_data/sdc.csv"

# import the sdc data
sdc_data = data_import(filePath=filePath)

sdc_data = pd.DataFrame()
sdc_data_chunked = pd.read_csv(filePath, delimiter=",", header=0, chunksize=1000)
for chunk in sdc_data_chunked:
    sdc_data = sdc_data.append(chunk, ignore_index=True)

sdc_data = sdc_data.sort_values('Unnamed: 0')
sdc_data = sdc_data.reset_index()
sdc_data = sdc_data.drop(columns=['index', 'Unnamed: 0'])

# Question: how many values are given?
#sdc_data.isna().sum()

# Questions: which values do i need?
# IPO Flag (Y/N)
#sum(sdc_data.loc[:, "IPOFlag(Y/N)"] == "Yes")
# Out: 18224
#sum(sdc_data.loc[:, "IPOFlag(Y/N)"] == "No")
# Out: 31503

# Issue Date
sdc_data['IssueDate'] = pd.to_datetime(sdc_data['IssueDate'], format='%Y-%m-%d')
sdc_data['FilingDate'] = pd.to_datetime(sdc_data['FilingDate'], format='%Y-%m-%d', errors='coerce')
sdc_data['Year'] = sdc_data['IssueDate'].dt.year
sdc_data_usa = sdc_data.loc[sdc_data['Nation'] == "United States", :]
sdc_data_usa_ipo = sdc_data_usa.loc[sdc_data['IPOFlagYN'] == 'Yes', :]

df = sdc_data_usa_ipo.copy()
print(df.shape)
df = df.dropna(subset=['FilingDate'])
print(df.shape)
df = df.dropna(subset=['OfferPrice'])
print(df.shape)
df = df.dropna(subset=['OriginalHighFilingPrice'])
print(df.shape)
df = df.dropna(subset=['OriginalLowFilingPrice'])
print(df.shape)
#df = df.dropna(subset=['LowPriceofFilingPriceRnge'])
#print(df.shape)
#df = df.dropna(subset=['HighPriceofFilingPriceRnge'])
#print(df.shape)

df_without_range = df.dropna(subset=['HighPriceofFilingPriceRnge'])
df_without_range = df.dropna(subset=['LowPriceofFilingPriceRnge'])

# Data per Year?
data_per_year = sdc_data.groupby('Year')['IssueDate'].size()

# How many data for US per Year?
data_US_per_year = sdc_data_usa.groupby('Year')['IssueDate'].count()

# How many data for US IPOs per Year?
ipo_per_year = sdc_data_usa_ipo.groupby('Year')['IssueDate'].count()

# How many OfferPrices are per year given?
ipo_US_with_data_per_year = df.groupby('Year')['OfferPrice'].count()

# How many Ranges are per year given?
ipo_US_with_range_data_per_year = df_without_range.groupby('Year')['OfferPrice'].count()

ipos = pd.merge(data_per_year, data_US_per_year, how='outer', on="Year")
ipos = pd.merge(ipos, ipo_per_year, how='outer', on="Year")
ipos = pd.merge(ipos, ipo_US_with_data_per_year, how='outer', on="Year")
ipos = pd.merge(ipos, ipo_US_with_range_data_per_year, how='outer', on='Year')
# create barplot with numbers of IPOs
fig, ax = plt.subplots()
width = 0.35
p1 = ax.bar(ipos.index, ipos['IssueDate_x'])
p2 = ax.bar(ipos.index, ipos['IssueDate_y'])
p3 = ax.bar(ipos.index, ipos['IssueDate'])
p4 = ax.bar(ipos.index, ipos['OfferPrice_x'])
p5 = ax.bar(ipos.index, ipos['OfferPrice_y'])

ax.legend((p1[0], p2[0], p3[0], p4[0], p5[0]),
          ('data in SDC',
           'US data in SDC',
           'US IPOs in SDC',
           'US IPOs in SDC with needed price data',
           'US IPOs in SDC with needed range data')
          )
ax.autoscale_view()
plt.show()


def clean_and_invert_numeric(data: pd.DataFrame, column: str):
    data.loc[:, column] = data[column].astype(str)
    data.loc[:, column] = data[column].str.replace('na', 'NaN')
    data.loc[:, column] = data[column].str.replace('nan', 'NaN')
    data.loc[:, column] = data[column].str.replace('NaNn', 'NaN')
    data.loc[:, column] = data[column].str.replace(',', '')
    data.loc[:, column] = data[column].replace('NaN', np.NAN)
    data.loc[:, column] = pd.to_numeric(data.loc[:, column])
    return data


for column in ['OfferPrice', 'OriginalHighFilingPrice', 'OriginalLowFilingPrice']:
    sdc_data_usa_ipo = clean_and_invert_numeric(sdc_data_usa_ipo, column)

# TODO: set date range
sdc_data_subset = sdc_data_subset.loc[sdc_data['Year'] > 2000]

# set Issue Date as index
sdc_data.index = sdc_data["IssueDate"]

sdc_data.groupby(["Year"]).count()

# TODO: Offer Price
# TODO: Original Low Filing Price
# TODO: Original High Filing Price

#sum(sdc_data.loc[["Filing Date"].notna() , ["Filing Date"]] == sdc_data.index)
#sum(sdc_data.loc[sdc_data["Filing Date"].notna() , ["Filing Date"]].values== sdc_data.loc[sdc_data["Filing Date"].notna() , :].index)

# TODO: how many values are since 1983 given?

# TODO: Get only numeric values
#def to_numeric(df, column):
#    df[df[[column]].apply(lambda x: x[0].isdigit(), axis=1)]
#to_numeric(sdc_data, "High Price of Filing Price Range")

type(sdc_data["HighPriceOfFilingPriceRange"][49724])
test = pd.to_numeric(sdc_data["HighPriceOfFilingPriceRange"], errors='coerce')
cleaned_data = sdc_data[pd.to_numeric(sdc_data["HighPriceOfFilingPriceRange"], errors='coerce').notnull()]
test.isna().sum()

# quick modeling

sdc_data_1987 = sdc_data.loc[(sdc_data["IssueDate"] > "1983-01-01") & (sdc_data["IssueDate"] < "1987-09-30"), :]
# 5009 examples from 1983 to Sept 1987

sum(sdc_data_1987.loc[:, "IPOFlag(Y/N)"] == "Yes")
# 2689 examples with IPO Flag

sdc_data_1987_IPO = sdc_data_1987.loc[sdc_data_1987["IPOFlag(Y/N)"] == "Yes", :]

sdc_data_1987_IPO.isna().sum()
# missing values
#Original Low Filing Price                        488
#Original High Filing Price                       489
#Low Price of Filing Price Range                2679
#High Price of Filing Price Range                2679

sdc_data_1987_IPO = sdc_data_1987_IPO.loc[sdc_data_1987_IPO["OriginalHighFilingPrice"].notna(), :]
sdc_data_1987_IPO = sdc_data_1987_IPO.drop(columns=["IssueDate"])
sdc_data_1987_IPO = sdc_data_1987_IPO.reset_index()


#sdc_data_1987_IPO = sdc_data[pd.to_numeric(sdc_data_1987_IPO["OfferPrice"])]
#sdc_data_1987_IPO = sdc_data[pd.to_numeric(sdc_data_1987_IPO["OriginalHighFilingPrice"])]

#features = np.array[("IssueDate", "IPOFlag(Y/N", "OfferPrice", "OriginalLowFilingPrice", "OriginalHighFilingPrice")]
#results = smf.ols('OfferPrice ~ OriginalLowFilingPrice + OriginalHighFilingPrice', data=sdc_data_1987_IPO).fit()
#print(results.summary())

#X = sdc_data_1987_IPO.loc[:, ["OriginalLowFilingPrice", "OriginalHighFilingPrice"]].values
#y = sdc_data_1987_IPO["OfferPrice"]
#X = sm.add_constant(X)

df = sdc_data
regex = r"\d+.\d+,\d+"

df[~df["OfferPrice"].str.contains(pat=regex, regex=True).fillna(value = True).values]
df[~df["OriginalHighFilingPrice"].str.contains(pat=regex, regex=True).fillna(value = True).values]
df[~df["OriginalLowFilingPrice"].str.contains(pat=regex, regex=True).fillna(value = True).values]

# Test two indizes
#2019-08-20 1.000,000
#2019-08-16 ,810
#2019-08-15 1,250
#2019-07-26 ,75
#2019-04-05 7.500,000

sample = df.loc[df.index.isin(["2019-08-20",
                               "2019-08-16",
                               "2019-08-15",
                               "2019-07-26",
                               "2019-04-05"]), :]

#test = df[pd.to_numeric(df["OriginalLowFilingPrice"], errors='coerce')]

verb = df.loc[df['Issuer'] == "Verb Technology Co Inc", :]


test = sample.loc[:, ['OfferPrice',
               'OriginalHighFilingPrice',
               'OriginalLowFilingPrice',
               'LowPriceOfFilingPriceRange',
               'HighPriceOfFilingPriceRange']]


