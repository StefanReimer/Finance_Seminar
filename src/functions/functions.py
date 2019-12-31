import numpy as np
import pandas as pd
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def data_import_chunkwise(filePath):
    logging.info("loading started...")
    data = pd.DataFrame()
    chunks = pd.read_csv(filePath, delimiter=",", header=0, chunksize=1000)
    for chunk in chunks:
        data = data.append(chunk, ignore_index=True)
    logging.info("loading finished.")
    print("The loaded data frame has " + str(data.shape[0]) + " rows and " + str(data.shape[1]) + " columns.")
    return data

def replace_hardcoded_columnames(df):
    newColumnNames = np.array(['FilingDate', 'IssueDate', 'Issuer', 'State', 'Nation',
                               'IPOFlag(Y/N)', 'OriginalIPOFlag(Y/N)', 'OfferPrice',
                               'TypeOfSecurity', 'Description', 'REITTypeCode',
                               'UnitInv.Trust', 'Depositary', ' DealNumber',
                               'Closed-endFund/TrustFlag(Y/N)', 'CUSIP', '9-DigitCUSIP',
                               'ProceedsAmt-inThisMkt($mil)', 'VentureBacked',
                               'GrossSpreadAs%ofPrncplAmt-inThisMkt',
                               'AllMgrRoleCode', 'PrimaryHi-TechIndustryCode',
                               'OriginalLowFilingPrice', 'OriginalHighFilingPrice',
                               'LowPriceOfFilingPriceRange',
                               'HighPriceOfFilingPriceRange'])
    df.columns = newColumnNames
    return df

# TODO: replace linebreak in array values
# def replaceLinebreak(string):
# sdc_data["IPO\nFlag\n(Y/N)"]
# columnNames = sdc_data.columns
# columnNames = columnNames.values
# map(np.char.replace(x, r'\n', ' '), columnNames)
#    stringReplaced = np.char.replace(string, r'\n', ' ')
#    return stringReplaced
# replaceLinebreak("ansadandnp\nasas")
# np.char.replace(i, r'\n', ' ')
# sdc_data = sdc_data.replace(r'\n',' ', regex=True)
