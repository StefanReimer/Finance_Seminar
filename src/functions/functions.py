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

def convert_NAs(data, column):
    """
    function to convert NAs as np.NaN in a dataFrame    
    @param df: dataFrame with NAs
    @return: dataFrame with NAs as np.NaN
    """
    df = data.copy()
    na_amount_before = df[column].isna().sum()
    df.loc[:, column] = df[column].astype(str)
    df.loc[:, column] = df[column].str.replace(',', '')
    df.loc[:, column] = df[column].str.replace('nan', 'NaN')
    data.loc[:, column] = data[column].str.replace('na', 'NaN')
    data.loc[:, column] = data[column].str.replace('NaNn', 'NaN')
    df.loc[:, column] = df[column].replace('NaN', np.NAN)
    na_amount_after = df[column].isna().sum()
    nonna_amount_after = df[column].notna().sum()
    print(f"{na_amount_after - na_amount_before} NAs have been created. "
          f"{nonna_amount_after} valid values are left. \n")    
    return df

def get_duplicates(df, column, comment=''):
    # function to print the sum of duplicates and return the duplicates
    duplicates = df.loc[:, [column]].duplicated()
    duplicates_sum = duplicates.sum()
    print(f'There are {duplicates_sum} duplicates in {column}{comment}.')
    return duplicates

def find_char_in_colnames(df, char_to_find):
    """ function to find colnames in a dataFrame with matching subcharacters
    @param df: dataFrame to search the columns
    @ param char_to_find: character, which should be searched for as substring in the dataframe
    @return: array with matching columns
    """
    col_names = df.columns.values
    matching_col_names = col_names[[char_to_find in i for i in col_names]]
    print(
        f"columns containing <<{char_to_find}>> are:"
        "\n"
        f"{matching_col_names}"
        "\n")
    #print(matching_col_names)
    #print('\t')
    return matching_col_names

def convert_date(df, column, format='%Y-%m-%d', errors = 'raise'):
    """
    function to convert characters to pandas datetime column in a dataFrame and print NA information.
    
    @param df: dataFrame with column to convert
    @param column: column to convert to datetime
    @param format: look at pd.to_datetime documentation
    @param errors: look at pd.to_datetime documentation
    @return: dataFrame with converted column
    """
    na_amount_before = df[column].isna().sum()
    df[column] = pd.to_datetime(df[column], format=format, errors=errors)
    na_amount_after = df[column].isna().sum()
    nonna_amount_after = df[column].notna().sum()
    print(f"{column} has been converted. \n"
          f"{na_amount_after - na_amount_before} NAs have been created. "
          f"{nonna_amount_after} valid values are left. \n")    
    return df

def convert_price(data, column, errors = 'raise'):
    """
    function to convert characters to numeric column in a dataFrame and print NA information.
    
    @param df: dataFrame with column to convert
    @return: dataFrame with converted column
    """
    df = data.copy()
    na_amount_before = df[column].isna().sum()
    df.loc[:, column] = df[column].astype(str)
    df.loc[:, column] = df[column].str.replace(',', '')
    df.loc[:, column] = df[column].str.replace('nan', 'NaN')
    # data.loc[:, column] = data[column].str.replace('na', 'NaN')
    # data.loc[:, column] = data[column].str.replace('NaNn', 'NaN')
    df.loc[:, column] = df[column].replace('NaN', np.NAN)
    df.loc[:, column] = pd.to_numeric(df.loc[:, column], errors=errors)
    na_amount_after = df[column].isna().sum()
    nonna_amount_after = df[column].notna().sum()
    print(f"{column} has been converted. \n"
          f"{na_amount_after - na_amount_before} NAs have been created. "
          f"{nonna_amount_after} valid values are left. \n")    
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
