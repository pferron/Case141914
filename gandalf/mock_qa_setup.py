""" Module used to set up avro files from csv file(s) for mock_qa mode
"""

import logging
import glob
import os
import pandas as pd
from gandalf import config
from gandalf import mock_setup_utils

LOGGER = logging.getLogger(__name__)

DTYPE_DICT = config.get_mock_qa_dtypes()

CONVERTERS_DICT = {
    'TURNDOWN_REASON_CODES': mock_setup_utils.list_converter,
    'CONCERNS': mock_setup_utils.list_converter,
    'CASH_ADVANCE_INDICATOR': lambda x: x == 'Y' if x else None,
    'DEVICE_MOBILE': lambda x: x.lower() if x else None
    }

FILL_NA_DICT = {'ANNUAL_INCOME': 0,
                'ADDITIONAL_INCOME': 0,
                'NUMBER_OF_CARDS': '0',
                'MINIMUM_SECURITY_DEPOSIT_AMOUNT': 0,
                'CREDIT_LINE': 0,
                'CAMARO_UM_BAND': 0, 'CAMARO_USM_BAND': 0,
                'AR1_USM_BAND': 0, 'AR1_UM_BAND': 0}

def set_up_mock_qa_df():
    """Read in mock QA CSV files from mock_qa directory
    and transform into pandas dataframe
    Returns
    -------
    pandas.core.DataFrame
        Dataframe containing all the mock QA input data
    Raises
    ------
    FileNotFoundError
        If no CSV files are found in mock_qa directory.
    """
    mock_qa_files = glob.glob(os.path.join(config.get_current_directory(), 'mock_qa/*.csv'))
    if not mock_qa_files:
        raise FileNotFoundError('No CSV files found in mock_qa directory.')
    df_list = []
    for filename in mock_qa_files:
        file_df = pd.read_csv(
            filename, header=0, dtype=DTYPE_DICT, converters=CONVERTERS_DICT
            )
        file_df = file_df[file_df['CREATION_TIMESTAMP'].notna()]
        file_df.fillna(FILL_NA_DICT, inplace=True)
        df_list.append(file_df)
    combo_df = pd.concat(df_list, axis=0)
    combo_df.columns = map(str.lower, combo_df.columns)
#     if 'number_of_cards' in combo_df.columns:
#         combo_df['total_unsecured_exposure'] = (
#             combo_df['number_of_cards'].apply(int) * 10_000
#         ).apply(str)
    combo_df.to_csv('combo_df.csv')
    return combo_df


def run():
    """ run """
    mock_qa_df = set_up_mock_qa_df()
    mock_setup_utils.create_mock_qa_data(mock_qa_df)
