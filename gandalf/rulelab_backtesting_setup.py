""" Module used to set up avro files from Snowflake data for backtest mode
"""
import logging
from gandalf import mock_setup_utils

# pylint: disable=invalid-name

LOGGER = logging.getLogger(__name__)

DTYPE_DICT = {
    'credit_line': 'Int64',
    'minimum_security_deposit_amount': 'Int64',
    'camaro_usm_band': 'Int64',
    'camaro_um_band': 'Int64',
    'ar1_usm_band': 'Int64',
    'ar1_um_band': 'Int64',
}

CONVERTERS_DICT = {
    'turndown_reason_codes': mock_setup_utils.list_converter,
    'concerns': mock_setup_utils.list_converter,
    'messages': mock_setup_utils.list_converter,
    'miscellaneous_statements': mock_setup_utils.list_converter
    }

FILL_NA_DICT = {'annual_income': 0,
                'additional_income': 0,
                'minimum_security_deposit_amount': 0,
                'credit_line': 0,
                'camaro_um_band': 0,
                'ar1_usm_band': 0,
                'ar1_um_band': 0
               }


def set_up_backtest_df(backtest_config, test_credit_policy_ID=None):
    """Run SQL queries to gather backtesting data
    and transform into pandas dataframe
    Returns
    -------
    pandas.core.DataFrame
        Dataframe containing all the backtest input data
    Raises
    ------
    ValueError
        If no backtest data is available for the given backtest_id
        or if there are Rulelab output errors associated with the backtest_id.
    """
    backtest_id = backtest_config['backtest_id']
    test_credit_policy_id = test_credit_policy_ID
    test_credit_policy_id = backtest_config['test_credit_policy_id']
    eid = backtest_config['eid']
    backtest_id_short = backtest_id[-12:]

    sql_config = {
        'backtest_id': backtest_id,
        'eid': eid,
        'backtest_id_short': backtest_id_short
    }
    result_df = mock_setup_utils.sql_to_df('backtest1.sql', sql_config)

    error_df = mock_setup_utils.sql_to_df('backtest2.sql', sql_config)

    if not error_df.empty:
        error_df.to_csv('error_df.csv', index=False)
        raise ValueError(
            'There are Rulelab output errors for backtest_id {0}.'.format(
                backtest_id
            )
        )

#     result_df = mock_setup_utils.sql_to_df('tst.sql', sql_config)

    if result_df.empty:
        raise ValueError(
            'No backtest data is available for backtest_id {0}.'.format(
                backtest_id
            )
        )

    result_df.columns = map(str.lower, result_df.columns)

    for column, dtype in DTYPE_DICT.items():
        if column in result_df:
            result_df[column] = result_df[column].astype(dtype)

    for column, func in CONVERTERS_DICT.items():
        if column in result_df:
            result_df[column] = result_df[column].apply(func)

    result_df.fillna(FILL_NA_DICT, inplace=True)

    if test_credit_policy_id:
        result_df[['test_credit_policy_id']] = test_credit_policy_id

    result_df.to_csv('result_df.csv')
    return result_df


def run(backtest_config):
    """run"""
    backtest_df = set_up_backtest_df(backtest_config)
    mock_setup_utils.create_mock_qa_data(backtest_df)
