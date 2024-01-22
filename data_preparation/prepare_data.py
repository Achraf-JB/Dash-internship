import os
import traceback
from datetime import timedelta

import pandas as pd
from data_extraction.github_data_handler import GithubConnector

from data_extraction.testguide__data_handler import TestGuideDataPreparer
import logging

from utilities.tools import get_pr_label, assign_status

logger = logging.getLogger(__name__)
class DataPreparation:
    """
        A class used to prepare and reformat data

        Attributes:
        -----------
        all_data: pandas DataFrame
            The combined DataFrame of all the extracted data from different sources.
        tg_connector: TgConnector object
            The object used to connect to and extract data from the TestGuide .
        tg_data: pandas DataFrame
            The DataFrame of test generation data extracted from the TestGuide .
        github_connector: GithubConnector object
            The object used to connect to and extract data from the GitHub repository.
        github_data: pandas DataFrame
            The DataFrame of pull request information extracted from the GitHub repository.
        workspace: str
            The workspace path where the generated tests and other artifacts will be saved.

        Methods:
        --------
        reformat_columns()
            Reformat data frame columns from JSON format to clear string formatting.
        cleaning_data()
            replace nan values by empty strings
        type_casting()
            R Convert columns in the Telegram and Github dataframes to their appropriate data types. Specifically:
            - Convert 'creationDate' and 'startDate' columns in the Telegram dataframe to separate 'creation_date',
              'creation_time', 'start_date', and 'start_time' columns. Convert these columns to datetime and time
              formats as appropriate.
            - Convert 'prNumber' column in both the Telegram and Github dataframes to string data type.
        prepare_all_data()
            Performs the following steps to prepare and merge data:
                1. Reformats columns of data
                2. Cleans data by removing null values and duplicates
                3. Type casts columns to appropriate data types
                4. Merges tg_data and github_data on 'prNumber' column
                5. Saves the merged data to a CSV file named 'tg_data.csv'
    """
    def __init__(self, workspace):
        #self.all_data = pd.DataFrame()
        self.all_data = pd.read_csv(r"C:\Users\HP\OneDrive\Bureau\sanity_check_dashboards\sanity_data.csv")
        self.testguide_data_handler = TestGuideDataPreparer()
        self.testguide_data_handler.extract_data_from_testguide()
        self.tg_data = self.testguide_data_handler.full_testguide_data
        self.github_data_handler = GithubConnector()
        self.github_data_handler.extract_pull_request_info()
        self.github_data = self.github_data_handler.pr_github_data
        self.workspace = workspace

    def reformat_columns(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
         Reformat data frame columns from JSON format to clear string formatting.

         This method reformats various columns in the Pandas DataFrame object 'tg_data', which contains the test generation data. The specific columns that
         are reformatted are: 'xilConfigRequirements', 'parameters', 'playbook.setup.steps', 'playbook.execution.testcases', and 'matchingXils'. The
         'github_data' DataFrame object is also modified by updating the 'Pr_Label' and 'status' columns.

        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            # splitting column xilConfigRequirements
            self.tg_data['xilConfigRequirements'] = self.tg_data['xilConfigRequirements'].apply(
                lambda x: f"- {dict(list(self.tg_data['xilConfigRequirements'][0])[1])['label']} "
                          f":{dict(list(self.tg_data['xilConfigRequirements'][0])[0])['value']}"
                          f" :{dict(list(self.tg_data['xilConfigRequirements'][0])[0])['value']} \n"
                if len(list(self.tg_data['xilConfigRequirements'][0])) > 1 else
                f"- {dict(list(self.tg_data['xilConfigRequirements'][0])[0])['label']} "
                f" :{dict(list(self.tg_data['xilConfigRequirements'][0])[0])['value']} \n")
            logger.debug(f"preparing and reformation of column xilConfigRequirements")

            # extracting starting date
            self.tg_data['creation_date'] = self.tg_data['creationDate'].apply(lambda x: x.split("T")[0] if 'T' in x else "2020-12-12")

            # extracting starting time
            self.tg_data['creation_time'] = self.tg_data['creationDate'].apply(lambda x: x.split("T")[1].split('.')[0] if 'T' in x else "00:00:00")
            self.tg_data['startDate'] = self.tg_data['startDate'].astype(str)
            logger.debug(f"preparing and reformation of column startDate")

            # extracting creation date
            self.tg_data['start_date'] = self.tg_data['startDate'].apply(lambda x: x.split("T")[0] if 'T' in x else "2020-12-12")
            logger.debug(f"preparing and reformation ofcolumn start_date")

            # extracting creation time
            self.tg_data['start_time'] = self.tg_data['startDate'].apply(lambda x: x.split("T")[1].split('.')[0] if 'T' in x else "00:00:00")
            self.tg_data['startDate'] = self.tg_data['startDate'].astype(str)
            logger.debug(f"preparing and reformation of column start_time")

            # preparing columns parameters
            self.tg_data['pdxPath'] = self.tg_data.parameters.apply(lambda x: str(dict(list(x)[0])['value']) if len(list(x[0])) > 1 else x)
            logger.debug(f"preparing and reformation of column parameters")

            # preparing columns prNumber
            self.tg_data['prNumber'] = self.tg_data.parameters.apply(lambda x: str(dict(list(x)[1])['value']) if len(list(x[0])) > 1 else x)

            logger.debug(f"preparing and reformation of column playbook.setup.steps")

            # preparing columns 'playbook.execution.testcases'
            self.tg_data['playbook.execution.testcases'] = self.tg_data['playbook.execution.testcases'].apply(
                lambda x: f"-relativePath: {dict(list(x)[0])['relativePath']}\n"
                          f"-type: {dict(list(x)[0])['type']}\n")
            logger.debug(f"preparing and reformation of column playbook.execution.testcases")

            # preparing columns 'matchingXils'
            self.tg_data['matchingXils'] = self.tg_data['matchingXils'].apply(lambda x: f"{dict(list(x)[0])['testbench']}")
            logger.debug(f"preparing and reformation of column matchingXils")

            # preparing pr label column
            self.github_data['Pr_Label'] = self.github_data['Pr_Label'].apply(get_pr_label)
            logger.debug(f"preparing and reformation of column Pr_Label")

            # prepare status column
            logger.debug(f"preparing and reformation of column status")
            self.github_data['status'] = self.github_data.Pr_Label
            self.github_data['status'] = self.github_data['status'].apply(assign_status)

            self.tg_data['inconsistency'] = self.tg_data['testguide_verdict'] + ' ' + self.github_data['status']

            self.tg_data['inconsistency'] = self.tg_data['inconsistency'].apply(lambda x: 'MATCH' if x.count("PASSED") == 2 else "Don't MATCH")

        except Exception as ex:
            traceback.print_exc()
            print(traceback.TracebackException)
            raise Exception(f"there was an error during the preparation of columns : error message : {ex}")
        else:
            logger.info("columns preparation finished successfully ")

    def cleaning_data(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        replace nan values by empty strings
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            self.tg_data.fillna("", inplace=True)
        except Exception as ex:
            raise Exception(f"there was an error during data cleaning : error message :  {ex}")
        else:
            logger.info("data cleaning finished successfully ")

    def type_casting(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
          Convert columns in the Telegram and Github dataframes to their appropriate data types. Specifically:
            - Convert 'creationDate' and 'startDate' columns in the Telegram dataframe to separate 'creation_date',
              'creation_time', 'start_date', and 'start_time' columns. Convert these columns to datetime and time
              formats as appropriate.
            - Convert 'prNumber' column in both the Telegram and Github dataframes to string data type.
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            self.tg_data['creationDate'] = self.tg_data['creationDate'].astype(str)
            logger.debug(f"type casting of of column creationDate")

            # changing date columns to date time format
            self.tg_data['start_date'] = pd.to_datetime(
                self.tg_data['start_date'])  # convert 'start_date' column to datetime type
            self.tg_data['creation_date'] = pd.to_datetime(self.tg_data['creation_date'])  # convert 'creation_date' column to datetime type
            logger.debug(f"type casting of  column creation_date")

            # changing time columns to  time format
            self.tg_data['creation_time'] = pd.to_datetime(self.tg_data['creation_time'],
                                                           format='%H:%M:%S').dt.time  # convert 'creation_time' column to time type
            logger.debug(f"type casting of  column creation_time")
            self.tg_data['start_time'] = pd.to_datetime(self.tg_data['start_time'],
                                                        format='%H:%M:%S').dt.time  # convert 'start_time' column to time type
            logger.debug(f"type casting of  column start_time")

            # changing prNumber columns to  time str on both data
            self.tg_data['prNumber'] = self.tg_data['prNumber'].astype('str')
            logger.debug(f"type casting of of column prNumber on tg data ")
            self.github_data['prNumber'] = self.github_data['prNumber'].astype('str')
            logger.debug(f"type casting of  column prNumber on github data ")

            # changing taskId columns to  time str
            self.tg_data['taskId'] = self.tg_data['taskId'].astype('str')
            logger.debug(f"type casting of  column taskId")

        except Exception as ex:
            raise Exception(f"there was an error during columns type casting : error message : {ex}")
        else:
            logger.info("columns type casting finished successfully ")

    @staticmethod
    def get_last_selected_data_days(data, number_of_days):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        get last selected number of days from the given data
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        # convert the 'Date' column to datetime format
        data['Date'] = pd.to_datetime(data['creation_date'])

        # get the start and end dates for the last 7 days
        end_date = data['Date'].max()
        start_date = end_date - timedelta(days=number_of_days)

        # filter the data frame based on the date range
        data = data[(data['Date'] >= start_date) & (data['Date'] <= end_date)]
        data.reset_index(inplace=True, drop=True)
        data['Date'] = data['Date'].dt.date
        return data

    def prepare_all_data(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Performs the following steps to prepare and merge data:
            1. Reformats columns of data
            2. Cleans data by removing null values and duplicates
            3. Type casts columns to appropriate data types
            4. Merges tg_data and github_data on 'prNumber' column
            5. Saves the merged data to a CSV file named 'tg_data.csv'
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            # reformat columns
            logger.info(f"start preparing and reformating columns ")
            self.reformat_columns()

            # cleaning data
            logger.info("start cleaning data")
            self.cleaning_data()

            # type casting
            logger.info("start type casting ")
            self.type_casting()

            # merging tg_data and github_data
            logger.info("merging test guide and github data ")
            self.all_data = pd.merge(self.tg_data, self.github_data, on='prNumber')

            # get last 7 days data
            self.all_data = self.get_last_selected_data_days(self.all_data)
            # saving Data
            logger.info("writing csv data")
            report_path = os.path.normpath(os.path.join(self.workspace, 'sanity_data.csv'))

            self.all_data.to_csv(report_path)
        except Exception as ex:
            logger.error(f"there was an error during data preparation error message {ex}")
        else:
            logger.info('data preparation finished successfully')
