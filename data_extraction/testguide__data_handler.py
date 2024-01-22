# -*- coding: utf-8 -*-
import ast
import datetime
import logging
import time
from typing import Dict

import pandas as pd
import requests
import urllib3

from utilities.authenticator import Authenticator
from utilities.component import ExtractedDataComponenent
from utilities.logger import logger
from utilities.tools import sleep

urllib3.disable_warnings()
logging.captureWarnings(True)


class TestguideDataExtractor:
    """
    A class that extracts and prepares data from TestGuide using an API.

    Args:
        host (str): The host URL of the TestGuide API.
        authenticators (str): The name of the authenticator to retrieve authentication data.
        projectId (int, optional): The project ID extracted from TestGuide. Defaults to 7.

    Attributes:
        host (str): The host URL of the TestGuide API.
        __authenticator (Authenticator): An instance of the Authenticator class.
        __auth_key (str): The authentication key retrieved from the authenticator.
        project_id (int): The project ID extracted from TestGuide.
        extracted_data (Dict[str, dict]): A dictionary to store the extracted data.

    """

    def __init__(
            self,
            host,
            authenticators,
            projectId=7
    ):
        self.host = host
        self.__authenticator = Authenticator(authenticators, host)
        self.__auth_key = self.__authenticator.get_authentication()
        # verify authentication
        self.__authenticator.verify_authentification()
        self.project_id = projectId
        self.extracted_data = {}

    def wait_for_readiness(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Blocks until TEST-GUIDE is ready to use.
        :return: None
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        url = f'{self.host}/api/health/ready'
        retry = 0
        while retry < 10:
            response = requests.get(url, verify=False)

            if response.status_code == 200:
                break
            elif response.status_code in [429, 503]:
                sleep(response.headers.get("Retry-After"))
            else:
                retry += 1
                raise Exception("Unexpected response")

        logger.info("test guide is ready")

    def extract_task_execution_data(
            self,
            time_period: datetime,
            names: str = "SanityCheckTesting",
            offset: int = 0,
            limit: int = 999,
            project_id: int = 7,
            sort: str = "DESC",
            ascending: str = 'PRIORITY',
            states: str = "FINISHED"
    ):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
            extract all needed data from tg using rest api
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        :param names: names of execution to be extracted ("SanityCheckTesting"...)
        :param offset: default value 0
        :param limit: number of lines extracted
        :param project_id: project id extracted from test guide
        :param sort:["DESC", "ASC"]
        :param ascending: ['PRIORITY', 'STATE', 'STARTED', 'ID', 'NAME', 'FINISHED', "CREATED"]
        :param states: ["FINISHED"]
        :param time_period: range of date when extracting data
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        logger.info(f"start data extraction for testguide task execution")
        # extract data using API
        # headers
        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        # Filter by the name and state, as before, and also by the created date
        payload = {
            "names": [names],
            "states": [states],
            "created": {
                "from": time_period.isoformat(),
                "to": datetime.datetime.now().isoformat()
            }
        }
        url = f"{self.host}/api/v2/execution/task/filter?projectId={project_id}&sort={ascending}" \
              f"&ascending={sort}&offset={offset}&limit={limit}&authKey={self.__auth_key}"
        number_of_retries = 0
        while number_of_retries < 3:
            try:
                if number_of_retries > 0:
                    logger.info("retrying after 60 second")
                    sleep(retryAfter=60)
                # Make the request to the REST API
                self.extracted_data[ExtractedDataComponenent.TASK_EXECUTION] = requests.post(url, verify=False,
                                                                                             headers=headers,
                                                                                             json=payload)
            except Exception as ex:
                logger.exception(ex)
                logger.error(f"failed to get data from test guide  {ex}")
                number_of_retries += 1
            else:
                self.extracted_data[ExtractedDataComponenent.TASK_EXECUTION].close()  # close the connection

                if self.extracted_data[ExtractedDataComponenent.TASK_EXECUTION].status_code == 200:
                    logger.info("task execution data is extracted successfully from test guide ")
                else:
                    logger.error(
                        f"Error accessing PRs API endpoint: {self.extracted_data[ExtractedDataComponenent.TASK_EXECUTION].content}")
                break

    def extract_task_reporting_data(
            self,
            time_period: datetime,
            testcases_list=None,
            offset: int = 0,
            limit: int = 999,
            project_id: int = 7,
    ):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
            extract all needed data from tg using rest api
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        :param testcases_list: list of testcases to be extracte d
        :param offset: default value 0
        :param limit: number of lines extracted
        :param project_id: project id extracted from test guide
        :param time_period: range of date when extracting data
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        logger.info(f"start data extraction for testguide task reporting")
        # extract data using API
        # headers
        if testcases_list is None:
            testcases_list = ["UT_CopyReport", "TC_Smoke", "TC_Flashing", "Prepare", "FlashingAnalysis",
                              "FlashingAnalysis"]
        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        # Filter by the name and state, as before, and also by the created date
        payload = {
            "testCaseName": testcases_list,

            "dateFrom": time_period.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "dateTo": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),

            "verdicts": [
                "NONE",
                "PASSED",
                "INCONCLUSIVE",
                "ERROR",
                "FAILED"
            ]
        }

        url = f"{self.host}/api/report/testCaseExecutions/filter?projectId={project_id}&" \
              f"offset={offset}&limit={limit}&authKey={self.__auth_key}"
        number_of_retries = 0
        while number_of_retries < 3:
            try:
                if number_of_retries > 0:
                    logger.info("retrying after 60 second")
                    sleep(retryAfter=60)
                # Make the request to the REST API
                self.extracted_data[ExtractedDataComponenent.TASK_REPORTING] = requests.post(url, verify=False,
                                                                                             headers=headers,
                                                                                             json=payload)
            except Exception as ex:
                logger.exception(ex)
                logger.error(f"failed to get data from test guide  {ex}")
                number_of_retries += 1
            else:
                self.extracted_data[ExtractedDataComponenent.TASK_REPORTING].close()  # close the connection

                if self.extracted_data[ExtractedDataComponenent.TASK_REPORTING].status_code == 200:
                    logger.info("task reporting data is extracted successfully from test guide ")
                else:
                    logger.error(
                        f"Error accessing PRs API endpoint: {self.extracted_data[ExtractedDataComponenent.TASK_REPORTING].content}")
                break


class TestGuideDataPreparer:
    """
    A class for preparing data extracted from TestGuide using API.

    Args:
        host (str): The host URL of the TestGuide API.
        authenticators (str): The authenticators for TestGuide API access.
        projectId (int): The project ID of TestGuide. Default is 7.

    Attributes:
        testguide_data_handler (TestguideDataExtractor): The TestguideDataExtractor instance for handling data extraction.
        testguide_task_reporting_data (pd.DataFrame): The DataFrame containing the extracted TestGuide task reporting data.
        testguide_task_execution_data (pd.DataFrame): The DataFrame containing the extracted TestGuide task execution data.
        full_testguide_data (pd.DataFrame): The DataFrame containing the merged TestGuide data.

    Methods:
        cleanup_data(): Cleans up the TestGuide task execution and task reporting data by replacing missing values with empty strings.
        rename_and_cast_columns(): Renames and casts the columns of the TestGuide task reporting data.
        prepare_testguide_data(): Prepares the TestGuide data by cleaning up the data, grouping the reporting data, and merging it with the task execution data.
        extract_data_from_testguide(number_of_days: int = 7): Extracts data from TestGuide for a specified number of days and prepares the data.

    """

    def __init__(
            self,
            host="https://tg1.tg-prod.bmwgroup.net",
            authenticators="tg1.tg-prod.bmwgroup.net",
            projectId=7
    ):
        self.testguide_data_handler = TestguideDataExtractor(host, authenticators, projectId)

        self.testguide_task_reporting_data = None
        self.testguide_task_execution_data = None
        self.full_testguide_data = pd.DataFrame()

    def cleanup_data(self):
        """
        Cleans up the TestGuide task execution and task reporting data by replacing missing values with empty strings.
F
        Returns:
            None

        """
        self.testguide_task_execution_data.fillna("", inplace=True)
        self.testguide_task_reporting_data.fillna("", inplace=True)

    def rename_and_cast_columns(self):
        """
        Renames and casts the columns of the TestGuide task reporting data.

        Returns:
            None

        """
        # Convert data type and rename columns
        self.testguide_task_reporting_data['TT_TASK_ID'] = self.testguide_task_reporting_data['TT_TASK_ID'].astype(
            'int64')
        self.testguide_task_reporting_data.rename(columns={"TT_TASK_ID": 'taskId'}, inplace=True)
        self.testguide_task_reporting_data.rename(columns={"verdict": 'testguide_verdict'}, inplace=True)
        self.testguide_task_reporting_data['testcases_verdict'] = self.testguide_task_reporting_data[
            'testcases_verdict'].apply(
            lambda x: x.replace('-', ''))
        self.testguide_task_reporting_data['testguide_verdict'] = self.testguide_task_reporting_data['testguide_verdict'].apply(
            lambda x: 'FAILED' if 'ERROR' in x else 'PASSED')

    def prepare_testguide_data(
            self
    ):
        """
        Prepares the TestGuide data by cleaning up the data, grouping the reporting data, and merging it with the task execution data.

        Returns:
            None

        """

        def get_constants(constants):
            """
            Extracts constants from the reporting data and returns them as a dictionary.

            Args:
                constants (list): List of constants in the reporting data.

            Returns:
                dict or None: Dictionary containing the extracted constants or None if there are no constants.

            """
            constants_dict = {}
            if constants:
                for constant in constants:
                    constants_dict[constant.get('key')] = constant.get('value')
                return constants_dict
            return None

        # Cleanup task reporting and task execution data
        self.cleanup_data()
        # prepare column testcase testcases_verdict
        logger.debug(f"preparing and reformation of column testcases_verdict")
        self.testguide_task_reporting_data['verdict'] = self.testguide_task_reporting_data['verdict'].apply(
            lambda x: x if 'ERROR' in x else "")
        self.testguide_task_reporting_data['testcases_verdict'] = self.testguide_task_reporting_data[
                                                                      'testCaseName'] + ' ' + \
                                                                  self.testguide_task_reporting_data['verdict']
        self.testguide_task_reporting_data['testcases_verdict'] = self.testguide_task_reporting_data[
            'testcases_verdict'].apply(
            lambda x: x.replace('ERROR', '') if 'ERROR' in x else "")

        # prepare column testcase verdict
        logger.debug(f"preparing and reformation of column verdict")

        self.testguide_task_reporting_data['constants'] = self.testguide_task_reporting_data['constants'].astype('str')
        # Group reporting data
        self.testguide_task_reporting_data['constants'] = self.testguide_task_reporting_data['constants'].apply(
            lambda x: ast.literal_eval(x))
        self.testguide_task_reporting_data['constants'] = self.testguide_task_reporting_data['constants'].apply(
            get_constants)
        self.testguide_task_reporting_data = self.testguide_task_reporting_data.groupby("reportId",
                                                                                        as_index=False).apply(
            lambda x: pd.Series({
                "testCaseName": '- '.join(x["testCaseName"]),
                "verdict": '- '.join(x["verdict"]),
                "testcases_verdict": '- '.join(x["testcases_verdict"]),
                "prNumber": "-".join(
                    x["constants"].apply(lambda constant_dict: constant_dict.get('prNumber', '')).unique().tolist()),
                "TT_TASK_ID": "-".join(
                    x["constants"].apply(lambda constant_dict: constant_dict.get('TT_TASK_ID', '')).unique().tolist()),
                "pdxPath": "-".join(
                    x["constants"].apply(lambda constant_dict: constant_dict.get('pdxPath', '')).unique().tolist()),
                "SanityType": "-".join(
                    x["constants"].apply(lambda constant_dict: constant_dict.get('SanityType', '')).unique().tolist()),
                "URL": "-".join(
                    x["constants"].apply(lambda constant_dict: constant_dict.get('URL', '')).unique().tolist()),

            }))
        # Convert data type and rename columns
        self.rename_and_cast_columns()

        # Merge reporting and execution data
        self.full_testguide_data = pd.merge(self.testguide_task_reporting_data, self.testguide_task_execution_data,
                                            on='taskId')

        # Display the head of the merged data
        self.full_testguide_data.head()

    def extract_data_from_testguide(
            self,
           # number_of_days: int = 7
            number_of_days
    ):
        """
        Extracts data from TestGuide for a specified number of days and prepares the data.

        Args:
            number_of_days (int): Number of days to extract data from. Default is 7.

        Returns:
            None

        """
        start_time = time.time()

        # Calculate the date 'number_of_days' ago
        time_period = datetime.datetime.now() - datetime.timedelta(days=number_of_days)

        # Extract task execution data
        self.testguide_data_handler.extract_task_execution_data(time_period=time_period)

        # Extract task reporting data
        self.testguide_data_handler.extract_task_reporting_data(time_period=time_period)

        test_guide_task_reporting_json_data = self.testguide_data_handler.extracted_data.get(
            ExtractedDataComponenent.TASK_REPORTING, None).json()
        test_guide_task_execution_json_data = self.testguide_data_handler.extracted_data.get(
            ExtractedDataComponenent.TASK_EXECUTION, None).json()
        # Get the extracted data from the data handler
        self.testguide_task_reporting_data = pd.json_normalize(test_guide_task_reporting_json_data)
        self.testguide_task_execution_data = pd.json_normalize(test_guide_task_execution_json_data)

        # Prepare TestGuide data
        self.prepare_testguide_data()

        end_time = time.time()
        running_time = end_time - start_time
        logger.info(f"data extraction from TestGuide finished. It took {running_time} seconds.")
