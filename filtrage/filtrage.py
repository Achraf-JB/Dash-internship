import os
import traceback
from datetime import timedelta

import pandas as pd
from data_extraction.github_data_handler import GithubConnector

from data_extraction.testguide__data_handler import TestGuideDataPreparer
import logging

from utilities.tools import get_pr_label, assign_status

logger = logging.getLogger(__name__)

from datetime import datetime, timedelta


class DataFilter:
    def __init__(self, data):
        self.data = data
        self.data['creation_date'] = pd.to_datetime(self.data['creation_date'])  # Convertir la colonne en Timestamp

    def filter_by_number_of_days(self, number_of_days):
        self.data['Date'] = pd.to_datetime(self.data['creation_date'])

        # get the start and end dates for the last 7 days
        end_date = self.data['Date'].max()
        #start_date = end_date - timedelta(days=number_of_days)
        start_date = end_date - timedelta(days=number_of_days - 1)
        # filter the data frame based on the date range
        data = self.data[(self.data['Date'] >= start_date) & (self.data['Date'] <= end_date)]
        data.reset_index(inplace=True, drop=True)
        data['Date'] = data['Date'].dt.date
        return data

