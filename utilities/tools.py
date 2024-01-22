import argparse
import logging
import os
import time
from argparse import Namespace
from datetime import datetime
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def sleep(retryAfter):
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    Blocks until the specified retry-after point in time is reached.
    :param retryAfter: Value of the Retry-After HTTP header
    :type: str
    :return: None
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """

    if not retryAfter:
        logger.info("Server didn't send a Retry-After header, using default value")
        sleepSec = 60
    elif retryAfter.isdigit():
        sleepSec = int(retryAfter)
    else:
        # RFC 1123 format: Thu, 01 Dec 1994 16:00:00 GMT
        dateRetryAfter = datetime.strptime(retryAfter, '%a, %d %b %Y %H:%M:%S GMT')
        dateNow = datetime.now()
        sleepSec = int(dateRetryAfter.timestamp() - dateNow.timestamp())
    logger.info(f"Retrying after {sleepSec} seconds")
    time.sleep(sleepSec)


def clean_output_directory(workspace):
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    Recursively removes all files and directories within a given directory.
    :param:workspace (str): The path of the directory to clean.
    :return:None
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    # Get the list of files and directories in the directory
    logger.debug(f"cleaning directory {workspace}")
    contents = os.listdir(workspace)

    # Iterate over each file and directory in the directory
    for item in contents:
        item_path = os.path.join(workspace, item)

        # If the item is a file, remove it
        if os.path.isfile(item_path):
            logger.debug(f"removing file {item_path}")
            os.remove(item_path)

        # If the item is a directory, recursively clean it
        elif os.path.isdir(item_path):
            logger.debug(f"removing directory {item_path}")
            clean_output_directory(item_path)
            os.rmdir(item_path)


def prepare_workspace(workspace):
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    Creates three new directories within the provided workspace directory. The new directories are named
    'grouped_by_date_and_branch', 'grouped_by_date', and 'grouped_by_date_and_machine'.

    :param workspace: A string representing the path to the workspace directory where the new directories should be created.
    :return: None
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    # Define the names of the new directories
    directories = ['grouped_by_date_and_branch', 'grouped_by_date', 'grouped_by_date_and_machine']

    # Create each new directory
    for directory in directories:
        logger.debug(f"creating directory {directory}")
        new_directory = os.path.join(workspace, directory)
        os.mkdir(new_directory)


def argument_parser() -> Namespace:
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    | parse generate coding data arguments which are
    |       - outputPath
    :return: parsed arguments
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--outputPath", type=str, help="Output path here to generate ExecutionPlan.json", required=True)

    return parser.parse_args()


def replace_with_tg_task_link(cell):
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    Given a string of comma-separated TG task IDs, replaces each ID with a link to the corresponding task on the BMW Group
    Task Gateway platform. Returns the updated string with links.

    Args:
    - cell: a string of comma-separated TG task IDs (e.g. "123, 456, 789")

    Returns: - a string with each task ID replaced by a link to the corresponding task on the BMW Group Task Gateway platform (e.g. "<a
    href='https://tg1.tg-prod.bmwgroup.net/execution/task/123?5'>123</a>, <a href='https://tg1.tg-prod.bmwgroup.net/execution/task/456?5'>456</a>,
    <a href='https://tg1.tg-prod.bmwgroup.net/execution/task/789?5'>789</a>")
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    task_ids = cell.split(",")
    new_task_id_list = []
    for task_id in task_ids:
        task_id = task_id.replace(" ", "")
        link = f"<a href='https://tg1.tg-prod.bmwgroup.net/execution/task/{task_id}'>{task_id}</a>"
        new_task_id_list.append(link)
    return ", ".join(new_task_id_list)


def replace_with_github_task_link(cell):
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    Given a string of comma-separated GitHub pull request IDs, replaces each ID with a link to the corresponding pull request
    on the BMW Group GitHub instance. Returns the updated string with links.

    Args:
    - cell: a string of comma-separated GitHub pull request IDs (e.g. "123, 456, 789")

    Returns: - a string with each pull request ID replaced by a link to the corresponding pull request on the BMW Group GitHub instance (e.g. "<a
    href='https://cc-github.bmwgroup.net/ipbasis/ipb/pull/123?5'>123</a>, <a href='https://cc-github.bmwgroup.net/ipbasis/ipb/pull/456?5'>456</a>,
    <a href='https://cc-github.bmwgroup.net/ipbasis/ipb/pull/789?5'>789</a>")
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    pull_request_id = cell.split(",")
    new_pull_request_list = []
    for pull_request in pull_request_id:
        pull_request = pull_request.replace(" ", "")
        link = f"<a href='https://cc-github.bmwgroup.net/ipbasis/ipb/pull/{pull_request}'>{pull_request}</a>"
        new_pull_request_list.append(link)
    return ", ".join(new_pull_request_list)


def assign_status(cell):
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    Returns a string indicating whether a given cell passes or fails a sanity check.

    Args:
    - cell (str): A string that may contain the text 'SANITY_CHECK_PASSED'.

    Returns:
    - A string: If the input string contains the text 'SANITY_CHECK_PASSED', the function returns
      'Passed'. Otherwise, it returns 'Failed'.
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """

    if 'SANITY_CHECK_PASSED' in str(cell).replace(" ", ""):
        message = 'PASSED'
    elif 'SANITY_CHECK_RED_FLAG' in str(cell).replace(" ", ""):
        message = 'RED_FLAG'
    elif 'SANITY_CHECK_FAILED' in str(cell).replace(" ", ""):
        message = 'FAILED'
    elif 'SANITY_CHECK_ERROR' in str(cell).replace(" ", ""):
        message = 'ERROR'
    elif 'SANITY_CHECK' and 'SWBK-PDX-READY':
        message = 'READY'
    else:
        message = "Unknown"
    return message


def get_pr_label(cell):
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    Returns a comma-separated string of accepted labels from a given string.

    Args:
    - cell (str): A string containing one or more comma-separated labels.

    Returns:
    - A string: A comma-separated string of accepted labels from the input string. The accepted
      labels are 'SANITY_CHECK_PASSED', 'SANITY_CHECK_FAILED', and 'SANITY_CHECK_ERROR'. If none of
      the labels in the input string are accepted, the function returns an empty string.
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    if 'SANITY_CHECK_PASSED' in str(cell).replace(" ", ""):
        return 'SANITY_CHECK_PASSED'
    elif 'SANITY_CHECK_RED_FLAG' in str(cell).replace(" ", ""):
        return 'SANITY_CHECK_RED_FLAG'
    elif 'SANITY_CHECK_FAILED' in str(cell).replace(" ", ""):
        return 'SANITY_CHECK_FAILED'
    elif 'SANITY_CHECK_ERROR' in str(cell).replace(" ", ""):
        return 'SANITY_CHECK_ERROR'
    elif 'SANITY_CHECK' and 'SWBK-PDX-READY':
        return 'PDX_READY_SANITY_CHECK'
    else:
        return "UNDEFINED"


def assign_colors(column) -> List[str]:
    """
    Assigns a color to each unique value in a pandas Series.

    Parameters
    ----------
    column : pd.Series
        A pandas Series containing the unique values to be colored.

    Returns
    -------
    List[str]
        A list of colors corresponding to the unique values in the input Series.
    """

    color_list = []
    for value in column:
        if value == "SANITY_CHECK_PASSED":
            color = "#4CAF50"
        elif value == "SANITY_CHECK_FAILED":
            color = "#BF0A30"
        elif value == "UNDEFINED":
            color = "#FFC090"
        elif value == "SANITY_CHECK_ERROR":
            color = "orange"
        elif value == "PDX_READY_SANITY_CHECK":
            color = "#4F97A3"
        else:
            color = "#990000"
        color_list.append(color)
    return color_list


def assign_pass_fail_colors(column) -> List[str]:
    """
    Assigns a color to each unique value in a pandas Series.

    Parameters
    ----------
    column : pd.Series
        A pandas Series containing the unique values to be colored.

    Returns
    -------
    List[str]
        A list of colors corresponding to the unique values in the input Series.
    """

    color_list = []
    for value in column:
        if value.replace(" ","") == "PASSED":
            color = "#4CAF50"
        elif value.replace(" ","") == "FAILED":
            color = "#BF0A30"
        else:
            color = "#990000"
        color_list.append(color)
    return color_list
