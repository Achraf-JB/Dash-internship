from enum import Enum

import plotly


class ColorScale(Enum):
    """
    A class representing a color scale for data visualization.

    Constants:
        COLOR_SCALE (List[str]): A color scale generated from the Blues palette.

    """

    COLOR_SCALE = plotly.colors.sequential.Blues


class TableColors(Enum):
    """
    A class representing the colors associated with tables.

    Constants:
        COLUMNS_COLOR (str): The color associated with the table columns.
        COLUMNS_FONT_COLOR (str): The font color associated with the table columns.
        ROWS_COLOR (str): The color associated with the table rows.

    """

    COLUMNS_COLOR = "#495057"
    COLUMNS_FONT_COLOR = "#ffffff"
    ROWS_COLOR = "#dee2e6"


class ExtractedDataComponenent:
    """
    A class representing the components of extracted data.

    Constants:
        TASK_EXECUTION (str): Represents the task execution data component.
        TASK_REPORTING (str): Represents the task reporting data component.

    """

    TASK_EXECUTION = "task_execution_data"
    TASK_REPORTING = "task_reporting_data"


class StatusColors(Enum):
    """
    A class representing the colors associated with different statuses.

    Constants:
        FAIL_COLOR (str): The color associated with the fail status.
        PASS_COLOR (str): The color associated with the pass status.
        ERROR_COLOR (str): The color associated with the error status.
        UNDEFINED_COLOR (str): The color associated with the undefined status.
        RED_FLAG_COLOR (str): The color associated with the red flag status.
        COLOR_DICT (dict): A dictionary mapping specific statuses to their corresponding colors.

    """

    FAIL_COLOR = "#FF5722"
    PASS_COLOR = "#4CAF50"
    ERROR_COLOR = "orange"
    UNDEFINED_COLOR = "#ced4da"
    RED_FLAG_COLOR = "#990000"
    COLOR_DICT = {
        'SANITY_CHECK_PASSED': '#4CAF50',
        'UNDEFINED': '#ced4da',
        'SANITY_CHECK_FAILED': '#FF5722',
        'SANITY_CHECK_RED_FLAG': "#990000",
        "SANITY_CHECK_ERROR": "orange",
        "PDX_READY_SANITY_CHECK": "#4F97A3"

    }
