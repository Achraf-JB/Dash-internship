from typing import Dict, Any
from data_preparation.prepare_data import DataPreparation
from filtrage.filtrage import DataFilter
from plots_preparation.group_by_date_and_machine import GrouperByDateMachine
from plots_preparation.group_data_by_date import GrouperByDate
from plots_preparation.group_data_by_date_and_branch import GrouperByDateBranchMachine
from utilities.generate_reports_tools import generate_html_for_each_domain, generate_global_report
from utilities.tools import clean_output_directory, prepare_workspace
import pandas as pd


class ReportGenerator:
    """
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    A class to generate reports from data extracted from test guide and Github.

    ...

    Attributes
    ----------
    workspace : str
        A string representing the directory where the reports will be generated.
    data : DataFrame
        A pandas DataFrame containing the extracted and prepared data.

    Methods
    -------
    prepare_data():
        Extract data from test guide and Github then prepare it (cleaning, type casting ...).
    clean_and_prepare_workspace():
        Clean the workspace directory and create new sub-folders to store the generated reports.
    generate_all_reports():
        Generate all reports in the specified directory.
        Steps:
        - Generate html reports for data grouped by date
        - Generate html reports for data grouped by date and Machine
        - Generate html reports for data grouped by date, branch, and machine
        - Generate Global Report.
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    """

    def __init__(self, workspace: str) -> None:
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
           Constructs all the necessary attributes for the ReportGenerator object.

           Parameters
           ----------
           workspace : str
               A string representing the directory where the reports will be generated.
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        self.workspace = workspace
        #self.data =Filtrage.get_last_selected_data_days(self.days)
        self.data = pd.read_csv(r"C:\Users\HP\OneDrive\Bureau\sanity_check_dashboards\sanity_data.csv")

    def prepare_data(self) -> Dict[str, Any]:
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Extract data from test guide and github then prepare it (cleaning, type casting ...)

        :return: A dictionary containing the extracted and prepared data
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        # Clean workspace
        self.clean_and_prepare_workspace()
        data_preparator = DataPreparation(workspace=self.workspace)
        data_preparator.prepare_all_data()
        return data_preparator.all_data

    def clean_and_prepare_workspace(self) -> None:
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Clean the workspace and create new sub-folders
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        clean_output_directory(workspace=self.workspace)
        prepare_workspace(workspace=self.workspace)

    def dash_date(self, value, nombre):
        data_filter = DataFilter(self.data)
        number_of_days = int(nombre)
        filtered_data = data_filter.filter_by_number_of_days(number_of_days)
        grouper_by_date = GrouperByDate(data=filtered_data, workspace=self.workspace,number_of_days=nombre)
        grouper_by_date.group_table_data()
        grouper_by_date.group_data_for_pie_chart()
        grouper_by_date.group_bar_chart_data()
        grouper_by_date.group_table_data()
        if value == "pr_label":
            return grouper_by_date.generate_bar_chart_plot_on_pr_label()
        elif value == "tg_verdict":
            return grouper_by_date.generate_bar_chart_plot_on_tg_verdict()
        else:
            return grouper_by_date.generate_table_bar_plot()

    def dash_machine(self, name, nombre):
        data_filter = DataFilter(self.data)
        number_of_days = int(nombre)
        filtered_data = data_filter.filter_by_number_of_days(number_of_days)
        grouper_by_date_machine = GrouperByDateMachine(data=filtered_data, workspace=self.workspace,number_of_days=nombre)
        grouper_by_date_machine.group_data()
        grouper_by_date_machine.genrate_grouped_data_for_each_machine()
        return grouper_by_date_machine.generate_plot_for_machine_donnee(name)

    def dash_machine_global(self,nombre):
        data_filter = DataFilter(self.data)
        number_of_days = int(nombre)
        filtered_data = data_filter.filter_by_number_of_days(number_of_days)
        grouper_by_date_machine = GrouperByDateMachine(data=filtered_data, workspace=self.workspace,number_of_days=nombre)
        grouper_by_date_machine.group_data()
        grouper_by_date_machine.genrate_grouped_data_for_each_machine()
        return grouper_by_date_machine.generate_plot_for_general_report()

    def dash_branche_global(self,nombre):
        data_filter = DataFilter(self.data)
        number_of_days = int(nombre)
        filtered_data = data_filter.filter_by_number_of_days(number_of_days)
        grouper_by_date_machine_branch = GrouperByDateBranchMachine(data=filtered_data, workspace=self.workspace,number_of_days=nombre)
        grouper_by_date_machine_branch.group_data()
        grouper_by_date_machine_branch.group_bar_chart_data()
        return grouper_by_date_machine_branch.generate_plot_for_general_report()

    def dash_branche_bar_chart(self,nombre):
        data_filter = DataFilter(self.data)
        number_of_days = int(nombre)
        filtered_data = data_filter.filter_by_number_of_days(number_of_days)
        grouper_by_date_machine_branch = GrouperByDateBranchMachine(data=filtered_data, workspace=self.workspace,number_of_days=nombre)
        grouper_by_date_machine_branch.group_data()
        grouper_by_date_machine_branch.group_bar_chart_data()
        return grouper_by_date_machine_branch.generate_statistucs_bar_chart()

    def dash_branch(self, name , nombre):
        data_filter = DataFilter(self.data)
        number_of_days = int(nombre)
        filtered_data = data_filter.filter_by_number_of_days(number_of_days)
        grouper_by_date_machine_branch = GrouperByDateBranchMachine(data=filtered_data, workspace=self.workspace,number_of_days=nombre)
        grouper_by_date_machine_branch.group_data()
        grouper_by_date_machine_branch.group_bar_chart_data()
        grouper_by_date_machine_branch.generate_plot_for_general_report()
        grouper_by_date_machine_branch.generate_statistucs_bar_chart()
        grouper_by_date_machine_branch.generate_grouped_data_for_each_machine()
        return grouper_by_date_machine_branch.generate_plot_for_target_donnee(name)

    def selection_machine(self):
        data = self.data
        return data.matchingXils.unique()

    def generate_all_reports(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Generate all reports in the specified directory then generate the global report

        Steps:
        - Generate html reports for data grouped by date
        - Generate html reports for data grouped by date and Machine
        - Generate html reports for data grouped by date, branch, and machine
        - Generate Global Report
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """

        # Generate html reports for data grouped by date
        grouper_by_date = GrouperByDate(data=self.data, workspace=self.workspace)
        grouper_by_date.group_table_data()
        grouper_by_date.group_data_for_pie_chart()
        grouper_by_date.group_bar_chart_data()
        grouper_by_date.group_table_data()

        grouper_by_date.generate_bar_chart_plot_on_pr_label()
        grouper_by_date.generate_bar_chart_plot_on_tg_verdict()
        grouper_by_date.generate_table_bar_plot()

        # Generate html reports for data grouped by date and Machine
        grouper_by_date_machine = GrouperByDateMachine(data=self.data, workspace=self.workspace)
        grouper_by_date_machine.group_data()
        grouper_by_date_machine.generate_plot_for_general_report()
        grouper_by_date_machine.genrate_grouped_data_for_each_machine()
        grouper_by_date_machine.generate_plots_for_each_machine()

        # Generate global report for data grouped by date and machine
        generate_html_for_each_domain(workspace=self.workspace, directory=grouper_by_date_machine.reports_directory)

        # Generate html reports for data grouped by date, branch, and machine
        grouper_by_date_machine_branch = GrouperByDateBranchMachine(data=self.data, workspace=self.workspace)
        grouper_by_date_machine_branch.group_data()
        grouper_by_date_machine_branch.group_bar_chart_data()
        grouper_by_date_machine_branch.generate_plot_for_general_report()
        grouper_by_date_machine_branch.generate_statistucs_bar_chart()
        grouper_by_date_machine_branch.generate_grouped_data_for_each_machine()
        grouper_by_date_machine_branch.generate_plots_for_each_machine()

        # Generate global report for data grouped by date, branch, and machine
        generate_html_for_each_domain(workspace=self.workspace,
                                      directory=grouper_by_date_machine_branch.reports_directory)

        # Generate Global Report
        generate_global_report(workspace=self.workspace)
