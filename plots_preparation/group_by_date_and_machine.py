import logging
import os
import pandas as pd
import plotly.colors
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
from filtrage.filtrage import DataFilter
from utilities.component import TableColors
from utilities.tools import replace_with_tg_task_link, replace_with_github_task_link

logger = logging.getLogger(__name__)


class GrouperByDateMachine:
    """
       A class that groups data by date and machine, and generates a bar chart and table containing data grouped by date and machine.

       Attributes:
           reports_directory (str): The directory in which the reports are stored.
           selected_columns (list): The columns that are selected from the input data.
           colorscale (list): A list of colors used in the plots.
           data_of_each_machine (list): A list containing grouped data for each machine.

       Methods:
           __init__(self, data: pd.DataFrame, workspace: str, number_of_days: int = 6) -> None:
               Initializes the GrouperByDateMachine object.
           group_data(self) -> None:
               Groups the data by date and machine.
           get_last_selected_data_days(self) -> None:
               Gets the last selected number of days from the given data.
           generate_plot_for_general_report(self, show_plot: bool = False) -> None:
               Generates a bar chart and table containing data grouped by date and machine for all machines.
       """
    reports_directory = "grouped_by_date_and_machine"
    selected_columns = ['matchingXils',
                        'name',
                        'taskId',
                        'prNumber',
                        'pdxPath',
                        'creation_date',
                        'Target_Branch',
                        'testcases_verdict',
                        'testguide_verdict',
                        'Pr_Label',
                        'state',
                        'inconsistency']

    # generate a color scale derived from orange
    colorscale = plotly.colors.sequential.Blues

    # list contain grouped data for each machine
    data_of_each_machine = []
    color_dict = {'SANITY_CHECK_PASSED': '#4CAF50',
                  'UNDEFINED': '#ced4da',
                  'SANITY_CHECK_FAILED': '#FF5722',
                  'SANITY_CHECK_RED_FLAG': "#990000",
                  "SANITY_CHECK_ERROR": "orange",
                  "PDX_READY_SANITY_CHECK": "#4F97A3"
                  }

    def __init__(self, data, workspace, number_of_days=6):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
       Initializes the GrouperByDateMachine object.

       Args:
           data (pd.DataFrame): The data to be grouped.
           workspace (str): The workspace directory.
           number_of_days (int): The number of days to display in the plots. Default is 6.
        --------------------------------------------------------------------------------------------------------------------------------------------------------
       """
        self.pivot_df = pd.DataFrame()
        self.data = data
        self.unique_machine_xils = list(self.data.matchingXils.unique())
        self.number_of_days = number_of_days
        self.grouped_data = pd.DataFrame()
        self.workspace = workspace

    def group_data(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        select only needed columns which are 'matchingXils', 'name','prNumber', 'pdxPath', 'creation_date', 'Target_Branch', 'status'
        then group data buu Date
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            logger.info("grouping data by date and machine xil")
            pr_info_data = self.data[self.selected_columns]
            pr_info_data = pr_info_data.rename(columns={'name': "NumberOf_PRs", "creation_date": "Date"})
            pr_info_data.Date = pr_info_data.Date.apply(lambda x: str(x).split("T")[0])

            # group the data by day and matchingXils, and apply a lambda function to concatenate the strings
            self.grouped_data = pr_info_data.groupby(['Date', 'matchingXils'], as_index=False).apply(lambda x: pd.Series({
                'NumberOf_PRs': ', '.join(x['NumberOf_PRs']),
                "taskId": ', '.join(x["taskId"].astype(str)),
                'pdxPath': ', '.join(x['pdxPath']),
                'prNumber': ', '.join(x['prNumber'].astype(str)),
                'inconsistency': ', '.join(x['inconsistency']),
                'Pr_Label': ', '.join(x['Pr_Label']),
                'Target_Branch': ', '.join(x['Target_Branch']),
                'testcases_verdict': ', '.join(x['testcases_verdict']),
                'testguide_verdict': ', '.join(x['testguide_verdict']),
                'state': ', '.join(x['state']),
            }))
            self.grouped_data['NumberOf_PRs'] = self.grouped_data['NumberOf_PRs'].apply(lambda x: len(x.split(',')))
            # Split matchingXils and pdxPath into multiple lines
            self.grouped_data['pdxPath'] = self.grouped_data['pdxPath'].str.replace(", ", "<br>")
            self.grouped_data['prNumber'] = self.grouped_data['prNumber'].apply(replace_with_github_task_link)
            self.grouped_data['prNumber'] = self.grouped_data['prNumber'].str.replace(", ", "<br>")
            self.grouped_data['Target_Branch'] = self.grouped_data['Target_Branch'].str.replace(", ", "<br>")
            self.grouped_data['taskId'] = self.grouped_data['taskId'].apply(replace_with_tg_task_link)
            self.grouped_data['state'] = self.grouped_data['state'].str.replace(", ", "<br>")
            self.grouped_data['testcases_verdict'] = self.grouped_data['testcases_verdict'].str.replace(", ", "<br>")
            self.grouped_data['testguide_verdict'] = self.grouped_data['testguide_verdict'].str.replace(", ", "<br>")
            self.grouped_data['inconsistency'] = self.grouped_data['inconsistency'].str.replace(", ", "<br>")
            self.grouped_data['Pr_Label'] = self.grouped_data['Pr_Label'].str.replace(", ", "<br>")
        except Exception as ex:
            raise Exception(f"there was an during grou^ping data by date and machine {ex}")
        else:
            logger.info("data grouped by date and machine xil finished successfully")

    def generate_plot_for_general_report(self, show_plot=True):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        generate bar chart and table containing data grouped by date and machine for all machines
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            logger.info("generating plot for data grouped by date and machine ")
            # create the table
            table = go.Table(
                header=dict(values=list(self.grouped_data.columns), fill_color=TableColors.COLUMNS_COLOR.value,
                            font_color=TableColors.COLUMNS_FONT_COLOR.value),
                cells=dict(values=[self.grouped_data.Date,
                                   self.grouped_data.matchingXils,
                                   self.grouped_data.NumberOf_PRs,
                                   self.grouped_data.taskId,
                                   self.grouped_data.prNumber,
                                   self.grouped_data.Target_Branch,
                                   self.grouped_data.testcases_verdict,
                                   self.grouped_data.testguide_verdict,
                                   self.grouped_data.state,
                                   self.grouped_data.Pr_Label,
                                   self.grouped_data.inconsistency
                                   ],
                           align="left",
                           fill_color=TableColors.ROWS_COLOR.value),
                columnwidth=[65, 80, 80, 60, 40, 60, 110, 75, 60,0])

            # create a dictionary that maps each value of the matchingXils column to a color in the colorscale variable
            color_dict = dict(zip(self.grouped_data.matchingXils.unique(), self.colorscale[2:]))

            # create the bar plot
            fig = go.Figure()
            for xil in self.grouped_data.matchingXils.unique():
                xil_df = self.grouped_data[self.grouped_data.matchingXils == xil].reset_index()
                fig.add_trace(go.Bar(x=xil_df.Date, y=xil_df.NumberOf_PRs, text=xil_df.NumberOf_PRs, name=xil, marker=dict(color=color_dict[xil])))

            # create subplots
            fig_subplots = make_subplots(rows=2, specs=[[{"type": "scatter"}], [{"type": "table"}]])

            # add the bar plot to the subplots
            for trace in fig.data:
                fig_subplots.add_trace(trace, row=1, col=1)

            # add the table to the subplots
            fig_subplots.add_trace(table, row=2, col=1)

            testbanches_names = " | ".join(self.grouped_data.matchingXils.unique())
            # update the layout of the subplots
            fig_subplots.update_layout(
                margin=dict(l=100, r=100, t=100, b=100),
                height=1600,
                width=1300,
                title=f'PRs by Date and matchingXils {testbanches_names}',
                showlegend=True,
                template='plotly_white',
                autosize=True
            )


            # html file path
            report_path = os.path.normpath(os.path.join(self.workspace, "grouped_by_date_and_machine/00__global__grouped_by_date_and_machine.html"))
            # Save the plot as an HTML file
            pio.write_html(fig_subplots, file=report_path)
            # show the subplots
            if show_plot:
                return fig_subplots
        except Exception as ex:
            raise Exception(f"there was an error during generating plots for data grouped by date and machine {ex}")

        else:
            logger.info("plots for data grouped by date and machine generated successfully ")

    def genrate_grouped_data_for_each_machine(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        group by data by date and machine XIl and return a list of all separated data
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:

            for machine in self.unique_machine_xils:
                logger.info("grouping data by date and target branch for each machine ")
                pr_info_data = self.data[self.selected_columns]

                pr_info_data = pr_info_data.rename(columns={'name': "NumberOf_PRs", "creation_date": "Date"})
                pr_info_data.taskId = pr_info_data.taskId.astype('str')
                logger.debug(f"generating data grouped by date for machine {machine}")
                pr_info_data = pr_info_data[pr_info_data['matchingXils'] == machine]
                pr_info_data.Date = pr_info_data.Date.apply(lambda x: str(x).split("T"[0]) if "T" in str(x) else str(x).split(" ")[0])

                # group the data by day and matchingXils, and apply a lambda function to concatenate the strings
                grouped_data = pr_info_data.groupby(['Date', 'Pr_Label', 'matchingXils'], as_index=False).apply(lambda x: pd.Series({
                    "NumberOf_PRs": ', '.join(x["NumberOf_PRs"]),
                    "taskId": ', '.join(x["taskId"]),
                    'prNumber': ', '.join(x['prNumber'].astype(str)),
                    'Target_Branch': ', '.join(x['Target_Branch']),
                    'testcases_verdict': ', '.join(x['testcases_verdict']),
                    'testguide_verdict': ', '.join(x['testguide_verdict']),
                    'state': ', '.join(x['state']),

                }))

                grouped_data["NumberOf_PRs"] = grouped_data["NumberOf_PRs"].apply(lambda x: len(x.split(',')))
                # split columns into multiple lines
                grouped_data['prNumber'] = grouped_data['prNumber'].apply(replace_with_github_task_link)
                grouped_data['prNumber'] = grouped_data['prNumber'].str.replace(", ", "<br>")
                grouped_data['Target_Branch'] = grouped_data['Target_Branch'].str.replace(", ", "<br>")
                grouped_data['taskId'] = grouped_data['taskId'].apply(replace_with_tg_task_link)
                self.grouped_data['testcases_verdict'] = self.grouped_data['testcases_verdict'].str.replace(", ", "<br>")
                self.grouped_data['testguide_verdict'] = self.grouped_data['testguide_verdict'].str.replace(", ", "<br>")
                grouped_data['state'] = grouped_data['state'].str.replace(", ", "<br>")
                pivot_df = grouped_data.pivot(index=['Date', 'matchingXils'], columns='Pr_Label', values='NumberOf_PRs')
                pivot_df.fillna(0, inplace=True)
                pivot_df = pivot_df.astype("int")
                # concatenate the index columns
                concatenated_index = pivot_df.index.map(lambda x: str(str(x[0]).split(" ")[0]))
                # set the concatenated index as the DataFrame's index
                pivot_df.set_index(concatenated_index, inplace=True)
                grouped_data.Date = grouped_data.Date.apply(lambda x: x.split("T")[0])
                grouped_data = pr_info_data.groupby(['Date', 'matchingXils'], as_index=False).apply(lambda x: pd.Series({
                    "NumberOf_PRs": ', '.join(x["NumberOf_PRs"]),
                    "taskId": ', '.join(x["taskId"]),
                    'prNumber': ', '.join(x['prNumber'].astype(str)),
                    'Target_Branch': ', '.join(x['Target_Branch']),
                    'testcases_verdict': ', '.join(x['testcases_verdict']),
                    'testguide_verdict': ', '.join(x['testguide_verdict']),
                    'state': ', '.join(x['state']),

                }))
                grouped_data["NumberOf_PRs"] = grouped_data["NumberOf_PRs"].apply(lambda x: len(x.split(',')))
                # split columns into multiple lines
                """grouped_data['pdxPath'] = grouped_data['pdxPath'].str.replace(", ", "<br>")"""
                grouped_data['prNumber'] = grouped_data['prNumber'].apply(replace_with_github_task_link)
                grouped_data['prNumber'] = grouped_data['prNumber'].str.replace(", ", "<br>")
                grouped_data['Target_Branch'] = grouped_data['Target_Branch'].str.replace(", ", "<br>")
                grouped_data['taskId'] = grouped_data['taskId'].apply(replace_with_tg_task_link)
                self.grouped_data['testcases_verdict'] = self.grouped_data['testcases_verdict'].str.replace(", ", "<br>")
                self.grouped_data['testguide_verdict'] = self.grouped_data['testguide_verdict'].str.replace(", ", "<br>")
                grouped_data['state'] = grouped_data['state'].str.replace(", ", "<br>")

                data = [grouped_data, pivot_df]
                self.data_of_each_machine.append(data)
        except Exception as ex:
            raise Exception(f"there was an error during generating data grouped by date  for each machine : error message ; {ex} ")
        else:
            logger.info("grouped data by date  for each machine is generated successfully ")

    def generate_plots_for_each_machine(self, show_plot=True):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Generates a bar chart and a table containing data grouped by date and machine for all machines.

        Parameters:
        -----------
        show_plot : bool, optional
            Whether to display the generated plot or not. Defaults to False.

        Returns:
        --------
        None

        Side Effects:
        -------------
        - Creates a table using Plotly's go.Table method with headers as the columns of the grouped_data DataFrame.
        - Creates a bar chart using Plotly's go.Bar method for each Target_Branch and their respective NumbeOf_PRs.
        - Creates a subplots figure using Plotly's make_subplots method.
        - Adds the created bar chart and table to the subplots figure.
        - Updates the layout of the subplots figure with a title, dimensions, and template.
        - Saves the subplots figure as an HTML file in the reports directory.
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            logger.info("generating plots for data grouped by date and branch for each machine ")

            # generate data for plots and html file for each machine
            for index, data in enumerate(self.data_of_each_machine):
                table_data = data[0]
                machine_name = table_data.matchingXils.unique()[0]
                logger.debug(f"generating plot grouped by date and target branch for machine {machine_name}")
                # create the table
                table = go.Table(
                    header=dict(values=list(table_data.columns), fill_color=TableColors.COLUMNS_COLOR.value, font_color=TableColors.COLUMNS_FONT_COLOR.value),
                    cells=dict(values=[table_data.Date,
                                       table_data.matchingXils,
                                       table_data.NumberOf_PRs,
                                       table_data.taskId,
                                       #table_data.pdxPath,
                                       table_data.prNumber,
                                       table_data.Target_Branch,
                                       table_data.Target_Branch,
                                       table_data.testcases_verdict,
                                       table_data.testguide_verdict,
                                       table_data.state,
                                       #table_data.Pr_Label,
                                       #table_data.inconsistency
                                       ],
                               align="left",
                               fill_color=TableColors.ROWS_COLOR.value),
                    columnwidth=[80, 80, 80, 60, 400, 60, 100, 130, 60])

                # create the bar plot
                fig = go.Figure()
                bar_chart_data = data[1]
                for column_name in bar_chart_data.columns:
                    fig.add_trace(go.Bar(
                        x=bar_chart_data.index,
                        y=bar_chart_data[column_name],
                        text=bar_chart_data[column_name],
                        textposition='inside',  # show the text labels inside the bars
                        name=column_name,
                        marker=dict(color=self.color_dict[column_name])))
                # create subplots
                fig_subplots = make_subplots(rows=2, specs=[[{"type": "scatter"}], [{"type": "table"}]])

                # add the bar plot to the subplots
                for trace in fig.data:
                    fig_subplots.add_trace(trace, row=1, col=1)
                # add the table to the subplots
                fig_subplots.add_trace(table, row=2, col=1)

                # update the layout of the subplots
                fig_subplots.update_layout(
                    margin=dict(l=100, r=100, t=100, b=100),
                    height=1600,
                    width=1800,
                    title=f'PRs by Date and matchingXils {machine_name}',
                    showlegend=True,
                    barmode='stack',
                    template='plotly_white'
                )


                report_path = os.path.normpath(os.path.join(self.workspace,
                                                            f"{self.reports_directory}/0{index + 1}__grouped_by_date_and_machine__{machine_name}.html"))
                # Save the plot as an HTML file
                pio.write_html(fig_subplots, file=report_path)
                # show the subplots
                if show_plot:
                    return fig_subplots

        except Exception as ex:
            raise Exception(f"there was an error during generating plots for data grouped by date  for each machine {ex}")

        else:
            logger.info("plots for data grouped by date for each machine generated successfully ")

    def generate_plot_for_machine_donnee(self, machine_name):
        """
        Generate a bar chart and table for the specified machine.

        Parameters:
        -----------
        machine_name : str
            The name of the machine for which to generate the plot.

        Returns:
        --------
        plotly.graph_objs.Figure
            The generated bar chart and table in a Plotly figure.

        Side Effects:
        -------------
        Creates a table using Plotly's go.Table method with headers as the columns of the grouped_data DataFrame.
        Creates a bar chart using Plotly's go.Bar method for each Target_Branch and their respective NumbeOf_PRs.
        Creates a subplots figure using Plotly's make_subplots method.
        Adds the created bar chart and table to the subplots figure.
        Updates the layout of the subplots figure with a title, dimensions, and template.
        """
        try:
            logger.info(f"generating plot for machine: {machine_name}")

            # Find the data corresponding to the specified machine name

            """for data in self.data_of_each_machine:
                if data[0].matchingXils.unique()[0] == machine_name:
                    selected_data = data
                    break"""
            selected_data = next(
                data for data in self.data_of_each_machine if data[0].matchingXils.unique()[0] == machine_name)

            if selected_data is None:
                raise ValueError(f"No data found for machine: {machine_name}")


            # Extract table_data and bar_chart_data from selected_data
            table_data, bar_chart_data = selected_data

            # Create the table
            table = go.Table(
                header=dict(values=list(table_data.columns), fill_color=TableColors.COLUMNS_COLOR.value,
                            font_color=TableColors.COLUMNS_FONT_COLOR.value),
                cells=dict(values=[table_data.Date,
                                   table_data.matchingXils,
                                   table_data.NumberOf_PRs,
                                   table_data.taskId,
                                   table_data.prNumber,
                                   table_data.Target_Branch,
                                   table_data.Target_Branch,
                                   table_data.testcases_verdict,
                                   table_data.testguide_verdict,
                                   table_data.state],
                           align="left",
                           fill_color=TableColors.ROWS_COLOR.value),
                columnwidth=[65, 65, 65, 60, 65, 65, 100, 80,60])

            # Create the bar plot
            fig = go.Figure()
            for column_name in bar_chart_data.columns:
                fig.add_trace(go.Bar(
                    x=bar_chart_data.index,
                    y=bar_chart_data[column_name],
                    text=bar_chart_data[column_name],
                    textposition='inside',  # show the text labels inside the bars
                    name=column_name,
                    marker=dict(color=self.color_dict[column_name])))

            # Create subplots
            fig_subplots = make_subplots(rows=2, specs=[[{"type": "scatter"}], [{"type": "table"}]])

            # Add the bar plot to the subplots
            for trace in fig.data:
                fig_subplots.add_trace(trace, row=1, col=1)
            # Add the table to the subplots
            fig_subplots.add_trace(table, row=2, col=1)

            # Update the layout of the subplots
            fig_subplots.update_layout(
                margin=dict(l=100, r=100, t=100, b=100),
                height=1600,
                width=1300,
                title=f'PRs by Date and matchingXils {machine_name}',
                showlegend=True,
                barmode='stack',
                template='plotly_white'
            )
            report_path = os.path.normpath(os.path.join(self.workspace,
                                                        f"{self.reports_directory}/__grouped_by_date_and_machine__{machine_name}.html"))
            # Save the plot as an HTML file
            pio.write_html(fig_subplots, file=report_path)
            return fig_subplots

        except Exception as ex:
            raise Exception(f"Error generating plot for machine {machine_name}: {ex}")
