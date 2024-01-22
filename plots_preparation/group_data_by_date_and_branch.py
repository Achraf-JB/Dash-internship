import os
import pandas as pd
import plotly.colors
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from utilities.component import TableColors
from utilities.logger import logger
from utilities.tools import replace_with_tg_task_link, replace_with_github_task_link


class GrouperByDateBranchMachine:
    reports_directory = "grouped_by_date_and_branch"
    selected_columns = ['matchingXils', 'name', 'taskId',
                        'prNumber', 'pdxPath', 'creation_date', 'Target_Branch', 'Pr_Label', 'state']
    color_dict = {'SANITY_CHECK_PASSED': '#4CAF50',
                  'UNDEFINED': '#ced4da',
                  'SANITY_CHECK_FAILED': '#FF5722',
                  'SANITY_CHECK_RED_FLAG': "#990000",
                  "SANITY_CHECK_ERROR": "orange",
                  "PDX_READY_SANITY_CHECK": "#4F97A3"

                  }
    # generate a color scale derived from Blues
    colorscale = plotly.colors.sequential.Blues

    # list contain grouped data for each machine
    data_of_each_machine = []

    def __init__(self, data, workspace, number_of_days=6):
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
            logger.info("grouping data by date and target branch for all machines")
            pr_info_data = self.data[self.selected_columns]
            pr_info_data = pr_info_data.rename(columns={'name': "NumbeOf_PRs", "creation_date": "Date"})

            # group the data by day and matchingXils, and apply a lambda function to concatenate the strings
            logger.debug("grouping data by date and target branch  ")
            pr_info_data.Date = pr_info_data.Date.apply(
                lambda x: str(x).split("T"[0]) if "T" in str(x) else str(x).split(" ")[0])
            self.grouped_data = pr_info_data.groupby(['Date', 'Target_Branch'], as_index=False).apply(
                lambda x: pd.Series({
                    "NumbeOf_PRs": ', '.join(x["NumbeOf_PRs"]),
                    "taskId": ', '.join(x["taskId"].astype(str)),
                    'pdxPath': ', '.join(x['pdxPath']),
                    'prNumber': ', '.join(x['prNumber'].astype(str)),
                    'Pr_Label': ', '.join(x['Pr_Label']),
                    'state': ', '.join(x['state'])

                }))
            logger.debug("splitting data into multiple lines ")
            self.grouped_data["NumbeOf_PRs"] = self.grouped_data["NumbeOf_PRs"].apply(lambda x: len(x.split(',')))
            # split columns into multiple lines
            self.grouped_data['pdxPath'] = self.grouped_data['pdxPath'].str.replace(", ", "<br>")
            self.grouped_data['prNumber'] = self.grouped_data['prNumber'].apply(replace_with_github_task_link)
            self.grouped_data['prNumber'] = self.grouped_data['prNumber'].str.replace(", ", "<br>")
            self.grouped_data['state'] = self.grouped_data['state'].str.replace(", ", "<br>")
            self.grouped_data['taskId'] = self.grouped_data['taskId'].apply(replace_with_tg_task_link)
            self.grouped_data['taskId'] = self.grouped_data['taskId'].str.replace(", ", "<br>")
            self.grouped_data['Pr_Label'] = self.grouped_data['Pr_Label'].str.replace(", ", "<br>")
        except Exception as ex:
            raise Exception(
                f"there was an error during generating data grouped by date and branch for all machines : error message ; {ex} ")
        else:
            logger.info("grouped data by date and branch for all machine is generated successfully ")

    def group_bar_chart_data(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        select only needed columns which are 'matchingXils', 'name','prNumber', 'pdxPath', 'creation_date', 'Target_Branch', 'status'
        then group data buu Date
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        logger.info("grouping data by date ")
        try:
            pr_info_data = self.data[self.selected_columns]
            pr_info_data = pr_info_data.rename(columns={'name': "NumbeOf_PRs", "creation_date": "Date"})
            pr_info_data.taskId = pr_info_data.taskId.astype('str')
            pr_info_data.Date = pr_info_data.Date.apply(
                lambda x: str(x).split("T"[0]) if "T" in str(x) else str(x).split(" ")[0])
            # group the data by day and matchingXils, and apply a lambda function to concatenate the strings
            grouped_data = pr_info_data.groupby(['Date', 'Target_Branch', 'Pr_Label'], as_index=False).apply(
                lambda x: pd.Series({
                    "NumbeOf_PRs": ', '.join(x["NumbeOf_PRs"]),
                    "taskId": ', '.join(x["taskId"]),
                    'pdxPath': ', '.join(x['pdxPath']),
                    'prNumber': ', '.join(x['prNumber'].astype(str)),
                    'state': ', '.join(x['state']),

                }))
            grouped_data["NumbeOf_PRs"] = grouped_data["NumbeOf_PRs"].apply(lambda x: len(x.split(',')))
            # split columns into multiple lines
            grouped_data['pdxPath'] = grouped_data['pdxPath'].str.replace(", ", "<br>")
            grouped_data['prNumber'] = grouped_data['prNumber'].apply(replace_with_github_task_link)
            grouped_data['prNumber'] = grouped_data['prNumber'].str.replace(", ", "<br>")
            grouped_data['taskId'] = grouped_data['taskId'].apply(replace_with_tg_task_link)
            grouped_data['taskId'] = grouped_data['taskId'].str.replace(", ", "<br>")
            grouped_data['state'] = grouped_data['state'].str.replace(", ", "<br>")
            self.pivot_df = grouped_data.pivot(index=['Date', 'Target_Branch'], columns='Pr_Label',
                                               values='NumbeOf_PRs')
            self.pivot_df.fillna(0, inplace=True)
            self.pivot_df = self.pivot_df.astype("int")

            # concatenate the index columns
            concatenated_index = self.pivot_df.index.map(lambda x: str(str(x[0]).split(" ")[0]) + '_' + str(x[1]))
            # set the concatenated index as the DataFrame's index
            self.pivot_df.set_index(concatenated_index, inplace=True)

        except Exception as ex:
            raise Exception(f"there was an error during grouping data by date : error message {ex}")
        else:
            logger.info('grouping data b date finished successfully')

    def generate_statistucs_bar_chart(self, show_plot=True):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Generates a bar chart showing the count of PRs by date and target branch, grouped by PR label.

        Args:
            show_plot (bool): If True, shows the plot interactively. Default is False.

        Returns:
            None

        Raises:
            Exception: If there is an error generating the plot.

        Notes:
            - The data used to generate the plot is stored in the `pivot_df` attribute.
            - The plot is saved as an HTML file in the `reports_directory` subdirectory of the workspace.
            - The name of the HTML file is "01__global__grouped_by_date_branch_and_pr_label.html".
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            logger.info("generating plot for data grouped by date and target branch for all machine ")
            # create the bar plot
            fig = go.Figure()

            for column_name in self.pivot_df.columns:
                fig.add_trace(go.Bar(
                    x=self.pivot_df.index,
                    y=self.pivot_df[column_name],
                    text=self.pivot_df[column_name],
                    textposition='inside',  # show the text labels inside the bars
                    name=column_name,
                    marker=dict(color=self.color_dict[column_name])))

            branch_names = " | ".join(self.grouped_data.Target_Branch.unique())
            # update the layout of the subplots
            fig.update_layout(
                margin=dict(l=100, r=100, t=100, b=100),
                height=1000,
                width=1300,
                title=f'PRs by Date and Target Branch {branch_names}',
                showlegend=True,
                barmode='stack',
                template='plotly_white'
            )

            # show the subplots

            report_path = os.path.normpath(os.path.join(self.workspace,
                                                        f"{self.reports_directory}/01__global__grouped_by_date_branch_and_pr_label.html"))

            pio.write_html(fig, file=report_path)
            if show_plot:
                return fig
        except Exception as ex:
            raise Exception(f"there was an error during generating plots for data grouped by date and branch {ex}")

        else:
            logger.info("plots for data grouped by date and branch for all machine generated successfully ")

    def generate_plot_for_general_report(self, show_plot=True):
        """
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

        """
        try:
            logger.info("generating plot for data grouped by date and target branch for all machine ")
            # create the table
            table = go.Table(
                header=dict(values=list(self.grouped_data.columns), fill_color=TableColors.COLUMNS_COLOR.value,
                            font_color=TableColors.COLUMNS_FONT_COLOR.value),
                cells=dict(
                    values=[self.grouped_data.Date, self.grouped_data.Target_Branch, self.grouped_data.NumbeOf_PRs,
                            self.grouped_data.taskId,
                            self.grouped_data.pdxPath, self.grouped_data.prNumber, self.grouped_data.Pr_Label,
                            self.grouped_data.state],
                    align="left",
                    fill_color=TableColors.ROWS_COLOR.value),
                columnwidth=[50, 50, 50, 40, 85, 50, 100, 50])
            # create a dictionary that maps each value of the matchingXils column to a color in the colorscale variable
            color_dict = dict(zip(self.grouped_data.Target_Branch.unique(), self.colorscale[3:]))

            # create the bar plot
            fig = go.Figure()

            for branch in self.grouped_data.Target_Branch.unique():
                branch_df = self.grouped_data[self.grouped_data.Target_Branch == branch].reset_index()
                fig.add_trace(go.Bar(x=branch_df.Date, y=branch_df.NumbeOf_PRs, text=branch_df.NumbeOf_PRs, name=branch,
                                     marker=dict(color=color_dict[branch])))

            # create subplots
            fig_subplots = make_subplots(rows=2, specs=[[{"type": "scatter"}], [{"type": "table"}]])

            # add the bar plot to the subplots
            for trace in fig.data:
                fig_subplots.add_trace(trace, row=1, col=1)

            # add the table to the subplots
            fig_subplots.add_trace(table, row=2, col=1)

            branch_names = " | ".join(self.grouped_data.Target_Branch.unique())
            # update the layout of the subplots
            fig_subplots.update_layout(
                margin=dict(l=100, r=100, t=100, b=100),
                height=1600,
                width=1300,
                title=f'PRs by Date and Target Branch {branch_names}',
                showlegend=True,
                template='plotly_white'
            )

            # html file path
            report_path = os.path.normpath(
                os.path.join(self.workspace, f"{self.reports_directory}/00__global__grouped_by_date_and_branch.html"))
            # Save the plot as an HTML file
            pio.write_html(fig_subplots, file=report_path)
            # show the subplots
            #if show_plot:
            return fig_subplots
        except Exception as ex:
            raise Exception(f"there was an error during generating plots for data grouped by date and branch {ex}")

        else:
            logger.info("plots for data grouped by date and branch for all machine generated successfully ")

    def generate_grouped_data_for_each_machine(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        filters the grouped data frame to include only the data for the last selected number of days for each machine
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            logger.info("grouping data by date and target branch for each machine ")
            for machine in self.unique_machine_xils:
                logger.debug(f"generating grouped data for machine {machine}")
                pr_info_data = self.data[self.selected_columns]
                pr_info_data = pr_info_data.rename(columns={'name': "NumbeOf_PRs", "creation_date": "Date"})
                pr_info_data = pr_info_data[pr_info_data['matchingXils'] == machine]
                # group the data by day and matchingXils, and apply a lambda function to concatenate the strings
                logger.debug(f"grouping data by date , machine xils and target branch ")
                grouped_df = pr_info_data.groupby(['Date', 'matchingXils', 'Target_Branch'], as_index=False).apply(
                    lambda x: pd.Series({
                        "NumbeOf_PRs": ', '.join(x["NumbeOf_PRs"]),
                        "taskId": ', '.join(x["taskId"].astype(str)),
                        'pdxPath': ', '.join(x['pdxPath']),
                        'prNumber': ', '.join(x['prNumber'].astype(str)),
                        'Pr_Label': ', '.join(x['Pr_Label']),
                        'state': ', '.join(x['state'])

                    }))
                grouped_df["NumbeOf_PRs"] = grouped_df["NumbeOf_PRs"].apply(lambda x: len(x.split(',')))
                logger.debug("splitting data into multiple lines ")
                # split columns into multiple lines
                grouped_df['pdxPath'] = grouped_df['pdxPath'].str.replace(", ", "<br>")
                grouped_df['prNumber'] = grouped_df['prNumber'].apply(replace_with_github_task_link)
                grouped_df['prNumber'] = grouped_df['prNumber'].str.replace(", ", "<br>")
                grouped_df['state'] = grouped_df['state'].str.replace(", ", "<br>")
                grouped_df['taskId'] = grouped_df['taskId'].apply(replace_with_tg_task_link)
                grouped_df['taskId'] = grouped_df['taskId'].str.replace(", ", "<br>")
                grouped_df['Pr_Label'] = grouped_df['Pr_Label'].str.replace(", ", "<br>")
                grouped_df = grouped_df[
                    ['Date', 'Target_Branch', 'NumbeOf_PRs', 'taskId', 'pdxPath', 'prNumber', 'state', 'Pr_Label',
                     'matchingXils']]
                pivot_df = grouped_df.pivot(index=['Date', 'Target_Branch'], columns='Pr_Label', values='NumbeOf_PRs')
                pivot_df.fillna(0, inplace=True)
                pivot_df = pivot_df.astype("int")
                # concatenate the index columns
                concatenated_index = pivot_df.index.map(lambda x: str(str(x[0]).split(" ")[0]) + '_' + str(x[1]))
                # set the concatenated index as the DataFrame's index
                pivot_df.set_index(concatenated_index, inplace=True)
                self.data_of_each_machine.append(grouped_df)
        except Exception as ex:
            raise Exception(
                f"there was an error during generating data grouped by date and branch for each machine : error message ; {ex} ")
        else:
            logger.info("grouped data by date and branch for each machine is generated successfully ")

    def generate_plots_for_each_machine(self, show_plot=False):
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
            for index, data in enumerate(self.data_of_each_machine):
                machine_name = data.matchingXils.unique()[0]
                logger.debug(f"generating plot grouped by date and target branch for machine {machine_name}")
                # create the table
                table = go.Table(header=dict(values=list(data.columns)[:len(list(data.columns)) - 1],
                                             fill_color=TableColors.COLUMNS_COLOR.value,
                                             font_color=TableColors.COLUMNS_FONT_COLOR.value),
                                 cells=dict(
                                     values=[data.Date, data.Target_Branch, data.NumbeOf_PRs, data.taskId,
                                             data.pdxPath, data.prNumber, data.Pr_Label, data.state],
                                     align="left",
                                     fill_color=TableColors.ROWS_COLOR.value),
                                 columnwidth=[80, 60, 55, 55, 400, 80, 120, 120])

                # create the bar plot
                fig = go.Figure()

                for column_name in self.pivot_df.columns:
                    fig.add_trace(go.Bar(
                        x=self.pivot_df.index,
                        y=self.pivot_df[column_name],
                        text=self.pivot_df[column_name],
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

                # show the subplots
                if show_plot:
                    fig_subplots.show()
                report_path = os.path.normpath(os.path.join(self.workspace,
                                                            f"{self.reports_directory}/0{index + 2}__grouped_by_date_and_branch__{machine_name}.html"))
                # Save the plot as an HTML file
                pio.write_html(fig_subplots, file=report_path)
        except Exception as ex:
            raise Exception(
                f"there was an error during generating plots for data grouped by date and branch for each machine {ex}")
        else:
            logger.info("plots for data grouped by date and branch for each machine generated successfully ")

    def generate_plot_for_target_donnee(self, selected_machine_name):
        """
        Generate a bar chart and a table containing data grouped by date and branch for the specified machine.

        Parameters:
        -----------
        selected_machine_name : str
            The name of the machine for which to generate the plot.
        show_plot : bool, optional
            Whether to display the generated plot or not. Defaults to False.

        Returns:
        --------
        None

        Side Effects:
        -------------
        Creates a table using Plotly's go.Table method with headers as the columns of the data DataFrame.
        Creates a bar chart using Plotly's go.Bar method for each Target_Branch and their respective NumbeOf_PRs.
        Creates a subplots figure using Plotly's make_subplots method.
        Adds the created bar chart and table to the subplots figure.
        Updates the layout of the subplots figure with a title, dimensions, and template.
        """
        try:
            logger.info(f"generating plots for data grouped by date and branch for machine {selected_machine_name}")

            selected_data = next(
                data for data in self.data_of_each_machine if data.matchingXils.unique()[0] == selected_machine_name)

            if selected_data is None:
                raise ValueError(f"No data found for machine: {selected_machine_name}")

            # Create the table
            table = go.Table(
                header=dict(values=list(selected_data.columns)[:len(list(selected_data.columns)) - 1],
                            fill_color=TableColors.COLUMNS_COLOR.value,
                            font_color=TableColors.COLUMNS_FONT_COLOR.value),
                cells=dict(
                    values=[selected_data.Date, selected_data.Target_Branch, selected_data.NumbeOf_PRs,
                            selected_data.taskId, selected_data.pdxPath, selected_data.prNumber, selected_data.Pr_Label,
                            selected_data.state],
                    align="left",
                    fill_color=TableColors.ROWS_COLOR.value),
                columnwidth=[50, 60, 55, 40, 85, 50, 95, 45])

            # Create the bar plot
            fig = go.Figure()
            for column_name in self.pivot_df.columns:
                fig.add_trace(go.Bar(
                    x=self.pivot_df.index,
                    y=self.pivot_df[column_name],
                    text=self.pivot_df[column_name],
                    textposition='inside',
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
                title=f'PRs by Date and matchingXils {selected_machine_name}',
                showlegend=True,
                barmode='stack',
                template='plotly_white'
            )
            report_path = os.path.normpath(os.path.join(self.workspace,
                                                        f"{self.reports_directory}/__grouped_by_date_and_branch__{selected_machine_name}.html"))
            # Save the plot as an HTML file
            pio.write_html(fig_subplots, file=report_path)
            return fig_subplots


        except Exception as ex:

            raise Exception(f"Error generating plot for machine {selected_machine_name}: {ex}")
