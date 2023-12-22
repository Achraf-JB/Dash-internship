import logging
import os
from filtrage.filtrage import DataFilter
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from utilities.component import TableColors, StatusColors
from utilities.tools import replace_with_tg_task_link, replace_with_github_task_link, assign_colors, \
    assign_pass_fail_colors

logger = logging.getLogger(__name__)


class GrouperByDate:
    selected_columns = ['matchingXils',
                        'name',
                        'taskId',
                        'prNumber',
                        'creation_date',
                        'Target_Branch',
                        'testcases_verdict',
                        'testguide_verdict',
                        'Pr_Label',
                        'state',
                        'inconsistency']
    reports_directory = "grouped_by_date"

    def __init__(self, data, workspace, number_of_days):
        self.pie_chart_data = pd.DataFrame()
        self.pivot_df = pd.DataFrame()
        self.data = data
        self.number_of_days = number_of_days
        self.grouped_data = pd.DataFrame()
        self.workspace = workspace


    def group_table_data(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        select only needed columns which are 'matchingXils', 'name','prNumber',  'creation_date', 'Target_Branch', 'status'
        then group data by Date
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        logger.info("grouping data by date ")
        try:
            pr_info_data = self.data[self.selected_columns]
            pr_info_data = pr_info_data.rename(columns={'name': "NumberOf_PRs", "creation_date": "Date"})
            pr_info_data.taskId = pr_info_data.taskId.astype('str')
            pr_info_data.Date = pr_info_data.Date.apply(
                lambda x: str(x).split("T")[0] if "T" in str(x) else str(x).split(" ")[0])
            # group the data by day and matchingXils, and apply a lambda function to concatenate the strings
            self.grouped_data = pr_info_data.groupby('Date', as_index=False).apply(lambda x: pd.Series({
                "NumberOf_PRs": ', '.join(x["NumberOf_PRs"]),
                "taskId": ', '.join(x["taskId"]),
                'prNumber': ', '.join(x['prNumber'].astype(str)),
                'Target_Branch': ', '.join(x['Target_Branch']),
                'testcases_verdict': ', '.join(x['testcases_verdict']),
                'testguide_verdict': ', '.join(x['testguide_verdict']),
                'state': ', '.join(x['state']),
                'Pr_Label': ', '.join(x['Pr_Label']),
                'inconsistency': ', '.join(x['inconsistency'])
            }))
            self.grouped_data["NumberOf_PRs"] = self.grouped_data["NumberOf_PRs"].apply(lambda x: len(x.split(',')))
            # split columns into multiple lines
            self.grouped_data['prNumber'] = self.grouped_data['prNumber'].apply(replace_with_github_task_link)
            #self.grouped_data['prNumber'] = self.grouped_data['prNumber'].str.replace(", ", "<br>")
            self.grouped_data['prNumber'] = self.grouped_data['prNumber'].astype(str).str.replace(", ", "<br>")
            self.grouped_data['Target_Branch'] = self.grouped_data['Target_Branch'].str.replace(", ", "<br>")
            self.grouped_data['taskId'] = self.grouped_data['taskId'].apply(replace_with_tg_task_link)
            self.grouped_data['state'] = self.grouped_data['state'].str.replace(", ", "<br>")
            self.grouped_data['testcases_verdict'] = self.grouped_data['testcases_verdict'].str.replace(", ", "<br>")
            self.grouped_data['testguide_verdict'] = self.grouped_data['testguide_verdict'].str.replace(", ", "<br>")
            self.grouped_data['inconsistency'] = self.grouped_data['inconsistency'].str.replace(", ", "<br>")
            self.grouped_data['Pr_Label'] = self.grouped_data['Pr_Label'].str.replace(", ", "<br>")
            #self.grouped_data.reset_index(drop=True, inplace=True)
            self.grouped_data = self.grouped_data.sort_values(by='Date', ascending=False)

        except Exception as ex:
            logger.exception(ex)
            raise Exception(f"there was an error during grouping data by date : error message {ex}")
        else:
            logger.info('grouping data b date finished successfully')

    def group_bar_chart_data(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        select only needed columns which are 'matchingXils', 'name','prNumber', 'creation_date', 'Target_Branch', 'status'
        then group data buu Date
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        logger.info("grouping data by date ")
        try:
            pr_info_data = self.data[self.selected_columns]
            pr_info_data = pr_info_data.rename(columns={'name': "NumberOf_PRs", "creation_date": "Date"})
            pr_info_data.taskId = pr_info_data.taskId.astype('str')
            pr_info_data.Date = pr_info_data.Date.apply(
                lambda x: str(x).split("T"[0]) if "T" in str(x) else str(x).split(" ")[0])
            # group the data by day and matchingXils, and apply a lambda function to concatenate the strings
            grouped_data = pr_info_data.groupby(['Date', 'Pr_Label'], as_index=False).apply(lambda x: pd.Series({
                "NumberOf_PRs": ', '.join(x["NumberOf_PRs"]),
                "taskId": ', '.join(x["taskId"]),
                'prNumber': ', '.join(x['prNumber'].astype(str)),
                'testcases_verdict': ', '.join(x['testcases_verdict']),
                'testguide_verdict': ', '.join(x['testguide_verdict']),
                'Target_Branch': ', '.join(x['Target_Branch']),
                'state': ', '.join(x['state']),

            }))
            grouped_data["NumberOf_PRs"] = grouped_data["NumberOf_PRs"].apply(lambda x: len(x.split(',')))
            # split columns into multiple lines
            grouped_data['prNumber'] = grouped_data['prNumber'].apply(replace_with_github_task_link)
            #grouped_data['prNumber'] = grouped_data['prNumber'].str.replace(", ", "<br>")
            grouped_data['prNumber'] = grouped_data['prNumber'].astype(str).str.replace(", ", "<br>")
            grouped_data['Target_Branch'] = grouped_data['Target_Branch'].str.replace(", ", "<br>")
            grouped_data['taskId'] = grouped_data['taskId'].apply(replace_with_tg_task_link)
            self.grouped_data['testcases_verdict'] = self.grouped_data['testcases_verdict'].str.replace(", ", "<br>")
            self.grouped_data['testguide_verdict'] = self.grouped_data['testguide_verdict'].str.replace(", ", "<br>")
            grouped_data['taskId'] = grouped_data['taskId'].str.replace(", ", "<br>")
            grouped_data['state'] = grouped_data['state'].str.replace(", ", "<br>")
            self.pivot_df = grouped_data.pivot(index='Date', columns='Pr_Label', values='NumberOf_PRs')
            self.pivot_df.fillna(0, inplace=True)
            self.pivot_df = self.pivot_df.astype("int")

            self.pivot_df = self.pivot_df.sort_values(by='Date', ascending=False)

        except Exception as ex:
            raise Exception(f"there was an error during grouping data by date : error message {ex}")
        else:
            logger.info('grouping data b date finished successfully')

    def group_data_for_pie_chart(self):
        """
        genetaye data to be used for pie chart plotting
        :return:
        """
        self.pie_chart_data = self.data["Pr_Label"].value_counts()

    def generate_bar_chart_plot_on_pr_label(self, show_plot=True):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Generates a bar chart containing statistics about pull requests

        Parameters:
        -----------
        show_plot : bool, optional
            Whether to display the generated plot or not. Defaults to False.

        Returns:
        --------
        None

        Side Effects:
        -------------
        - Creates a horizontal bar chart using Plotly's go.Bar method
        - Updates the layout of the subplots figure with a title, dimensions, and template.
        - Saves the subplots figure as an HTML file in the reports directory.
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            logger.info(f"generating plots for data grouped by date ")
            # generate pie chart data
            pie_chart_data = self.data["Pr_Label"].value_counts()
            # create the bar chart
            fig = go.Figure()
            # create bar chart
            color_list = assign_colors(column=pie_chart_data.index)
            bar_chart = go.Bar(x=pie_chart_data.values,
                               y=pie_chart_data.index,
                               text=pie_chart_data.values,
                               textposition='inside',
                               orientation='h',
                               marker=dict(color=color_list),
                               showlegend=False)
            # create pie chart
            pie_chart = go.Pie(labels=pie_chart_data.index, values=pie_chart_data.values,
                               marker=dict(colors=color_list))
            # add the bar chart to the subplots
            fig.add_trace(bar_chart)

            # create subplots
            fig_subplots = make_subplots(cols=2, specs=[[{"type": "pie"}, {"type": "scatter"}]], horizontal_spacing=0.3)

            # add the table to the subplots
            fig_subplots.add_trace(pie_chart, row=1, col=1)
            fig_subplots.add_trace(bar_chart, row=1, col=2)
            # fig_subplots.update_layout(col= 2,showlegend=True)

            # update the layout of the subplots
            fig_subplots.update_layout(
                margin=dict(l=100, r=100, t=100),
                height=600,
                width=1200,
                title='General statistics Pull requests Status Based On Pull Request Label',
                showlegend=True,
                template='plotly_white',
                xaxis_title='Count',
                yaxis_title='Status',
                barmode='stack',
                xaxis={'categoryorder': 'category ascending'}
            )
            # show the subplots


            report_path = os.path.join(self.workspace,
                                       f"{self.reports_directory}/00_pr_statistics_bar_chart_per_label.html")
            # Save the plot as an HTML file
            pio.write_html(fig_subplots, file=report_path)
            """if show_plot:
                fig1 = html.Div(children=[
                    html.H1(children='hello dash'),
                    dcc.Graph(id='fig1', figure=fig_subplots),
                ])
                return fig1"""
            if show_plot:
               return fig_subplots
        except Exception as ex:
            raise Exception(f"there was an error during generating bar chart plot for data grouped by date {ex} ")
        else:
            logger.info("bar chart plot for data grouped by date generated successfully ")

    def generate_bar_chart_plot_on_tg_verdict(self, show_plot=True):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        Generates a bar chart containing statistics about pull requests

        Parameters:
        -----------
        show_plot : bool, optional
            Whether to display the generated plot or not. Defaults to False.

        Returns:
        --------
        None

        Side Effects:
        -------------
        - Creates a horizontal bar chart using Plotly's go.Bar method
        - Updates the layout of the subplots figure with a title, dimensions, and template.
        - Saves the subplots figure as an HTML file in the reports directory.
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        try:
            logger.info(f"generating plots for data grouped by date ")
            # generate pie chart data
            pie_chart_data = self.data["testguide_verdict"].value_counts()
            # create the bar chart
            fig = go.Figure()
            # create bar chart
            color_list = assign_pass_fail_colors(column=pie_chart_data.index)
            bar_chart = go.Bar(x=pie_chart_data.values,
                               y=pie_chart_data.index,
                               text=pie_chart_data.values,
                               textposition='inside',
                               orientation='h',
                               marker=dict(color=color_list),
                               showlegend=False)
            # create pie chart
            pie_chart = go.Pie(labels=pie_chart_data.index, values=pie_chart_data.values,
                               marker=dict(colors=color_list))
            # add the bar chart to the subplots
            fig.add_trace(bar_chart)

            # create subplots
            fig_subplots = make_subplots(cols=2, specs=[[{"type": "pie"}, {"type": "scatter"}]], horizontal_spacing=0.3)

            # add the table to the subplots
            fig_subplots.add_trace(pie_chart, row=1, col=1)
            fig_subplots.add_trace(bar_chart, row=1, col=2)
            # fig_subplots.update_layout(col= 2,showlegend=True)

            # update the layout of the subplots
            fig_subplots.update_layout(
                margin=dict(l=100, r=100, t=100),
                height=600,
                width=1200,
                title='General statistics Pull requests Status Based On TestGuide Verdict',
                showlegend=True,
                template='plotly_white',
                xaxis_title='Count',
                yaxis_title='Status',
                barmode='stack',
                xaxis={'categoryorder': 'category ascending'}
            )


            report_path = os.path.join(self.workspace,
                                       f"{self.reports_directory}/00_pr_statistics_bar_chart_per_tg_verdict.html")

            # Save the plot as an HTML file
            pio.write_html(fig_subplots, file=report_path)
            # show the subplots
            if show_plot:
                return fig_subplots
        except Exception as ex:
            raise Exception(f"there was an error during generating bar chart plot for data grouped by date {ex} ")
        else:
            logger.info("bar chart plot for data grouped by date generated successfully ")

    def generate_table_bar_plot(self, show_plot=True):
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
            logger.info(f"generating plots for data grouped by date ")
            # create the table
            table = go.Table(
                header=dict(values=list(self.grouped_data.columns),
                            fill_color=TableColors.COLUMNS_COLOR.value,
                            font_color=TableColors.COLUMNS_FONT_COLOR.value),
                cells=dict(values=[self.grouped_data.Date,
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
                columnwidth=[80, 85, 70, 70, 100, 120, 90, 90, 130,80])

            # create the bar plot
            fig = go.Figure()

            for column_name in self.pivot_df.columns:
                fig.add_trace(go.Bar(
                    x=self.pivot_df.index,
                    y=self.pivot_df[column_name],
                    text=self.pivot_df[column_name],
                    textposition='inside',  # show the text labels inside the bars
                    name=column_name,
                    marker=dict(color=StatusColors.COLOR_DICT.value[column_name])))

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
                height=1400,
                width=1350,
                title='Pull Request Grouped by Date',
                showlegend=True,
                template='plotly_white', barmode='stack', xaxis={'categoryorder': 'category ascending'}
            )



            report_path = os.path.normpath(os.path.join(self.workspace, "grouped_by_date/grouped_data_statistics.html"))
            # Save the plot as an HTML file
            pio.write_html(fig_subplots, file=report_path)
            # show the subplots
            if show_plot:
                return fig_subplots
        except Exception as ex:
            raise Exception(f"there was an error during generating plots for data grouped by date {ex} ")
        else:
            logger.info("plots for data grouped by date generated successfully ")
