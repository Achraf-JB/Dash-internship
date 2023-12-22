import os

import plotly.colors
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots


def generate_html_for_each_domain(workspace, directory):
    """
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    generate global html report for each domain
    :param workspace: absolute path of the workspace
    :param directory: name of the directory where the report will be generate
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    html_files = []
    output_directory = os.path.normpath(os.path.join(workspace, directory))
    for file in os.listdir(str(output_directory)):
        if file.endswith('.html'):
            with open(os.path.join(output_directory, file), 'r', encoding='utf-8') as f:
                html_files.append(f.read())
    concatenated_html = '\n'.join(html_files)
    output_file_path = os.path.normpath(os.path.join(output_directory, 'Global_report.html'))
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(concatenated_html)


def generate_global_report(workspace):
    """
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    generate global html report for all domains
    :param workspace: absolute path of the workspace
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    # generate global report for grouping by date and branch
    html_files_content = []
    html_files = [f'{workspace}{os.sep}grouped_by_date{os.sep}Global_report.html', f'{workspace}{os.sep}grouped_by_date_and_machine{os.sep}Global_report.html',
                  f'{workspace}{os.sep}grouped_by_date_and_branch{os.sep}Global_report.html']
    for file in html_files:
        with open(file, 'r', encoding='utf-8', errors='ignore') as f:
            html_files_content.append(f.read())
    concatenated_html = '\n'.join(html_files_content)
    output_file_path = os.path.normpath(os.path.join(workspace, 'Global.html'))

    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(concatenated_html)


def generate_bar_chart_table_plot(grouped_data, workspace, columns, values, columns_size, sub_directory_name, file_name, show_plot=False):
    """
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    generate bar chart and table containing data grouped by date and machine for all maachines
    :return:
    --------------------------------------------------------------------------------------------------------------------------------------------------------
    """
    # generate a color scale derived from orange
    colorscale = plotly.colors.sequential.Oranges
    # generate data for plots and html file for each machine
    for index, data in enumerate(grouped_data):
        # create the table
        table = go.Table(header=dict(values=list(columns), fill_color=colorscale[4]),
                         cells=dict(values=values,
                                    align="left",
                                    fill_color=colorscale[2]),
                         columnwidth=columns_size)

        # create a dictionary that maps each value of the matchingXils column to a color in the colorscale variable
        color_dict = dict(zip(data.matchingXils.unique(), colorscale[1:]))

        # create the bar plot
        fig = go.Figure()

        for xil in data.matchingXils.unique():
            xil_df = data[data.matchingXils == xil].reset_index()
            fig.add_trace(go.Bar(x=xil_df.Date, y=xil_df.NumbeOf_PRs, text=xil_df.NumbeOf_PRs, name=xil, marker=dict(color=color_dict[xil])))

        # create subplots
        fig_subplots = make_subplots(rows=2, specs=[[{"type": "scatter"}], [{"type": "table"}]])

        # add the bar plot to the subplots
        for trace in fig.data:
            fig_subplots.add_trace(trace, row=1, col=1)

        # add the table to the subplots
        fig_subplots.add_trace(table, row=2, col=1)

        testbanches_names = " | ".join(data.matchingXils.unique())
        # update the layout of the subplots
        fig_subplots.update_layout(
            margin=dict(l=100, r=100, t=100, b=100),
            height=1600,
            width=1400,
            title=f'PRs by Date and matchingXils {testbanches_names}',
            showlegend=True,
            template='plotly_white'
        )

        # show the subplots
        if show_plot:
            fig_subplots.show()
        # html file path
        report_path = os.path.normpath(os.path.join(workspace,
                                                    f"{sub_directory_name}/0{index + 1}__{file_name}.html"))
        # Save the plot as an HTML file
        pio.write_html(fig_subplots, file=report_path)
