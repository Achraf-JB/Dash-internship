import netrc
import time

import pandas as pd
import requests

#from utilities.logger import logger


class GithubConnector:
    base_url = "https://cc-github.bmwgroup.net/api/v3"
    authenticators = "cc-github.bmwgroup.net"
    def __init__(self):
        self.pr_github_data = pd.DataFrame()
        self.__token = self.__get_authentication()
        self.pr_number_list = []
        self.pr_label_list = []
        self.target_branch_list = []
        self.response = {}

    def __get_authentication(self) -> str:
        """
        ------------------------------------------------------------------------------------------------------------------------------------------------------------
        get authentication key from netrc file
        :return: token for connection to github
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        auth = netrc.netrc().authenticators(self.authenticators)

        if auth is None:
            raise Exception(f"Could not retrieve authentication data for '{self.base_url}'!")
        else:
            logger.info("github token extracted successfully")

        token = auth[2]
        return token

    def generate_data_frame(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        create pandas data frame from response object
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        # create github pr dataframe
        pr_number_serie = pd.Series(self.pr_number_list)
        pr_label_serie = pd.Series(self.pr_label_list)
        target_branch_serie = pd.Series(self.target_branch_list)
        self.pr_github_data = pd.DataFrame({"prNumber": pr_number_serie, "Pr_Label": pr_label_serie, "Target_Branch": target_branch_serie})

    def extract_pull_request_info(self, per_page: int = 100):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        extract pull request information from github which are :
        - Pull request number
        - Pull request target branch
        - Pull Request Label's
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        :param per_page: number of pr's extracted per page
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        start_time = time.time()
        pulls_url = f"{self.base_url}/repos/ipbasis/ipb/pulls"
        headers = {"Authorization": f"token {self.__token}"}

        # Define parameters for the pull requests API
        params = {"state": "all", "sort": "created", "direction": "desc", "per_page": per_page, "page": 1}

        # Send a request to the pull requests API
        while True:
            self.response = requests.get(pulls_url, headers=headers, params=params)

            # Check if the response was successful
            if self.response.status_code == 200:
                # Loop through each pull request in the response
                for pr in self.response.json():
                    # Extract the pull request number, labels, and target branch
                    self.pr_number_list.append(str(pr['number']))
                    pr_label = ', '.join([label['name'] for label in pr['labels']])
                    self.pr_label_list.append(pr_label)
                    self.target_branch_list.append(pr['base']['ref'])
                    # logger.debug(f"PR #{pr['number']}: Labels={pr_label}, Target Branch={pr['base']['ref']}, State={pr['state']}")

                # Check if there are more pages of results
                link_header = self.response.headers.get('Link')
                if link_header and 'rel="next"' in link_header:
                    # There is a next page of results, update the 'page' parameter and continue
                    params['page'] += 1
                else:
                    # No more pages of results, break out of the loop
                    logger.info(f'o more pages of results, break out of the loop')
                    break
            else:
                logger.error(f"Error accessing PRs API endpoint: {self.response.content} stopping data extraction")
                break
            if len(self.pr_number_list) > 1500:
                break
        # generate pr information from github dataframe
        self.generate_data_frame()

        end_time = time.time()
        running_time = end_time - start_time
        logger.info(f"data extraction from github finished , it take {running_time} seconds")
