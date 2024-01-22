import logging
import netrc



from retry import retry

logger = logging.getLogger(__name__)


class Authenticator:
    """
    A class that handles authentication and retrieves an authentication key from the netrc file.

    Args:
        authenticator (str): The name of the authenticator to retrieve authentication data from the netrc file.
        host (str): The host for which the authentication key is required.

    Attributes:
        authenticator (str): The name of the authenticator.
        host (str): The host for which the authentication key is required.
        __auth_key (str): The retrieved authentication key.

    Raises:
        Exception: If authentication data cannot be retrieved for the specified host.

    """

    def __init__(self, authenticator, host, project_id=7):
        self.project_id = project_id
        self.authenticator = authenticator
        self.host = host
        self.__auth_key = self.get_authentication()

    def get_authentication(self) -> str:
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        get authentication key from netrc file
        :return: token for connection to github
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        auth = netrc.netrc().authenticators(self.authenticator)

        if auth is None:
            raise Exception(f"Could not retrieve authentication data for '{self.host}'!")
        else:
            logger.info(f"{self.host} token extracted successfully")

        token = auth[2]
        return token

    @retry(tries=10, max_delay=30)
    def verify_authentification(self):
        """
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        verify authentication connection
        :return:
        --------------------------------------------------------------------------------------------------------------------------------------------------------
        """
        url = f'{self.host}/api/releases?authKey={self.__auth_key}&projectId={self.project_id}'

        r = requests.get(url, verify=False)

        if r.status_code < 300:
            logger.info("API access successful.")
        else:
            logger.error("No API access! Check your settings.")
