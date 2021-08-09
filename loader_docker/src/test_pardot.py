import boto3

from pypardot.client import PardotAPI, PardotAPIError

from loader import get_client_pardot, test_connection_pardot


def test_pardot():
    c = get_client_pardot()

    test_connection_pardot()


if __name__ == "__main__":
    test_pardot()
