"""AWS functions
"""
from os import environ

import boto3


def get_credentials(profile: str = None) -> (str, str, str):
    """Reads credentials from AWS session for the given profile"""

    # Create a session object
    session = boto3.Session(profile_name=profile)
    # read credentials from session
    credentials = session.get_credentials()
    # read access key
    access_key = credentials.access_key
    # read secret key
    secret_key = credentials.secret_key
    # read default region
    region = session.region_name

    return access_key, secret_key, region


def set_env_to_credentials(profile: str = None) -> None:
    """Sets environment variables to AWS credentials"""

    access_key, secret_key, region = get_credentials(profile)
    environ["AWS_ACCESS_KEY_ID"] = access_key
    environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    environ["AWS_DEFAULT_REGION"] = region
