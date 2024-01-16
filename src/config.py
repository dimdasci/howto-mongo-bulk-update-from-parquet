"""
Application configuration.
Look at https://www.dynaconf.com for more information.
"""

from dynaconf import Dynaconf

config = Dynaconf(
    envvar_prefix="BULK_UPDATE",
    settings_files=["config.yaml"],
    core_loaders=["YAML"],
    environments=False,
    load_dotenv=True,
)
