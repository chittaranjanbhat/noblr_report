import os
import yaml


class NoblrConfig:
    """"
    Configuration file parser and convenience class
    """

    def __init__(self, config_file):
        self._yml = yaml.full_load(config_file)

    def notify_email(self):
        email = self._yml['config']['notify']['email']
        if isinstance(email, list):
            recipient = ','.join(email)
        else:
            recipient = email
        return recipient

    def fs_xls_path(self):
        return self._yml['config']['localfs']['xls_path']

    def fs_sql_path(self):
        return self._yml['config']['localfs']['sql_path']

    def get_secret_name(self):
        return self._yml['config']['secrets']['secret_name']

    def get_secret_region(self):
        return self._yml['config']['secrets']['secret_region']

    def get_postgres_jdbcUrl(self):
        return self._yml['config']['postgresJDBC']['jdbcUrl']

    def get_postgres_jdbcDatabase(self):
        return self._yml['config']['postgresJDBC']['jdbcDatabase']

    def get_postgres_jdbcSchema(self):
        return self._yml['config']['postgresJDBC']['jdbcSchema']

    def get_postgres_user(self):
        return self._yml['config']['postgresJDBC']['postgres_user']

    def get_postgres_pwd(self):
        return self._yml['config']['postgresJDBC']['postgres_pwd']


if __name__ == '__main__':
    configFile = "/conf/config_dev.yml"
    config = open(configFile, "r")

    print(f"local xls file path : {config.fs_xls_path()}")