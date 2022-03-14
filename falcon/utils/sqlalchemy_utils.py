from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


class SQLAlchemyUtils:
    @staticmethod
    def get_database_uri(vault_data):
        return URL(**vault_data)

    @staticmethod
    def get_engine(vault_data):
        engine = create_engine(URL(**vault_data))
        return engine
