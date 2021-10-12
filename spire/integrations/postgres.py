from contextlib import contextmanager
from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from spire.config import config
from spire.api import ConnectorException


class SpireDBConnector:
    """
    Uses a database engine to create a SQLALchemy session
    context for loading data from postgres into Spire via the workflow ORM.
    Can also be used as a session registry to maintain a global
    session for continuously connecting with e.g. workflows.
    """

    def __init__(self):
        self.make_session = self._session_factory()
        self.make_scoped_session = scoped_session(self.make_session)

    def _session_factory(self):
        """Instantiates a new session"""
        conn_str = "postgresql+psycopg2://{user}:{password}@{host}/{dbname}".format(
            host=config.DB_HOSTNAME,
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
        )
        self.engine = create_engine(conn_str)
        return sessionmaker(bind=self.engine)

    def refresh_session_factory(self):
        """As the current desing works as singleton, we need to reinitialize with
        updated config values if config object changes
        """
        self.__init__()

    def session_transaction(self, func):
        """
        This method is a decorator that takes a func such as a
        Workflow instance method, creates a SQLAlchemy session and
        manages its context, and wraps this around the function.
        """

        @contextmanager
        def session_context(self, session=None):
            """
            session_context creates the session transaction and try/except/finally
            logic to rollback failed transactions. It yields a session which creates
            the session context in wrap_session
            """
            session = session or self.make_session()
            try:
                yield session
                if any([session.new, session.dirty, session.deleted]):
                    session.commit()
            except Exception as e:
                session.rollback()
                raise ConnectorException(e)
            finally:
                session.close()

        @wraps(func)
        def wrap_session(*args, **kwargs):
            """
            Uses the session context from session_context, sets the session
            as a keyword arg for the original func, and returns it.
            The context produced by session_context will execute the rest of
            the try/except/finally block even though the func is returned; serving as
            an outer 'finally' block
            """
            session = kwargs.pop("session", None)
            with session_context(self, session=session) as session:
                kwargs["session"] = session
                return func(*args, **kwargs)

        return wrap_session


connector = SpireDBConnector()
