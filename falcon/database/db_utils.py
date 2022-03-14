from sqlalchemy import Table, select, and_, insert, delete, update
from collections import namedtuple
import json


class DBUtils:
    """
    Database utility class
    """

    def __init__(self, db_connection, engine, metadata_brand, metadata_common, metadata_settings):
        """
        Initialization for DBUtils Class
        Args:
            db_connection:
            engine:
            metadata_brand:
            metadata_common:
            metadata_settings:
        """
        self.db_connection = db_connection
        self.engine = engine
        self.metadata_common = metadata_common
        self.metadata_settings = metadata_settings
        self.metadata_brand = metadata_brand

    @classmethod
    def init_with_db_config(cls, db_config):
        """
        Args:
            db_config:
        """
        db_connection = db_config["connection"]
        engine = db_config["engine"]
        metadata_common = db_config["metadata_common"]
        metadata_settings = db_config["metadata_settings"]
        metadata_brand = db_config["metadata_brand"]
        return cls(db_connection, engine, metadata_brand, metadata_common, metadata_settings)

    @classmethod
    def init_without_db_config(cls, db_connection, engine, **kwargs):
        """
        Args:
            db_connection:
            engine:
            kwargs:
        """
        db_connection = db_connection
        engine = engine
        metadata_common = kwargs.get("metadata_common")
        metadata_settings = kwargs.get("metadata_settings")
        metadata_brand = kwargs.get("metadata_brand")
        return cls(db_connection, engine, metadata_brand, metadata_common, metadata_settings)

    def get_social_platform_id(self, platform_name):
        """[summary]

        Arguments:
            db_connection {[type]} -- [description]
            social_account_type {[type]} -- [description]
            metadata {[type]} -- [description]
            engine {[type]} -- [description]

        Returns:
            [type] -- [description]
        """
        social_platform_table = Table(
            "social_platform_details", self.metadata_common, autoload=True, autoload_with=self.engine
        )

        with self.db_connection.begin() as trans:
            stmt1 = select([social_platform_table]).where(
                social_platform_table.columns.platform_name == platform_name
            )
            result_proxy1 = self.db_connection.execute(stmt1)

        platform_id = result_proxy1.fetchone()[0]
        return platform_id

    def get_brand_id(self, brand_name):
        """
           Args:
               brand_name:
        """
        brand_details = Table("brand_details", self.metadata_common, autoload=True, autoload_with=self.engine)

        with self.db_connection.begin() as trans:
            stmt = select([brand_details]).where(brand_details.columns.brand_name == brand_name)
            result_proxy = self.db_connection.execute(stmt)
            brand_id = result_proxy.fetchone()[0]
            return brand_id

    def get_brand_config(self, brand_name, social_platform_id):
        """[summary]

        Arguments:
            db_connection {[type]} -- connecting the postgres
            brand_name {[String]} - specific brand name
            social_account_id {[type]} -- facebook/twitter
            metadata {[type]} -- [description]
            engine {[type]} -- [description]

        Returns:
            [json] -- [config of specific brand & account]
        """
        brand_config = Table("config", self.metadata_settings, autoload=True, autoload_with=self.engine)
        cond1 = and_(brand_config.c.social_platform_id == social_platform_id,
                     brand_config.c.brand_name == brand_name)
        with self.db_connection.begin() as trans:
            stmt1 = select([brand_config]).where(cond1)
            result_set = self.db_connection.execute(stmt1)
            config = (result_set.fetchone())

        return dict(config)

    def get_all_brand_config(self):
        """[summary]

        Arguments:
            db_connection {[type]} -- connecting the postgres
            metadata {[type]} -- [description]
            engine {[type]} -- [description]

        Returns:
            [json] -- [configs of all brand & account]
        """
        config = Table("config", self.metadata_settings, autoload=True, autoload_with=self.engine)
        with self.db_connection.begin() as trans:
            stmt1 = select([config])
            result_set = self.db_connection.execute(stmt1)
            configs = json.dumps([dict(row) for row in result_set])

        return json.loads(configs)

    def get_social_partner_accounts(self, social_partner_accounts, platform_name, partner_brand_name):
        """
        Args:
            social_partner_accounts:
            platform_name:
            partner_brand_name:
        """

        PartnerRecord = namedtuple(
            "PartnerIDRecord",
            "social_platform_name, partner_brand_name, client_id, client_name, client_service_id, service_user_id",
        )
        with self.db_connection.begin() as trans:
            stmt1 = (
                select([social_partner_accounts])
                    .where(
                    social_partner_accounts.columns.social_platform_name
                    == platform_name
                )
                    .where(social_partner_accounts.columns.partner_brand_name == partner_brand_name)
            )
            result_proxy1 = self.db_connection.execute(stmt1)

        if result_proxy1.rowcount > 0:  # Already a value exists
            results1 = result_proxy1.first()
            return PartnerRecord(*results1)

    def delete_social_partner_accounts(self, social_partner_accounts):
        """
        Args:
            social_partner_accounts:
        """
        with self.db_connection.begin() as trans:
            stmt2 = delete(social_partner_accounts)
            result_proxy2 = self.db_connection.execute(stmt2)
            return result_proxy2.rowcount

    def insert_into_partner_accounts(self, social_partner_accounts, client_accounts):
        """
        Args:
            social_partner_accounts:
            client_accounts:
        """
        with self.db_connection.begin() as trans:
            stmt3 = insert(social_partner_accounts).values(
                social_platform_name=client_accounts["account_type"],
                partner_brand_name=client_accounts["name"],
                client_id=client_accounts["client_id"],
                client_name=client_accounts["client_name"],
                client_service_id=client_accounts["client_service_id"],
                service_user_id=client_accounts["service_user_id"],
            )
            result_proxy = self.db_connection.execute(stmt3)
            return result_proxy.rowcount

    def get_socialcopy_last_update_time(self, socialcopy_last_update_time, social_platform_id, brand_id):
        """
        Args:
            socialcopy_last_update_time:
            social_platform_id:
            brand_id:
        """
        last_recorded_ts = None
        with self.db_connection.begin() as trans:
            cond = and_(socialcopy_last_update_time.columns.social_platform_id == social_platform_id,
                        socialcopy_last_update_time.columns.brand_id == brand_id)
            stmt1 = select([socialcopy_last_update_time]).where(cond)
            result_proxy1 = self.db_connection.execute(stmt1)

        if result_proxy1.rowcount > 0:  # Already a value exists
            results1 = result_proxy1.first()
            last_recorded_ts = results1.ts_epoch

        return last_recorded_ts

    def insert_socialcopy_last_update_time(self, socialcopy_last_update_time, social_platform_id, brand_id, ts_epoch):
        """
        Args:
            socialcopy_last_update_time:
            social_platform_id:
            brand_id:
            ts_epoch:
        """
        with self.db_connection.begin() as trans:
            stmt2 = insert(socialcopy_last_update_time).values(brand_id=brand_id,
                                                               social_platform_id=social_platform_id, ts_epoch=ts_epoch
                                                               )
            result_proxy2 = self.db_connection.execute(stmt2)
            return result_proxy2.rowcount

    def update_socialcopy_last_update_time(self, socialcopy_last_update_time, social_platform_id, brand_id, ts_epoch):
        """
        Args:
            socialcopy_last_update_time
            social_platform_id
            brand_id:
            ts_epoch:
        """
        with self.db_connection.begin() as trans:
            cond = and_(socialcopy_last_update_time.columns.social_platform_id == social_platform_id,
                        socialcopy_last_update_time.columns.brand_id == brand_id)
            stmt3 = update(socialcopy_last_update_time).values(ts_epoch=ts_epoch)
            stmt3 = stmt3.where(cond)
            result_proxy3 = self.db_connection.execute(stmt3)
            return result_proxy3.rowcount

    def get_later_details(self, later_db, element):
        """
        Args:
            later_db:
            element:
        """
        with self.db_connection.begin() as trans:
            stmt1 = select([later_db]).where(
                later_db.columns.content_item_id
                == element["content_item_id"]
            )
            result = self.db_connection.execute(stmt1)
            return result

    def insert_element_to_table(self, table, element):
        """
        Args:
            table:
            element:
        """
        result_proxy = self.db_connection.execute(
            table.insert(), element
        )
        return result_proxy.rowcount

    def delete_from_table(self, db, predicate=None):
        """
        Args:
            db:
            predicate:
        """
        stmt1 = delete(db).where(predicate)
        result_proxy = self.db_connection.execute(stmt1)
        return result_proxy

    def select_from_table(self, db, predicate=None):
        """
        Args:
            db:
            predicate:
        """
        stmt3 = select([db]) if predicate is None else select([db]).where(predicate)
        result_proxy = self.db_connection.execute(stmt3)
        return result_proxy
