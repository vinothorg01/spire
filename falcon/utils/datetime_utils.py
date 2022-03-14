import pendulum


class DateTimeUtils:
    @staticmethod
    def get_now_epoch_time(tz="UTC"):
        return pendulum.now(tz=tz).int_timestamp

    @staticmethod
    def get_now_datetime(tz="UTC"):
        return pendulum.now(tz=tz)

    @staticmethod
    def get_now_datetime_str(tz="UTC"):
        return pendulum.now(tz=tz).to_datetime_string()

    @staticmethod
    def convert_epochtime_todatetime_str(epoch_time, tz="UTC"):
        return pendulum.from_timestamp(epoch_time, tz=tz).to_datetime_string()

    @staticmethod
    def convert_datetimestr_datetime(datetime_str, tz="UTC"):
        return pendulum.parse(datetime_str, tz=tz)

    @staticmethod
    def convert_datetimestr_epochtime(datetime_str, tz="UTC"):
        return pendulum.parse(datetime_str, tz=tz).int_timestamp

    @staticmethod
    def convert_socialflow_createtime_epochtime(datetime_string, tz="America/New_York"):
        """

        Args:
            datetime_string:
            tz: Pass the timezone which is used for Socialflow, and convert to UTC

        Returns:

        """
        dt = pendulum.from_format(datetime_string, "MM-DD-YYYY HH:mm A", tz=tz)
        return dt.in_tz(tz="UTC").int_timestamp


class AnchorDateTimeUtils:
    @staticmethod
    def get_currenthours_from_anchorpoint(anchor_date):
        """[summary]

        Arguments:
            anchor_date {[type]} -- [description]

        Returns:
            [type] -- [description]
        """
        return (
            pendulum.now(tz="UTC").int_timestamp
            - pendulum.parse(anchor_date, tz="UTC").int_timestamp
        ) // 3600

    @staticmethod
    def get_definedhours_from_anchorpoint(anchor_date, defined_date):
        """[summary]

        Arguments:
            anchor_date {[type]} -- [description]
            defined_date {[type]} -- [description]

        Returns:
            [type] -- [description]
        """
        return (
            pendulum.parse(defined_date, tz="UTC").int_timestamp
            - pendulum.parse(anchor_date, tz="UTC").int_timestamp
        ) // 3600
