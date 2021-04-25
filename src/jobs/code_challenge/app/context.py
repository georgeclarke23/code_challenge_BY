import os


class Context:
    def __init__(self, app_name=None, spark=None, logger=None):
        self.logger = logger
        self.app_name = app_name
        self.spark = spark
        self.state = {}

    def get(self, key):
        """
        To get variables in the local state or the environment
        :param key: name of the variable
        :return:
        """
        if key in self.state:
            return self.state[key]
        else:
            return os.environ[key]

    def set(self, *args, **kwargs):
        """
        Set variables to local state
        :param args:
        :param kwargs:
        :return:
        """
        if len(args) == 0 and len(kwargs) == 0:
            raise ValueError(f"Setting variables onto the state requires named arguments or a dictionary")
        for arg in args:
            if not isinstance(arg, dict):
                raise ValueError("Setting variables onto the state does not support non dictionary or non-named arguments")
            self.state.update(arg)
        if len(kwargs) > 0:
            self.state.update(kwargs)