from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from ..context import Context


class ExecutionContainer(ABC):
    def __init__(self, context: Context):
        self.context = context
        self.__logger = self.context.logger.getLogger(__name__)
        self._dataframe = None

    @abstractmethod
    def execute(self):
        """Run the execution to produce the dataframe"""

    @property
    def dataframe(self):
        """Return the output dataframe from this pipeline"""
        if not self._dataframe:
            self.execute()
        return self._dataframe

    @dataframe.setter
    def dataframe(self, df: DataFrame):
        if not isinstance(df, DataFrame):
            self.__logger.error("cannot set unknown type as dataframe")
            raise ValueError("Not a valid dataframe instance")
        self._dataframe = df

    def exec(self):
        """Force the execution of the dataframe pipeline"""
        self.dataframe = self.execute()
        return self
