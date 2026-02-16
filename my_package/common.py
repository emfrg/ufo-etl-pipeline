import logging
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession


class Task(ABC):
    """
    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.logger provides access to the Spark-compatible logger
    """

    def __init__(self, spark=None, init_conf=None):
        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()

    @staticmethod
    def _prepare_spark(spark) -> SparkSession:
        if not spark:
            return SparkSession.builder.getOrCreate()
        else:
            return spark

    def _prepare_logger(self):
        return logging.getLogger(self.__class__.__name__)

    
    def read_table(self, path: str, frmt: str = "DELTA", opt: dict = {}):
        """Read external tables"""
        df = self.spark.read.load(format=frmt, path=path, **opt)
        return df
       
    def read_managed_table(self, table_name: str):
        """Read manaed tables"""
        df = self.spark.table(table_name)
        return df    
    
    def write_managed_table(self, df, table_name: str, mode: str ="OVERWRITE", storage_path: str=None):
        if storage_path:
            df.write.mode(mode).option("path", storage_path).saveAsTable(table_name)
        else:
            df.write.mode(mode).saveAsTable(table_name)

    @abstractmethod
    def launch(self):
        """
        Main method of the job.
        :return:
        """
        pass