from abc import ABCMeta

from .spark_task import SparkTask


class GoodpathTask(SparkTask, metaclass=ABCMeta):

    def run(self):
        self.logger.info('Running goodpath task')
        test_df = self.spark.createDataFrame(["hello", "pyspark"], "string").toDF("test")
        test_df.show()
