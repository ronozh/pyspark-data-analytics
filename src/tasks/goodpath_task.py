from abc import ABCMeta

from .base_task import BaseTask


class GoodpathTask(BaseTask, metaclass=ABCMeta):

    def run(self):
        print('Running goodpath task')
        test_df = self.spark.createDataFrame(["hello", "pyspark"], "string").toDF("test")
        test_df.show()
