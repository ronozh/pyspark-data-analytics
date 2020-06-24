from abc import ABCMeta
import pandas as pd

from .spark_task import SparkTask


class StockAnalyticsTask(SparkTask, metaclass=ABCMeta):
    
    def run(self):
        self.logger.info('Running stock analytics task')
        dfs = pd.read_html('https://au.finance.yahoo.com/quote/WBK/history?p=WBK')
        wbk = self.spark.createDataFrame(dfs[0])
        wbk.show()
