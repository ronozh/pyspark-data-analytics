from abc import ABCMeta, abstractmethod

from helper.log import get_logger

class SparkTask(metaclass=ABCMeta):

    def __init__(self, spark):
        self.spark = spark
        self.logger = get_logger(self.spark.sparkContext, 
                                 self.__class__.__name__, 
                                 'INFO')


    @abstractmethod
    def run(self):
        raise NotImplementedError('All tasks should implement a run method!')
