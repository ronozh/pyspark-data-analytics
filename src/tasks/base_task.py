from abc import ABCMeta, abstractmethod

class BaseTask(metaclass=ABCMeta):

    def __init__(self, spark):
        self.spark = spark


    @abstractmethod
    def run(self):
        raise NotImplementedError('All tasks should implement a run method!')
