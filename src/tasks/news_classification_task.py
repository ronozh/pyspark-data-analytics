from abc import ABCMeta
import pyspark.sql.functions as F
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from .spark_task import SparkTask

class NewsClassificationTask(SparkTask, metaclass=ABCMeta):
    
    def run(self):
        self.logger.info('Running news classification task')
        
        self.logger.info('loading news raw data...')
        train_dataset, test_dataset = self.load_data()
        
        self.logger.info('creating binary classification evaluator...')
        self.evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction')
        
        self.logger.info('preparing machine learning pipeline...')
        self.pipeline = self.prepare_pipeline()
        
        self.logger.info('training...')
        self.train(train_dataset)
        
        self.logger.info('evaluating AUCROC at test dataset...')
        self.evaluation(test_dataset)
        
    def load_data(self):
        df = self.spark.read.csv("./data/news.csv", header = True, inferSchema=True, sep = "|")
        train, test =  df.randomSplit([0.8, 0.2], seed = 2020)
        return (train, test)
    
    def prepare_pipeline(self):
        tokenizer = RegexTokenizer(inputCol="content", outputCol="tokens", pattern="\\W")
        remover = StopWordsRemover(inputCol='tokens', outputCol='tokens_filtered')
        cv = CountVectorizer(inputCol='tokens_filtered', outputCol='count_vec')
        idf = IDF(inputCol='count_vec', outputCol='features')
        label_indexer = StringIndexer(inputCol = 'type', outputCol = 'label')
        nb = NaiveBayes()
        pipeline = Pipeline(stages = [tokenizer, remover, cv, idf, label_indexer, nb])
        
        return pipeline
    
    def train(self, train_dataset):
        paramGrid = (ParamGridBuilder()
                     .addGrid(NaiveBayes.smoothing, [0.1, 0.5, 1.0])
                     .build())
        
        crossval = CrossValidator(estimator=self.pipeline, 
                                  estimatorParamMaps=paramGrid, 
                                  evaluator=self.evaluator,
                                  numFolds=5)
        
        self.model = crossval.fit(train_dataset)
    
    def evaluation(self, test_dataset):
        predictions = self.model.transform(test_dataset)
        auc = self.evaluator.evaluate(predictions)
        self.logger.info(f'AUCROC on test dataset is {auc}')
        
    
    def save(self):
        pass