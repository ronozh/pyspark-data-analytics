import argparse
import sys
import time
import importlib
import stringcase
import pdb

from helper.spark import create_spark_session
from tasks.spark_task import SparkTask


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', type=str, required=True)
    return parser.parse_args(argv[1:])


def main(argv):
    args = parse_args(argv)
    
    # Load the task module / class via meta programming approach
    # e.g., GoodpathTask -> goodpath_task
    name = stringcase.snakecase(args.task)
    try:
        task_module = importlib.import_module(f'tasks.{name}')
    except ImportError:
        raise ValueError(f'Invalid Task: {args.task} task not found')
    
    start = time.time()
    
    klass = getattr(task_module, args.task)
    try:
        spark = create_spark_session(name)
        sc = spark.sparkContext
        sc.setLogLevel('WARN')
        klass(spark).run()
    except Exception as e:
        raise (e)
    finally:
        sc.stop()                

    end = time.time()
    print(f'Task {name} took {end-start} seconds')

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
