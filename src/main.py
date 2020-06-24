import argparse
import sys
import time
import importlib
import stringcase

from helper.spark import create_spark_session


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', type=str, required=True)
    return parser.parse_args(argv[1:])


def main(argv):
    args = parse_args(argv)
    
    # Load the task module / class via meta programming approach
    # GoodpathTask -> goodpath_task
    name = stringcase.snakecase(args.task)
    try:
        task_module = importlib.import_module(f'tasks.{name}')
    except ImportError:
        raise ValueError(f'Invalid Task: {args.task} task not found')
    
    spark = create_spark_session(name)
    sc = spark.sparkContext
    
    try:
        start = time.time()

        # Instantiate the task module class with spark session and args and run it
        klass = getattr(task_module, args.task)
        klass(spark).run()

        end = time.time()

        print(f'Task {name} took {end-start} seconds')

    except Exception as e:
        raise (e)
    finally:
        # Clean up the spark context
        sc.stop()
    
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
