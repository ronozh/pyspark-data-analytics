def get_logger(sc, name, level):
    log4j = sc._jvm.org.apache.log4j  # pylint: disable=protected-access
    logger = log4j.LogManager.getLogger(name)
    logger.setLevel(log4j.Level.toLevel(level))
    return logger
