from logging import INFO, Formatter, StreamHandler, getLogger


def get_logger(level=INFO):
    logger = getLogger("gcslock")

    if not logger.hasHandlers():
        handler = StreamHandler()

        logger.setLevel(level)
        handler.setLevel(level)
        handler.setFormatter(
            Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )

        logger.addHandler(handler)

    return logger
