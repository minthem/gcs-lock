import logging

import pytest

from gcslock._logger import get_logger


@pytest.mark.unittest
class TestGetLogger:
    @pytest.fixture(autouse=True)
    def reset_gcslock_logger(self):
        logger = logging.getLogger("gcslock")

        # 前後でクリーンアップ（ハンドラ・レベル・伝搬）
        def _cleanup():
            for h in list(logger.handlers):
                logger.removeHandler(h)
                try:
                    h.close()
                except Exception:
                    pass
            logger.setLevel(logging.NOTSET)
            logger.propagate = False

        _cleanup()
        yield
        _cleanup()

    def test_first_call_creates_streamhandler_and_formatter(self):
        logger = get_logger()  # default INFO
        assert logger.name == "gcslock"
        assert logger.level == logging.INFO
        assert logger.hasHandlers()
        assert len(logger.handlers) == 1

        h = logger.handlers[0]
        assert isinstance(h, logging.StreamHandler)
        assert h.level == logging.INFO

        # フォーマット設定を確認（文字列そのもの）
        fmt = getattr(h.formatter, "_fmt", "")
        assert fmt == "%(asctime)s %(levelname)s %(name)s %(message)s"

    def test_idempotent_second_call_does_not_add_handler(self):
        logger1 = get_logger()
        n1 = len(logger1.handlers)

        logger2 = get_logger()
        n2 = len(logger2.handlers)

        assert logger1 is logger2
        assert n2 == n1  # 追加されない
        assert logger2.level == logging.INFO  # 変更されない

    def test_level_applied_only_on_first_call(self):
        # 初回呼び出しで WARNING を設定
        logger = get_logger(level=logging.WARNING)
        assert logger.level == logging.WARNING
        assert len(logger.handlers) == 1
        assert logger.handlers[0].level == logging.WARNING

        # 2回目で DEBUG を指定しても、既にハンドラがあるため何も変わらない
        logger2 = get_logger(level=logging.DEBUG)
        assert logger2 is logger
        assert logger2.level == logging.WARNING
        assert logger2.handlers[0].level == logging.WARNING

    def test_preserves_preexisting_handler_and_level(self):
        # 先にユーザー側でハンドラを付けているケース
        logger = logging.getLogger("gcslock")
        pre = logging.StreamHandler()
        pre.setLevel(logging.ERROR)
        pre.setFormatter(logging.Formatter("X %(levelname)s %(name)s %(message)s"))
        logger.addHandler(pre)
        logger.setLevel(logging.ERROR)

        # get_logger を呼んでも既存が優先され、変更されない
        lg = get_logger(level=logging.INFO)
        assert lg is logger
        assert len(lg.handlers) == 1
        assert lg.level == logging.ERROR
        assert lg.handlers[0].level == logging.ERROR

        # フォーマットも既存のまま
        assert getattr(lg.handlers[0].formatter, "_fmt", "").startswith("X ")

    def test_log_output_format_contains_level_name_logger_and_message(self, capsys):
        logger = get_logger()  # INFO
        logger.propagate = False  # 親への二重出力を避ける

        logger.info("hello")
        captured = capsys.readouterr()

        # StreamHandler のデフォルトは stderr
        line = captured.err.strip().splitlines()[-1]
        # 先頭は asctime（可変）なので、後半の安定部分を確認
        assert " INFO gcslock hello" in line
