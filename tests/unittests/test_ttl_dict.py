import time

import pytest

from gcslock._ttl_dict import TTLDict


@pytest.mark.unittest
class TestTTLDict:
    def test_set_and_get_with_default_ttl(self, monkeypatch):
        now = [100.0]  # リストで可変にしてモック内から更新
        monkeypatch.setattr(time, "monotonic", lambda: now[0])

        d = TTLDict(default_ttl=10)
        d["a"] = 123  # デフォルト TTL で保存
        assert d["a"] == 123

        # 有効期限内
        now[0] += 5
        assert d["a"] == 123

        # 有効期限超過
        now[0] += 6
        with pytest.raises(KeyError, match="a is expired"):
            _ = d["a"]

    def test_set_with_custom_ttl(self, monkeypatch):
        now = [200.0]
        monkeypatch.setattr(time, "monotonic", lambda: now[0])

        d = TTLDict(default_ttl=10)
        d["x"] = (999, 3)  # TTL=3で保存
        assert d["x"] == 999

        now[0] += 4
        with pytest.raises(KeyError, match="x is expired"):
            _ = d["x"]

    def test_key_not_found(self):
        d = TTLDict()
        with pytest.raises(KeyError, match="no_such_key not found"):
            _ = d["no_such_key"]

    def test_delete_and_len_iter(self):
        d = TTLDict()
        d["a"] = 1
        d["b"] = 2
        assert len(d) == 2
        assert set(d) == {"a", "b"}

        del d["a"]
        assert "a" not in d._ttl_dict
        assert len(d) == 1

    def test_thread_safety_lock_usage(self, monkeypatch):
        # RLock をモック化して呼び出しが行われるか確認
        class DummyLock:
            def __enter__(self):
                pass

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        lock = DummyLock()
        monkeypatch.setattr("gcslock._ttl_dict.RLock", lambda: lock)

        d = TTLDict()
        d["k"] = 42
        assert d["k"] == 42
        del d["k"]
