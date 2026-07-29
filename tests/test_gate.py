import pytest

from backbone_pull.flow import assert_complete


def test_assert_complete_zero_does_not_raise():
    assert_complete(0)  # tidak boleh raise


def test_assert_complete_positive_raises_with_count():
    with pytest.raises(RuntimeError) as e:
        assert_complete(3)
    msg = str(e.value)
    assert "3" in msg and "pull_failures" in msg
