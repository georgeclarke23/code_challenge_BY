import pytest

from src.jobs.code_challenge.app.context import Context

@pytest.fixture
def fake_context():
    return Context()