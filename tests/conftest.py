"""Fixture definitions."""

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def page_html() -> str:
    html_path = Path(__file__).parent / "static" / "matrix.html"

    with open(html_path, "r") as fptr:
        html = fptr.read()

    return html


class MockResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        return None
