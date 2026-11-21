"""The dashboard renders every tab from the committed evidence without errors (spec T09)."""
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402


def test_dashboard_renders_without_exceptions():
    at = AppTest.from_file("src/visualisation/app.py", default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    assert len(at.tabs) == 7
    assert any("Equal to oracle" in m.label for m in at.metric)
    assert any(m.value == "yes" for m in at.metric if m.label == "Equal to oracle")


def test_dashboard_names_its_sources():
    at = AppTest.from_file("src/visualisation/app.py", default_timeout=60).run()
    captions = [c.value for c in at.caption]
    assert sum(c.startswith("Source:") for c in captions) >= 10
    assert any("Open-Meteo.com" in c for c in captions)          # licence attribution
