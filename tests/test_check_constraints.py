from __future__ import annotations

from pathlib import Path

from tools import check_constraints


def _write(path: Path, content: str) -> None:
    path.write_text(content.strip() + "\n")


def test_constraints_check_passes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(
        tmp_path / "requirements.in",
        """
        requests>=2.0
        pyyaml>=6.0
        """,
    )
    _write(
        tmp_path / "requirements-dev.in",
        """
        -r requirements.in
        pytest-cov
        """,
    )
    _write(
        tmp_path / "requirements.constraints.txt",
        """
        requests==2.32.2
        pyyaml==6.0.1
        """,
    )
    _write(
        tmp_path / "requirements-dev.constraints.txt",
        """
        -r requirements.constraints.txt
        pytest-cov==5.0.0
        """,
    )

    assert check_constraints.main() == 0


def test_constraints_check_reports_missing_pin(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "requirements.in", "requests>=2.0\n")
    _write(tmp_path / "requirements-dev.in", "-r requirements.in\n")
    _write(tmp_path / "requirements.constraints.txt", "")
    _write(tmp_path / "requirements-dev.constraints.txt", "-r requirements.constraints.txt\n")

    assert check_constraints.main() == 1
    captured = capsys.readouterr().out
    assert "requirements.constraints.txt: missing pin for requests" in captured


def test_constraints_check_reports_missing_include(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "requirements.in", "requests>=2.0\n")
    _write(tmp_path / "requirements-dev.in", "-r requirements.in\npytest-cov\n")
    _write(tmp_path / "requirements.constraints.txt", "requests==2.32.2\n")
    _write(tmp_path / "requirements-dev.constraints.txt", "pytest-cov==5.0.0\n")

    assert check_constraints.main() == 1
    captured = capsys.readouterr().out
    assert "missing '-r requirements.constraints.txt' include" in captured
