from __future__ import annotations

from subprocess import CompletedProcess

import pytest

from scripts.run_batch import has_compatible_java_runtime, parse_java_major_version


def test_parse_java_major_version_supports_legacy_format() -> None:
    output = 'java version "1.8.0_442"\nJava(TM) SE Runtime Environment'
    assert parse_java_major_version(output) == 8


def test_parse_java_major_version_supports_modern_format() -> None:
    output = 'openjdk version "17.0.12" 2024-07-16'
    assert parse_java_major_version(output) == 17


def test_parse_java_major_version_returns_none_for_unexpected_output() -> None:
    assert parse_java_major_version("no version token") is None


def test_has_compatible_java_runtime_false_for_incompatible_major(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_run(*_args: object, **_kwargs: object) -> CompletedProcess[str]:
        return CompletedProcess(
            args=["java", "-version"],
            returncode=0,
            stdout="",
            stderr='openjdk version "1.8.0_442"',
        )

    monkeypatch.setattr("scripts.run_batch.subprocess.run", _fake_run)

    assert has_compatible_java_runtime() is False


def test_has_compatible_java_runtime_true_for_supported_major(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_run(*_args: object, **_kwargs: object) -> CompletedProcess[str]:
        return CompletedProcess(
            args=["java", "-version"],
            returncode=0,
            stdout="",
            stderr='openjdk version "17.0.12"',
        )

    monkeypatch.setattr("scripts.run_batch.subprocess.run", _fake_run)

    assert has_compatible_java_runtime() is True
