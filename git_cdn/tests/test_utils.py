# Standard Library
from dataclasses import dataclass
from typing import List

from git_cdn.util import remove_git_credentials


def test_find_gitpath():
    @dataclass
    class TestCase:
        name: str
        input: List[str]
        expected: List[str]

    testcases = [
        TestCase(
            name="no git url",
            input=["git", "clone", "--progress"],
            expected=["git", "clone", "--progress"],
        ),
        TestCase(
            name="secret present https",
            input=[
                "git",
                "clone",
                "https://username:secret_token@gitlab.com/grouperenault/git_cdn.git",
            ],
            expected=[
                "git",
                "clone",
                "https://username:*****@gitlab.com/grouperenault/git_cdn.git",
            ],
        ),
        TestCase(
            name="whith ssh",
            input=["git", "clone", "git@github.com:test/rock-paper-scissors.git"],
            expected=["git", "clone", "git@github.com:test/rock-paper-scissors.git"],
        ),
    ]

    for case in testcases:
        actual = remove_git_credentials(case.input)
        assert (
            case.expected == actual
        ), f"failed test {case.name} expected {case.expected}, actual {actual}"
