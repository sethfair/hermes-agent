"""Tests for Bot API 10.1 Rich Messages (sendRichMessage) on Telegram.

Final / new-message replies opportunistically use ``sendRichMessage`` with the
RAW agent markdown so tables, task lists, etc. render natively. The legacy
MarkdownV2 ``send_message`` path stays as the fallback for unsupported /
oversized content and for transports that lack the endpoint.

The ``telegram`` package is mocked by ``tests/gateway/conftest.py``
(:func:`_ensure_telegram_mock`), so these tests construct a real
``TelegramAdapter`` and wire a mock bot.
"""

import asyncio
import itertools
import logging
import random
import re
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
from plugins.platforms.telegram.adapter import TelegramAdapter
from telegram.error import BadRequest, NetworkError, TimedOut


# Content exercising rich-only constructs: a heading, a real Markdown table,
# and a task list. Pipes / brackets must survive untouched into the payload.
RICH_CONTENT = "## Results\n\n| Case | Status |\n|---|---|\n| rich | ✅ |\n\n- [x] table renders"
CJK_RICH_CONTENT = "## 持仓\n\n| 项目 | 状态 |\n|---|---|\n| 早盘 | 正常 |"
ASTRAL_CJK_RICH_CONTENT = "## Rare Han\n\n| glyph | status |\n|---|---|\n| \U00030000 | ok |"
TABLE_ONLY_CONTENT = (
    "| Team | W | L | GB |\n"
    "|---|---|---|---|\n"
    "| Red Sox | 36 | 34 | 6.0 |\n"
    "| Dodgers | 40 | 30 | 2.0 |"
)
DANGEROUS_DETAILS_MATH = (
    "<details><summary>Complex proof</summary>\n\n"
    "$$\\sum_{i=1}^{n} i = \\frac{n(n+1)}{2}$$\n\n"
    "And inline \\(\\alpha + \\beta\\)\n"
    "</details>"
)

# PTB 22.6's real unknown-endpoint errors: do_api_request can raise
# EndPointNotFound for Bot API 404s, and the request layer can wrap that same
# missing endpoint as InvalidToken. Use class names here so the tests don't
# depend on optional PTB internals.
EndPointNotFound = type("EndPointNotFound", (Exception,), {})
InvalidToken = type("InvalidToken", (Exception,), {})
PTB_ENDPOINT_NOT_FOUND = EndPointNotFound(
    "Endpoint 'sendRichMessage' not found in Bot API"
)
PTB_INVALID_TOKEN_404 = InvalidToken(
    "Either the bot token was rejected by Telegram or the endpoint "
    "'sendRichMessage' does not exist."
)


def _make_adapter(extra=None):
    """Build a TelegramAdapter with a mock bot wired for the rich path."""
    config = PlatformConfig(
        enabled=True,
        token="fake-token",
        extra={"rich_messages": True, **(extra or {})},
    )
    adapter = TelegramAdapter(config)
    bot = MagicMock()
    # do_api_request as an AsyncMock makes inspect.iscoroutinefunction(...) True,
    # so _bot_supports_rich() is satisfied (real Bot.do_api_request is async too).
    bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=123))
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))
    bot.send_chat_action = AsyncMock()  # keeps the post-send typing re-trigger quiet
    bot.send_message_draft = AsyncMock(return_value=True)  # legacy draft fallback
    bot.edit_message_text = AsyncMock(return_value=MagicMock(message_id=1))  # legacy edit path
    bot.delete_message = AsyncMock(return_value=True)
    adapter._bot = bot
    return adapter


def _rich_api_kwargs(adapter):
    """Return the api_kwargs dict from the single sendRichMessage call."""
    call = adapter._bot.do_api_request.call_args
    assert call.args[0] == "sendRichMessage"
    return call.kwargs["api_kwargs"]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["send", "edit", "draft"])
async def test_rich_delivery_keeps_literal_hash_references(operation):
    adapter = _make_adapter({"rich_drafts": True})
    content = (
        "#89 remains open.\n\n- #181 reported a problem.\n\n> #tag is literal.\n\n"
        "## Real heading\n\n- [x] rich delivery\n\n`#89` stays code."
    )
    if operation == "send":
        result = await adapter.send("12345", content)
        assert result.success
    elif operation == "edit":
        result = await adapter.edit_message("12345", "123", content, finalize=True)
        assert result.success
    else:
        assert await adapter.send_draft("12345", 123, content)
    assert adapter._bot is not None
    payload = adapter._bot.do_api_request.call_args.kwargs["api_kwargs"]["rich_message"]
    assert r"\#89 remains open." in payload["markdown"]
    assert r"- \#181 reported a problem." in payload["markdown"]
    assert r"> \#tag is literal." in payload["markdown"]
    assert "## Real heading" in payload["markdown"]
    assert "`#89` stays code." in payload["markdown"]
    adapter._bot.send_message.assert_not_called()
    adapter._bot.edit_message_text.assert_not_called()
    adapter._bot.send_message_draft.assert_not_called()


@pytest.mark.parametrize("content", [
    "# Heading\n\n## Heading 2\n\n###### Heading 6",
    "Use #89 and https://example.com/#anchor inline.",
    r"\#89 is already escaped.",
    "`code\n#89\ncode`",
    "``code ` #89``",
    "```python\n#89 is a comment\n```",
    "~~~python\n#89 is a comment\n~~~",
    "````markdown\n```\n#89\n```\n````",
    "```python\n#89 unfinished fence",
    "$$\n#89\n$$",
])
def test_literal_hash_normalization_preserves_markdown_regions(content):
    from plugins.platforms.telegram.rich_markdown import escape_literal_hash_prefixes

    assert escape_literal_hash_prefixes(content) == content
    escaped = escape_literal_hash_prefixes("1. #89\n\n> - ##tag\n\n#release")
    assert escaped == "1. \\#89\n\n> - \\#\\#tag\n\n\\#release"
    assert escape_literal_hash_prefixes(escaped) == escaped
    # Inline ```x``` is a code span, not an unclosed block fence swallowing the rest of the reply.
    assert escape_literal_hash_prefixes("Use ```x``` here.\n\n#89 open.") == "Use ```x``` here.\n\n\\#89 open."


@pytest.mark.parametrize("content, expected", [
    # A backtick run with text after it is code inside the fence, not its closer.
    ("```\ntext ``` trailing\n#inside\n```\n#outside", "```\ntext ``` trailing\n#inside\n```\n\\#outside"),
    ("~~~~\n~~~ x\n#inside\n~~~~\n#outside", "~~~~\n~~~ x\n#inside\n~~~~\n\\#outside"),
    # A quoted fence ends with its blockquote.
    ("> ```\n> #inside\n#outside", "> ```\n> #inside\n\\#outside"),
    # A code span never pairs across a blank line.
    ("`open\n\n#89\nclose`", "`open\n\n\\#89\nclose`"),
    # A prose $$ is not math reaching into the fence below it, and math never crosses a paragraph.
    ("Your shell PID is $$ here.\n\n```bash\necho $$\n#!/usr/bin/env bash\n```\n#89",
     "Your shell PID is $$ here.\n\n```bash\necho $$\n#!/usr/bin/env bash\n```\n\\#89"),
    ("> Use $$ x\n> ```\n> $$\n> #not math\n> ```", "> Use $$ x\n> ```\n> $$\n> #not math\n> ```"),
    ("$$x\n\n#89\n$$", "$$x\n\n\\#89\n$$"),
    # An empty list item ends at the blank line, so a 3-space fence after it is a top-level fence.
    ("- \n\n   ```\n#89\n```\n#90", "- \n\n   ```\n#89\n```\n\\#90"),
])
def test_literal_hash_fences_close_like_commonmark(content, expected):
    from plugins.platforms.telegram.rich_markdown import escape_literal_hash_prefixes

    assert escape_literal_hash_prefixes(content) == expected


def test_rich_payload_is_linear_in_unclosed_backtick_runs():
    """A full rich-cap reply of unclosed backtick runs of distinct lengths (`a``a```a...) builds its
    payload well inside one second; a per-run rescan blocked the event loop for seconds."""
    adapter = _make_adapter()
    runs, k = [], 1
    while sum(map(len, runs)) < adapter.RICH_MESSAGE_MAX_CHARS:
        runs.append("`" * k + "a")
        k += 1
    content = "#89\n" + "".join(runs)[: adapter.RICH_MESSAGE_MAX_CHARS - 4]

    started = time.perf_counter()
    payload = adapter._rich_message_payload(content)

    assert time.perf_counter() - started < 1.0
    assert payload["markdown"].startswith("\\#89")

    # Fence-like lines that never close, and backtick runs split by paragraphs.
    unit = "```py\ntext ``` trailing\n#89\n> ```\n`open\n\n"
    content = (unit * (adapter.RICH_MESSAGE_MAX_CHARS // len(unit)))[: adapter.RICH_MESSAGE_MAX_CHARS]
    started = time.perf_counter()
    adapter._rich_message_payload(content)
    assert time.perf_counter() - started < 1.0


_RICH_CAP = TelegramAdapter.RICH_MESSAGE_MAX_CHARS


def _pipe_heavy_cases(cap):
    """Contents at the full rich cap that stress the table regex."""
    return [
        # The short first line forces newline processing; the long last line has no newline, which
        # is the old header's quadratic failure.
        pytest.param("x\n" + "|" * (cap - 2), id="header-pipes-no-newline"),
        pytest.param("|" * (cap - 3) + "\nxx", id="header-pipes-then-prose"),
        # Header, then a delimiter candidate of only whitespace: the old two `[ \t]*` split it at
        # every offset (tens of seconds).
        pytest.param("|\n" + " " * (cap - 3) + "x", id="delimiter-spaces"),
        pytest.param("|\n" + "\t" * (cap - 3) + "x", id="delimiter-tabs"),
        # A pipe between the runs does not stop the quadratic split (old ~7 s).
        pytest.param("|\n" + " " * (cap // 2) + "|" + " " * (cap // 2 - 5) + "x", id="delimiter-ws-pipe-ws"),
        # Guards for the unchanged data-row group: linear before and after.
        pytest.param("|a|b|\n|-|-|\n" + "|" * (cap - 12), id="data-row-pipe-heavy"),
        pytest.param("|a|b|\n|-|-|" + "\n|x|y|" * ((cap - 11) // 6), id="data-rows-many-short"),
        pytest.param("|a|b|\n|-|-|\n" + "x" * (cap - 12), id="data-row-pipeless-long"),
    ]


@pytest.mark.parametrize("content", _pipe_heavy_cases(_RICH_CAP))
def test_rich_payload_is_linear_in_pipe_heavy_lines(content):
    """Pipe-heavy and whitespace-heavy table candidates at the full rich cap build their payload in
    well under a second; the table regex's overlapping stars were quadratic on them.

    The bound matches the neighbouring backtick test (tests/AGENTS.md asks for >= 2 s on shared
    runners; the new code takes milliseconds and the 2x-cap test below keeps the discrimination).
    """
    adapter = _make_adapter()
    assert len(content) <= adapter.RICH_MESSAGE_MAX_CHARS

    started = time.perf_counter()
    payload = adapter._rich_message_payload(content)

    assert time.perf_counter() - started < 1.0
    assert payload["markdown"]


def test_table_regex_stays_linear_on_double_cap_pipes():
    """A direct search over twice the cap of pipes (old ~5 s) scales linearly, so a faster CPU
    cannot let a quadratic regex pass."""
    from plugins.platforms.telegram.rich_markdown import _RICH_TABLE_RE

    text = "|" * (2 * _RICH_CAP)

    started = time.perf_counter()
    assert _RICH_TABLE_RE.search(text) is None

    assert time.perf_counter() - started < 1.0


def test_rich_linebreaks_are_linear_in_closed_fences_without_a_table():
    """Many tiny closed fences and no table normalize well inside two seconds and stay verbatim;
    re-searching the whole remaining text for a table after every fence is quadratic (16k fences
    took ~24s)."""
    from plugins.platforms.telegram.rich_markdown import rich_normalize_linebreaks

    text = "```\nx```\n" * 16384

    started = time.perf_counter()
    out = rich_normalize_linebreaks(text)

    assert time.perf_counter() - started < 2.0
    assert out.count("```\nx```") == 16384


def test_rich_protected_regions_match_the_alternation_regex():
    """The linear region scan finds exactly the spans the original fence|table regex did."""
    import random
    import re

    from plugins.platforms.telegram.rich_markdown import _iter_rich_protected_regions

    original = re.compile(
        r'(?:```[^\n]*\n[\s\S]*?```)'
        r'|(?:^[^\n]*\|[^\n]*\n'
        r'[ \t]*\|?[ \t]*:?-+:?[ \t]*(?:\|[ \t]*:?-+:?[ \t]*)+\|?[ \t]*'
        r'(?:\n[^\n]*\|[^\n]*)*)',
        re.MULTILINE)
    pieces = ["```", "``", "`", "\n", "\n", "a", " ", "|", "---", ":-:", "| a | b |", "py", "~~~"]
    rng = random.Random(89)
    for _ in range(3000):
        text = "".join(rng.choice(pieces) for _ in range(rng.randint(1, 40)))
        assert list(_iter_rich_protected_regions(text)) == [m.span() for m in original.finditer(text)], text


# --- Table regex rewrite: exact-span equivalence with the frozen pre-rewrite pattern -------------
# The linear rewrite must not move a single span, so everything below compares span LISTS against
# this test-local copy of the old, quadratic pattern (the oracle). It is never used by production.
_OLD_TABLE_SRC = (
    r'^[^\n]*\|[^\n]*\n'
    r'[ \t]*\|?[ \t]*:?-+:?[ \t]*(?:\|[ \t]*:?-+:?[ \t]*)+\|?[ \t]*'
    r'(?:\n[^\n]*\|[^\n]*)*'
)
_OLD_TABLE_RE = re.compile(_OLD_TABLE_SRC, re.MULTILINE)
_OLD_ALTERNATION_RE = re.compile(r'(?:```[^\n]*\n[\s\S]*?```)|(?:' + _OLD_TABLE_SRC + ')', re.MULTILINE)

_TABLE_ALPHABET = ("|", "-", ":", " ", "\t", "\n", "x")
_FUZZ_PIECES = [
    "|", "-", ":", " ", "\t", "\n", "\n\n", "x", "\r\n", "\r", "x|y", " " * 7, "-" * 9, "|" * 5,
    "| a | b |", "|-|-|", "|---|---|", ":-:", " :--- ", "---:", "| :-- | --: |",
    "`", "``", "```", "````", "~~~", "py", "> ",
]

# (text, number of protected regions the old alternation finds). Counts are the old behaviour,
# including its quirks (CRLF delimiters and lone CRs are not tables).
_PINNED_TABLE_CASES = [
    pytest.param("| a | b |\n|---|---|\n| 1 | 2 |", 1, id="plain"),
    pytest.param("a | b\n--|--\n1 | 2", 1, id="no-outer-pipes"),
    pytest.param("| a | b | c |\n|:---|:---:|---:|\n| 1 | 2 | 3 |", 1, id="alignment"),
    pytest.param("| a | b |\n \t|---|---|\n| 1 | 2 |", 1, id="delimiter-leading-whitespace"),
    pytest.param("`a|b`\n|-|-|\nrow | x", 1, id="inline-code-header"),
    pytest.param("```\n| a | b |\n|---|---|\n```", 1, id="fence-holds-table"),
    pytest.param("| a | b |\r\n|---|---|\r\n| 1 | 2 |\r\n", 1, id="crlf-stops-at-cr"),
    pytest.param("| a | b |\r|---|---|\r| 1 | 2 |", 0, id="lone-cr"),
    pytest.param("| a | b |\n|-|-|", 1, id="no-trailing-newline"),
    pytest.param("| a | b |\n|-|-|\n", 1, id="trailing-newline-outside-span"),
    pytest.param("| a | b |\n|-|-|\n| 1 | 2 |\nprose", 1, id="followed-by-prose"),
    pytest.param("| a | b |\n|-|-|\n\n| c | d |\n|-|-|", 2, id="two-tables"),
    pytest.param("| a | b |\n|-|-|\n| c | d |\n|-|-|", 1, id="stacked-delimiter-is-a-row"),
    pytest.param("|---|---|", 0, id="delimiter-only"),
    pytest.param("a\n---\nb", 0, id="one-column-no-pipe"),
    pytest.param("| a |\n|---|", 0, id="one-column-pipe"),
    pytest.param("| a | b |\n|-|-|\n```\nx\n```", 2, id="fence-after-table"),
    # Maya advisory A1 backstops: a fence/table tie (fence wins) and a table cached before a fence
    # that covers it, followed by a later real table.
    pytest.param("```|\n|-|-|\n```", 1, id="A1-fence-table-tie"),
    pytest.param("```x\n|a|\n|-|-|\n```\n|b|\n|-|-|", 2, id="A1-stale-cached-table"),
    pytest.param("|" * 64, 0, id="long-pipes"),
    pytest.param("|\n" + " " * 64 + "x", 0, id="whitespace-delimiter-fails"),
    pytest.param("|\n" + " " * 32 + "|" + " " * 32 + "x", 0, id="ws-pipe-ws-delimiter-fails"),
]


def _spans(regex, text):
    return [m.span() for m in regex.finditer(text)]


def _fuzz_corpus(seed, count):
    rng = random.Random(seed)
    return ["".join(rng.choice(_FUZZ_PIECES) for _ in range(rng.randint(1, 60))) for _ in range(count)]


def test_rich_table_regex_matches_old_spans_exhaustively_to_length_7():
    """Every string of length 0-7 over the table alphabet (960,800 of them; length 7 is the shortest
    header + multi-column delimiter + data row) yields identical table spans."""
    from plugins.platforms.telegram.rich_markdown import _RICH_TABLE_RE

    checked = 0
    for length in range(8):
        for chars in itertools.product(_TABLE_ALPHABET, repeat=length):
            text = "".join(chars)
            checked += 1
            assert _spans(_RICH_TABLE_RE, text) == _spans(_OLD_TABLE_RE, text), repr(text)
    assert checked == sum(7**n for n in range(8))


def test_rich_table_regex_matches_old_search_from_every_offset():
    """The region scan resumes with ``search(text, pos)``; every offset of every string up to
    length 6 over the table alphabet finds the same span (or none) as the old pattern."""
    from plugins.platforms.telegram.rich_markdown import _RICH_TABLE_RE

    def found(regex, text, pos):
        m = regex.search(text, pos)
        return m.span() if m else None

    for length in range(7):
        for chars in itertools.product(_TABLE_ALPHABET, repeat=length):
            text = "".join(chars)
            for pos in range(len(text) + 1):
                assert found(_RICH_TABLE_RE, text, pos) == found(_OLD_TABLE_RE, text, pos), (text, pos)


def test_rich_protected_regions_match_old_alternation_on_seeded_fuzz():
    """50,000 strings of table, fence, CRLF/CR and whitespace pieces: the table regex spans and the
    complete protected-region scan equal the old fence|table alternation."""
    from plugins.platforms.telegram.rich_markdown import _RICH_TABLE_RE, _iter_rich_protected_regions

    for text in _fuzz_corpus(89, 50_000):
        assert _spans(_RICH_TABLE_RE, text) == _spans(_OLD_TABLE_RE, text), repr(text)
        assert list(_iter_rich_protected_regions(text)) == _spans(_OLD_ALTERNATION_RE, text), repr(text)


@pytest.mark.parametrize(("text", "regions"), _PINNED_TABLE_CASES)
def test_rich_table_pinned_cases_match_old_spans(text, regions):
    from plugins.platforms.telegram.rich_markdown import _RICH_TABLE_RE, _iter_rich_protected_regions

    old_regions = _spans(_OLD_ALTERNATION_RE, text)
    assert len(old_regions) == regions
    assert list(_iter_rich_protected_regions(text)) == old_regions
    assert _spans(_RICH_TABLE_RE, text) == _spans(_OLD_TABLE_RE, text)


# Mutation strength: the corpus above must be able to tell the old behaviour from plausible
# near-misses, or a large corpus proves nothing. Mutants live only in the tests.
_HEADER = r'^[^\n|]*\|[^\n]*\n'
_DELIMITER = r'[ \t]*(?:\|[ \t]*)?:?-+:?[ \t]*(?:\|[ \t]*:?-+:?[ \t]*)+\|?[ \t]*'
_ROWS = r'(?:\n[^\n]*\|[^\n]*)*'


def _compose(header=_HEADER, delimiter=_DELIMITER, rows=_ROWS):
    return re.compile(header + delimiter + rows, re.MULTILINE)


_TABLE_MUTANTS = {
    "header-stops-at-second-pipe": _compose(header=r'^[^\n|]*\|[^\n|]*\n'),
    "header-loses-line-anchor": _compose(header=r'[^\n|]*\|[^\n]*\n'),
    "delimiter-loses-leading-whitespace": _compose(delimiter=_DELIMITER[len(r'[ \t]*'):]),
    "delimiter-extra-column-optional": _compose(delimiter=_DELIMITER.replace(')+', ')*', 1)),
    "delimiter-dashes-optional": _compose(delimiter=_DELIMITER.replace('-+', '-*')),
    "delimiter-loses-trailing-whitespace": _compose(delimiter=_DELIMITER[:-len(r'[ \t]*')]),
    "rows-allow-pipeless": _compose(rows=r'(?:\n[^\n]*)*'),
    "rows-pipe-at-column-zero": _compose(rows=r'(?:\n\|[^\n]*)*'),
    "rows-swallow-trailing-newline": _compose(rows=_ROWS + r'\n?'),
}


def _scan_regions(text, table_re, *, fence_wins_tie=True, recompute_stale_table=True):
    """Test-local copy of the production region scan with two switchable defects (the mutants)."""
    fence_re = re.compile(r'```[^\n]*\n[\s\S]*?```')
    pos, fence, table, out = 0, None, None, []
    while True:
        if fence is None or fence.start() < pos:
            i = text.find('```', pos)
            fence = fence_re.match(text, i) if i != -1 else None
        if table is None or (recompute_stale_table and table.start() < pos):
            table = table_re.search(text, pos)
        hit = fence
        if fence is None or (table is not None and (
                table.start() < fence.start() or (not fence_wins_tie and table.start() == fence.start()))):
            hit = table
        if hit is None:
            return out
        out.append(hit.span())
        if hit.end() <= pos:  # a mutant that stops advancing would loop forever; end the scan, spans already diverge
            return out
        pos = hit.end()


def _mutation_corpus():
    return [case.values[0] for case in _PINNED_TABLE_CASES] + _fuzz_corpus(89, 50_000)


def test_rich_table_fuzz_corpus_kills_every_table_regex_mutant():
    corpus = _mutation_corpus()
    # The composition of the unmutated parts is the production pattern's behaviour.
    assert all(_spans(_compose(), t) == _spans(_OLD_TABLE_RE, t) for t in corpus)

    survivors = [
        name for name, mutant in _TABLE_MUTANTS.items()
        if all(_spans(mutant, t) == _spans(_OLD_TABLE_RE, t) for t in corpus)
    ]
    assert survivors == []


def test_rich_table_fuzz_corpus_kills_region_scan_mutants():
    from plugins.platforms.telegram.rich_markdown import _RICH_TABLE_RE

    corpus = _mutation_corpus()
    # The test-local scan, undamaged, is the production scan.
    assert all(_scan_regions(t, _RICH_TABLE_RE) == _spans(_OLD_ALTERNATION_RE, t) for t in corpus)

    mutants = {
        "table-wins-tie": {"fence_wins_tie": False},
        "stale-table-not-recomputed": {"recompute_stale_table": False},
    }
    survivors = [
        name for name, flags in mutants.items()
        if all(_scan_regions(t, _RICH_TABLE_RE, **flags) == _spans(_OLD_ALTERNATION_RE, t) for t in corpus)
    ]
    assert survivors == []


@pytest.mark.asyncio
async def test_details_without_math_still_uses_rich_send():
    adapter = _make_adapter()

    result = await adapter.send(
        "12345",
        "<details><summary>Notes</summary>\nNo equations here.\n</details>",
    )

    assert result.success is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_awaited_once()
    bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_math_outside_details_still_uses_rich_send():
    adapter = _make_adapter()

    result = await adapter.send("12345", "Outside details: $$x^2 + y^2$$")

    assert result.success is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_awaited_once()
    bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_astral_cjk_rich_content_skips_rich_send_to_avoid_tdesktop_garble():
    adapter = _make_adapter()

    result = await adapter.send("12345", ASTRAL_CJK_RICH_CONTENT)

    assert result.success is True
    adapter._bot.do_api_request.assert_not_called()
    adapter._bot.send_message.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [CJK_RICH_CONTENT, ASTRAL_CJK_RICH_CONTENT])
async def test_cjk_rich_content_can_be_opted_in(content):
    adapter = _make_adapter(extra={"allow_cjk_rich_messages": True})

    result = await adapter.send("12345", content)

    assert result.success is True
    api_kwargs = _rich_api_kwargs(adapter)
    assert api_kwargs["rich_message"]["markdown"] == content
    adapter._bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_plain_markdown_stays_on_legacy_path():
    """Ordinary replies (no table/task-list/details/math) stay on the legacy
    MarkdownV2 path for consistent client rendering, even with rich enabled."""
    adapter = _make_adapter()

    result = await adapter.send("12345", "Hello **there**\n\nA normal reply.")

    assert result.success is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_not_called()
    bot.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_expect_edits_metadata_keeps_preview_on_legacy_path():
    adapter = _make_adapter()

    result = await adapter.send(
        "12345",
        RICH_CONTENT,
        metadata={"expect_edits": True},
    )

    assert result.success is True
    # Streaming preview sends will be edited later, so they must not be born as
    # rich messages until Hermes wires rich_message edits directly.
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_not_called()
    bot.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_oversized_content_skips_rich_and_chunks():
    adapter = _make_adapter()
    # > 32,768 characters -> rich pre-check fails, legacy chunking takes over.
    oversized = "a" * 40000
    assert len(oversized) > TelegramAdapter.RICH_MESSAGE_MAX_CHARS

    result = await adapter.send("12345", oversized)

    assert result.success is True
    adapter._bot.do_api_request.assert_not_called()
    # Oversized content is split into multiple legacy chunks.
    assert adapter._bot.send_message.await_count > 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        BadRequest("can't parse rich message"),
        BadRequest("Method not found"),
    ],
)
async def test_permanent_rich_error_falls_back_to_legacy(exc):
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=exc)

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is True
    adapter._bot.do_api_request.assert_awaited_once()
    adapter._bot.send_message.assert_awaited()  # legacy fallback ran


@pytest.mark.asyncio
async def test_unknown_endpoint_error_falls_back_to_legacy():
    """A non-BadRequest 'Method not found' (old PTB/endpoint) degrades gracefully."""
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=RuntimeError("Method not found"))

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is True
    adapter._bot.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_capability_error_latches_rich_send_off():
    """Endpoint-missing errors latch rich off so later sends skip the
    doomed extra roundtrip entirely."""
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=RuntimeError("Method not found"))

    result = await adapter.send("12345", RICH_CONTENT)
    assert result.success is True
    assert adapter._rich_send_disabled is True

    # Second send skips rich entirely (no second do_api_request call).
    adapter._bot.do_api_request.reset_mock()
    adapter._bot.send_message.reset_mock()
    result2 = await adapter.send("12345", RICH_CONTENT)
    assert result2.success is True
    adapter._bot.do_api_request.assert_not_called()
    adapter._bot.send_message.assert_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [PTB_ENDPOINT_NOT_FOUND, PTB_INVALID_TOKEN_404])
async def test_real_ptb_endpoint_missing_falls_back_and_latches_off(exc):
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=exc)

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_awaited_once()
    bot.send_message.assert_awaited()
    assert adapter._rich_send_disabled is True


@pytest.mark.asyncio
async def test_per_message_bad_request_does_not_latch_off():
    """A parser/limit BadRequest is per-message — rich must stay enabled
    for subsequent messages."""
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=BadRequest("can't parse rich message"))

    result = await adapter.send("12345", RICH_CONTENT)
    assert result.success is True
    assert adapter._rich_send_disabled is False

    # Next message re-attempts rich.
    adapter._bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=124))
    result2 = await adapter.send("12345", RICH_CONTENT)
    assert result2.success is True
    adapter._bot.do_api_request.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [TimedOut("timed out"), NetworkError("connection reset")])
async def test_transient_rich_error_does_not_legacy_resend(exc):
    """Transient transport errors must NOT trigger a legacy resend (duplicate risk)."""
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=exc)

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is False
    adapter._bot.do_api_request.assert_awaited_once()
    adapter._bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_rich_transport_error_redacts_bot_token_even_when_redaction_disabled(monkeypatch):
    from agent import redact

    monkeypatch.setattr(redact, "_REDACT_ENABLED", False)
    token = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(
        side_effect=NetworkError(
            f"Timed out requesting https://api.telegram.org/bot{token}/sendRichMessage"
        )
    )

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is False
    assert result.error is not None
    assert token not in result.error
    assert "bot123456789:***/sendRichMessage" in result.error
    adapter._bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_send_error_redacts_bot_token_without_traceback(monkeypatch, caplog):
    from agent import redact

    monkeypatch.setattr(redact, "_REDACT_ENABLED", False)
    token = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
    adapter = _make_adapter({"rich_messages": False})
    adapter._bot.send_message = AsyncMock(
        side_effect=BadRequest(
            f"Bad Request: https://api.telegram.org/bot{token}/sendMessage"
        )
    )

    with caplog.at_level(logging.ERROR):
        result = await adapter.send("12345", "Plain legacy content.")

    assert result.success is False
    assert result.error is not None
    assert token not in result.error
    assert "bot123456789:***/sendMessage" in result.error
    assert token not in caplog.text
    assert "bot123456789:***/sendMessage" in caplog.text
    adapter._bot.do_api_request.assert_not_called()


@pytest.mark.asyncio
async def test_routing_direct_messages_topic_id_drops_message_thread_id():
    adapter = _make_adapter()

    await adapter.send("-100123", RICH_CONTENT, metadata={"direct_messages_topic_id": "20189"})

    api_kwargs = _rich_api_kwargs(adapter)
    assert api_kwargs["direct_messages_topic_id"] == 20189
    # _thread_kwargs_for_send pairs the topic id with message_thread_id=None;
    # the rich payload must drop the None key, not send a stray field.
    assert "message_thread_id" not in api_kwargs






# ── Streaming drafts: sendRichMessageDraft ─────────────────────────────


@pytest.mark.asyncio
async def test_cjk_rich_content_skips_rich_draft_to_avoid_tdesktop_garble():
    adapter = _make_adapter(extra={"rich_drafts": True})
    adapter._bot.do_api_request = AsyncMock(return_value=True)

    result = await adapter.send_draft("12345", draft_id=7, content=CJK_RICH_CONTENT)

    assert result.success is True
    adapter._bot.do_api_request.assert_not_called()
    adapter._bot.send_message_draft.assert_awaited_once()


# ----------------------------------------------------------------------
# prefers_fresh_final_streaming: root DMs stay on the no-duplicate edit/draft
# path (#47048). DM topics that degrade off drafts still need a fresh
# sendRichMessage so tables are not flattened by format_message.
# ----------------------------------------------------------------------
def test_prefers_fresh_final_streaming_stays_disabled_when_rich_enabled():
    adapter = _make_adapter()
    assert adapter.prefers_fresh_final_streaming(RICH_CONTENT) is False
    assert adapter.prefers_fresh_final_streaming(RICH_CONTENT, None) is False


def test_prefers_fresh_final_streaming_for_dm_topic_tables():
    adapter = _make_adapter()
    topic_meta = {
        "thread_id": "20189",
        "telegram_dm_topic_reply_fallback": True,
        "direct_messages_topic_id": "20189",
        "telegram_reply_to_message_id": "42",
    }
    assert adapter.prefers_fresh_final_streaming(RICH_CONTENT, topic_meta) is True
    assert adapter.prefers_fresh_final_streaming("Just a sentence.", topic_meta) is False
    assert adapter.prefers_fresh_final_streaming(
        RICH_CONTENT, {"direct_messages_topic_id": "20189"}
    ) is True
    # The documented telegram_-prefixed alias is honored through the same
    # canonical accessor the send path uses (gateway/delivery.py treats the
    # two keys as equivalent) — an alias-only lane must not flatten tables.
    assert adapter.prefers_fresh_final_streaming(
        RICH_CONTENT, {"telegram_direct_messages_topic_id": "20189"}
    ) is True


@pytest.mark.asyncio
async def test_legacy_draft_stream_finalizes_with_persistent_rich_message():
    """A plain draft must not force the persistent final to MarkdownV2."""
    adapter = _make_adapter()  # rich messages on, rich drafts off
    assert adapter.supports_draft_streaming(chat_type="dm") is True

    consumer = GatewayStreamConsumer(
        adapter,
        "12345",
        StreamConsumerConfig(transport="auto", chat_type="dm", cursor=""),
    )
    consumer._use_draft_streaming = True

    delivered = await consumer._send_or_edit(RICH_CONTENT, finalize=True)

    assert delivered is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_awaited_once()
    assert bot.do_api_request.call_args.args[0] == "sendRichMessage"
    bot.send_message.assert_not_called()


# ----------------------------------------------------------------------
# supports_draft_streaming: rich_drafts controls draft rendering, not whether
# Telegram's ephemeral DM draft transport is available.  Keeping that transport
# lets the persistent final use sendRichMessage instead of relying on an
# edit-in-place conversion from a plain message.
# ----------------------------------------------------------------------




@pytest.mark.asyncio
async def test_rich_table_uses_raw_plain_draft_before_persistent_rich_final():
    adapter = _make_adapter()  # rich messages on, rich drafts off

    result = await adapter.send_draft("12345", draft_id=7, content=RICH_CONTENT)

    assert result.success is True
    adapter._bot.do_api_request.assert_not_called()
    adapter._bot.send_message_draft.assert_awaited_once_with(
        chat_id=12345,
        draft_id=7,
        text=RICH_CONTENT,
    )


@pytest.mark.asyncio
async def test_dm_table_stream_persists_through_send_rich_message():
    """Exercise the reporter's transport: ephemeral DM draft, then rich final."""
    adapter = _make_adapter()  # rich messages on, rich drafts off
    consumer = GatewayStreamConsumer(
        adapter,
        "12345",
        StreamConsumerConfig(
            transport="auto",
            chat_type="dm",
            edit_interval=0.01,
            buffer_threshold=1,
            cursor="",
        ),
    )

    task = asyncio.create_task(consumer.run())
    consumer.on_delta(RICH_CONTENT)
    await asyncio.sleep(0.05)
    consumer.finish()
    await task

    adapter._bot.send_message_draft.assert_awaited()
    draft_kwargs = adapter._bot.send_message_draft.call_args.kwargs
    assert draft_kwargs["text"] == RICH_CONTENT
    assert "parse_mode" not in draft_kwargs
    rich_endpoints = [call.args[0] for call in adapter._bot.do_api_request.await_args_list]
    assert rich_endpoints == ["sendRichMessage"]
    adapter._bot.edit_message_text.assert_not_called()
    adapter._bot.send_message.assert_not_called()


TOPIC_METADATA = {
    "thread_id": "20189",
    "telegram_dm_topic_reply_fallback": True,
    "direct_messages_topic_id": "20189",
    "telegram_reply_to_message_id": "42",
}

# Shape from the Telegram iOS DM-topic report: blank line, then a GFM table.
TOPIC_TABLE = (
    "Here's a table:\n"
    "\n"
    "| Sport | Followed? | Notes |\n"
    "|---|---|---|\n"
    "| F1 | ✅ | |\n"
    "| MLB | ✅ | |\n"
    "| LoL | ✅ | |\n"
)


@pytest.mark.asyncio
async def test_send_draft_routes_dm_topic_thread_id_as_int():
    """Drafts must use the same integer thread routing as send(), not the
    raw string thread_id. Telegram rejects the string on private topics."""
    adapter = _make_adapter()

    result = await adapter.send_draft(
        "12345", draft_id=7, content=TOPIC_TABLE, metadata=TOPIC_METADATA,
    )

    assert result.success is True
    kwargs = adapter._bot.send_message_draft.call_args.kwargs
    assert kwargs["message_thread_id"] == 20189
    assert kwargs["text"] == TOPIC_TABLE
    assert "parse_mode" not in kwargs


@pytest.mark.asyncio
async def test_dm_topic_table_stream_uses_send_rich_message():
    """Happy-path topic stream: drafts land, persistent final is rich."""
    adapter = _make_adapter()
    consumer = GatewayStreamConsumer(
        adapter,
        "12345",
        StreamConsumerConfig(
            transport="auto",
            chat_type="dm",
            edit_interval=0.01,
            buffer_threshold=1,
            cursor="",
        ),
        metadata=dict(TOPIC_METADATA),
        initial_reply_to_id="42",
    )

    task = asyncio.create_task(consumer.run())
    consumer.on_delta(TOPIC_TABLE)
    await asyncio.sleep(0.05)
    consumer.finish()
    await task

    adapter._bot.send_message_draft.assert_awaited()
    draft_kwargs = adapter._bot.send_message_draft.call_args.kwargs
    assert draft_kwargs["text"] == TOPIC_TABLE
    assert draft_kwargs["message_thread_id"] == 20189
    rich_endpoints = [call.args[0] for call in adapter._bot.do_api_request.await_args_list]
    # Invariant, not a frozen call list: the persistent final goes through
    # sendRichMessage, and no rich DRAFT frames fire (rich_drafts is off).
    assert "sendRichMessage" in rich_endpoints
    assert "sendRichMessageDraft" not in rich_endpoints
    adapter._bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_dm_topic_table_survives_when_drafts_degrade_to_edit():
    """Reporter path: sendMessageDraft fails in a private topic, Telegram
    then rejects a rich edit of the plain MarkdownV2 preview. The final
    must still persist through sendRichMessage — not convert_table_to_bullets.
    """
    adapter = _make_adapter()
    adapter._bot.send_message_draft = AsyncMock(
        side_effect=BadRequest("Bad Request: message thread not found")
    )

    async def _api(endpoint, api_kwargs=None, **kwargs):
        if endpoint == "editMessageText" and api_kwargs and "rich_message" in api_kwargs:
            raise BadRequest("can't parse rich message")
        if endpoint == "sendRichMessage":
            return SimpleNamespace(message_id=123)
        return SimpleNamespace(message_id=1)

    adapter._bot.do_api_request = AsyncMock(side_effect=_api)

    consumer = GatewayStreamConsumer(
        adapter,
        "12345",
        StreamConsumerConfig(
            transport="auto",
            chat_type="dm",
            edit_interval=0.01,
            buffer_threshold=1,
            cursor="",
        ),
        metadata=dict(TOPIC_METADATA),
        initial_reply_to_id="42",
    )

    task = asyncio.create_task(consumer.run())
    consumer.on_delta(TOPIC_TABLE)
    await asyncio.sleep(0.08)
    consumer.finish()
    await task

    rich_endpoints = [call.args[0] for call in adapter._bot.do_api_request.await_args_list]
    assert "sendRichMessage" in rich_endpoints
    rich_kwargs = None
    for call in adapter._bot.do_api_request.await_args_list:
        if call.args[0] == "sendRichMessage":
            rich_kwargs = call.kwargs["api_kwargs"]
            break
    assert rich_kwargs is not None
    assert "| F1 |" in rich_kwargs["rich_message"]["markdown"]
    # Degraded preview is deleted so the user is not left with the bullet rewrite.
    adapter._bot.delete_message.assert_awaited()


def test_supports_draft_streaming_enabled_when_rich_drafts_opt_in():
    adapter = _make_adapter(extra={"rich_drafts": True})
    assert adapter.supports_draft_streaming(chat_type="dm") is True
    assert adapter.supports_draft_streaming(chat_type="group") is False


def test_supports_draft_streaming_legacy_when_rich_messages_off():
    adapter = _make_adapter(extra={"rich_messages": False})
    assert adapter.supports_draft_streaming(chat_type="dm") is True


# ----------------------------------------------------------------------
# streaming_overflow_limit: with rich on, the stream consumer may accumulate up
# to the 32,768-char rich cap before splitting, so a reply that fits one
# sendRichMessage / sendRichMessageDraft isn't fragmented at the 4,096 limit.
# ----------------------------------------------------------------------


def test_streaming_overflow_limit_none_when_rich_latched_off():
    adapter = _make_adapter()
    adapter._rich_send_disabled = True
    assert adapter.streaming_overflow_limit() is None


# ----------------------------------------------------------------------------
# Rich finalize via editMessageText (Bot API 10.1 rich_message edit param).
# Streamed previews finalize by editing the existing message IN PLACE as rich,
# so tables/task lists survive without a fresh send + delete (no duplicate).
# ----------------------------------------------------------------------------


def _rich_edit_kwargs(adapter):
    """Return the api_kwargs dict from the single editMessageText rich call."""
    call = adapter._bot.do_api_request.call_args
    assert call.args[0] == "editMessageText"
    return call.kwargs["api_kwargs"]


@pytest.mark.asyncio
async def test_finalize_edit_uses_rich_for_table_content():
    """Finalizing a streamed preview whose content is a table edits the
    existing message IN PLACE via editMessageText's rich_message param —
    no fresh send, no delete, no duplicate."""
    adapter = _make_adapter()

    result = await adapter.edit_message(
        "12345", "555", RICH_CONTENT, finalize=True,
    )

    assert result.success is True
    assert result.message_id == "555"  # same message, edited in place
    api_kwargs = _rich_edit_kwargs(adapter)
    assert api_kwargs["message_id"] == 555
    # RAW markdown is passed through so table pipes survive.
    assert api_kwargs["rich_message"]["markdown"] == RICH_CONTENT
    # No fresh send / delete — the whole point of the in-place rich edit.
    adapter._bot.edit_message_text.assert_not_called()
    adapter._bot.delete_message.assert_not_called()


@pytest.mark.asyncio
async def test_finalize_edit_dm_topic_omits_send_only_routing_fields():
    """DM-topic metadata must not make a rich edit look like a new send.

    Telegram identifies an edit by chat_id + message_id. Passing topic-routing
    fields on editMessageText rejects the rich request, after which the legacy
    formatter permanently rewrites the table into bullet groups.
    """
    adapter = _make_adapter()

    async def _api(endpoint, api_kwargs=None, **kwargs):
        assert endpoint == "editMessageText"
        has_send_routing = (
            "message_thread_id" in api_kwargs
            or "direct_messages_topic_id" in api_kwargs
        )
        if has_send_routing:
            raise BadRequest("unexpected topic routing on editMessageText")
        return True

    adapter._bot.do_api_request = AsyncMock(side_effect=_api)

    result = await adapter.edit_message(
        "12345", "555", TOPIC_TABLE, finalize=True, metadata=TOPIC_METADATA,
    )

    assert result.success is True
    api_kwargs = _rich_edit_kwargs(adapter)
    assert api_kwargs["message_id"] == 555
    assert "message_thread_id" not in api_kwargs
    assert "direct_messages_topic_id" not in api_kwargs
    assert "| F1 |" in api_kwargs["rich_message"]["markdown"]
    adapter._bot.edit_message_text.assert_not_called()


@pytest.mark.asyncio
async def test_finalize_edit_cjk_rich_content_can_be_opted_in():
    adapter = _make_adapter(extra={"allow_cjk_rich_messages": True})

    result = await adapter.edit_message(
        "12345", "555", CJK_RICH_CONTENT, finalize=True,
    )

    assert result.success is True
    assert result.message_id == "555"
    api_kwargs = _rich_edit_kwargs(adapter)
    assert api_kwargs["message_id"] == 555
    assert api_kwargs["rich_message"]["markdown"] == CJK_RICH_CONTENT
    adapter._bot.edit_message_text.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_edit_error_logs_redacted_bot_token_without_traceback(monkeypatch, caplog):
    from agent import redact

    monkeypatch.setattr(redact, "_REDACT_ENABLED", False)
    token = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
    adapter = _make_adapter()
    adapter._bot.edit_message_text = AsyncMock(
        side_effect=BadRequest(
            f"Bad Request: https://api.telegram.org/bot{token}/editMessageText"
        )
    )

    with caplog.at_level(logging.WARNING):
        result = await adapter.edit_message(
            "12345", "555", "Just a normal answer.", finalize=True,
        )

    assert result.success is False
    assert result.error is not None
    assert token not in result.error
    assert "bot123456789:***/editMessageText" in result.error
    assert token not in caplog.text
    assert "bot123456789:***/editMessageText" in caplog.text


# --------------------------------------------------------------------------
# Rich-reply recovery (#47375): Telegram does not echo a sendRichMessage's
# content in reply_to_message (.text/.caption empty, .api_kwargs None), so we
# record message_id -> text at send time and recover it on inbound reply.
# --------------------------------------------------------------------------


def _reply_message(reply_to_id, *, reply_text=None, reply_caption=None, quote_text=None):
    """Build a mock inbound reply Message for _build_message_event."""
    replied = SimpleNamespace(
        message_id=int(reply_to_id),
        text=reply_text,
        caption=reply_caption,
    )
    quote = SimpleNamespace(text=quote_text) if quote_text is not None else None
    return SimpleNamespace(
        message_id=999,
        chat=SimpleNamespace(id=12345, type="private", title=None, full_name="U"),
        from_user=SimpleNamespace(
            id=42, username="u", first_name="U", last_name=None,
            full_name="U", is_bot=False,
        ),
        text="what did this mean?",
        caption=None,
        reply_to_message=replied,
        quote=quote,
        message_thread_id=None,
        is_topic_message=False,
        entities=[],
        date=None,
    )


def _reply_message_with_rich_blocks(
    reply_to_id,
    *,
    blocks,
    quote_text=None,
    api_kwargs_factory=dict,
):
    """Build a reply whose echoed content lives only in api_kwargs.rich_message."""
    replied = SimpleNamespace(
        message_id=int(reply_to_id),
        text=None,
        caption=None,
        api_kwargs=api_kwargs_factory({"rich_message": {"blocks": blocks}}),
    )
    quote = SimpleNamespace(text=quote_text) if quote_text is not None else None
    return SimpleNamespace(
        message_id=999,
        chat=SimpleNamespace(id=12345, type="private", title=None, full_name="U"),
        from_user=SimpleNamespace(
            id=42, username="u", first_name="U", last_name=None,
            full_name="U", is_bot=False,
        ),
        text="what did this mean?",
        caption=None,
        reply_to_message=replied,
        quote=quote,
        message_thread_id=None,
        is_topic_message=False,
        entities=[],
        date=None,
    )


@pytest.mark.asyncio
async def test_rich_reply_records_and_recovers_text(monkeypatch, tmp_path):
    """A reply to a rich-sent message resolves the original text via the index."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.event import MessageType
    from gateway import rich_sent_store

    adapter = _make_adapter()

    # _try_send_rich records (chat_id, message_id) -> content on a successful
    # rich send. Drive that path directly so the test doesn't depend on send()
    # gating heuristics (length, content shape) choosing the rich path.
    adapter._bot.do_api_request = AsyncMock(
        return_value=SimpleNamespace(message_id=678)
    )
    send_result = await adapter._try_send_rich(
        "12345", "Your morning briefing: CI is green.", None, None,
    )
    assert send_result is not None and send_result.success is True
    assert send_result.message_id == "678"
    assert rich_sent_store.lookup("12345", "678") == "Your morning briefing: CI is green."

    # Inbound reply carries NO text/caption (the rich-message blind spot).
    event = adapter._build_message_event(
        _reply_message("678"), MessageType.TEXT,
    )
    assert event.reply_to_message_id == "678"
    assert event.reply_to_text == "Your morning briefing: CI is green."


def test_rich_reply_and_inbound_blocks_share_one_flattener():
    """Replied-to rich blocks and inbound rich blocks read through ONE parser: ``children`` spans,
    labelled list items and a ``mappingproxy`` api_kwargs all flatten the same way in both."""
    from types import MappingProxyType

    from gateway.platforms.event import MessageType
    from plugins.platforms.telegram.telegram_entities import rich_message_text

    blocks = [
        {"type": "paragraph", "text": ["See ", {"type": "bold", "children": ["the plan"]}]},
        {"type": "list", "items": [{"label": "1.", "blocks": [{"type": "paragraph", "text": "ship it"}]}]},
    ]
    msg = _reply_message_with_rich_blocks("678", blocks=blocks, api_kwargs_factory=MappingProxyType)

    event = _make_adapter()._build_message_event(msg, MessageType.TEXT)

    assert event.reply_to_text == "See the plan\n1. ship it"
    assert rich_message_text(msg.reply_to_message) == event.reply_to_text


def test_reply_to_deeply_nested_rich_message_still_reaches_the_agent():
    """A plain reply quoting a pathologically nested rich message (valid JSON, 2,000 levels of blocks
    and of inline spans) is still delivered: the flattener drops the over-deep content instead of
    raising RecursionError out of ``_build_message_event``."""
    from gateway.platforms.event import MessageType

    block = {"type": "paragraph", "text": "leaf"}
    span = "deep"
    for _ in range(2000):
        block = {"type": "quote", "blocks": [block]}
        span = {"type": "bold", "children": [span]}
    blocks = [{"type": "paragraph", "text": ["top ", {"type": "bold", "text": "line"}]}, block,
              {"type": "paragraph", "text": span}]
    msg = _reply_message_with_rich_blocks("678", blocks=blocks)

    event = _make_adapter()._build_message_event(msg, MessageType.TEXT)

    assert event.reply_to_message_id == "678"
    assert event.reply_to_text == "top line"


def test_rich_message_filter_leaves_media_messages_to_the_media_handler():
    """``rich_message_filter`` (registered ahead of the media handler) claims a rich-only message but not a
    caption-less photo that also carries ``rich_message``: that photo must still be downloaded as media."""
    from plugins.platforms.telegram.telegram_entities import rich_message_filter

    class _MessageFilter:  # PTB's MessageFilter: subclass overrides filter(message)
        def __init__(self, name=None):
            self.name = name

    rich_filter = rich_message_filter(SimpleNamespace(MessageFilter=_MessageFilter))
    rich = {"rich_message": {"blocks": [{"type": "paragraph", "text": "look at this"}]}}
    photo = [SimpleNamespace(file_id="f", width=1, height=1)]

    assert rich_filter.filter(SimpleNamespace(text=None, effective_attachment=None, api_kwargs=rich)) is True
    assert rich_filter.filter(SimpleNamespace(text=None, photo=photo, effective_attachment=photo,
                                              api_kwargs=rich)) is False


def test_rich_message_text_keeps_tables_details_math_links_and_custom_emoji():
    """Bot API 10.1 block and span types whose content is not under ``text``/``items``/``blocks`` still reach
    the agent: table rows, a details summary, math, a hidden link's URL (inlined like a ``text_link``) and a
    custom emoji's alternative text. The mention scan (``skip_code``) does not see the inlined URL."""
    from plugins.platforms.telegram.telegram_entities import rich_message_text

    blocks = [
        {"type": "table", "cells": [[{"text": "Model"}, {"text": "Price"}], [{"text": "HD103UJ"}, {"text": "20 EUR"}]]},
        {"type": "details", "summary": "Specs", "blocks": [{"type": "paragraph", "text": "1TB"}]},
        {"type": "mathematical_expression", "expression": "E=mc^2"},
        {"type": "paragraph", "text": [
            "ad ", {"type": "url", "text": "here", "url": "https://example.com/?u=@hermes_bot"}, " ",
            {"type": "url", "text": "https://example.com/a", "url": "https://example.com/a"}, " ",
            {"type": "mathematical_expression", "expression": "x^2"}, " ",
            {"type": "custom_emoji", "custom_emoji_id": "5", "alternative_text": "\U0001F44D"}]},
    ]
    msg = SimpleNamespace(api_kwargs={"rich_message": {"blocks": blocks}})

    assert rich_message_text(msg) == (
        "Model | Price\nHD103UJ | 20 EUR\nSpecs\n1TB\nE=mc^2\n"
        "ad here (https://example.com/?u=@hermes_bot) https://example.com/a x^2 \U0001F44D")
    assert "@hermes_bot" not in rich_message_text(msg, skip_code=True)
    malformed = [{"type": "list", "items": 5}, {"type": "table", "cells": 5}, {"type": "paragraph", "text": "ok"}]
    assert rich_message_text(SimpleNamespace(api_kwargs={"rich_message": {"blocks": malformed}})) == "ok"


@pytest.mark.asyncio
async def test_rich_send_degrades_unopenable_link_targets_but_keeps_urls_and_code():
    """Telegram cannot open ``[Title](Title)`` or ``@session:`` targets and would show the syntax."""
    adapter = _make_adapter()

    result = await adapter.send(
        "12345", RICH_CONTENT + "\n\nIn [Fix Research](Fix Research) via [S](@session:p/1); [Docs](https://x.y) `[k](v w)`")

    assert result.success is True
    assert _rich_api_kwargs(adapter)["rich_message"]["markdown"] == (
        RICH_CONTENT + "\n\nIn Fix Research via S; [Docs](https://x.y) `[k](v w)`")
