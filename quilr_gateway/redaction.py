"""Incremental redaction of common, bounded PII shapes."""

import re

REPLACEMENT = "[REDACTED]"
# Supports ordinary ASCII email addresses (no quoted local parts or IDNs).
LOCAL = r"A-Za-z0-9.!#$%&'*+/=?^_`{|}~-"
DOMAIN = r"(?=[A-Za-z0-9.-]{1,253}(?![A-Za-z0-9.-]))(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63}(?![A-Za-z0-9-])"
EMAIL = rf"(?<![{LOCAL}])[{LOCAL}]{{1,64}}@{DOMAIN}"
CARD = r"(?<![0-9])(?:[0-9][ -]?){12,18}[0-9](?![0-9])"
SSN = r"(?<![0-9])[0-9]{3}-[0-9]{2}-[0-9]{4}(?![0-9])"
PII = re.compile(f"{EMAIL}|{CARD}|{SSN}")
OVERLAP = 320
SLICE = 4096


class StreamRedactor:
    def __init__(self) -> None:
        self.pending = ""
        self.previous = ""

    def _drain(self, *, final: bool = False) -> str:
        data = self.previous + self.pending
        start = len(self.previous)
        cut = len(data) if final else max(start, len(data) - OVERLAP)
        if not final:
            for i in range(len(data) - 1, cut - 1, -1):
                char = data[i]
                # Emails never cross these delimiters. Cards can cross a single
                # space after a digit, so that boundary must remain buffered.
                if char in "\n\r\t,;:()[]<>\"" or (
                    char == " " and (i == 0 or not data[i - 1].isascii() or not data[i - 1].isdigit())
                ):
                    cut = i + 1
                    break
        if cut <= start:
            return ""

        output = []
        cursor = start
        for match in PII.finditer(data, start):
            if match.end() <= start:
                continue
            if match.start() >= cut:
                break
            # The fallback cut can land inside a completed match. Consume the
            # entire match so its suffix can never be emitted in the next delta.
            output.append(data[cursor:max(cursor, match.start())])
            output.append(REPLACEMENT)
            cursor = match.end()
            cut = max(cut, cursor)
        output.append(data[cursor:cut])
        self.pending = data[cut:]
        self.previous = data[cut - 1:cut]
        return "".join(output)

    def feed(self, delta: str):
        for index in range(0, len(delta), SLICE):
            self.pending += delta[index:index + SLICE]
            if output := self._drain():
                yield output

    def finish(self) -> str:
        return self._drain(final=True)
