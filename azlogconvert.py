#!/usr/bin/env python3

import csv
from dataclasses import dataclass
from typing import Iterable, Optional
import datetime
import pytz
from collections import defaultdict
import re
import os
import sys

LOG_LEVELS = ("info", "warn", "fail")
BRACKET_PATTERN = re.compile(r'\[\d]')
LOCAL_TZ = pytz.timezone("Europe/Warsaw")


@dataclass(frozen=True)
class LogEntryHeader:
    level: str
    message: str


@dataclass(frozen=True)
class LogRecord:
    ticks: int
    message: str

    @property
    def entry_header(self) -> Optional[LogEntryHeader]:
        msg = self.message

        for level in LOG_LEVELS:
            if not msg.startswith(f"{level}: "):
                continue

            msg = msg[len(level) + 2:]

            return LogEntryHeader(level=level, message=msg)

        return None

    @property
    def occurred_at(self) -> datetime.datetime:
        return pytz.UTC.localize(datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds=self.ticks / 10))


@dataclass(frozen=True)
class LogEntry:
    occurred_at: datetime.datetime
    tag: str
    level: str
    message: str

    def format(self):
        return f"{self.occurred_at.astimezone(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S.%f')} {self.level} {self.message}"


class OpenedLogEntry:
    occurred_at: datetime.datetime
    tag: str
    level: str

    def __init__(self, header: LogEntryHeader, occurred_at: datetime.datetime):
        self.level = header.level
        self.tag = normalize_tag(header.message)
        self.occurred_at = occurred_at
        self.__messages = []

    def add_message(self, message: str):
        self.__messages.append(message)

    def close(self) -> LogEntry:
        if not self.__messages:
            raise ValueError(f"Entry {self.tag} at {self.occurred_at} has no messages")

        return LogEntry(self.occurred_at, self.tag, self.level, "\n".join(self.__messages).strip())


def convert_files(file_path: str):
    file_path = os.path.realpath(file_path)

    with open(file_path, mode='r', encoding='utf-8') as f:
        records = split_records(convert_records(read_records(f)))

    file_name = os.path.basename(file_path)
    dir_name = os.path.dirname(file_path)
    log_dir_name = os.path.join(dir_name, file_name + "_logs")

    os.mkdir(log_dir_name)

    for tag, records in records.items():
        with open(os.path.join(log_dir_name, f'{tag}.txt'), mode='w', encoding='utf-8') as out_file:
            write_log_file(records, out_file)


def read_records(source) -> Iterable[LogRecord]:
    reader = csv.DictReader(source)

    for row in reader:
        yield LogRecord(ticks=int(row["eventTickCount"]), message=row["message"])


def convert_records(source: Iterable[LogRecord]) -> Iterable[LogEntry]:
    opened: Optional[OpenedLogEntry] = None

    for record in source:
        header, occurred_at = (record.entry_header, record.occurred_at)

        if header is None:
            if not opened:
                raise ValueError(f"Entry: {record.message} without a header")

            opened.add_message(record.message)

            continue

        if opened:
            yield opened.close()

        opened = OpenedLogEntry(header, occurred_at)


def split_records(source: Iterable[LogEntry]) -> dict[str, list[LogEntry]]:
    groups = defaultdict(list)

    for entry in source:
        groups[entry.tag].append(entry)

    return groups


def normalize_tag(source: str) -> str:
    return BRACKET_PATTERN.sub("", source).replace(".", "_").strip()


def write_log_file(source: Iterable[LogEntry], file):
    file.writelines(it.format() + '\n' for it in source)


def entry_point():
    args = sys.argv[1:]

    if not args:
        print("Usage azlogconvert FILE")
        return

    convert_files(args[0])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    entry_point()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
