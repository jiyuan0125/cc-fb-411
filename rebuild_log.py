#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 archive 内的月度 tar.gz 包重建运单轨迹索引。

设计约束：
  * 只以包内数据为准，不读取旧 index.csv；
  * 索引整体生成后原子替换，重建中断不会留下半新半旧的索引；
  * 坏包、空包、重名文件（以及包内坏文件）分别登记；
  * 索引行数与包内文件数做对账，对账不通过则不落盘。

用法：
    python3 -I rebuild_log.py \
        --archive ./archive --index ./index.csv --registries ./registries
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import tarfile
import tempfile
import time
import zlib
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

PACK_NAME_RE = re.compile(r"^(\d{4}-(?:0[1-9]|1[0-2]))\.tar\.gz$")
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
NODE_FIELDS = ("节点时间", "站点", "事件")

INDEX_HEADER = ["包月", "运单号", "首时间", "末时间", "节点数", "是否跨月"]
BAD_PACK_HEADER = ["包文件", "包月", "字节数", "错误类型", "错误信息"]
EMPTY_PACK_HEADER = ["包文件", "包月", "字节数", "说明"]
DUP_FILE_HEADER = ["包文件", "包月", "运单号", "被索引文件", "重复文件", "内容是否一致", "处理结果"]
BAD_FILE_HEADER = ["包文件", "包月", "文件名", "错误类型", "错误信息", "处理结果"]

EXIT_OK = 0
EXIT_FATAL = 1  # 无法读取 archive 目录等致命错误
EXIT_UNBALANCED = 2  # 对账不通过，拒绝落盘

# 单成员读取上限，防止异常包（压缩炸弹/超大成员）拖垮重建
MEMBER_LIMIT = 64 * 1024 * 1024
STALE_TEMP_SECONDS = 600

# 一个节点：(节点时间, 站点, 事件)
Node = Tuple[datetime, str, str]
# 一个运单在某月包内的一份轨迹：(包月, 文件名, 节点列表)
Segment = Tuple[str, str, List[Node]]


class FileProblem(Exception):
    """包内单个文件不可用（登记后跳过）。"""

    def __init__(self, kind: str, detail: str) -> None:
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


def parse_waybill(data: bytes, filename: str) -> Tuple[str, List[Node]]:
    """解析一份运单 JSON，任何结构/格式问题都抛 FileProblem。"""
    try:
        obj = json.loads(data.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FileProblem("JSON解析失败", str(exc))

    if not isinstance(obj, dict):
        raise FileProblem("结构不符", "顶层不是 JSON 对象")

    waybill = obj.get("运单号")
    if not isinstance(waybill, str) or not waybill.strip():
        raise FileProblem("运单号缺失", "运单号为空或不是字符串")

    base = os.path.basename(filename)
    if not base.endswith(".json"):
        raise FileProblem("非JSON文件", f"成员 {base!r} 不是 .json 文件")
    if not (base == waybill + ".json" or base.startswith(waybill + "-")):
        raise FileProblem("运单号与文件名不一致", f"文件名 {base!r}，包内运单号 {waybill!r}")

    nodes = obj.get("节点")
    if not isinstance(nodes, list):
        raise FileProblem("节点字段缺失", "节点不存在或不是数组")
    if not nodes:
        # 没有节点就没有首末时间，无法成行；登记为坏文件，保证仍参与对账
        raise FileProblem("空轨迹", "节点数组为空，无法确定首末时间")

    parsed: List[Node] = []
    for idx, node in enumerate(nodes, 1):
        if not isinstance(node, dict):
            raise FileProblem("节点格式错误", f"第 {idx} 个节点不是对象")
        values = []
        for field in NODE_FIELDS:
            value = node.get(field)
            if not isinstance(value, str) or not value:
                raise FileProblem("节点字段不完整", f"第 {idx} 个节点缺少 {field}")
            values.append(value)
        try:
            when = datetime.strptime(values[0], TS_FORMAT)
        except ValueError:
            raise FileProblem("节点时间格式错误", f"第 {idx} 个节点: {values[0]!r}")
        parsed.append((when, values[1], values[2]))

    return waybill, parsed


def load_pack(path: str) -> List[Tuple[str, Optional[bytes], Optional[str]]]:
    """一次性读出包内全部成员；任何解压/读取错误都让整包作废。

    返回 [(成员名, 内容或 None, 非普通文件/超限说明或 None)]。
    """
    entries: List[Tuple[str, Optional[bytes], Optional[str]]] = []
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            if member.isdir():
                continue
            if not member.isreg():
                entries.append((member.name, None, "非普通文件成员"))
                continue
            if member.size > MEMBER_LIMIT:
                entries.append((member.name, None, f"成员超过 {MEMBER_LIMIT} 字节上限"))
                continue
            handle = archive.extractfile(member)
            data = handle.read() if handle is not None else b""
            entries.append((member.name, data, None))
    return entries


def atomic_write_csv(
    path: str,
    header: Sequence[str],
    rows: Iterable[Sequence[Any]],
    footer: Optional[Sequence[str]] = None,
) -> None:
    """先写同目录临时文件再 os.replace，保证目标文件要么是旧版要么是完整新版。"""
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    base = os.path.basename(path)
    # 清理上次中断遗留的本脚本临时文件（只清足够旧的，避免误删并发运行的临时文件）
    now = time.time()
    for name in os.listdir(directory):
        if not name.startswith("." + base + ".tmp-"):
            continue
        stale_path = os.path.join(directory, name)
        try:
            if now - os.path.getmtime(stale_path) > STALE_TEMP_SECONDS:
                os.unlink(stale_path)
        except OSError:
            pass

    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix="." + base + ".tmp-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(header)
            writer.writerows(rows)
            if footer:
                for line in footer:
                    handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        # mkstemp 默认 0600，产物应与普通文件一样可读
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def merge_segments(segments: List[Segment]) -> Dict[str, Any]:
    """合并同一运单在各月的分段。

    * 分段间完全相同的节点只计一次（分段边界可能重复）；
    * 同一文件内重复出现的节点按原样计数；
    * 首/末时间取全部节点时间的最小/最大值。
    """
    ordered = sorted(segments, key=lambda seg: (seg[0], seg[1]))
    counters = [Counter(seg[2]) for seg in ordered]
    merged: Counter = Counter()
    for counter in counters:
        for key, count in counter.items():
            if count > merged[key]:
                merged[key] = count

    times = list(merged.keys())
    total = sum(merged.values())
    first, last = min(times), max(times)
    first_dt, last_dt = first[0], last[0]
    months = {(dt.year, dt.month) for dt, _, _ in times}

    return {
        "pack_month": first_dt.strftime("%Y-%m"),
        "first": first_dt.strftime(TS_FORMAT),
        "last": last_dt.strftime(TS_FORMAT),
        "nodes": total,
        "cross_month": len(months) > 1,
    }


def new_pack_stats() -> Dict[str, int]:
    return {"files": 0, "indexed": 0, "duplicate": 0, "bad_file": 0, "primary": 0, "continuation": 0}


def rebuild(archive_dir: str) -> Dict[str, Any]:
    """扫描 archive_dir 并计算结果；不做任何写盘。"""
    bad_packs: List[List[Any]] = []
    empty_packs: List[List[Any]] = []
    duplicate_files: List[List[Any]] = []
    bad_files: List[List[Any]] = []
    stats: Dict[str, Dict[str, int]] = {}

    waybills: Dict[str, Dict[str, Segment]] = defaultdict(dict)

    try:
        names = sorted(os.listdir(archive_dir))
    except OSError as exc:
        raise ArchiveUnavailable(f"无法读取 archive 目录 {archive_dir!r}: {exc}")

    packs_found = 0
    ignored: List[str] = []

    for name in names:
        if not name.endswith(".tar.gz"):
            ignored.append(name)
            continue
        packs_found += 1
        path = os.path.join(archive_dir, name)
        size = os.path.getsize(path)
        match = PACK_NAME_RE.match(name)
        if not match:
            bad_packs.append([name, "", size, "包名不合规", "包名不是 YYYY-MM.tar.gz"])
            continue
        pack_month = match.group(1)

        try:
            entries = load_pack(path)
        except (tarfile.TarError, OSError, EOFError, zlib.error) as exc:
            bad_packs.append([name, pack_month, size, type(exc).__name__, str(exc)])
            continue

        regular = [(member, data) for member, data, note in entries if note is None]
        for member, _data, note in entries:
            if note is not None:
                bad_files.append([name, pack_month, member, note, "", "跳过（不计入普通文件）"])

        if not regular:
            empty_packs.append([name, pack_month, size, "包内无普通文件成员"])
            continue

        pack_stats = new_pack_stats()
        pack_stats["files"] = len(regular)
        stats[pack_month] = pack_stats

        parsed: List[Tuple[str, str, bytes, List[Node]]] = []
        for member, data in regular:
            base = os.path.basename(member)
            try:
                waybill, nodes = parse_waybill(data, member)
            except FileProblem as exc:
                bad_files.append([name, pack_month, base, exc.kind, exc.detail, "跳过"])
                pack_stats["bad_file"] += 1
                continue
            parsed.append((waybill, base, data, nodes))

        # 同一包内按运单号分组，处理重名文件
        by_waybill: Dict[str, List[Tuple[str, bytes, List[Node]]]] = defaultdict(list)
        for waybill, base, data, nodes in parsed:
            by_waybill[waybill].append((base, data, nodes))

        for waybill, copies in by_waybill.items():
            copies.sort(key=lambda item: item[0])
            preferred = waybill + ".json"
            chosen = next((item for item in copies if item[0] == preferred), copies[0])
            for base, data, _nodes in copies:
                if base == chosen[0]:
                    continue
                same = "是" if data == chosen[1] else "否"
                duplicate_files.append([name, pack_month, waybill, chosen[0], base, same, "不重复计入索引"])
                pack_stats["duplicate"] += 1

            waybills[waybill][pack_month] = (pack_month, chosen[0], chosen[2])
            pack_stats["indexed"] += 1

    # 合并跨月分段
    rows: List[List[Any]] = []
    continuation_by_month: Dict[str, int] = defaultdict(int)
    for waybill in sorted(waybills):
        segments = list(waybills[waybill].values())
        merged = merge_segments(segments)
        rows.append([
            merged["pack_month"],
            waybill,
            merged["first"],
            merged["last"],
            merged["nodes"],
            "是" if merged["cross_month"] else "否",
        ])
        # 主档＝含最早节点的分段所在包；其余同运单分段算“续档”，按包月登记
        primary_seg = min(segments, key=lambda seg: min(seg[2]))
        stats[primary_seg[0]]["primary"] += 1
        for seg in segments:
            if seg is not primary_seg:
                continuation_by_month[seg[0]] += 1

    for month, count in continuation_by_month.items():
        if month in stats:
            stats[month]["continuation"] = count
        else:
            stats[month] = new_pack_stats()
            stats[month]["continuation"] = count

    rows.sort(key=lambda row: (row[0], row[1]))

    # ---- 对账 ----
    total_files = sum(item["files"] for item in stats.values())
    total_indexed = sum(item["indexed"] for item in stats.values())
    total_dup = sum(item["duplicate"] for item in stats.values())
    total_bad_file = sum(item["bad_file"] for item in stats.values())
    total_primary = sum(item["primary"] for item in stats.values())
    total_continuation = sum(item["continuation"] for item in stats.values())
    total_rows = len(rows)

    problems: List[str] = []
    if total_files != total_indexed + total_dup + total_bad_file:
        problems.append(
            f"全局文件数 {total_files} ≠ 入索引 {total_indexed} + 副本 {total_dup} + 坏文件 {total_bad_file}"
        )
    if total_indexed != total_primary + total_continuation:
        problems.append(
            f"入索引文件数 {total_indexed} ≠ 主档文件 {total_primary} + 续档文件 {total_continuation}"
        )
    if total_rows != total_primary:
        problems.append(f"索引行 {total_rows} ≠ 主档文件 {total_primary}")
    for month, item in sorted(stats.items()):
        if item["files"] != item["indexed"] + item["duplicate"] + item["bad_file"]:
            problems.append(
                f"{month}: 文件 {item['files']} ≠ 入索引 {item['indexed']} + 副本 "
                f"{item['duplicate']} + 坏文件 {item['bad_file']}"
            )
        if item["indexed"] != item["primary"] + item["continuation"]:
            problems.append(
                f"{month}: 入索引 {item['indexed']} ≠ 主档 {item['primary']} + 续档 {item['continuation']}"
            )
    if problems:
        raise ReconciliationError(problems)

    return {
        "rows": rows,
        "stats": stats,
        "bad_packs": sorted(bad_packs, key=lambda row: str(row[0])),
        "empty_packs": sorted(empty_packs, key=lambda row: str(row[0])),
        "duplicate_files": sorted(duplicate_files, key=lambda row: (str(row[1]), str(row[2]), str(row[4]))),
        "bad_files": sorted(bad_files, key=lambda row: (str(row[1]), str(row[2]))),
        "counts": {
            "packs": packs_found,
            "readable": len(stats),
            "empty": len(empty_packs),
            "bad": len(bad_packs),
            "files": total_files,
            "indexed": total_indexed,
            "rows": total_rows,
            "primary": total_primary,
            "continuation": total_continuation,
            "duplicate": total_dup,
            "bad_file": total_bad_file,
        },
        "ignored": ignored,
    }


class ReconciliationError(Exception):
    def __init__(self, problems: List[str]) -> None:
        super().__init__("\n".join(problems))
        self.problems = problems


class ArchiveUnavailable(Exception):
    """archive 目录无法读取，属于环境错误而非数据问题。"""


def build_footer(result: Dict[str, Any], archive_dir: str, registries_dir: str) -> List[str]:
    counts = result["counts"]
    stats = result["stats"]
    bad_packs = result["bad_packs"]
    empty_packs = result["empty_packs"]
    generated = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")

    lines = [
        "# " + "=" * 72,
        "# 以下为重建口径与对账说明，不是索引数据。",
        f"# 生成时间: {generated}",
        f"# 数据来源: {os.path.abspath(archive_dir)}/*.tar.gz（{counts['packs']} 个包）",
        f"# 包状态: 可读 {counts['readable']} 个，空包 {counts['empty']} 个，坏包 {counts['bad']} 个",
    ]
    for row in bad_packs:
        lines.append(f"# 坏包: {row[0]}（{row[3]}）")
    for row in empty_packs:
        lines.append(f"# 空包: {row[0]}")

    lines += [
        "# 归属口径: 每行一个运单；包月＝该运单轨迹最早节点所在自然月。跨月运单不拆行，"
        "前后两段合并后归入起始月，是否跨月记“是”；后半段位于末时间所在月的包内。",
        "# 时间口径: 首时间/末时间取合并后全部节点时间的最小/最大值，不按文件内数组首尾。",
        "# 节点数口径: 合并后的节点记录数；不同分段间完全相同的节点（时间+站点+事件）只计一次，"
        "同一文件内重复记录按原样计数。",
        "# 是否跨月口径: 轨迹最早节点与最晚节点不在同一自然月记“是”，否则记“否”。",
        "# 重名文件口径: 每包每运单只取一份入索引（优先“运单号.json”，否则文件名最小者），"
        "其余副本登记、不重复计入行数；副本与被索引文件内容是否一致记在登记表中。",
        f"# 对账: 可读包普通文件总数 {counts['files']} ＝ 入索引 {counts['indexed']}（主档 "
        f"{counts['primary']} ＋ 续档 {counts['continuation']}）＋ 重名副本 {counts['duplicate']} ＋ "
        f"坏文件 {counts['bad_file']}；索引行数 {counts['rows']} ＝ 主档 {counts['primary']}",
    ]
    for month in sorted(stats):
        item = stats[month]
        lines.append(
            f"# 分包对账: {month} 文件 {item['files']} ＝ 主档 {item['primary']} ＋ "
            f"续档 {item['continuation']} ＋ 副本 {item['duplicate']} ＋ 坏文件 {item['bad_file']}"
        )
    lines += [
        "# 坏包内文件无法读取，不参与对账，其运单未进入本索引。",
        f"# 登记表: {os.path.abspath(registries_dir)}/"
        "{bad_packs,empty_packs,duplicate_files,bad_files}.csv",
        "# " + "=" * 72,
    ]
    return lines


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="从 archive 月度包重建运单轨迹索引")
    parser.add_argument("--archive", default="./archive", help="月度 tar.gz 包目录（默认 ./archive）")
    parser.add_argument("--index", default="./index.csv", help="索引输出路径（默认 ./index.csv）")
    parser.add_argument("--registries", default="./registries", help="登记表输出目录（默认 ./registries）")
    args = parser.parse_args(argv)

    try:
        result = rebuild(args.archive)
    except ArchiveUnavailable as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_FATAL
    except ReconciliationError as exc:
        print("对账不通过，未写入任何文件：", file=sys.stderr)
        for problem in exc.problems:
            print("  - " + problem, file=sys.stderr)
        return EXIT_UNBALANCED

    footer = build_footer(result, args.archive, args.registries)

    # 先写登记表，最后原子替换索引：索引是权威产物，最后落盘。
    atomic_write_csv(os.path.join(args.registries, "bad_packs.csv"), BAD_PACK_HEADER, result["bad_packs"])
    atomic_write_csv(os.path.join(args.registries, "empty_packs.csv"), EMPTY_PACK_HEADER, result["empty_packs"])
    atomic_write_csv(
        os.path.join(args.registries, "duplicate_files.csv"), DUP_FILE_HEADER, result["duplicate_files"]
    )
    atomic_write_csv(os.path.join(args.registries, "bad_files.csv"), BAD_FILE_HEADER, result["bad_files"])
    atomic_write_csv(args.index, INDEX_HEADER, result["rows"], footer=footer)

    counts = result["counts"]
    print(
        f"完成: 索引 {counts['rows']} 行；可读包 {counts['readable']} 个，空包 {counts['empty']} 个，"
        f"坏包 {counts['bad']} 个；重名副本 {counts['duplicate']} 份，坏文件 {counts['bad_file']} 个。"
    )
    print(
        f"对账: 文件 {counts['files']} = 主档 {counts['primary']} + 续档 {counts['continuation']} + "
        f"副本 {counts['duplicate']} + 坏文件 {counts['bad_file']}；索引行 {counts['rows']} = 主档"
    )
    print(f"索引: {os.path.abspath(args.index)}；登记表: {os.path.abspath(args.registries)}/")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
