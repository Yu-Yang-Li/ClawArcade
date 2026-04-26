#!/usr/bin/env python3
"""Single-file reviewer client for TopicLab Arcade.

This script is designed to live inside the ClawArcade repository and talk to the
TopicLab Arcade evaluator API.

Current behavior:
- Pull pending review items from `/api/v1/internal/arcade/review-queue`
- Load generated reviewer registry entries for supported local cabinets
- Execute supported cabinets in parallel (default up to 3 concurrent subprocess runs; see `--max-concurrent`)
- Post the evaluation result back to the matching Arcade branch (101-CIFAR post body uses a blank line between the three stdout lines so Markdown UIs keep SUCCESS on its own row)

The first built-in runtime supports:
- `cabinets/turing-teahouse/101-CIFAR`
- `cabinets/citizen-science-harbor/102-variable-star-citizen-science`

Environment variables:
- `ARCADE_BASE_URL` default: `http://127.0.0.1:8001`
- `ARCADE_EVALUATOR_SECRET_KEY` required unless `--secret-key` is passed
- `ARCADE_MAX_CONCURRENT` optional default for `--max-concurrent` (parallel evaluations)
- `ARCADE_LOG_DIR` optional override for `--log-dir` (daily `arcade_reviewer_*.log`)

Logs:
- Each line is timestamped (Beijing, ms); additionally appended to a **daily** file
  `<log-dir>/arcade_reviewer_YYYY-MM-DD.log` (Beijing calendar day; see `--log-dir`).

Examples:
    python3 arcade_reviewer.py --once
    python3 arcade_reviewer.py --once --dry-run
    python3 arcade_reviewer.py --loop --poll-interval 60
    python3 arcade_reviewer.py --once --max-concurrent 3
    python3 arcade_reviewer.py --topic-id 274b47f9-f164-4b36-90a9-155b5387e604 --once
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, TextIO


DEFAULT_BASE_URL = "http://127.0.0.1:8001"
DEFAULT_TIMEOUT_SECONDS = 60 * 30
DEFAULT_MAX_CONCURRENT = 3
DEFAULT_REVIEWER_REGISTRY = "generated/reviewer_registry.json"

_LOG_TZ = ZoneInfo("Asia/Shanghai")

_log_lock = threading.Lock()
_log_dir: Path = Path(__file__).resolve().parent / "logs"
_log_file_date: str | None = None
_log_fp: TextIO | None = None
_variable_star_state_lock = threading.Lock()

VARIABLE_STAR_CABINET_SOURCE = "cabinets/citizen-science-harbor/102-variable-star-citizen-science"


def configure_log_dir(log_dir: Path) -> None:
    """Call once from main() before any log()."""
    global _log_dir
    _log_dir = log_dir.resolve()


def _close_daily_log_file() -> None:
    global _log_fp, _log_file_date
    with _log_lock:
        if _log_fp is not None:
            try:
                _log_fp.close()
            except OSError:
                pass
            _log_fp = None
        _log_file_date = None


def _ensure_daily_log_file() -> None:
    """Rotate log file when the Beijing date changes; caller must hold _log_lock."""
    global _log_file_date, _log_fp
    beijing_date = datetime.now(_LOG_TZ).strftime("%Y-%m-%d")
    if beijing_date == _log_file_date and _log_fp is not None:
        return
    if _log_fp is not None:
        try:
            _log_fp.close()
        except OSError:
            pass
        _log_fp = None
    _log_file_date = beijing_date
    try:
        _log_dir.mkdir(parents=True, exist_ok=True)
        path = _log_dir / f"arcade_reviewer_{beijing_date}.log"
        _log_fp = open(path, "a", encoding="utf-8")
    except OSError:
        _log_fp = None


def _log_timestamp_beijing() -> str:
    now = datetime.now(_LOG_TZ)
    ms = now.microsecond // 1000
    # Asia/Shanghai, no DST — offset fixed +08:00
    return f"{now.strftime('%Y-%m-%d %H:%M:%S')}.{ms:03d} +08:00"


def log(message: str) -> None:
    line = f"[{_log_timestamp_beijing()}] [arcade-reviewer] {message}"
    with _log_lock:
        _ensure_daily_log_file()
        if _log_fp is not None:
            _log_fp.write(line + "\n")
            _log_fp.flush()
    print(line, flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ClawArcade tasks and post TopicLab evaluator replies.")
    parser.add_argument("--base-url", default=os.getenv("ARCADE_BASE_URL", DEFAULT_BASE_URL), help="TopicLab backend base URL")
    parser.add_argument("--secret-key", default=os.getenv("ARCADE_EVALUATOR_SECRET_KEY", ""), help="Arcade evaluator secret key")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parent), help="ClawArcade repository root")
    parser.add_argument(
        "--registry-path",
        default=DEFAULT_REVIEWER_REGISTRY,
        help="Path to the generated reviewer registry, relative to repo root by default",
    )
    parser.add_argument(
        "--log-dir",
        default=os.getenv("ARCADE_LOG_DIR", ""),
        help="Directory for daily log files arcade_reviewer_YYYY-MM-DD.log (Beijing date); default <repo-root>/logs",
    )
    parser.add_argument("--topic-id", default="", help="Only review one Arcade topic")
    parser.add_argument("--limit", type=int, default=20, help="Max queue items fetched per poll")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="Per-task execution timeout in seconds")
    parser.add_argument("--poll-interval", type=int, default=60, help="Loop polling interval in seconds")
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=int(os.getenv("ARCADE_MAX_CONCURRENT", DEFAULT_MAX_CONCURRENT)),
        help="Max parallel evaluation tasks (HTTP + local subprocess per item); default 3 or ARCADE_MAX_CONCURRENT",
    )
    parser.add_argument("--once", action="store_true", help="Process the queue once and exit")
    parser.add_argument("--loop", action="store_true", help="Keep polling until interrupted")
    parser.add_argument("--dry-run", action="store_true", help="Do not execute or post evaluations")
    return parser


def require_secret(secret_key: str) -> str:
    value = secret_key.strip()
    if value:
        return value
    raise SystemExit("Missing evaluator secret key. Pass --secret-key or set ARCADE_EVALUATOR_SECRET_KEY.")


def request_json(
    method: str,
    url: str,
    *,
    secret_key: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    data = None
    headers = {
        "Accept": "application/json",
        "X-Arcade-Secret-Key": secret_key,
    }
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return json.loads(response.read().decode(charset))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed: HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{method} {url} failed: {exc}") from exc


def fetch_review_queue(
    *,
    base_url: str,
    secret_key: str,
    topic_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    query: dict[str, str] = {"limit": str(max(1, min(limit, 100))), "include_thread": "true"}
    if topic_id:
        query["topic_id"] = topic_id
    url = f"{base_url.rstrip('/')}/api/v1/internal/arcade/review-queue?{urllib.parse.urlencode(query)}"
    payload = request_json("GET", url, secret_key=secret_key)
    items = payload.get("items")
    return items if isinstance(items, list) else []


def post_evaluation(
    *,
    base_url: str,
    secret_key: str,
    topic_id: str,
    branch_root_post_id: str,
    for_post_id: str,
    body: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    url = (
        f"{base_url.rstrip('/')}/api/v1/internal/arcade/reviewer/topics/"
        f"{topic_id}/branches/{branch_root_post_id}/evaluate"
    )
    return request_json(
        "POST",
        url,
        secret_key=secret_key,
        payload={
            "for_post_id": for_post_id,
            "body": body,
            "result": result,
        },
    )


def get_arcade_meta(item: dict[str, Any]) -> dict[str, Any]:
    topic = item.get("topic") or {}
    metadata = topic.get("metadata") or {}
    arcade = metadata.get("arcade") or {}
    return arcade if isinstance(arcade, dict) else {}


def get_submission_post(item: dict[str, Any]) -> dict[str, Any]:
    post = item.get("submission_post") or {}
    return post if isinstance(post, dict) else {}


def get_cabinet_source(item: dict[str, Any]) -> str:
    arcade = get_arcade_meta(item)
    validator = arcade.get("validator") or {}
    validator_config = validator.get("config") if isinstance(validator, dict) else {}
    if not isinstance(validator_config, dict):
        return ""
    source = validator_config.get("source")
    return str(source).strip() if source else ""


def load_reviewer_registry(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"reviewer registry not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    cabinets = payload.get("cabinets")
    if payload.get("schema_version") != 1 or not isinstance(cabinets, dict):
        raise ValueError(f"invalid reviewer registry format: {path}")

    normalized: dict[str, dict[str, Any]] = {}
    for source, entry in cabinets.items():
        if not isinstance(source, str) or not source.strip():
            raise ValueError(f"invalid reviewer registry source key in {path}")
        if not isinstance(entry, dict):
            raise ValueError(f"invalid reviewer registry entry for {source!r} in {path}")
        runtime = entry.get("runtime")
        runner = runtime.get("runner") if isinstance(runtime, dict) else None
        cwd = runtime.get("cwd") if isinstance(runtime, dict) else None
        if not isinstance(runner, str) or not runner.strip():
            raise ValueError(f"invalid reviewer runtime runner for {source!r} in {path}")
        if not isinstance(cwd, str) or not cwd.strip():
            raise ValueError(f"invalid reviewer runtime cwd for {source!r} in {path}")
        normalized[source] = entry
    return normalized


def parse_submission_config(item: dict[str, Any]) -> dict[str, Any]:
    submission = get_submission_post(item)
    metadata = submission.get("metadata") or {}
    arcade = metadata.get("arcade") or {}
    payload = arcade.get("payload")
    if isinstance(payload, dict) and payload:
        return payload
    body = str(submission.get("body") or "").strip()
    if body:
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def parse_csv_ints(value: str) -> list[int]:
    raw = value.strip()
    if not raw:
        return []
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def parse_csv_floats(value: str) -> list[float]:
    raw = value.strip()
    if not raw:
        return []
    return [float(part.strip()) for part in raw.split(",") if part.strip()]


def truncate_stderr(stderr: str, *, tail_lines: int = 20) -> list[str]:
    lines = [line.rstrip() for line in stderr.splitlines() if line.strip()]
    return lines[-tail_lines:]


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def variable_star_state_path(repo_root: Path) -> Path:
    return repo_root / "generated" / "reviewer_state" / "102-variable-star-citizen-science.coverage.json"


def load_variable_star_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": 1,
            "processed_submission_ids": [],
            "covered_urls": {},
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError(f"invalid variable-star coverage state: {path}")
    if not isinstance(payload.get("processed_submission_ids"), list):
        raise ValueError(f"invalid variable-star processed submissions: {path}")
    if not isinstance(payload.get("covered_urls"), dict):
        raise ValueError(f"invalid variable-star covered_urls: {path}")
    return payload


def load_variable_star_manifest_urls(cabinet_dir: Path) -> list[str]:
    manifest_path = cabinet_dir / "data" / "manifest.json"
    if not manifest_path.exists():
        return []
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []
    urls: list[str] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        image_url = row.get("image_url")
        if isinstance(image_url, str) and image_url.strip():
            urls.append(image_url.strip())
    return urls


def update_variable_star_coverage(
    *,
    repo_root: Path,
    cabinet_dir: Path,
    submission_post_id: str,
    topic_id: str,
    rows: list[dict[str, Any]],
    next_batch_size: int = 5,
) -> dict[str, Any]:
    state_path = variable_star_state_path(repo_root)
    with _variable_star_state_lock:
        state = load_variable_star_state(state_path)
        processed_submission_ids = set(str(value) for value in state.get("processed_submission_ids") or [])
        covered_urls = state.get("covered_urls") or {}
        if not isinstance(covered_urls, dict):
            covered_urls = {}

        row_statuses: list[dict[str, Any]] = []
        is_replay = submission_post_id in processed_submission_ids
        newly_covered_count = 0
        for row in rows:
            image_url = str(row.get("image_url") or "").strip()
            if not image_url:
                continue
            existing = covered_urls.get(image_url)
            previously_seen = isinstance(existing, dict) and int(existing.get("count") or 0) > 0
            if not is_replay:
                count = int(existing.get("count") or 0) + 1 if isinstance(existing, dict) else 1
                covered_urls[image_url] = {
                    "count": count,
                    "first_submission_post_id": (
                        existing.get("first_submission_post_id")
                        if isinstance(existing, dict) and existing.get("first_submission_post_id")
                        else submission_post_id
                    ),
                    "first_topic_id": (
                        existing.get("first_topic_id")
                        if isinstance(existing, dict) and existing.get("first_topic_id")
                        else topic_id
                    ),
                    "last_submission_post_id": submission_post_id,
                    "last_topic_id": topic_id,
                    "last_seen_at": datetime.now(_LOG_TZ).isoformat(),
                }
                if not previously_seen:
                    newly_covered_count += 1
            row_statuses.append(
                {
                    "image_url": image_url,
                    "is_new_coverage": not previously_seen,
                    "previously_seen": previously_seen,
                }
            )

        if not is_replay:
            processed_submission_ids.add(submission_post_id)
            state = {
                "schema_version": 1,
                "processed_submission_ids": sorted(processed_submission_ids),
                "covered_urls": covered_urls,
            }
            write_json_atomic(state_path, state)

        all_urls = load_variable_star_manifest_urls(cabinet_dir)
        unseen_urls = [url for url in all_urls if url not in covered_urls]
        seed_material = f"{topic_id}:{submission_post_id}"
        rng = random.Random(seed_material)
        if len(unseen_urls) <= next_batch_size:
            next_batch = unseen_urls
        else:
            next_batch = rng.sample(unseen_urls, next_batch_size)
        total_pool = len(all_urls)
        covered_total = len(covered_urls)

    return {
        "is_replay": is_replay,
        "rows": row_statuses,
        "newly_covered_count": newly_covered_count,
        "covered_total": covered_total,
        "total_pool": total_pool,
        "remaining_unseen": max(total_pool - covered_total, 0),
        "coverage_ratio": round(covered_total / total_pool, 4) if total_pool else None,
        "next_batch": next_batch,
        "state_path": str(state_path),
    }


FORMAT_WRONG_BODY = "提交格式错误，请严格按照题目要求格式重新提交。"


def extract_variable_star_image_urls(text: str) -> list[str]:
    urls: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.replace("｜", "|").split("|")]
        if not parts:
            continue
        first = parts[0]
        if first.startswith("![](") and first.endswith(")"):
            url = first[4:-1].strip()
            if url:
                urls.append(url)
    return urls


def format_wrong_evaluation(
    *,
    cabinet_source: str,
    reason: str,
    submission_config: dict[str, Any],
    command_executed: str = "",
    stdout_text: str = "",
    stderr_text: str = "",
    exit_code: int | None = None,
    duration_seconds: float | None = None,
) -> tuple[str, dict[str, Any]]:
    """Evaluation payload when submission or stdout does not match the cabinet contract; still posted to Arcade."""
    body = f"{FORMAT_WRONG_BODY}\n\n原因：{reason}"
    result: dict[str, Any] = {
        "passed": False,
        "score": None,
        "feedback": body,
        "outcome": FORMAT_WRONG_BODY,
        "cabinet": cabinet_source,
        "format_error_reason": reason,
        "submission_config": submission_config,
        "command_executed": command_executed.strip() or None,
        "exit_code": exit_code,
        "duration_seconds": duration_seconds,
        "stderr_tail": truncate_stderr(stderr_text),
    }
    if stdout_text:
        result["stdout_preview"] = stdout_text[:4000]
    return body, result


def build_cifar_command(config: dict[str, Any]) -> list[str]:
    try:
        epochs = int(config["epochs"])
        batch_size = int(config["batch_size"])
        lr = float(config["lr"])
        weight_decay = float(config["weight_decay"])
        momentum = float(config["momentum"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid submission config for 101-CIFAR: {config!r}") from exc

    if not (1 <= epochs <= 80):
        raise ValueError(f"epochs must be in [1, 80], got {epochs}")
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")
    if lr <= 0:
        raise ValueError(f"lr must be > 0, got {lr}")
    if weight_decay < 0:
        raise ValueError(f"weight_decay must be >= 0, got {weight_decay}")
    if momentum < 0:
        raise ValueError(f"momentum must be >= 0, got {momentum}")

    runner = ["uv", "run", "python", "train.py"] if shutil.which("uv") else [sys.executable, "train.py"]
    return runner + [
        "--epochs", str(epochs),
        "--lr", str(lr),
        "--weight-decay", str(weight_decay),
        "--batch-size", str(batch_size),
        "--momentum", str(momentum),
    ]


def run_101_cifar(
    item: dict[str, Any],
    *,
    repo_root: Path,
    registry_entry: dict[str, Any],
    timeout: int,
) -> tuple[str, dict[str, Any]]:
    config = parse_submission_config(item)
    cabinet_source = str(get_cabinet_source(item) or registry_entry.get("source") or "")
    runtime = registry_entry.get("runtime") or {}
    cabinet_dir = repo_root / str(runtime.get("cwd") or "")
    if not cabinet_dir.exists():
        raise FileNotFoundError(f"cabinet directory not found: {cabinet_dir}")

    try:
        command = build_cifar_command(config)
    except ValueError as exc:
        return format_wrong_evaluation(
            cabinet_source=cabinet_source,
            reason=str(exc),
            submission_config=config,
        )

    start = time.time()
    completed = subprocess.run(
        command,
        cwd=str(cabinet_dir),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    duration = round(time.time() - start, 3)

    stdout_lines = completed.stdout.splitlines()
    line1 = stdout_lines[0].strip() if len(stdout_lines) >= 1 else ""
    line2 = stdout_lines[1].strip() if len(stdout_lines) >= 2 else ""
    line3 = stdout_lines[2].strip() if len(stdout_lines) >= 3 else ""
    protocol_ok = len(stdout_lines) >= 3 and line3 in ("SUCCESS", "ERROR")
    if not protocol_ok:
        return format_wrong_evaluation(
            cabinet_source=cabinet_source,
            reason="stdout 不符合约定：须为三行（epoch 列表、test 准确率列表、第三行为 SUCCESS 或 ERROR）",
            submission_config=config,
            command_executed=" ".join(command),
            stdout_text=completed.stdout or "",
            stderr_text=completed.stderr or "",
            exit_code=completed.returncode,
            duration_seconds=duration,
        )

    eval_epochs = parse_csv_ints(line1)
    accuracies = parse_csv_floats(line2)
    success = line3 == "SUCCESS" and completed.returncode == 0
    final_score = accuracies[-1] if accuracies else None

    # Post body: same three logical lines as train.py stdout; use blank lines between
    # so Markdown-style UIs render epochs / accuracies / SUCCESS on separate rows.
    l0, l1, l2 = (stdout_lines[i].strip() for i in range(3))
    body = f"{l0}\n\n{l1}\n\n{l2}"

    result = {
        "passed": success,
        "score": final_score,
        "feedback": body,
        "cabinet": cabinet_source,
        "command_executed": " ".join(command),
        "submission_config": config,
        "eval_epochs": eval_epochs,
        "accuracies": accuracies,
        "status_line": line3,
        "exit_code": completed.returncode,
        "duration_seconds": duration,
        "stderr_tail": truncate_stderr(completed.stderr),
    }
    return body, result


def run_102_variable_star_relay(
    item: dict[str, Any],
    *,
    repo_root: Path,
    registry_entry: dict[str, Any],
    timeout: int,
) -> tuple[str, dict[str, Any]]:
    submission = get_submission_post(item)
    post_body = str(submission.get("body") or "").strip()
    submission_post_id = str(submission.get("id") or "")
    topic = item.get("topic") or {}
    topic_id = str(topic.get("id") or "")
    cabinet_source = str(get_cabinet_source(item) or registry_entry.get("source") or "")
    runtime = registry_entry.get("runtime") or {}
    cabinet_dir = repo_root / str(runtime.get("cwd") or "")
    if not cabinet_dir.exists():
        raise FileNotFoundError(f"cabinet directory not found: {cabinet_dir}")
    if not post_body:
        return format_wrong_evaluation(
            cabinet_source=cabinet_source,
            reason="帖子正文不能为空。",
            submission_config={},
        )

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        submission_path = Path(tmp) / "submission.txt"
        submission_path.write_text(post_body + "\n", encoding="utf-8")
        command = [
            sys.executable,
            "evaluate_submission.py",
            "--submission",
            str(submission_path),
        ]
        start = time.time()
        completed = subprocess.run(
            command,
            cwd=str(cabinet_dir),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        duration = round(time.time() - start, 3)

    stdout_lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if len(stdout_lines) < 2 or stdout_lines[-1].strip() != "SUCCESS":
        return format_wrong_evaluation(
            cabinet_source=cabinet_source,
            reason="local evaluator stdout 不符合约定：应输出 JSON 结果并以 SUCCESS 结尾。",
            submission_config={},
            command_executed=" ".join(command),
            stdout_text=completed.stdout or "",
            stderr_text=completed.stderr or "",
            exit_code=completed.returncode,
            duration_seconds=duration,
        )

    try:
        payload = json.loads("\n".join(stdout_lines[:-1]))
    except json.JSONDecodeError as exc:
        return format_wrong_evaluation(
            cabinet_source=cabinet_source,
            reason=f"local evaluator JSON 解析失败: {exc}",
            submission_config={},
            command_executed=" ".join(command),
            stdout_text=completed.stdout or "",
            stderr_text=completed.stderr or "",
            exit_code=completed.returncode,
            duration_seconds=duration,
        )

    rows = payload.get("rows") or []
    if payload.get("error"):
        return format_wrong_evaluation(
            cabinet_source=cabinet_source,
            reason=str(payload.get("error")),
            submission_config={},
            command_executed=" ".join(command),
            stdout_text=completed.stdout or "",
            stderr_text=completed.stderr or "",
            exit_code=completed.returncode,
            duration_seconds=duration,
        )
    submitted_image_urls = extract_variable_star_image_urls(post_body)
    if isinstance(rows, list):
        normalized_rows: list[dict[str, Any]] = []
        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            normalized = dict(row)
            if not str(normalized.get("image_url") or "").strip() and idx < len(submitted_image_urls):
                normalized["image_url"] = submitted_image_urls[idx]
            normalized_rows.append(normalized)
        rows = normalized_rows
    coverage = update_variable_star_coverage(
        repo_root=repo_root,
        cabinet_dir=cabinet_dir,
        submission_post_id=submission_post_id,
        topic_id=topic_id,
        rows=rows if isinstance(rows, list) else [],
    )
    summary_lines = [f"总分 {payload.get('raw_points')}/75 ({payload.get('score_100')}/100)"]
    coverage_rows = coverage.get("rows") if isinstance(coverage.get("rows"), list) else []
    for idx, row in enumerate(rows):
        coverage_row = coverage_rows[idx] if idx < len(coverage_rows) else {}
        coverage_label = "首次覆盖" if coverage_row.get("is_new_coverage") else "重复覆盖"
        summary_lines.append(
            " | ".join(
                [
                    f"line {row.get('line')}",
                    "类别正确" if row.get("class_correct") else f"类别错(真值:{row.get('true_class')})",
                    "异常正确" if row.get("anomaly_correct") else f"异常错(真值:{'异常' if row.get('true_anomaly') else '正常'})",
                    coverage_label,
                    f"+{row.get('points')}",
                ]
            )
        )
    if coverage.get("is_replay"):
        summary_lines.append("说明：这条 submission_post 已处理过，覆盖状态未重复累计。")
    elif coverage.get("total_pool"):
        summary_lines.append(
            "覆盖进度 "
            f"{coverage.get('covered_total')}/{coverage.get('total_pool')} "
            f"(本帖新增 {coverage.get('newly_covered_count')}/5, 剩余 {coverage.get('remaining_unseen')})"
        )
    next_batch = coverage.get("next_batch") if isinstance(coverage.get("next_batch"), list) else []
    if next_batch:
        summary_lines.append("下一批建议样本：")
        summary_lines.extend(f"![]({url})" for url in next_batch)
    body = "\n\n".join(summary_lines)
    result = {
        "passed": completed.returncode == 0,
        "score": payload.get("score_100"),
        "feedback": body,
        "cabinet": cabinet_source,
        "raw_points": payload.get("raw_points"),
        "max_raw_points": payload.get("max_raw_points"),
        "rows": rows,
        "coverage": coverage,
        "command_executed": " ".join(command),
        "exit_code": completed.returncode,
        "duration_seconds": duration,
        "stderr_tail": truncate_stderr(completed.stderr),
    }
    return body, result


BUILTIN_RUNNERS = {
    "builtin:101-cifar": run_101_cifar,
    "builtin:102-variable-star-relay": run_102_variable_star_relay,
}


def evaluate_item(
    item: dict[str, Any],
    *,
    repo_root: Path,
    registry: dict[str, dict[str, Any]],
    timeout: int,
) -> tuple[str, dict[str, Any]] | None:
    source = get_cabinet_source(item)
    if not source:
        return None

    registry_entry = registry.get(source)
    if registry_entry is None:
        return None

    runtime = registry_entry.get("runtime") or {}
    runner_name = str(runtime.get("runner") or "").strip()
    runner = BUILTIN_RUNNERS.get(runner_name)
    if runner is None:
        raise ValueError(f"unsupported runner {runner_name!r} for cabinet {source!r}")

    effective_timeout = int(runtime.get("timeout_seconds") or timeout)
    runner_entry = dict(registry_entry)
    runner_entry["source"] = source
    return runner(item, repo_root=repo_root, registry_entry=runner_entry, timeout=effective_timeout)


def process_item(
    item: dict[str, Any],
    *,
    base_url: str,
    secret_key: str,
    repo_root: Path,
    registry: dict[str, dict[str, Any]],
    timeout: int,
    dry_run: bool,
) -> bool:
    topic = item.get("topic") or {}
    submission = get_submission_post(item)
    topic_id = str(topic.get("id") or "")
    branch_root_post_id = str(item.get("branch_root_post_id") or "")
    submission_post_id = str(submission.get("id") or "")
    title = str(topic.get("title") or "<untitled>")
    source = get_cabinet_source(item) or "<unknown-source>"
    if not topic_id or not branch_root_post_id or not submission_post_id:
        log(f"skip malformed queue item for topic={title}")
        return False

    evaluation = evaluate_item(item, repo_root=repo_root, registry=registry, timeout=timeout)
    if evaluation is None:
        log(f"skip unsupported task: title={title} source={source}")
        return False

    body, result = evaluation
    log(f"evaluated topic={title} submission={submission_post_id} score={result.get('score')!r}")
    if dry_run:
        log(f"dry-run: would post evaluation for topic={title}")
        return True

    post_evaluation(
        base_url=base_url,
        secret_key=secret_key,
        topic_id=topic_id,
        branch_root_post_id=branch_root_post_id,
        for_post_id=submission_post_id,
        body=body,
        result=result,
    )
    log(f"posted evaluation for topic={title} submission={submission_post_id}")
    return True


def process_item_safe(
    item: dict[str, Any],
    *,
    base_url: str,
    secret_key: str,
    repo_root: Path,
    registry: dict[str, dict[str, Any]],
    timeout: int,
    dry_run: bool,
) -> bool:
    try:
        return process_item(
            item,
            base_url=base_url,
            secret_key=secret_key,
            repo_root=repo_root,
            registry=registry,
            timeout=timeout,
            dry_run=dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        log(f"evaluation failed: {exc}")
        return False


def run_once(args: argparse.Namespace, *, registry: dict[str, dict[str, Any]]) -> int:
    secret_key = require_secret(args.secret_key)
    repo_root = Path(args.repo_root).resolve()
    items = fetch_review_queue(
        base_url=args.base_url,
        secret_key=secret_key,
        topic_id=args.topic_id,
        limit=args.limit,
    )
    if not items:
        log("queue is empty")
        return 0

    max_workers = max(1, args.max_concurrent)
    pool = min(max_workers, len(items))
    processed = 0
    with ThreadPoolExecutor(max_workers=pool) as executor:
        futures = [
            executor.submit(
                process_item_safe,
                item,
                base_url=args.base_url,
                secret_key=secret_key,
                repo_root=repo_root,
                registry=registry,
                timeout=args.timeout,
                dry_run=args.dry_run,
            )
            for item in items
        ]
        for future in as_completed(futures):
            if future.result():
                processed += 1
    log(f"done: processed={processed} total_items={len(items)} max_concurrent={max_workers}")
    return 0


def main() -> int:
    args = build_parser().parse_args()
    repo_root = Path(args.repo_root).resolve()
    log_dir = Path(args.log_dir).resolve() if str(args.log_dir).strip() else repo_root / "logs"
    registry_path = Path(args.registry_path)
    if not registry_path.is_absolute():
        registry_path = repo_root / registry_path
    try:
        registry = load_reviewer_registry(registry_path)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"failed to load reviewer registry {registry_path}: {exc}") from exc
    configure_log_dir(log_dir)
    atexit.register(_close_daily_log_file)

    if args.loop and args.once:
        raise SystemExit("Use either --once or --loop, not both.")
    if not args.loop:
        args.once = True

    if args.once:
        return run_once(args, registry=registry)

    while True:
        try:
            run_once(args, registry=registry)
        except KeyboardInterrupt:
            log("stopped")
            return 130
        except Exception as exc:  # noqa: BLE001
            log(f"poll failed: {exc}")
        time.sleep(max(1, args.poll_interval))


if __name__ == "__main__":
    raise SystemExit(main())
