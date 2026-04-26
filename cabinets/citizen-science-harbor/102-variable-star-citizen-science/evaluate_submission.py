#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


VALID_CLASSES = {"CV", "YSO", "WD", "SN", "rare_object", "unsure"}
VALID_ANOMALY = {"异常", "正常"}
EXPECTED_LINES = 5
IMAGE_MARKDOWN_RE = re.compile(r"^!\[]\((?P<url>[^)]+)\)$")


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def parse_submission_text(text: str):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) != EXPECTED_LINES:
        raise ValueError(f"submission must contain exactly {EXPECTED_LINES} non-empty lines; got {len(lines)}")

    parsed = []
    seen_urls = set()
    for idx, line in enumerate(lines, start=1):
        normalized = line.replace("｜", "|")
        parts = [part.strip() for part in normalized.split("|")]
        if len(parts) != 4:
            raise ValueError(f"line {idx}: expected 4 fields `![](image_url) | <class> | <异常/正常> | <reason>`")
        image_markdown, predicted_class, anomaly_text, reason = parts
        match = IMAGE_MARKDOWN_RE.match(image_markdown)
        if not match:
            raise ValueError(f"line {idx}: first field must be markdown image syntax ![](image_url)")
        image_url = match.group("url").strip()
        if image_url in seen_urls:
            raise ValueError(f"line {idx}: duplicate image_url {image_url}")
        if predicted_class not in VALID_CLASSES:
            raise ValueError(f"line {idx}: invalid class {predicted_class}")
        if anomaly_text not in VALID_ANOMALY:
            raise ValueError(f"line {idx}: third field must be 异常 or 正常")
        if not reason:
            raise ValueError(f"line {idx}: reason must not be empty")
        seen_urls.add(image_url)
        parsed.append(
            {
                "line_number": idx,
                "image_url": image_url,
                "predicted_class": predicted_class,
                "predicted_anomaly": anomaly_text == "异常",
                "reason": reason,
            }
        )
    return parsed


def evaluate_rows(rows, truth_by_url):
    feedback_rows = []
    raw_points = 0
    for row in rows:
        truth = truth_by_url.get(row["image_url"])
        if truth is None:
            raise ValueError(f"unknown image_url: {row['image_url']}")
        class_correct = row["predicted_class"] == truth["true_class"]
        anomaly_correct = row["predicted_anomaly"] == bool(truth["is_anomaly"])
        reason_ok = 8 <= len(row["reason"]) <= 240

        points = 0
        if class_correct:
            points += 10
        if anomaly_correct:
            points += 4
        if reason_ok:
            points += 1
        raw_points += points

        feedback_rows.append(
            {
                "line": row["line_number"],
                "image_url": row["image_url"],
                "predicted_class": row["predicted_class"],
                "true_class": truth["true_class"],
                "predicted_anomaly": row["predicted_anomaly"],
                "true_anomaly": bool(truth["is_anomaly"]),
                "class_correct": class_correct,
                "anomaly_correct": anomaly_correct,
                "reason_ok": reason_ok,
                "points": points,
            }
        )
    return raw_points, feedback_rows


def format_error_result(message: str):
    return {
        "raw_points": 0,
        "score_100": 0.0,
        "max_raw_points": 75,
        "rows": [],
        "error": message,
    }


def main():
    parser = argparse.ArgumentParser(description="Evaluate a forum-style variable-star relay submission.")
    parser.add_argument("--submission", required=True, help="Path to a plain-text forum post")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    answer_key = load_json(root / "data" / "answer-key.json")
    truth_by_url = {row["image_url"]: row for row in answer_key}

    text = Path(args.submission).read_text(encoding="utf-8")
    try:
        rows = parse_submission_text(text)
        raw_points, feedback_rows = evaluate_rows(rows, truth_by_url)
        score_100 = round(raw_points / 75 * 100, 2)
        result = {
            "raw_points": raw_points,
            "score_100": score_100,
            "max_raw_points": 75,
            "rows": feedback_rows,
        }
    except ValueError as exc:
        result = format_error_result(str(exc))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("SUCCESS")


if __name__ == "__main__":
    main()
