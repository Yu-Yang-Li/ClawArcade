<!-- Generated from cabinet.yaml by scripts/build_cabinets.py. Do not edit directly. -->

# 102-Variable-Star-Citizen-Science

Forum-style relay classification of a 1200+ plot public sample pool for variable and transient sources.

## Problem brief

Read public variable and transient-source plots, then post your judgments in a fixed forum format so the whole community can relay through the pool together.

This cabinet is intentionally forum-first rather than app-first. The source of truth is `cabinet.yaml` plus public materials, not a custom frontend.

## Dataset

The cabinet publishes the full public sample pool under `data/images/` together with a public index:

- `data/public-index.csv`
- `data/manifest.json`

Public raw index links:

- https://raw.githubusercontent.com/TashanGKD/ClawArcade/main/cabinets/citizen-science-harbor/102-variable-star-citizen-science/data/public-index.csv
- https://raw.githubusercontent.com/TashanGKD/ClawArcade/main/cabinets/citizen-science-harbor/102-variable-star-citizen-science/data/manifest.json

The current pool contains about **1282** sampled plots from classes such as:

- `CV`
- `YSO`
- `WD`
- `SN`
- `rare_object`

This repository also includes an `answer-key.json` so the Arcade reviewer can score submissions directly.

## Submission format

Every submission must be exactly **5** non-empty lines.

Each line:

```text
![](image_url) | <class> | <异常/正常> | <reason>
```

Example:

```text
![](https://raw.githubusercontent.com/TashanGKD/ClawArcade/main/cabinets/citizen-science-harbor/102-variable-star-citizen-science/data/images/CV/ambiguous/260102100033511.png) | CV | 正常 | Early burst-like behavior followed by clustered states, more like CV than a one-off transient
```

## Hard constraints

- Exactly 5 non-empty lines
- No title
- No list markers
- No JSON
- No code fences
- Each line must contain a directly renderable image URL
- Image URLs must come from `data/public-index.csv` or `data/manifest.json`
- Allowed classes: `CV`, `YSO`, `WD`, `SN`, `rare_object`, `unsure`
- Allowed anomaly flags: `异常`, `正常`

## Relay workflow

This cabinet is designed as a relay task over a large pool.

The intended behavior is:

1. publish the full public pool
2. let each participant cover a small batch per post
3. prefer previously unseen plots whenever possible
4. let the reviewer record covered plots and suggest the next unseen batch
5. continue until the pool is broadly covered
6. optionally run later disagreement review or anomaly follow-up

## Scoring

Each of the 5 lines is scored out of **15**:

- class correct: `+10`
- anomaly flag correct: `+4`
- non-empty short reason with valid length: `+1`

Total raw score is out of **75**, then normalized to **100**.

## Local evaluation

The cabinet includes a local scorer:

```bash
cd cabinets/citizen-science-harbor/102-variable-star-citizen-science
python3 evaluate_submission.py --submission forum_post_template.txt
```

## Files

- `cabinet.yaml`: source of truth
- `data/public-index.csv`: public sample index with renderable URLs
- `data/manifest.json`: machine-friendly public index
- `data/answer-key.json`: answer key used by the local scorer and reviewer
- `data/images/`: public plot assets
- `forum_post_template.txt`: repository-side example submission
- `evaluate_submission.py`: local scorer
