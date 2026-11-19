# AGENTS.md

Rules for the agent implementing this lab. The plan is `docs/PLAN.md`; it is
the contract. Where this file and the plan disagree, this file wins.

## Scope of this run

- Work the milestones in order: M0, M1, M2, M3, then the follow-up
  milestones L1, L2, L3 ("Follow-up milestones" in the plan). One milestone
  per session unless the launcher prompt says otherwise.
- Out of scope: Databricks, ADLS or any other external environment, "Other
  OSS worth testing" and "Parked experiments". Do not start them. You may
  list ideas for them at the end of a report.
- L milestones must not change the M0-M3 harness behaviour or rewrite their
  evidence. Add new modules, Compose profiles and tests instead; if a shared
  module must change, keep `uv run pytest -q` green and say why in the report.
- Before starting a milestone, read `docs/PLAN.md`, every file in `reports/`
  and `git log --oneline`. Continue from what is committed; do not redo a
  milestone that has a report with a pass verdict.

## Hard limits

- Local only. Never add a git remote, push, open issues or pull requests, or
  post anywhere. Suspected upstream problems go to `drafts/` as Markdown.
- No paid services, no cloud accounts, no credentials beyond the synthetic
  lab ones. Synthetic data only.
- Docker: one Compose project named `cdclab`, every published port bound to
  `127.0.0.1` and configurable through `compose/.env`. Only create, stop or
  remove containers, volumes and networks of that project
  (`docker compose -p cdclab ...`). Never run `docker system prune`, any
  `docker * prune`, or touch containers you did not create.
- Do not write outside this repository, except Docker's own storage and the
  `uv` cache.
- Stop and write `reports/BLOCKED.md` (what you tried, the exact error, what
  decision is needed) when a step needs a change in scope, a paid service,
  access to an external environment, or when you are stuck after three
  distinct attempts at the same problem. Then end the session.

## Evidence and honesty

- Never invent a version, digest, flag, config key or result. Verify each one
  from the pinned upstream docs, release page, image manifest or source, and
  record where it came from in the report.
- A milestone passes only on its plan's **Pass** criteria. If a criterion is
  not met, say so in the report with the evidence; a partial result is still
  a useful report. Give every run one of the plan's explicit outcomes.
- Raw evidence goes under `runs/<UTC timestamp>-<label>/` and is committed.
  Reports cite those paths.
- A check that never failed proves nothing: every comparison check must be
  shown failing on its fault fixture before it is trusted (plan, M1 step 1).

## Each milestone ends with

1. `reports/Mx.md`: verdict (pass, partial, blocked), what was run (exact
   commands), pinned versions and digests, results, limitations, open
   questions.
2. Tests passing: `uv run pytest -q`.
3. One local commit, conventional subject such as `feat(m0): prove reader
   path with update and delete`. No AI attribution trailers.

## Host notes (Windows 11, Docker Desktop)

- The shell locale is it-IT: PowerShell reformats decimals and dates. Do all
  data handling, timing and number formatting in Python, never in PowerShell.
- Python through `uv` only (`uv add`, `uv run`), with `uv.lock` committed.
- Use Python's `time.monotonic_ns()` for latency and wall-clock UTC for
  timestamps written to evidence files.
- Docker Desktop may be stopped; if `docker version` fails, start it and wait
  for the engine before anything else.
- Prefer `docker compose -p cdclab -f compose/docker-compose.yml ...` from the
  repository root, and record the exact invocation.

## Writing

Plain, sober prose in reports: short sentences, no marketing tone, no
em-dashes. State the outcome first, then the evidence.
