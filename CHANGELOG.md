# Changelog

## Unreleased

- Add adaptive process-level concurrency when both generative models use
  Ollama Cloud, while preserving sequential local and mixed-model processing.
- Honour cloud throttling and `Retry-After` without consuming article failure
  attempts, renew leases during inference, and serialize finalisation per topic.
- Measure parallel throughput using coordinator active wall time and add a
  deterministic synthetic concurrency benchmark.
- Estimate completion during archive scans from both queued jobs and entries
  still below the persistent backfill cursor.
- Include timestamps, job context, exception chains, and HTTP/cURL diagnostics
  in Recent errors and the worker log.
- Split processing throughput into rolling last-hour and all-time active-time
  averages, using coordinator wall time for parallel cloud batches.
- Give newly arrived and updated entries immediate, arrival-ordered priority
  over existing live and archive jobs without interrupting in-flight requests.

## 0.4.0 - 2026-08-28

- Add mark-read topics that automatically read matching originals without
  creating synthetic FreshRSS objects.
- Add an optional per-topic verification feed with match explanations and
  Restore controls for auditing mark-read rules.

## 0.3.6 - 2026-08-28

- Locate the FreshRSS CLI bootstrap in split application/extension layouts,
  including LinuxServer.io containers with FreshRSS under `/app/www` and
  extensions under `/config/www/freshrss/extensions`.
- Stream archive entries in small pages, cache only feed/category identifiers,
  and interleave scanning with classification to support 128 MB PHP limits.

## 0.3.5 - 2026-08-28

- Queue new and updated entries before News Deduplicator through FreshRSS hook
  priority.
- Expose the pending state of an exact article revision so News Deduplicator
  can wait until Topic Digest classification reaches a terminal result.
- Keep configuration saves on the Topic Digest settings page.

## 0.3.4 - 2026-08-27

- Clarify that the project is exploratory and intended for personal FreshRSS
  setups.

## 0.3.3 - 2026-08-27

- Promote the Topic Digests category directly below Favourites in the sidebar.
- Add informative navigation markers for the category, low-priority digests,
  and high-priority topic feeds without changing stored names.

## 0.3.2 - 2026-08-27

- Order digest events by their effective event date rather than the date of the
  latest coverage.
- Label effective event dates and RSS publication timestamps explicitly.

## 0.3.1 - 2026-08-27

- Pin a living event overview above the individual articles in every
  high-priority topic feed.
- Preserve the overview's read state during reconciliation and presentation
  changes; only a new event marks an existing overview unread.

## 0.3.0 - 2026-08-27

- Add a per-topic low-priority digest or high-priority topic-feed presentation.
- Materialise high-priority matches as normal unread FreshRSS entries carrying
  the original title, author, content, link, date, tags, and enclosures.
- Preserve mirrored read and favourite state during reconciliation.
- Add match-explanation and Restore controls to high-priority entries.
- Keep high-priority feeds on normal FreshRSS unread filtering while low-priority
  feeds continue to open their living digest in all-articles mode.
- Safely replace generated feed contents when switching presentation type.
- Reject in-flight results after a topic or pipeline revision changes.

## 0.2.6 - 2026-08-27

- Make Restart perform a complete digest rebuild under the current topic rules.
- Clear generated events immediately and reclassify retained articles.
- Restore sources that no longer match to the normal unread stream.
- Preserve summaries, topic rules, exclusions, and explicit restore decisions.

## 0.2.5 - 2026-08-27

- Add a user-triggered, revision-safe worker restart control.

## 0.2.4 - 2026-08-27

- Tighten topic classification with explicit semantic-domain checks.
- Order events and sources by their newest coverage.

## 0.2.3 - 2026-08-27

- Derive average throughput from active-time samples attached to completed jobs.

## 0.2.2 - 2026-08-27

- Optionally keep living topic digests visible in unread-only mode.

## 0.2.0 - 2026-08-27

- Batch topic and event decisions while preserving exact revision-safe caches.

## 0.1.0 - 2026-08-27

- Add topic rules, archive backfill, synthetic feeds, event grouping, Restore
  controls, exclusion suggestions, Ollama processing, and worker status.
