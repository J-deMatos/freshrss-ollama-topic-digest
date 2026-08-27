# Changelog

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
