# Changelog

## 0.5.2 - 2026-08-29

- Require a non-empty article summary and event title in the structured schema
  and prompt, and immediately make one corrective request if an Ollama engine
  still returns empty fields. A deterministic model could previously satisfy
  the schema with empty strings, then repeat the same unusable answer on every
  job retry even when the article contained substantial text.
- Actually discard newly encountered pre-GUID queue artefacts instead of
  recording them as processed-and-skipped articles. Also rerun the versioned
  cleanup for installations where 0.5.1's cleanup had already been bypassed.

## 0.5.1 - 2026-08-29

- Stop counting stale queue rows as articles that were processed and skipped.
  0.5.0 gave them an honest label but still recorded one permanent "skipped"
  row each, so they kept accumulating in "Articles processed" and pushed the
  real skip reasons down a list they dominated. They are queue bookkeeping, not
  articles: the ones already recorded that way are now dropped and folded into a
  single "Stale queue rows discarded" total on the status page.
- Add `cli/prune-stale-jobs.php` to clear the ones still waiting in the queue,
  instead of having the worker claim, look up and permanently record each one.
  It reports the count by default and deletes only with `--apply`, refuses to
  run while the worker holds its lock or while the archive scan is active, and
  loses no articles — one that still exists is queued again under its current
  id by the archive scan. Deleting them is deliberately not automatic: until a
  scan has completed, a row queued before GUIDs were recorded cannot be told
  apart from one whose article is still live.
- Keep those rows out of the average-throughput figure. Each was recorded with
  the near-zero time it took to discover the article was unreachable, which
  counted as a completed article and inflated the articles-per-hour estimate.

## 0.5.0 - 2026-08-29

- Fix high-priority topic feeds never dropping the articles they generated for
  matches that no longer exist, so a rebuild left them full of unread articles
  belonging to topics showing zero matched sources. The pruning called
  FreshRSS's `cleanOldEntries()` with no options, which appends a literal
  "AND (1=0)" and therefore deletes nothing at all, and reported success. Every
  article that should survive is now stamped as seen first, which makes that
  API remove exactly the rest; favourites are still kept.
- Move "Rescan archive" out of the per-topic controls. It sends no topic and
  always rescanned everything, but sat among Edit, Pause and Delete, so it read
  as scoped to one topic and invited running it once per topic to no effect.
- Add "Re-examine skipped articles", which re-queues skipped articles for a
  fresh decision. An archive rescan cannot do this: it only revisits articles
  FreshRSS still lists, so a skipped row whose id no longer matches a live
  article was unreachable by any control and stayed skipped permanently.
- Fix "Reset logs" erasing the recorded reason each skipped article was
  skipped. That reason is the only account of a settled outcome anywhere, and
  wiping it turned those results into an unexplainable "(no reason recorded)"
  tally on the status page.
- Stop reporting queue rows still keyed by an id FreshRSS renumbered at commit
  time as deleted articles. Rows queued before GUIDs were recorded have no way
  left to resolve them, and the article itself is queued again under its
  current id by the archive scan, so they are bookkeeping artefacts rather than
  deletions; existing ones are discarded — including those whose reason "Reset
  logs" had already erased, which an archive rescan could never reach either —
  and any remaining ones now say what they are rather than claiming the entry
  is gone.
- Classify an entry that carries only a headline and a link, such as a video
  post, from its title instead of failing it four times over an empty summary
  that was the correct answer. Restricted to entries that really are that thin:
  an empty summary for an article with real text stays a retryable error rather
  than becoming a silent guess made on the headline alone.
- Stop reporting two different things as "failed". The worker log counted every
  failure in a batch, including ones about to be retried, while the status page
  counted only jobs that had exhausted their retries, so the same word gave two
  numbers that looked like they contradicted each other. The log now reports
  errors, articles awaiting retry, and permanent failures separately.
- Show how many processed articles were classified and how many were skipped,
  with a breakdown of the skip reasons, instead of only their sum. A skipped
  article never reaches Ollama, and skips are intentionally kept out of the
  recent-errors list, so the difference between classifying a backlog and
  walking past it could previously only be found by querying the database.
- Count articles that errored but are still queued for another attempt, which
  were otherwise indistinguishable from untouched queue work.
- Put the required JSON schema, and its exact key names, in the prompt itself.
  The schema was only ever sent in the request's "format" field, where Ollama
  turns it into a decoding constraint the reply cannot violate — but Ollama
  Cloud does not implement that constraint, so on Cloud the schema was
  discarded and the prompt then asked the model to match "the given schema"
  without ever having given it one. Models filled the gap by inventing key
  names ("title", "event", "publication_date", "topics"), which failed
  validation identically on every retry until the article failed permanently.
- Drop unexpected extra fields from an otherwise complete structured reply
  instead of refusing it. Discarding a correct answer over one surplus key only
  loses the article, and on an endpoint that cannot enforce the schema that is
  a routine way for a reply to arrive. A reply still missing any required field
  is rejected as before, and no undeclared field is passed on.
- Fix digest events titled "Topic 2 Decision" and similar. The topic-decision
  request asked the model for an event_title without ever saying what it should
  contain, so some models labelled the decision instead of the event. Such a
  title also gave every article judged against that topic the same event
  fingerprint, collapsing unrelated articles into one event and making a single
  Restore reject all of them. The request now specifies what the title is for,
  and a title made up only of decision vocabulary and numbering is treated as
  no title, falling back to the one derived from the article itself.
- Accept a yes/no verdict a model wrote as a word rather than a JSON boolean
  ("different", "yes", "same event", 0/1) when reshaping a flat id-to-verdict
  reply, instead of failing the whole article. Anything outside that vocabulary
  is still refused rather than guessed at.
- Stop asking the local structuring model to reinterpret a reply that was valid
  JSON in the wrong shape. It cannot know what the wrongly-shaped values meant,
  so it invented replacements and wrote its own confusion into the explanation,
  which then failed validation with a thoroughly misleading error. It is now
  used only for genuinely unstructured replies, which is what it is for.
- Say which part of a decision batch was actually wrong — a repeated ID, an ID
  that was not in the batch, a non-boolean verdict, or an out-of-range
  confidence — instead of one message listing all of them as possibilities.

- Let an exclusion suggestion be reworded before it is approved, and show the
  article it came from (linked, where the link is safe to follow) next to it. A
  suggestion is generated from a single article's title, which is usually far
  too specific to work as an exclusion rule verbatim.
- Hand an article back to the normal unread stream when its job gives up after
  exhausting its retries. Such a job never reaches the end of processing, so an
  article awaiting a restart's restore would otherwise stay marked read
  indefinitely with nothing left to un-read it.
- Ignore a recorded "read by a FreshRSS filter" marker for an article that is
  not actually read. The marker is recorded against the entry's provisional id,
  which FreshRSS may later assign to a different article; that article would
  then inherit the marker and be skipped and un-read as though a filter had
  read it.
- Select forced topic candidates by id instead of comparing whole candidate
  structures for identity, which was both needlessly expensive and wrong for
  two topics that happened to compare equal by value.
- Build one worker processor per batch rather than one per article, so a batch
  no longer re-reads the whole configuration twenty times over.

- Fix the queue never converging while the automatic Ollama Cloud fallback was
  in play: the pipeline and analysis hashes were derived from the *effective*
  profile, so every automatic cloud/local flip invalidated the stored hash of
  every queued job at once. Each job then took the "pipeline changed" branch,
  re-queued itself and did no classification at all — churning quickly, since
  that branch makes no Ollama call, while matching nothing — and every summary
  and embedding was recomputed on each flip. Both hashes, and the embedding
  invalidation, now follow the profile the user configured, so a temporary
  fallback no longer disturbs work already queued.
- Fix the GUID-based recovery of a stale entry id never running on the jobs
  that needed it most: rows queued before the `guid` column existed keep an
  empty GUID, and every re-queue of an already-current job (including a
  backfill rescan) returns early without filling it in. That early return now
  backfills a missing GUID.
- Fix a restart failing to reprocess any job the worker happened to complete
  between clearing the pipeline hashes and stamping the new one; such a job
  kept an empty hash permanently.
- Reverse the read-marking of previously matched articles first after a
  restart, instead of leaving them queued behind the entire archive backlog at
  the default archive priority, where undoing a restart's own read-marking
  could take days.
- Keep Topic Digest paused after "Restart and rebuild" if it was already
  paused beforehand, instead of always resuming it.
- Hand a claimed job back to the queue when the pipeline moved on underneath
  it, instead of leaving it held until its lease expires; four such expiries
  marked an otherwise healthy article failed.
- Stop taking a SQLite write lock on every page load to check the classifier
  revision, and stop rewriting every synthetic feed, overview entry, and
  materialised article on every settings page load. Both competed with the
  worker for the write lock.

- Fix a fatal "database is locked" crash: every request and CLI process ran
  the full schema migration, including a write, on every single
  construction. It now checks the schema version first and only touches the
  database when actually out of date, instead of contending for the write
  lock against a busy worker on every page load.

- Fix articles wrongly reported as "Entry no longer exists" when they were
  still present: FreshRSS only assigns an entry's final id when committing
  new entries, after the id this extension captured at enqueue time, so a
  stored id could be stale from the very first lookup. Recover it via the
  entry's stable GUID before giving up.
- Hide "skipped" jobs (entry deleted, protected, filter-read, no eligible
  topic) from the settings page's recent-errors list; they are intentional,
  expected outcomes, not failures worth attention.

- Skip queuing an article that a FreshRSS filter rule auto-marked read on
  import, instead of processing it like any other new article. This is now
  tracked durably (not just for the current request), and falls back to
  re-evaluating the entry against the feed's/category's/global current
  "mark as read" filters for already-read articles with no recorded match
  (e.g. from before this exclusion existed), since FreshRSS itself does not
  keep a record of why an article became read.
- Enqueue the entire archive backfill up front instead of pacing it against
  the current queue depth, so "queued" (and the processing-time estimate)
  reflects the true amount of outstanding work from the start.
- Apply the filter-read exclusion during "Restart and rebuild Topic Digest"
  too, not just to newly-arriving articles, so a restart correctly skips
  filter-read articles and reverses their previous Topic Digest read-marking
  via the existing rebuild-restore mechanism.

- Add a local/Ollama Cloud profile toggle that keeps separate URLs and models
  for each and switches between them without retyping either.
- Give article summarisation a larger token budget and report when a
  structured Ollama response was cut off by the token limit instead of a
  generic invalid-JSON error.
- Include the model name, done_reason, and a snippet of Ollama's raw response
  in structured-output error messages, and show each recent error's state,
  attempt count, and last-updated time on the settings page.
- Add a "View complete log" option on the settings page showing the worker
  log file, which could previously only be reset, not viewed.
- Recover a structured reply on the local profile's Ollama endpoint when a
  primary endpoint (notably Ollama Cloud, which does not support the JSON
  schema "format" constraint) returns free text instead of JSON, with an
  optional dedicated structuring model.
- Include the HTTP status, curl error, response body snippet, model, and
  target URL when a request to Ollama itself fails, instead of a bare
  "Ollama request failed."
- Explain specifically which part of a topic or event decision batch was
  invalid (wrong count, wrong fields, or an unknown ID/value), with a raw
  response snippet, instead of one generic message for all three cases.
- Tolerate case/punctuation differences in a returned candidate ID (e.g.
  "E-130" for "e:130") instead of rejecting the whole event-decision batch.
- Automatically fall back from the Ollama Cloud profile to the local one for
  20 minutes when Cloud rejects a request for account/plan reasons (HTTP
  402/429, e.g. a usage limit), then automatically try Cloud again; shown on
  the settings page while active. Switching (automatically or manually)
  invalidates cached embeddings so they are never compared across models.
- Fix the processing-time estimate getting stuck on "Calculating…" whenever
  the archive backfill scan was still running, even though a valid
  processing rate and queue length were already known.
- Fix the per-article "Restore" control on high-priority topic entries
  submitting to the wrong action (a double-escaped URL turned "&amp;" into
  "&amp;amp;", so the browser only saw the controller parameter, dropping the
  action and 404ing).
- Fix the global stats popover rendering right-aligned instead of tucked
  under the left edge of its trigger button.
- Recover topic/event decision batches when a model answers with a flat
  id-to-boolean map instead of the required array of decision objects, by
  deterministically reshaping it instead of asking another model to (which
  only ever reconstructed one item instead of the whole batch).

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
