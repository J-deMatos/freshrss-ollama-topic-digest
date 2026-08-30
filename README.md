# FreshRSS Topic Digest

A per-user FreshRSS extension that organises matching news using a local Ollama
service. Each topic can be a low-priority living digest, a high-priority feed
of normal FreshRSS articles, or a rule that automatically marks matches read.

## Project status

This is an exploratory project for personal FreshRSS setups. Expect changes as
the matching rules and interface improve. Back up your FreshRSS data before
trying it on an important installation, and treat the results as suggestions
to review rather than authoritative news.

## Requirements

- FreshRSS with PHP 8.1 or newer and SQLite PDO support
- Ollama reachable from the FreshRSS container or host
- PHP `exec()` for the automatic worker, or a cron job for the CLI worker
- Write access to FreshRSS's per-user extension data directory

The default Ollama URL is `http://ollama:11434`, suitable when FreshRSS and
Ollama share a Docker network. Do not use `localhost` unless Ollama runs inside
the FreshRSS container.

## Installation

Place this repository at exactly:

```text
FreshRSS/extensions/xExtension-TopicDigest/
```

LinuxServer.io installations instead place additional extensions under
`/config/www/freshrss/extensions/xExtension-TopicDigest/`. The CLI worker
automatically locates the FreshRSS core under `/app/www` in that layout.

For example:

```sh
git clone 'https://github.com/OWNER/REPOSITORY.git' \
	/var/www/FreshRSS/extensions/xExtension-TopicDigest
```

Enable **Topic Digest** for the intended FreshRSS user, open its settings, test
the Ollama connection, and create one or more topics. Configuration and the
SQLite sidecar remain in FreshRSS's per-user data area during upgrades.

## Ollama models

The CPU-oriented defaults are `qwen3.5:4b` for summaries, `qwen3.5:9b` for
topic and event decisions, and `qwen3-embedding:0.6b` for embeddings. Install
them with:

```sh
ollama pull qwen3.5:4b
ollama pull qwen3.5:9b
ollama pull qwen3-embedding:0.6b
```

When News Deduplicator is installed, both extensions reuse the same per-user
article summary and embedding only when the article content and model names
match. Neither extension has a runtime dependency on the other.

When both are enabled, Topic Digest receives each new article first at the
FreshRSS queue hook, so classification is queued before deduplication.

## Local and Ollama Cloud profiles

The Ollama settings hold two independent profiles, **Local** and **Ollama
Cloud**, each with its own URL and model names. Switch the active profile
radio and save to change which one is used; the other profile's settings stay
saved so you don't have to re-enter them when you switch back. Switching
profiles reclassifies the embedding cache automatically if the embedding
model changed.

When Cloud rejects a request with an account or plan limit (HTTP 402/429), the
worker uses the local profile for 20 minutes. After that cooldown, the next
worker batch tries Cloud again automatically; another limit response starts a
new cooldown. The selected profile remains Cloud throughout, so no manual
switch back is needed.

Queue bookkeeping follows the profile you selected, not the one temporarily in
use during an automatic fallback, so a fallback and its recovery never
invalidate work that is already queued. The trade-off is that summaries and
embeddings produced while the fallback is active are cached under the selected
profile's identity; mismatched embedding dimensions are scored as "no
similarity", so this only blurs which topics get shortlisted for an article,
never which ones it is actually compared against.

Ollama Cloud does not support the JSON schema "format" constraint that
enforces a structured reply. Every request therefore also states the required
schema and its exact field names in the prompt, so a cloud model is told what
shape to produce rather than being expected to guess it; a reply that arrives
with extra fields is accepted and trimmed, and one missing a required field is
still rejected. A cloud model may nonetheless answer in plain text or with a
wrongly shaped object. When it answers in plain text, Topic Digest resends it to a
model on the local profile's URL (its own structured-output support is
unaffected) and asks it to extract the same fields. This "structuring model"
defaults to the local profile's summary model; set an explicit one in
**Structuring model** if you'd rather use something smaller or faster for
that recovery step. If both the primary reply and the structuring fallback
fail, the error on the settings page includes the model name, done_reason,
and a snippet of the raw reply for troubleshooting, and the full worker log
is available under **View complete log**.

## Topic presentations

Each topic has one of two presentations:

- **Low priority** groups matches as dated events in one living digest entry. A
  genuinely new event marks that entry unread; extra coverage does not.
- **High priority** pins the same living event overview at the top, followed by
  one normal unread FreshRSS entry for every matched source. The individual
  entries retain the original title, author, article content, link, publication
  date, tags, and enclosures and can be read or favourited normally.

Both presentations live under the synthetic **Topic Digests** category and are
excluded from the main stream, network refresh, archive scanning, and matching.
Changing presentation replaces only extension-owned generated objects and
preserves the topic rule and its source memberships.

A high-priority feed's generated articles are removed when their match goes
away, including when a rebuild clears every match at once, so the sidebar's
`[ number ]` of matched sources and the feed's unread count stay consistent.
Favourited articles are kept, as everywhere else in this extension.

**Mark read** topics store matches, explanations, events, and Restore decisions
without creating a synthetic feed by default. A per-topic verification option
can expose the same living digest temporarily for auditing; it does not change
the automatic read behavior.

For prominence, the extension places **🧭 Topic Digests** directly below
Favourites in the sidebar. Topic feeds use **🗂️** for low priority and **⚡** for
high priority. These are interface markers only; stored category and feed names
remain unchanged.

## Matching behaviour

Each topic has an inclusion description, explicit exclusions, confidence,
feed/category scope, and history period. Matching reports share revision-safe
summaries, decisions, source memberships, and event grouping in either mode.

The category opens all topic feeds, and an optional display setting keeps them
visible with zero unread entries. Low-priority topics open their living entry
even in unread-only mode; high-priority topics retain ordinary FreshRSS read
filtering for their individual articles. Their overview is dated just after the
newest matched article so it remains first in the topic list. Sidebar topic names
show their aggregated source count as `[ number ]` alongside FreshRSS's normal
unread counter.

Events are ordered newest-first by their effective event date. Source links
within an event are ordered by RSS publication time; both dates are labelled in
the overview. Classification uses the topic name, inclusion description, and
exclusions, and requires direct evidence rather than a shared keyword.
Structured requests evaluate up to eight topics or ten event candidates at
once. Exact revision-safe decisions are cached.

## Restore and rebuild

Matching source articles are marked read only after the living digest or
high-priority topic copy is persisted successfully.
Favourites and explicit manual-unread choices remain protected. **Restore** and
**Restore all** return sources to the normal unread stream and create a pending
exclusion suggestion; topic rules change only after user approval.

A suggestion is worded from the restored article's title, which is normally too
specific to exclude similar articles on its own, so the settings page shows the
suggestion as an editable field alongside a link to the article it came from.
Reword it before choosing **Approve**, and the wording you submit is what gets
added to the topic's exclusions; **Dismiss** discards the suggestion and changes
nothing.
High-priority entries expose their stored match explanation and a **Restore**
control after the original article body.

**Rescan archive** re-queues every archived article once, for all topics
together — it is a single global action, not a per-topic one. Matches,
summaries and decisions are kept and reused, so it is far cheaper than a
rebuild; use it to pick up articles whose queue entry became unusable. It can
only revisit articles FreshRSS still lists, so **Re-examine skipped articles**
exists alongside it to re-queue skipped articles directly; skips that are still
correct cost almost nothing to re-derive, since they are decided by rule with
no Ollama call.

**Restart and rebuild Topic Digest** immediately clears generated memberships,
retains reusable summaries and user-authored rules, and reclassifies retained
articles. Sources that still match return to the rebuilt digest; sources that
no longer match return to the normal unread stream. Previously matched
articles are reclassified ahead of the rest of the queue, so the read-marking a
restart needs to undo is undone first rather than after the whole backlog. A
restart leaves the worker paused if it was already paused.

Note that a restart requeues every known article, and the archive backfill
enqueues the whole archive up front, so the queue legitimately jumps to the
size of your entire history. Articles outside every topic's history period, or
already read by a FreshRSS filter, are resolved by rule alone with no Ollama
call, so a large part of that queue can drain very quickly without producing
any matches.

## Worker and status

A single locked worker starts automatically, uses expiring leases and retry
backoff, and reloads after live code or configuration changes. The navigation
status panel reports the queue, throughput, estimated completion time, events,
sources, and failures, with Pause/Resume and rebuild controls.

The settings page distinguishes the outcomes, which are easy to confuse:
**Failed permanently** counts only jobs that have exhausted their retries,
while **Errored, awaiting retry** counts jobs that failed and are queued for
another attempt — the worker log reports both, plus the errors seen in that
batch. **Articles processed** covers classified and skipped articles together;
each is also shown separately, with a breakdown of why articles were skipped.
A skipped article never reaches Ollama, so a backlog dominated by skips drains
quickly and produces no matches, which is expected rather than a fault. Skips
are decided by rule before any classification happens — the article was deleted,
protected, already read by a FreshRSS filter, or outside every topic's scope —
so a skip is never a judgement about an article's topic and never a
misclassification. "Reset logs" clears failure text but keeps these reasons.

**Stale queue rows discarded** is separate from all of that, and deliberately
outside "Articles processed". Rows queued before the extension recorded article
GUIDs are keyed by an id FreshRSS renumbered when it committed the entry, so
nothing can resolve them back to an article. They are queue bookkeeping, not
articles, and counting each one as processed-and-skipped described work that
never happened. Ones already recorded that way are dropped automatically and
added to this total. Ones still waiting in the queue are not touched
automatically — until the archive scan has completed, a row queued before GUIDs
were recorded cannot be told apart from one whose article is still live — so
clear them explicitly once the scan has finished:

```sh
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/prune-stale-jobs.php \
	--user alice          # reports how many there are
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/prune-stale-jobs.php \
	--user alice --apply  # deletes them
```

No article is lost either way: one that still exists is queued again under its
current id by the archive scan. The command refuses to run while the worker
holds its lock, and while the archive scan is still active.

When both extensions are enabled, Topic Digest queues first and News
Deduplicator waits for its classification of the same article revision. Topic
matches can therefore mark the source read before deduplication; a non-match
continues through News Deduplicator normally. Pausing or disabling Topic Digest
does not pause News Deduplicator.

If automatic execution is unavailable, run the worker from cron:

```sh
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/process.php \
	--user alice --limit 20
```

Average throughput includes only monotonic active time for successfully
completed jobs. Pauses, archive scanning, failures, idle time, daemon downtime,
wall-clock changes, and system suspend are excluded.

Processing can speed up noticeably without anything being wrong:

- Cached article summaries and embeddings are reused whenever an article's
  content and the model configuration are unchanged — restarting only clears
  topic/event decisions, so no Ollama call happens for those articles' summary
  step, just a database read.
- "No active topic includes this article" and "Marked read by a FreshRSS
  filter" are both pure rule checks with no Ollama call at all, so a backlog
  with a lot of either flies through quickly.
- The active model itself may simply be fast.

None of that skips real classification work; it only skips work that was
already done or never needed an LLM call. If processing speed still seems
concerning, check the recent-errors list and failed count rather than the
throughput number alone, and — on the Cloud profile — keep an eye on your
usage quota, since sustained high throughput consumes it faster.

## Development and tests

Run the extension tests against a FreshRSS source checkout with its development
dependencies installed:

```sh
FRESHRSS_PATH=/path/to/FreshRSS \
	/path/to/FreshRSS/vendor/bin/phpunit -c phpunit.xml.dist
```

Lint PHP before a release:

```sh
find . -type f \( -name '*.php' -o -name '*.phtml' \) \
	-exec php -l {} \;
```

Create an installable archive from a tag with:

```sh
git archive --format=tar.gz \
	--prefix=xExtension-TopicDigest/ \
	-o xExtension-TopicDigest.tar.gz HEAD
```

See [CHANGELOG.md](CHANGELOG.md) for release notes.

## License

This extension is distributed under the GNU Affero General Public License,
version 3. See [LICENSE](LICENSE).
