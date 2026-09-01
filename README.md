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

## Provider architecture

Text inference (summaries and topic/event decisions) and embeddings are
independently configured providers:

- A **primary text provider**: Local Ollama, Ollama Cloud, or a generic
  **OpenAI-compatible** chat-completions endpoint (Bearer authentication,
  `POST {base_url}/chat/completions`) — OpenRouter, Groq, or anything else
  speaking the same protocol. No provider is assumed by name; only the base
  URL, API key, and model names are configured.
- An optional **fallback text provider**, used only while the primary is
  rejecting requests for account/plan reasons (HTTP 402/429, or a response
  body that looks like exhausted quota/credits on some other status). It can
  be Local Ollama (the default, reproducing the original automatic Ollama
  Cloud → local behaviour, generalized to any non-local primary), an
  OpenAI-compatible endpoint, or disabled entirely.
- An **embedding provider**, always Ollama-compatible and fully independent
  of whichever text provider(s) are configured — an OpenAI-compatible
  endpoint is never sent embedding requests, since none is required to
  support them.

Each of the three is a separate class behind small `TopicDigestTextProvider`/
`TopicDigestEmbeddingProvider` interfaces
(`TopicDigestOllama`, `TopicDigestOpenAICompatible`), assembled per request by
`TopicDigestTextProviderChain`. `TopicDigestProcessor` only ever talks to
these abstractions, never to a specific provider's wire format.

## Configuring a primary + fallback

Pick the **primary text provider** (Local Ollama, Ollama Cloud, or
OpenAI-compatible) and fill in its section; the other sections' settings stay
saved so switching back later doesn't need re-entering them. Then pick a
**fallback text provider** — the default, "Local Ollama", is what earlier
versions did automatically and unconditionally for a Cloud primary.

Example: Ollama Cloud as primary, OpenRouter as fallback, for the free Cloud
allowance first and a cheap continuation once it's used up:

```text
Primary text provider:    Ollama Cloud
    Summary model:        gpt-oss:20b-cloud
    Decision model:       gpt-oss:20b-cloud

Fallback text provider:   OpenAI-compatible
    Base URL:             https://openrouter.ai/api/v1
    API key:              <your OpenRouter key>
    Summary model:        openai/gpt-oss-20b
    Decision model:       openai/gpt-oss-20b

Embedding provider:       Local Ollama
    Embedding model:      qwen3-embedding:0.6b
```

The same shape works with Groq instead:

```text
Base URL:  https://api.groq.com/openai/v1
```

**Keep embeddings local.** An embedding model only needs to run once per
article and topic description, is much cheaper than text generation, and
"Local Ollama" is the recommended embedding provider regardless of which text
provider(s) you use for summaries and decisions.

## Quota fallback behaviour

When the primary rejects a request for account/plan reasons, the worker uses
the fallback for about 20 minutes, then automatically tries the primary again;
another limit response starts a new cooldown. **The saved primary provider
never changes** — this is purely a temporary, automatic substitution, visible
on the settings page as "Effective text provider" and "Fallback active
until".

A temporary fallback, by itself, never requeues or invalidates the processing
backlog, and articles already summarised/embedded before the fallback started
are reused as-is once the fallback (or the primary again, after recovery)
picks up the queue — only the article that hasn't reached that stage yet
actually calls the model. Changing a provider's *configuration* (its model
name, its URL) is a different, deliberate action, and does invalidate
whatever depended on the old value, exactly as changing a single Ollama
profile's model always has.

Ollama Cloud, and many OpenAI-compatible endpoints, don't fully support (or
don't support at all) a JSON-schema-constrained reply. Every request therefore
also states the required schema and its exact field names in the prompt, so a
model is told what shape to produce rather than being expected to guess it; a
reply that arrives with extra fields is accepted and trimmed, and one missing
a required field is still rejected. The OpenAI-compatible provider prefers
native `response_format: {"type": "json_schema", ...}` when it works, and
also sends `reasoning_effort: "low"` to cap hidden reasoning-token usage on
reasoning-capable models (e.g. `gpt-oss`, `gpt-5-nano`); it retries once
without both if the endpoint rejects that request shape outright, and
separately retries once with a much larger token budget if a reasoning
model still spends its whole budget reasoning and emits no visible content.
A model may nonetheless answer
in plain text or with a wrongly shaped object;
when it does, Topic Digest resends it to a model on the local Ollama profile's
URL (its own structured-output support is unaffected) and asks it to extract
the same fields. This "structuring model" defaults to the local profile's
summary model; set an explicit one in **Structuring model** if you'd rather
use something smaller or faster for that recovery step. If both the primary
reply and the structuring fallback fail, the error on the settings page
includes the provider, model name, HTTP status, and a snippet of the raw
reply for troubleshooting (never the API key), and the full worker log is
available under **View complete log**.

## API key security

An OpenAI-compatible API key is stored with your other Topic Digest settings
(the same per-user configuration FreshRSS already uses for this extension —
treat it with the same care as your FreshRSS login). It is never logged,
never included in an error message, never sent to your browser once saved,
and never appears on the status page: the settings field is always rendered
blank, and a blank submission means "keep the saved key unchanged" — use the
"Clear the saved API key" checkbox to actually remove it.

## Provider/model changes and cached results

Summaries, embeddings, and topic/event decisions are all cached, keyed
(among other things) on which provider and model actually produced them. It
does **not** matter which provider happens to be effective at any given
moment (primary or a temporary fallback): that distinction only ever affects
*which* provider is asked to do new work, never whether already-cached work
is considered valid.

Changing the **summary or embedding model** (either provider's, or the
embedding provider's) restarts the digest: it unmatches already-matched
articles and reclassifies the whole backlog against the new value, the same
behaviour a single-profile setup always had, because the summaries and
embeddings those matches were based on are no longer trustworthy.

Changing only the **judge model** does not restart the digest. Topic and
event decisions are cached against the judge model that produced them, so a
change is picked up automatically the next time a decision is actually
needed — a new article, a new candidate event — without redoing every
decision already made. If you want to deliberately re-judge already-matched
articles against a new judge model, restart the digest manually from its
settings page.

## Topic presentations

Each topic has one of two presentations:

- **Low priority** groups matches as dated events in one living digest entry. A
  genuinely new event marks that entry unread; extra coverage does not.
- **High priority** pins the same living event overview at the top, followed by
  one normal unread FreshRSS entry per matched *event*. The individual entries
  retain the original title, author, article content, link, publication date,
  tags, and enclosures and can be read or favourited normally.

Both presentations live under the synthetic **Topic Digests** category and are
excluded from the main stream, network refresh, archive scanning, and matching.
Changing presentation replaces only extension-owned generated objects and
preserves the topic rule and its source memberships.

When several matched articles describe the same event — near-duplicate
republishes, or the same story reported by more than one feed — only the
source that first opened the event gets its own entry in a high-priority
feed; the other sources still count toward the match and are listed, each
with its own explanation, in the pinned overview, but do not add further
entries for what would otherwise read as the same story shown twice. If that
entry's source stops matching or is Restored, the next-earliest remaining
source takes its place.

A high-priority feed's generated articles are removed when their match goes
away, including when a rebuild clears every match at once, so the sidebar's
`[ number ]` of matched sources and the feed's unread count stay consistent.
Favourited articles are kept, as everywhere else in this extension.

An installation that already ran a high-priority topic before one entry per
event became the rule keeps whichever extra per-source entries it had already
created until that topic's feed is next resynchronised with pruning enabled —
normally its next "Restart and rebuild" pass. To drop the extra entries
without also re-deriving every classification decision (which "Restart and
rebuild" does, at the cost of an Ollama call per previously matched article),
run:

```sh
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/resync-high-priority-feeds.php \
	--user alice          # reports current vs. target entry counts
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/resync-high-priority-feeds.php \
	--user alice --apply  # resyncs every high-priority topic to match
```

It only re-synchronises each topic's high-priority feed from its existing
matches and events, making no Ollama calls and requeuing no jobs. Like the
other maintenance commands, it refuses to run while the worker holds its
lock.

That resync only touches the generated Topic Digest entries; it never marks
anything read or unread in the original feed. Marking a matched source's
original article read there is a separate step that runs when the match is
first made, and gaps in it across earlier versions can leave some already-
matched articles unread in their original feed even though they are properly
represented in the digest. To retroactively apply that same read-marking rule
— skipping favourites, articles the user manually kept unread, and anything
whose content has changed since it matched — to every currently matched
source, once, run:

```sh
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/mark-matched-sources-read.php \
	--user alice          # reports what would be marked read, and why anything is skipped
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/mark-matched-sources-read.php \
	--user alice --apply  # marks them read
```

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

## Worker concurrency

**Batch size** and **concurrency** are different settings. Batch size
(`--batch`/`--limit`) is how many articles the worker considers in one run.
**Worker concurrency** (1-8, default **1**) is how many of those articles may
have text inference (summarization, topic judging) in flight *at the same
time*. The default reproduces the original, fully sequential worker exactly —
nothing changes until this is raised.

**It only ever applies to an OpenAI-compatible endpoint.** Whether the
effective text provider is local Ollama or Ollama Cloud, the setting is
silently ignored and processing stays fully sequential, one article at a
time, regardless of its value. Ollama is a single server process without an
OpenAI-compatible API's multi-tenant concurrent-request handling, and this
project's own recommended models already target modest CPU/GPU hardware —
asking it to run several articles' inference at once would add queuing and
contention at best and risk starving or exhausting memory on a constrained
host at worst, for no real throughput gain. Embeddings, which always go to
Ollama regardless of which text provider is configured, are likewise never
sent concurrently, even while text inference to an OpenAI-compatible endpoint
is running concurrently for the same batch.

With an OpenAI-compatible primary or fallback in effect, **4** is a
reasonable starting point, and **8** is worth trying only if the provider's
own rate limits and this host's resources comfortably allow it.

Concurrency only ever overlaps the *inference* stage between articles.
Everything that mutates shared state for event grouping — deciding whether an
article starts a new event or joins an existing one, source membership,
generated-feed synchronization, and read/unread marking — always commits one
article at a time, never concurrently, specifically so that two articles about
the same real-world event can never race into creating two separate events.

Set it on the settings page, or override it per invocation:

```sh
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/daemon.php \
	--user alice --batch 20 --concurrency 4
php /var/www/FreshRSS/extensions/xExtension-TopicDigest/cli/process.php \
	--user alice --limit 20 --concurrency 4
```

`--batch`/`--limit` keep their existing meaning exactly (up to that many
articles total for the invocation); `--concurrency` only bounds how many of
them may be worked on simultaneously within that limit.

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
