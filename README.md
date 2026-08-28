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
High-priority entries expose their stored match explanation and a **Restore**
control after the original article body.

**Restart and rebuild Topic Digest** immediately clears generated memberships,
retains reusable summaries and user-authored rules, and reclassifies retained
articles. Sources that still match return to the rebuilt digest; sources that
no longer match return to the normal unread stream.

## Worker and status

A single locked worker starts automatically, uses expiring leases and retry
backoff, and reloads after live code or configuration changes. The navigation
status panel reports the queue, throughput, estimated completion time, events,
sources, and failures, with Pause/Resume and rebuild controls.

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
