<?php
declare(strict_types=1);

final class TopicDigestStore {
	private const MAX_ATTEMPTS = 4;
	private const LIVE_PRIORITY_BASE = 1_000_000_000;
	private PDO $pdo;

	public function __construct(string $path) {
		$directory = dirname($path);
		if (!is_dir($directory) && !mkdir($directory, 0700, true) && !is_dir($directory)) {
			throw new RuntimeException('Cannot create the Topic Digest data directory.');
		}
		$this->pdo = new PDO('sqlite:' . $path, null, null, [
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
			PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
		]);
		$this->pdo->exec('PRAGMA busy_timeout = 5000');
		$this->pdo->exec('PRAGMA journal_mode = WAL');
		$this->pdo->exec('PRAGMA synchronous = NORMAL');
		$this->migrate();
	}

	private function migrate(): void {
		$this->pdo->exec(<<<'SQL'
			CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
			CREATE TABLE IF NOT EXISTS topics (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL,
				description TEXT NOT NULL,
				exclusions TEXT NOT NULL DEFAULT '[]',
				enabled INTEGER NOT NULL DEFAULT 1,
				confidence REAL NOT NULL DEFAULT 0.85,
				all_feeds INTEGER NOT NULL DEFAULT 1,
				all_categories INTEGER NOT NULL DEFAULT 0,
				feed_ids TEXT NOT NULL DEFAULT '[]',
				category_ids TEXT NOT NULL DEFAULT '[]',
				backfill_mode TEXT NOT NULL DEFAULT 'days',
				backfill_days INTEGER NOT NULL DEFAULT 90,
				topic_type TEXT NOT NULL DEFAULT 'digest',
				show_verification INTEGER NOT NULL DEFAULT 0,
				feed_id INTEGER,
				entry_id TEXT,
				description_embedding TEXT,
				rule_hash TEXT NOT NULL,
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL
			);
			CREATE TABLE IF NOT EXISTS jobs (
				entry_id TEXT PRIMARY KEY,
				feed_id INTEGER NOT NULL,
				category_id INTEGER NOT NULL,
				title TEXT NOT NULL,
				author TEXT NOT NULL,
				link TEXT NOT NULL,
				published_at INTEGER NOT NULL,
				content_hash TEXT NOT NULL,
				pipeline_hash TEXT NOT NULL,
				rss_text TEXT NOT NULL,
				is_archive INTEGER NOT NULL DEFAULT 0,
				priority INTEGER NOT NULL DEFAULT 100,
				state TEXT NOT NULL DEFAULT 'pending',
				attempts INTEGER NOT NULL DEFAULT 0,
				available_at INTEGER NOT NULL DEFAULT 0,
				lease_until INTEGER NOT NULL DEFAULT 0,
				error TEXT NOT NULL DEFAULT '',
				processing_seconds REAL NOT NULL DEFAULT 0,
				processing_content_hash TEXT NOT NULL DEFAULT '',
				processing_pipeline_hash TEXT NOT NULL DEFAULT '',
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL
			);
			CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs(state, available_at, priority, published_at);
			CREATE TABLE IF NOT EXISTS summaries (
				entry_id TEXT PRIMARY KEY,
				content_hash TEXT NOT NULL,
				analysis_hash TEXT NOT NULL DEFAULT '',
				feed_name TEXT NOT NULL,
				summary TEXT NOT NULL,
				source_text TEXT NOT NULL,
				embedding TEXT NOT NULL,
				updated_at INTEGER NOT NULL
			);
			CREATE TABLE IF NOT EXISTS events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				topic_id INTEGER NOT NULL,
				title TEXT NOT NULL,
				occurred_at INTEGER NOT NULL,
				explanation TEXT NOT NULL,
				embedding TEXT NOT NULL,
				fingerprint TEXT NOT NULL,
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL
			);
			CREATE INDEX IF NOT EXISTS events_topic_date_idx ON events(topic_id, occurred_at DESC);
			CREATE TABLE IF NOT EXISTS sources (
				topic_id INTEGER NOT NULL,
				event_id INTEGER NOT NULL,
				entry_id TEXT NOT NULL,
				feed_name TEXT NOT NULL,
				title TEXT NOT NULL,
				link TEXT NOT NULL,
				published_at INTEGER NOT NULL,
				content_hash TEXT NOT NULL,
				explanation TEXT NOT NULL,
				added_at INTEGER NOT NULL,
				PRIMARY KEY(topic_id, entry_id)
			);
			CREATE INDEX IF NOT EXISTS sources_event_idx ON sources(event_id);
			CREATE TABLE IF NOT EXISTS rejections (
				topic_id INTEGER NOT NULL,
				entry_id TEXT NOT NULL DEFAULT '',
				fingerprint TEXT NOT NULL DEFAULT '',
				title TEXT NOT NULL DEFAULT '',
				explanation TEXT NOT NULL DEFAULT '',
				embedding TEXT NOT NULL DEFAULT '[]',
				occurred_at INTEGER NOT NULL DEFAULT 0,
				created_at INTEGER NOT NULL,
				PRIMARY KEY(topic_id, entry_id, fingerprint)
			);
			CREATE TABLE IF NOT EXISTS suggestions (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				topic_id INTEGER NOT NULL,
				text TEXT NOT NULL,
				state TEXT NOT NULL DEFAULT 'pending',
				created_at INTEGER NOT NULL
			);
			CREATE TABLE IF NOT EXISTS overrides (
				entry_id TEXT PRIMARY KEY,
				kind TEXT NOT NULL,
				created_at INTEGER NOT NULL
			);
			CREATE TABLE IF NOT EXISTS rebuild_restores (
				entry_id TEXT PRIMARY KEY,
				created_at INTEGER NOT NULL
			);
			CREATE TABLE IF NOT EXISTS topic_decisions (
				entry_id TEXT NOT NULL,
				content_hash TEXT NOT NULL,
				topic_id INTEGER NOT NULL,
				rule_hash TEXT NOT NULL,
				judge_model TEXT NOT NULL,
				matches INTEGER NOT NULL,
				confidence REAL NOT NULL,
				reason TEXT NOT NULL,
				event_title TEXT NOT NULL,
				updated_at INTEGER NOT NULL,
				PRIMARY KEY(entry_id,content_hash,topic_id,rule_hash,judge_model)
			);
			CREATE TABLE IF NOT EXISTS event_decisions (
				entry_id TEXT NOT NULL,
				content_hash TEXT NOT NULL,
				topic_id INTEGER NOT NULL,
				candidate_id TEXT NOT NULL,
				candidate_hash TEXT NOT NULL,
				judge_model TEXT NOT NULL,
				same_event INTEGER NOT NULL,
				confidence REAL NOT NULL,
				reason TEXT NOT NULL,
				updated_at INTEGER NOT NULL,
				PRIMARY KEY(entry_id,content_hash,topic_id,candidate_id,candidate_hash,judge_model)
			);
			CREATE TABLE IF NOT EXISTS processing_samples (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				kind TEXT NOT NULL,
				pipeline_hash TEXT NOT NULL,
				articles INTEGER NOT NULL,
				active_seconds REAL NOT NULL,
				created_at INTEGER NOT NULL
			);
			CREATE INDEX IF NOT EXISTS processing_samples_created_idx ON processing_samples(created_at);
			SQL);
		if (!$this->hasColumn('summaries', 'analysis_hash')) {
			$this->pdo->exec("ALTER TABLE summaries ADD COLUMN analysis_hash TEXT NOT NULL DEFAULT ''");
		}
		if (!$this->hasColumn('jobs', 'is_archive')) {
			$this->pdo->exec('ALTER TABLE jobs ADD COLUMN is_archive INTEGER NOT NULL DEFAULT 0');
		}
		if (!$this->hasColumn('jobs', 'processing_seconds')) {
			$this->pdo->exec('ALTER TABLE jobs ADD COLUMN processing_seconds REAL NOT NULL DEFAULT 0');
		}
		if (!$this->hasColumn('jobs', 'processing_content_hash')) {
			$this->pdo->exec("ALTER TABLE jobs ADD COLUMN processing_content_hash TEXT NOT NULL DEFAULT ''");
		}
		if (!$this->hasColumn('jobs', 'processing_pipeline_hash')) {
			$this->pdo->exec("ALTER TABLE jobs ADD COLUMN processing_pipeline_hash TEXT NOT NULL DEFAULT ''");
		}
		if (!$this->hasColumn('topics', 'topic_type')) {
			$this->pdo->exec("ALTER TABLE topics ADD COLUMN topic_type TEXT NOT NULL DEFAULT 'digest'");
		}
		if (!$this->hasColumn('topics', 'show_verification')) {
			$this->pdo->exec('ALTER TABLE topics ADD COLUMN show_verification INTEGER NOT NULL DEFAULT 0');
		}
		foreach (['title' => "''", 'explanation' => "''", 'embedding' => "'[]'"] as $column => $default) {
			if (!$this->hasColumn('rejections', $column)) {
				$this->pdo->exec("ALTER TABLE rejections ADD COLUMN {$column} TEXT NOT NULL DEFAULT {$default}");
			}
		}
		if (!$this->hasColumn('rejections', 'occurred_at')) {
			$this->pdo->exec('ALTER TABLE rejections ADD COLUMN occurred_at INTEGER NOT NULL DEFAULT 0');
		}
		if ($this->getMeta('processing_metrics_revision') !== '2') {
			$this->setMeta('active_processing_seconds', '0');
			$this->setMeta('active_processed_articles', '0');
			$this->setMeta('processing_metrics_revision', '2');
			$this->setMeta('processing_metrics_sample_revision', '0');
		}
		$this->pdo->exec('PRAGMA user_version = 7');
	}

	/** @param array<string,mixed> $values */
	public function saveTopic(array $values, ?int $id = null): int {
		$now = time();
		$name = trim((string)($values['name'] ?? ''));
		$description = trim((string)($values['description'] ?? ''));
		if ($name === '' || $description === '') {
			throw new InvalidArgumentException('Topic name and description are required.');
		}
		if (mb_strlen($name, 'UTF-8') > 200 || mb_strlen($description, 'UTF-8') > 10000) {
			throw new InvalidArgumentException('The topic name or description is too long.');
		}
		$exclusions = $this->stringList(is_array($values['exclusions'] ?? null) ? $values['exclusions'] : []);
		$feeds = $this->idList(is_array($values['feed_ids'] ?? null) ? $values['feed_ids'] : []);
		$categories = $this->idList(is_array($values['category_ids'] ?? null) ? $values['category_ids'] : []);
		$mode = in_array($values['backfill_mode'] ?? '', ['days', 'all', 'future'], true)
			? (string)$values['backfill_mode'] : 'days';
		$topicType = in_array($values['topic_type'] ?? '', ['digest', 'feed', 'mark_read'], true)
			? (string)$values['topic_type'] : 'digest';
		$showVerification = $topicType === 'mark_read' && !empty($values['show_verification']) ? 1 : 0;
		$rawConfidence = $values['confidence'] ?? 0.85;
		if (!is_numeric($rawConfidence) || (float)$rawConfidence < 0.0 || (float)$rawConfidence > 1.0) {
			throw new InvalidArgumentException('Topic confidence must be a number between 0 and 1.');
		}
		$confidence = (float)$rawConfidence;
		$days = min(3650, max(1, (int)($values['backfill_days'] ?? 90)));
		$hash = hash('sha256', json_encode([$description, $exclusions, $confidence], JSON_THROW_ON_ERROR));
		$params = [
			$name, $description, json_encode($exclusions, JSON_THROW_ON_ERROR), !empty($values['enabled']) ? 1 : 0,
			$confidence, !empty($values['all_feeds']) ? 1 : 0, !empty($values['all_categories']) ? 1 : 0,
			json_encode($feeds, JSON_THROW_ON_ERROR), json_encode($categories, JSON_THROW_ON_ERROR),
			$mode, $days, $topicType, $showVerification, $hash,
		];
		if ($id === null) {
			$statement = $this->pdo->prepare('INSERT INTO topics(name,description,exclusions,enabled,confidence,'
				. 'all_feeds,all_categories,feed_ids,category_ids,backfill_mode,backfill_days,topic_type,show_verification,'
				. 'rule_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)');
			$statement->execute([...$params, $now, $now]);
			return (int)$this->pdo->lastInsertId();
		}
		$existing = $this->topic($id);
		if ($existing === null) {
			throw new InvalidArgumentException('Unknown topic.');
		}
		$statement = $this->pdo->prepare('UPDATE topics SET name=?,description=?,exclusions=?,enabled=?,confidence=?,'
			. 'all_feeds=?,all_categories=?,feed_ids=?,category_ids=?,backfill_mode=?,backfill_days=?,topic_type=?,'
			. 'show_verification=?,rule_hash=?,description_embedding=?,updated_at=? WHERE id=?');
		$embedding = hash_equals((string)$existing['rule_hash'], $hash) ? $existing['description_embedding'] : null;
		$statement->execute([...$params, $embedding, $now, $id]);
		return $id;
	}

	/** @return array<string,mixed>|null */
	public function topic(int $id): ?array {
		$row = $this->row('SELECT t.*,(SELECT COUNT(*) FROM events e WHERE e.topic_id=t.id) AS event_count,'
			. '(SELECT COUNT(*) FROM sources s WHERE s.topic_id=t.id) AS article_count FROM topics t WHERE t.id=?', [$id]);
		return $row === null ? null : $this->decodeTopic($row);
	}

	/** @return list<array<string,mixed>> */
	public function topics(bool $enabledOnly = false): array {
		$statement = $this->pdo->query('SELECT t.*,'
			. '(SELECT COUNT(*) FROM events e WHERE e.topic_id=t.id) AS event_count,'
			. '(SELECT COUNT(*) FROM sources s WHERE s.topic_id=t.id) AS article_count FROM topics t'
			. ($enabledOnly ? ' WHERE t.enabled=1' : '') . ' ORDER BY t.name');
		$rows = $statement === false ? [] : $statement->fetchAll();
		return array_map(fn(array $row): array => $this->decodeTopic($row), $rows);
	}

	/** @param array<string,mixed> $row @return array<string,mixed> */
	private function decodeTopic(array $row): array {
		foreach (['exclusions', 'feed_ids', 'category_ids'] as $key) {
			$decoded = json_decode((string)$row[$key], true);
			$row[$key] = is_array($decoded) ? $decoded : [];
		}
		foreach (['enabled', 'all_feeds', 'all_categories', 'show_verification'] as $key) {
			$row[$key] = (bool)$row[$key];
		}
		$row['event_count'] = (int)($row['event_count'] ?? 0);
		$row['article_count'] = (int)($row['article_count'] ?? 0);
		return $row;
	}

	public function setTopicEnabled(int $id, bool $enabled): void {
		$this->execute('UPDATE topics SET enabled=?,updated_at=? WHERE id=?', [$enabled ? 1 : 0, time(), $id]);
	}

	/** @return array{feed_id:int|null,entry_id:string|null}|null */
	public function deleteTopic(int $id): ?array {
		$topic = $this->topic($id);
		if ($topic === null) {
			return null;
		}
		$this->pdo->beginTransaction();
		try {
			$this->execute('DELETE FROM suggestions WHERE topic_id=?', [$id]);
			$this->execute('DELETE FROM rejections WHERE topic_id=?', [$id]);
			$this->execute('DELETE FROM topic_decisions WHERE topic_id=?', [$id]);
			$this->execute('DELETE FROM event_decisions WHERE topic_id=?', [$id]);
			$this->execute('DELETE FROM sources WHERE topic_id=?', [$id]);
			$this->execute('DELETE FROM events WHERE topic_id=?', [$id]);
			$this->execute('DELETE FROM topics WHERE id=?', [$id]);
			$this->pdo->commit();
		} catch (Throwable $e) {
			$this->pdo->rollBack();
			throw $e;
		}
		return [
			'feed_id' => $topic['feed_id'] === null ? null : (int)$topic['feed_id'],
			'entry_id' => $topic['entry_id'] === null ? null : (string)$topic['entry_id'],
		];
	}

	public function attachSynthetic(int $topicId, ?int $feedId, ?string $entryId): void {
		$this->execute('UPDATE topics SET feed_id=?,entry_id=?,updated_at=? WHERE id=?', [$feedId, $entryId, time(), $topicId]);
	}

	/** @param list<float> $embedding */
	public function saveTopicEmbedding(int $topicId, string $ruleHash, array $embedding): bool {
		$statement = $this->pdo->prepare('UPDATE topics SET description_embedding=? WHERE id=? AND rule_hash=?');
		$statement->execute([json_encode($embedding, JSON_THROW_ON_ERROR), $topicId, $ruleHash]);
		return $statement->rowCount() === 1;
	}

	public function invalidateEmbeddings(): void {
		$this->pdo->exec('UPDATE topics SET description_embedding=NULL');
		$this->pdo->exec("UPDATE events SET embedding='[]'");
	}

	/**
	 * Clear generated digest memberships and requeue every known article for the
	 * current rules while retaining expensive article summaries and user rules.
	 *
	 * @return list<string> Previously matched source entry IDs.
	 */
	public function rebuildDigests(): array {
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			$statement = $this->pdo->query('SELECT DISTINCT entry_id FROM sources ORDER BY entry_id');
			$entryIds = $statement === false ? [] : array_map('strval', $statement->fetchAll(PDO::FETCH_COLUMN));
			$now = time();
			$this->pdo->exec('INSERT OR IGNORE INTO rebuild_restores(entry_id,created_at) '
				. 'SELECT DISTINCT entry_id,' . $now . ' FROM sources');
			$this->pdo->exec('DELETE FROM topic_decisions');
			$this->pdo->exec('DELETE FROM event_decisions');
			$this->pdo->exec('DELETE FROM sources');
			$this->pdo->exec('DELETE FROM events');
			$this->execute("UPDATE jobs SET state='pending',attempts=0,available_at=0,lease_until=0,error='',"
				. "pipeline_hash='',processing_seconds=0,processing_content_hash='',processing_pipeline_hash='',updated_at=?",
				[$now]);
			$this->setMeta('pipeline_revision', (string)($this->pipelineRevision() + 1));
			$this->setMeta('parallel_metrics_pipeline_hash', '');
			$this->setMeta('parallel_active_seconds', '0');
			$this->setMeta('parallel_processed_articles', '0');
			$this->setMeta('backfill_cursor', '99999999999999999999');
			$this->setMeta('backfill_active', '1');
			$this->pdo->commit();
			return $entryIds;
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	public function prepareRebuildJobs(string $pipelineHash): int {
		$statement = $this->pdo->prepare("UPDATE jobs SET pipeline_hash=?,updated_at=? WHERE pipeline_hash='' AND state='pending'");
		$statement->execute([$pipelineHash, time()]);
		return $statement->rowCount();
	}

	public function isPendingRebuildRestore(string $entryId): bool {
		return $this->row('SELECT 1 FROM rebuild_restores WHERE entry_id=?', [$entryId]) !== null;
	}

	public function completeRebuildRestore(string $entryId): void {
		$this->execute('DELETE FROM rebuild_restores WHERE entry_id=?', [$entryId]);
	}

	public static function livePriority(?int $arrivalOrder = null): int {
		return self::LIVE_PRIORITY_BASE + ($arrivalOrder ?? (int)floor(microtime(true) * 1000));
	}

	public function enqueue(FreshRSS_Entry $entry, int $categoryId, string $pipelineHash, int $priority = 100,
		int $grace = 0, bool $archive = false): bool {
		$now = time();
		$existing = $this->row('SELECT content_hash,pipeline_hash,state,is_archive FROM jobs WHERE entry_id=?', [$entry->id()]);
		if ($existing !== null && hash_equals((string)$existing['content_hash'], $entry->hash())
				&& hash_equals((string)$existing['pipeline_hash'], $pipelineHash) && $existing['state'] !== 'failed') {
			if (!$archive && (bool)$existing['is_archive']) {
				$this->execute('UPDATE jobs SET is_archive=0,priority=MAX(priority,?),available_at=MIN(available_at,?),updated_at=? '
					. 'WHERE entry_id=?', [$priority, $now + max(0, min(300, $grace)), $now, $entry->id()]);
			}
			return false;
		}
		$statement = $this->pdo->prepare(<<<'SQL'
			INSERT INTO jobs(entry_id,feed_id,category_id,title,author,link,published_at,content_hash,pipeline_hash,
				rss_text,is_archive,priority,state,attempts,available_at,lease_until,error,created_at,updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'pending',0,?,0,'',?,?)
			ON CONFLICT(entry_id) DO UPDATE SET feed_id=excluded.feed_id,category_id=excluded.category_id,
				title=excluded.title,author=excluded.author,link=excluded.link,published_at=excluded.published_at,
				content_hash=excluded.content_hash,pipeline_hash=excluded.pipeline_hash,rss_text=excluded.rss_text,
				is_archive=excluded.is_archive,
				priority=MAX(jobs.priority,excluded.priority),state='pending',attempts=0,available_at=excluded.available_at,
				lease_until=0,error='',created_at=excluded.created_at,updated_at=excluded.updated_at
			SQL);
		$statement->execute([
			$entry->id(), $entry->feedId(), $categoryId,
			mb_substr(htmlspecialchars_decode($entry->title(), ENT_QUOTES), 0, 1000),
			mb_substr(htmlspecialchars_decode($entry->authors(true), ENT_QUOTES), 0, 1000),
			mb_substr(htmlspecialchars_decode($entry->link(raw: true), ENT_QUOTES), 0, 4096),
			$entry->date(true), $entry->hash(), $pipelineHash, mb_strcut($entry->content(false), 0, 100000, 'UTF-8'),
			$archive ? 1 : 0,
			$priority, $now + max(0, min(300, $grace)), $now, $now,
		]);
		return true;
	}

	/** @return array<string,mixed>|null */
	public function claim(int $leaseSeconds): ?array {
		$now = time();
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			if ($this->isPaused()) {
				$this->pdo->commit();
				return null;
			}
			$this->execute("UPDATE jobs SET state=CASE WHEN attempts+1>=? THEN 'failed' ELSE 'pending' END,"
				. 'attempts=attempts+1,available_at=?,lease_until=0,error=?,updated_at=? '
				. "WHERE state='processing' AND lease_until<?", [self::MAX_ATTEMPTS, $now + 60, 'Worker lease expired.', $now, $now]);
			$row = $this->row("SELECT * FROM jobs WHERE state='pending' AND available_at<=? "
				. 'ORDER BY priority DESC,published_at DESC LIMIT 1', [$now]);
			if ($row !== null) {
				$this->execute("UPDATE jobs SET state='processing',lease_until=?,updated_at=? WHERE entry_id=?",
					[$now + max(60, $leaseSeconds), $now, $row['entry_id']]);
			}
			$this->pdo->commit();
			return $row;
		} catch (Throwable $e) {
			$this->pdo->rollBack();
			throw $e;
		}
	}

	/** @param array<string,mixed> $job */
	public function isCurrentJob(array $job): bool {
		$row = $this->row("SELECT content_hash,pipeline_hash,state FROM jobs WHERE entry_id=?", [$job['entry_id']]);
		return $row !== null && $row['state'] === 'processing'
			&& hash_equals((string)$row['content_hash'], (string)$job['content_hash'])
			&& hash_equals((string)$row['pipeline_hash'], (string)$job['pipeline_hash']);
	}

	/** @param array<string,mixed> $job */
	public function renewLease(array $job, int $leaseSeconds): bool {
		$statement = $this->pdo->prepare("UPDATE jobs SET lease_until=?,updated_at=? WHERE entry_id=? AND state='processing' "
			. 'AND content_hash=? AND pipeline_hash=?');
		$now = time();
		$statement->execute([$now + max(60, $leaseSeconds), $now, $job['entry_id'], $job['content_hash'], $job['pipeline_hash']]);
		return $statement->rowCount() === 1;
	}

	public function classificationPending(string $entryId, string $contentHash): bool {
		$row = $this->row('SELECT content_hash,state FROM jobs WHERE entry_id=?', [$entryId]);
		return $row !== null && hash_equals((string)$row['content_hash'], $contentHash)
			&& in_array((string)$row['state'], ['pending', 'processing'], true);
	}

	/** @param array<string,mixed> $job @param list<float> $embedding */
	public function saveSummaryIfCurrent(array $job, string $analysisHash, string $feedName, string $summary,
		string $sourceText, array $embedding): bool {
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			if (!$this->isCurrentJob($job)) {
				$this->pdo->commit();
				return false;
			}
			$this->execute('INSERT INTO summaries(entry_id,content_hash,analysis_hash,feed_name,summary,source_text,embedding,updated_at) '
				. 'VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(entry_id) DO UPDATE SET content_hash=excluded.content_hash,'
				. 'analysis_hash=excluded.analysis_hash,feed_name=excluded.feed_name,summary=excluded.summary,source_text=excluded.source_text,'
				. 'embedding=excluded.embedding,updated_at=excluded.updated_at', [
					$job['entry_id'], $job['content_hash'], $analysisHash, $feedName, mb_substr($summary, 0, 10000),
					mb_substr($sourceText, 0, 50000), json_encode($embedding, JSON_THROW_ON_ERROR), time(),
				]);
			$this->pdo->commit();
			return true;
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	/** @return array<string,mixed>|null */
	public function summary(string $entryId): ?array {
		return $this->row('SELECT * FROM summaries WHERE entry_id=?', [$entryId]);
	}

	/** @return array{matches:bool,confidence:float,reason:string,event_title:string}|null */
	public function topicDecision(string $entryId, string $contentHash, int $topicId, string $ruleHash,
		string $judgeModel): ?array {
		$row = $this->row('SELECT matches,confidence,reason,event_title FROM topic_decisions '
			. 'WHERE entry_id=? AND content_hash=? AND topic_id=? AND rule_hash=? AND judge_model=?',
			[$entryId, $contentHash, $topicId, $ruleHash, $judgeModel]);
		return $row === null ? null : ['matches' => (bool)$row['matches'], 'confidence' => (float)$row['confidence'],
			'reason' => (string)$row['reason'], 'event_title' => (string)$row['event_title']];
	}

	/** @param array{matches:bool,confidence:float,reason:string,event_title:string} $decision */
	public function saveTopicDecision(string $entryId, string $contentHash, int $topicId, string $ruleHash,
		string $judgeModel, array $decision): void {
		$this->execute('INSERT INTO topic_decisions(entry_id,content_hash,topic_id,rule_hash,judge_model,matches,'
			. 'confidence,reason,event_title,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) '
			. 'ON CONFLICT(entry_id,content_hash,topic_id,rule_hash,judge_model) DO UPDATE SET '
			. 'matches=excluded.matches,confidence=excluded.confidence,reason=excluded.reason,'
			. 'event_title=excluded.event_title,updated_at=excluded.updated_at', [
				$entryId, $contentHash, $topicId, $ruleHash, $judgeModel, $decision['matches'] ? 1 : 0,
				$decision['confidence'], mb_substr($decision['reason'], 0, 4000),
				mb_substr($decision['event_title'], 0, 1000), time(),
			]);
		$this->execute('DELETE FROM topic_decisions WHERE entry_id=? AND topic_id=? '
			. 'AND NOT(content_hash=? AND rule_hash=? AND judge_model=?)',
			[$entryId, $topicId, $contentHash, $ruleHash, $judgeModel]);
	}

	/** @return array{same_event:bool,confidence:float,reason:string}|null */
	public function eventDecision(string $entryId, string $contentHash, int $topicId, string $candidateId,
		string $candidateHash, string $judgeModel): ?array {
		$row = $this->row('SELECT same_event,confidence,reason FROM event_decisions WHERE entry_id=? AND content_hash=? '
			. 'AND topic_id=? AND candidate_id=? AND candidate_hash=? AND judge_model=?',
			[$entryId, $contentHash, $topicId, $candidateId, $candidateHash, $judgeModel]);
		return $row === null ? null : ['same_event' => (bool)$row['same_event'],
			'confidence' => (float)$row['confidence'], 'reason' => (string)$row['reason']];
	}

	/** @param array{same_event:bool,confidence:float,reason:string} $decision */
	public function saveEventDecision(string $entryId, string $contentHash, int $topicId, string $candidateId,
		string $candidateHash, string $judgeModel, array $decision): void {
		$this->execute('INSERT INTO event_decisions(entry_id,content_hash,topic_id,candidate_id,candidate_hash,judge_model,'
			. 'same_event,confidence,reason,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) '
			. 'ON CONFLICT(entry_id,content_hash,topic_id,candidate_id,candidate_hash,judge_model) DO UPDATE SET '
			. 'same_event=excluded.same_event,confidence=excluded.confidence,reason=excluded.reason,updated_at=excluded.updated_at', [
				$entryId, $contentHash, $topicId, $candidateId, $candidateHash, $judgeModel,
				$decision['same_event'] ? 1 : 0, $decision['confidence'], mb_substr($decision['reason'], 0, 4000), time(),
			]);
		$this->execute('DELETE FROM event_decisions WHERE entry_id=? AND topic_id=? AND candidate_id=? '
			. 'AND NOT(content_hash=? AND candidate_hash=? AND judge_model=?)',
			[$entryId, $topicId, $candidateId, $contentHash, $candidateHash, $judgeModel]);
	}

	/** @return list<array<string,mixed>> */
	public function eventCandidates(int $topicId, int $since): array {
		$statement = $this->pdo->prepare('SELECT * FROM events WHERE topic_id=? AND occurred_at>=? ORDER BY occurred_at DESC LIMIT 50');
		$statement->execute([$topicId, $since]);
		return $statement->fetchAll();
	}

	/** @return list<int> */
	public function detachChangedSources(string $entryId, string $contentHash): array {
		$statement = $this->pdo->prepare('SELECT topic_id,event_id FROM sources WHERE entry_id=? AND content_hash<>?');
		$statement->execute([$entryId, $contentHash]);
		$rows = $statement->fetchAll();
		if ($rows === []) {
			return [];
		}
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			$this->execute('DELETE FROM sources WHERE entry_id=? AND content_hash<>?', [$entryId, $contentHash]);
			foreach ($rows as $row) {
				$this->removeEmptyEvent((int)$row['event_id']);
			}
			$this->pdo->commit();
		} catch (Throwable $e) {
			$this->pdo->rollBack();
			throw $e;
		}
		return array_values(array_unique(array_map(static fn(array $row): int => (int)$row['topic_id'], $rows)));
	}

	/** @return list<int> */
	public function changedSourceTopicIds(string $entryId, string $contentHash): array {
		$statement = $this->pdo->prepare('SELECT DISTINCT topic_id FROM sources WHERE entry_id=? AND content_hash<>? ORDER BY topic_id');
		$statement->execute([$entryId, $contentHash]);
		return array_map('intval', $statement->fetchAll(PDO::FETCH_COLUMN));
	}

	public function detachChangedSourceForTopic(int $topicId, string $entryId, string $contentHash): bool {
		$source = $this->row('SELECT event_id FROM sources WHERE topic_id=? AND entry_id=? AND content_hash<>?',
			[$topicId, $entryId, $contentHash]);
		if ($source === null) {
			return false;
		}
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			$this->execute('DELETE FROM sources WHERE topic_id=? AND entry_id=? AND content_hash<>?',
				[$topicId, $entryId, $contentHash]);
			$this->removeEmptyEvent((int)$source['event_id']);
			$this->pdo->commit();
			return true;
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	public function isRejected(int $topicId, string $entryId, string $fingerprint = ''): bool {
		return $this->row('SELECT 1 FROM rejections WHERE topic_id=? AND (entry_id=? OR (fingerprint<>? AND fingerprint=?)) LIMIT 1',
			[$topicId, $entryId, '', $fingerprint]) !== null;
	}

	/** @return list<array<string,mixed>> */
	public function rejectedEventCandidates(int $topicId): array {
		$statement = $this->pdo->prepare("SELECT fingerprint,title,explanation,embedding,occurred_at FROM rejections "
			. "WHERE topic_id=? AND fingerprint<>'' ORDER BY created_at DESC LIMIT 50");
		$statement->execute([$topicId]);
		return $statement->fetchAll();
	}

	/** @return list<int> */
	public function topicIdsForSource(string $entryId): array {
		$statement = $this->pdo->prepare('SELECT topic_id FROM sources WHERE entry_id=? ORDER BY topic_id');
		$statement->execute([$entryId]);
		return array_map('intval', $statement->fetchAll(PDO::FETCH_COLUMN));
	}

	/** @return array<string,mixed>|null */
	public function source(int $topicId, string $entryId): ?array {
		return $this->row('SELECT * FROM sources WHERE topic_id=? AND entry_id=?', [$topicId, $entryId]);
	}

	public function removeSourceMembership(int $topicId, string $entryId): bool {
		$source = $this->row('SELECT event_id FROM sources WHERE topic_id=? AND entry_id=?', [$topicId, $entryId]);
		if ($source === null) {
			return false;
		}
		$this->execute('DELETE FROM sources WHERE topic_id=? AND entry_id=?', [$topicId, $entryId]);
		$this->removeEmptyEvent((int)$source['event_id']);
		return true;
	}

	/** @param array<string,mixed> $job @param list<float> $embedding @return array{event_id:int,new_event:bool}|null */
	public function addMatch(int $topicId, array $job, string $feedName, string $eventTitle, int $occurredAt,
		string $explanation, array $embedding, ?int $eventId): ?array {
		if ($this->topic($topicId) === null) {
			throw new InvalidArgumentException('Unknown topic.');
		}
		$existing = $this->row('SELECT event_id FROM sources WHERE topic_id=? AND entry_id=?', [$topicId, $job['entry_id']]);
		if ($existing !== null) {
			return ['event_id' => (int)$existing['event_id'], 'new_event' => false];
		}
		$now = time();
		$fingerprint = hash('sha256', mb_strtolower(trim($eventTitle), 'UTF-8'));
		if ($this->isRejected($topicId, (string)$job['entry_id'], $fingerprint)) {
			throw new DomainException('This source or event was restored previously.');
		}
		if ($eventId !== null && $this->row('SELECT 1 FROM events WHERE id=? AND topic_id=?', [$eventId, $topicId]) === null) {
			throw new InvalidArgumentException('The selected digest event does not belong to this topic.');
		}
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			$currentJob = $this->row('SELECT state,content_hash,pipeline_hash FROM jobs WHERE entry_id=?', [$job['entry_id']]);
			if ($currentJob !== null && ($currentJob['state'] !== 'processing'
					|| !hash_equals((string)$currentJob['content_hash'], (string)$job['content_hash'])
					|| !hash_equals((string)$currentJob['pipeline_hash'], (string)$job['pipeline_hash']))) {
				$this->pdo->commit();
				return null;
			}
			$newEvent = $eventId === null;
			if ($eventId === null) {
				$this->execute('INSERT INTO events(topic_id,title,occurred_at,explanation,embedding,fingerprint,created_at,updated_at) '
					. 'VALUES(?,?,?,?,?,?,?,?)', [$topicId, mb_substr($eventTitle, 0, 1000), $occurredAt,
						mb_substr($explanation, 0, 4000), json_encode($embedding, JSON_THROW_ON_ERROR), $fingerprint, $now, $now]);
				$eventId = (int)$this->pdo->lastInsertId();
			} else {
				$this->execute("UPDATE events SET occurred_at=MAX(occurred_at,?),"
					. "embedding=CASE WHEN embedding='[]' THEN ? ELSE embedding END,updated_at=? WHERE id=? AND topic_id=?",
					[$occurredAt, json_encode($embedding, JSON_THROW_ON_ERROR), $now, $eventId, $topicId]);
			}
			$this->execute('INSERT INTO sources(topic_id,event_id,entry_id,feed_name,title,link,published_at,content_hash,'
				. 'explanation,added_at) VALUES(?,?,?,?,?,?,?,?,?,?)', [$topicId, $eventId, $job['entry_id'], $feedName,
					$job['title'], $job['link'], $job['published_at'], $job['content_hash'], mb_substr($explanation, 0, 4000), $now]);
			$this->pdo->commit();
			return ['event_id' => $eventId, 'new_event' => $newEvent];
		} catch (Throwable $e) {
			$this->pdo->rollBack();
			throw $e;
		}
	}

	/** @return list<array<string,mixed>> */
	public function events(int $topicId): array {
		$statement = $this->pdo->prepare('SELECT e.*,COALESCE(MAX(s.published_at),e.occurred_at) AS latest_addition_at,'
			. 'COALESCE(MAX(s.rowid),0) AS latest_source_order FROM events e LEFT JOIN sources s ON s.event_id=e.id '
			. 'WHERE e.topic_id=? GROUP BY e.id ORDER BY e.occurred_at DESC,latest_source_order DESC,e.id DESC');
		$statement->execute([$topicId]);
		$events = $statement->fetchAll();
		$source = $this->pdo->prepare('SELECT * FROM sources WHERE event_id=? ORDER BY published_at DESC,entry_id');
		foreach ($events as &$event) {
			$source->execute([$event['id']]);
			$event['sources'] = $source->fetchAll();
		}
		return $events;
	}

	/** @return list<string> */
	public function restoreSource(int $topicId, string $entryId): array {
		$source = $this->row('SELECT * FROM sources WHERE topic_id=? AND entry_id=?', [$topicId, $entryId]);
		if ($source === null) {
			return [];
		}
		$this->execute('INSERT OR IGNORE INTO rejections(topic_id,entry_id,fingerprint,created_at) VALUES(?,?,?,?)',
			[$topicId, $entryId, '', time()]);
		$this->execute('DELETE FROM sources WHERE topic_id=? AND entry_id=?', [$topicId, $entryId]);
		$this->removeEmptyEvent((int)$source['event_id']);
		$this->createSuggestion($topicId, 'Exclude items like: ' . (string)$source['title']);
		return [$entryId];
	}

	/** @return list<string> */
	public function restoreEvent(int $topicId, int $eventId): array {
		$event = $this->row('SELECT * FROM events WHERE id=? AND topic_id=?', [$eventId, $topicId]);
		if ($event === null) {
			return [];
		}
		$statement = $this->pdo->prepare('SELECT entry_id FROM sources WHERE event_id=?');
		$statement->execute([$eventId]);
		$ids = array_map('strval', $statement->fetchAll(PDO::FETCH_COLUMN));
		$this->execute('INSERT OR IGNORE INTO rejections(topic_id,entry_id,fingerprint,title,explanation,embedding,occurred_at,created_at) '
			. 'VALUES(?,?,?,?,?,?,?,?)', [$topicId, '', $event['fingerprint'], $event['title'], $event['explanation'],
				$event['embedding'], $event['occurred_at'], time()]);
		$this->execute('DELETE FROM sources WHERE event_id=?', [$eventId]);
		$this->execute('DELETE FROM events WHERE id=?', [$eventId]);
		$this->createSuggestion($topicId, 'Exclude events like: ' . (string)$event['title']);
		return $ids;
	}

	private function removeEmptyEvent(int $eventId): void {
		if ($this->row('SELECT 1 FROM sources WHERE event_id=? LIMIT 1', [$eventId]) === null) {
			$this->execute('DELETE FROM events WHERE id=?', [$eventId]);
		}
	}

	private function createSuggestion(int $topicId, string $text): void {
		$this->execute("INSERT INTO suggestions(topic_id,text,state,created_at) VALUES(?,?,'pending',?)",
			[$topicId, mb_substr($text, 0, 1000), time()]);
	}

	/** @return list<array<string,mixed>> */
	public function suggestions(int $topicId): array {
		$statement = $this->pdo->prepare("SELECT * FROM suggestions WHERE topic_id=? AND state='pending' ORDER BY id");
		$statement->execute([$topicId]);
		return $statement->fetchAll();
	}

	public function resolveSuggestion(int $topicId, int $id, bool $approve): void {
		$suggestion = $this->row("SELECT * FROM suggestions WHERE id=? AND topic_id=? AND state='pending'", [$id, $topicId]);
		if ($suggestion === null) {
			throw new InvalidArgumentException('Unknown exclusion suggestion.');
		}
		if ($approve) {
			$topic = $this->topic($topicId);
			if ($topic === null) {
				throw new InvalidArgumentException('Unknown topic.');
			}
			$topic['exclusions'][] = (string)$suggestion['text'];
			$this->saveTopic($topic, $topicId);
		}
		$this->execute("UPDATE suggestions SET state=? WHERE id=?", [$approve ? 'approved' : 'dismissed', $id]);
	}

	public function recordManualUnread(string $entryId): void {
		$this->execute("INSERT INTO overrides(entry_id,kind,created_at) VALUES(?,'manual_unread',?) "
			. "ON CONFLICT(entry_id) DO UPDATE SET kind='manual_unread',created_at=excluded.created_at", [$entryId, time()]);
	}

	public function clearManualUnread(string $entryId): void {
		$this->execute("DELETE FROM overrides WHERE entry_id=? AND kind='manual_unread'", [$entryId]);
	}

	public function recordRebuildUnread(string $entryId): void {
		$this->execute("INSERT INTO overrides(entry_id,kind,created_at) VALUES(?,'rebuild_unread',?) "
			. "ON CONFLICT(entry_id) DO UPDATE SET kind='rebuild_unread',created_at=excluded.created_at", [$entryId, time()]);
	}

	public function clearRebuildUnread(string $entryId): void {
		$this->execute("DELETE FROM overrides WHERE entry_id=? AND kind='rebuild_unread'", [$entryId]);
	}

	public function isRebuildUnread(string $entryId): bool {
		return $this->row("SELECT 1 FROM overrides WHERE entry_id=? AND kind='rebuild_unread'", [$entryId]) !== null;
	}

	public function isProtected(string $entryId): bool {
		return $this->row("SELECT 1 FROM overrides WHERE entry_id=? AND kind='manual_unread'", [$entryId]) !== null;
	}

	/** @param array<string,mixed> $job */
	public function completeCurrent(array $job, string $state = 'done', string $error = ''): bool {
		$statement = $this->pdo->prepare("UPDATE jobs SET state=?,lease_until=0,error=?,updated_at=? WHERE entry_id=? "
			. "AND state='processing' AND content_hash=? AND pipeline_hash=?");
		$statement->execute([$state, mb_substr($error, 0, 4000), time(), $job['entry_id'],
			$job['content_hash'], $job['pipeline_hash']]);
		return $statement->rowCount() === 1;
	}

	/** @param array<string,mixed> $job */
	public function failCurrent(array $job, string $error): bool {
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			$current = $this->row("SELECT attempts FROM jobs WHERE entry_id=? AND state='processing' "
				. 'AND content_hash=? AND pipeline_hash=?', [$job['entry_id'], $job['content_hash'], $job['pipeline_hash']]);
			if ($current === null) {
				$this->pdo->commit();
				return false;
			}
			$attempts = (int)$current['attempts'] + 1;
			$this->execute("UPDATE jobs SET state=?,attempts=?,available_at=?,lease_until=0,error=?,updated_at=? WHERE entry_id=?",
				[$attempts >= self::MAX_ATTEMPTS ? 'failed' : 'pending', $attempts,
					$attempts >= self::MAX_ATTEMPTS ? 0 : time() + min(900, 30 * (2 ** $attempts)),
					mb_substr($error, 0, 4000), time(), $job['entry_id']]);
			$this->pdo->commit();
			return true;
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	/** @param array<string,mixed> $job */
	public function releaseCurrent(array $job, int $delaySeconds, string $reason): bool {
		$statement = $this->pdo->prepare("UPDATE jobs SET state='pending',available_at=?,lease_until=0,error=?,updated_at=? "
			. "WHERE entry_id=? AND state='processing' AND content_hash=? AND pipeline_hash=?");
		$statement->execute([time() + max(1, min(21600, $delaySeconds)), mb_substr($reason, 0, 4000), time(),
			$job['entry_id'], $job['content_hash'], $job['pipeline_hash']]);
		return $statement->rowCount() === 1;
	}

	/** @param array<string,mixed> $job */
	public function deferCurrent(array $job, int $delaySeconds = 1): bool {
		$statement = $this->pdo->prepare("UPDATE jobs SET state='pending',available_at=?,lease_until=0,error='',updated_at=? "
			. "WHERE entry_id=? AND state='processing' AND content_hash=? AND pipeline_hash=?");
		$now = time();
		$statement->execute([$now + max(1, min(60, $delaySeconds)), $now,
			$job['entry_id'], $job['content_hash'], $job['pipeline_hash']]);
		return $statement->rowCount() === 1;
	}

	/** @return array{target:int,successes:int,cooldown_until:int,backoff_level:int,last_status:int} */
	public function cloudConcurrencyState(): array {
		return [
			'target' => max(1, min(16, (int)($this->getMeta('cloud_target_concurrency') ?? 2))),
			'successes' => max(0, (int)($this->getMeta('cloud_successes') ?? 0)),
			'cooldown_until' => max(0, (int)($this->getMeta('cloud_cooldown_until') ?? 0)),
			'backoff_level' => max(0, min(10, (int)($this->getMeta('cloud_backoff_level') ?? 0))),
			'last_status' => max(0, (int)($this->getMeta('cloud_last_status') ?? 0)),
		];
	}

	/** @param array{target:int,successes:int,cooldown_until:int,backoff_level:int,last_status:int} $state */
	public function saveCloudConcurrencyState(array $state): void {
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			$this->setMeta('cloud_target_concurrency', (string)max(1, min(16, $state['target'])));
			$this->setMeta('cloud_successes', (string)max(0, $state['successes']));
			$this->setMeta('cloud_cooldown_until', (string)max(0, $state['cooldown_until']));
			$this->setMeta('cloud_backoff_level', (string)max(0, min(10, $state['backoff_level'])));
			$this->setMeta('cloud_last_status', (string)max(0, $state['last_status']));
			$this->pdo->commit();
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	public function retryFailed(): int {
		$statement = $this->pdo->prepare("UPDATE jobs SET state='pending',attempts=0,available_at=0,error='',updated_at=? WHERE state='failed'");
		$statement->execute([time()]);
		return $statement->rowCount();
	}

	public function clearErrors(): int {
		$statement = $this->pdo->prepare("UPDATE jobs SET error='' WHERE error<>''");
		$statement->execute();
		return $statement->rowCount();
	}

	public function startBackfill(): void {
		$this->setMeta('pipeline_revision', (string)($this->pipelineRevision() + 1));
		$this->setMeta('parallel_metrics_pipeline_hash', '');
		$this->setMeta('parallel_active_seconds', '0');
		$this->setMeta('parallel_processed_articles', '0');
		$this->setMeta('backfill_cursor', '99999999999999999999');
		$this->setMeta('backfill_active', '1');
	}

	public function ensureClassifierRevision(string $revision): bool {
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			if ($this->getMeta('classifier_revision') === $revision) {
				$this->pdo->commit();
				return false;
			}
			$this->setMeta('classifier_revision', $revision);
			if ((int)$this->pdo->query('SELECT COUNT(*) FROM topics')->fetchColumn() > 0) {
				$this->startBackfill();
			}
			$this->pdo->commit();
			return true;
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	public function pipelineRevision(): int {
		return (int)($this->getMeta('pipeline_revision') ?? 0);
	}

	/** @return array{active:bool,cursor:string} */
	public function backfill(): array {
		return ['active' => $this->getMeta('backfill_active') === '1',
			'cursor' => $this->getMeta('backfill_cursor') ?? '99999999999999999999'];
	}

	public function advanceBackfill(string $cursor, bool $active): void {
		$this->setMeta('backfill_cursor', $cursor);
		$this->setMeta('backfill_active', $active ? '1' : '0');
	}

	public function isPaused(): bool {
		return $this->getMeta('processing_paused') === '1';
	}

	public function setPaused(bool $paused): void {
		$this->setMeta('processing_paused', $paused ? '1' : '0');
	}

	public function workerRestartRevision(): int {
		return (int)($this->getMeta('worker_restart_revision') ?? 0);
	}

	public function requestWorkerRestart(): int {
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			$revision = $this->workerRestartRevision() + 1;
			$this->setMeta('worker_restart_revision', (string)$revision);
			$this->pdo->commit();
			return $revision;
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	public function recordProcessingActivity(string $entryId, float $seconds): bool {
		$seconds = max(0.000000001, $seconds);
		$now = time();
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			$statement = $this->pdo->prepare("UPDATE jobs SET processing_seconds=?,processing_content_hash=content_hash,"
				. "processing_pipeline_hash=pipeline_hash WHERE entry_id=? AND state IN ('done','skipped')");
			$statement->execute([$seconds, $entryId]);
			$recorded = $statement->rowCount() === 1;
			if ($recorded) {
				$this->execute("INSERT INTO processing_samples(kind,pipeline_hash,articles,active_seconds,created_at) "
					. "SELECT 'serial',pipeline_hash,1,?,? FROM jobs WHERE entry_id=?", [$seconds, $now, $entryId]);
				$this->execute('DELETE FROM processing_samples WHERE created_at<?', [$now - 7200]);
			}
			$this->pdo->commit();
			return $recorded;
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	public function recordParallelActivity(string $pipelineHash, int $articles, float $seconds): void {
		if ($articles < 1 || $seconds <= 0.0) {
			return;
		}
		$this->pdo->exec('BEGIN IMMEDIATE');
		try {
			if (!hash_equals($this->getMeta('parallel_metrics_pipeline_hash') ?? '', $pipelineHash)) {
				$this->setMeta('parallel_metrics_pipeline_hash', $pipelineHash);
				$this->setMeta('parallel_active_seconds', '0');
				$this->setMeta('parallel_processed_articles', '0');
			}
			$this->setMeta('parallel_active_seconds', (string)((float)($this->getMeta('parallel_active_seconds') ?? 0) + $seconds));
			$this->setMeta('parallel_processed_articles',
				(string)((int)($this->getMeta('parallel_processed_articles') ?? 0) + $articles));
			$now = time();
			$this->execute('INSERT INTO processing_samples(kind,pipeline_hash,articles,active_seconds,created_at) '
				. "VALUES('parallel',?,?,?,?)", [$pipelineHash, $articles, $seconds, $now]);
			$this->execute('DELETE FROM processing_samples WHERE created_at<?', [$now - 7200]);
			$this->pdo->commit();
		} catch (Throwable $e) {
			if ($this->pdo->inTransaction()) {
				$this->pdo->rollBack();
			}
			throw $e;
		}
	}

	/** @return array<string,int|float|string> */
	public function status(): array {
		$counts = ['pending' => 0, 'processing' => 0, 'done' => 0, 'skipped' => 0, 'failed' => 0];
		$statement = $this->pdo->query('SELECT state,COUNT(*) AS count FROM jobs GROUP BY state');
		foreach ($statement === false ? [] : $statement->fetchAll() as $row) {
			$counts[(string)$row['state']] = (int)$row['count'];
		}
		$result = $counts;
		$result['queued'] = $counts['pending'] + $counts['processing'];
		$ready = $this->pdo->prepare("SELECT COUNT(*) FROM jobs WHERE state='pending' AND available_at<=?");
		$ready->execute([time()]);
		$result['ready'] = (int)$ready->fetchColumn();
		$result['processed'] = $counts['done'] + $counts['skipped'];
		$result['topics'] = (int)$this->pdo->query('SELECT COUNT(*) FROM topics')->fetchColumn();
		$result['events'] = (int)$this->pdo->query('SELECT COUNT(*) FROM events')->fetchColumn();
		$result['sources'] = (int)$this->pdo->query('SELECT COUNT(*) FROM sources')->fetchColumn();
		$metric = $this->row("SELECT COUNT(*) AS articles,COALESCE(SUM(processing_seconds),0) AS seconds FROM jobs "
			. "WHERE state IN ('done','skipped') AND processing_seconds>0 "
			. 'AND processing_content_hash=content_hash AND processing_pipeline_hash=pipeline_hash');
		$result['active_processing_seconds'] = (float)($metric['seconds'] ?? 0.0);
		$result['active_processed_articles'] = (int)($metric['articles'] ?? 0);
		$parallelArticles = (int)($this->getMeta('parallel_processed_articles') ?? 0);
		$parallelSeconds = (float)($this->getMeta('parallel_active_seconds') ?? 0);
		if ($parallelArticles > 0 && $parallelSeconds > 0.0) {
			$result['active_processing_seconds'] = $parallelSeconds;
			$result['active_processed_articles'] = $parallelArticles;
		}
		$result['average_ready'] = $result['active_processing_seconds'] > 0.0
			&& $result['active_processed_articles'] > 0 ? 1 : 0;
		$result['average_per_hour'] = $result['average_ready'] !== 0
			? round(($result['active_processed_articles'] * 3600) / $result['active_processing_seconds'], 1) : 0.0;
		$recentSql = 'SELECT COALESCE(SUM(articles),0) AS articles,COALESCE(SUM(active_seconds),0) AS seconds '
			. 'FROM processing_samples WHERE created_at>=?';
		$recentValues = [time() - 3600];
		if ($parallelArticles > 0 && $parallelSeconds > 0.0) {
			$recentSql .= " AND kind='parallel' AND pipeline_hash=?";
			$recentValues[] = $this->getMeta('parallel_metrics_pipeline_hash') ?? '';
		} else {
			$recentSql .= " AND kind='serial' AND EXISTS (SELECT 1 FROM jobs j "
				. 'WHERE j.pipeline_hash=processing_samples.pipeline_hash '
				. 'AND j.processing_pipeline_hash=j.pipeline_hash)';
		}
		$recentMetric = $this->row($recentSql, $recentValues);
		$result['last_hour_active_seconds'] = (float)($recentMetric['seconds'] ?? 0.0);
		$result['last_hour_processed_articles'] = (int)($recentMetric['articles'] ?? 0);
		$result['last_hour_average_ready'] = $result['last_hour_active_seconds'] > 0.0
			&& $result['last_hour_processed_articles'] > 0 ? 1 : 0;
		$result['last_hour_average_per_hour'] = $result['last_hour_average_ready'] !== 0
			? round(($result['last_hour_processed_articles'] * 3600) / $result['last_hour_active_seconds'], 1) : 0.0;
		$result['backfill_active'] = $this->backfill()['active'] ? 1 : 0;
		$result['paused'] = $this->isPaused() ? 1 : 0;
		return $result;
	}

	/** @return list<array<string,mixed>> */
	public function recentErrors(): array {
		$statement = $this->pdo->query('SELECT entry_id,feed_id,title,state,error,attempts,available_at,updated_at '
			. "FROM jobs WHERE error<>'' ORDER BY updated_at DESC LIMIT 20");
		return $statement === false ? [] : $statement->fetchAll();
	}

	/** @param array<mixed> $values @return list<string> */
	private function stringList(array $values): array {
		$values = array_map(static fn($value): string => mb_substr(trim((string)$value), 0, 1000, 'UTF-8'),
			array_slice($values, 0, 100));
		return array_values(array_unique(array_filter($values)));
	}

	/** @param array<mixed> $values @return list<int> */
	private function idList(array $values): array {
		return array_values(array_unique(array_filter(array_map('intval', $values), static fn(int $id): bool => $id > 0)));
	}

	/** @param list<mixed> $values @return array<string,mixed>|null */
	private function row(string $sql, array $values = []): ?array {
		$statement = $this->pdo->prepare($sql);
		$statement->execute($values);
		$row = $statement->fetch();
		return is_array($row) ? $row : null;
	}

	/** @param list<mixed> $values */
	private function execute(string $sql, array $values = []): void {
		$statement = $this->pdo->prepare($sql);
		$statement->execute($values);
	}

	private function getMeta(string $key): ?string {
		$row = $this->row('SELECT value FROM meta WHERE key=?', [$key]);
		return $row === null ? null : (string)$row['value'];
	}

	private function setMeta(string $key, string $value): void {
		$this->execute('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', [$key, $value]);
	}

	private function hasColumn(string $table, string $column): bool {
		$statement = $this->pdo->query('PRAGMA table_info(' . $table . ')');
		foreach ($statement === false ? [] : $statement->fetchAll() as $row) {
			if (($row['name'] ?? null) === $column) {
				return true;
			}
		}
		return false;
	}
}
