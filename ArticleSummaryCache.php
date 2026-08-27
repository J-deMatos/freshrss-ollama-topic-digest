<?php
declare(strict_types=1);

if (!class_exists('FreshRSS_ArticleSummaryCache', false)) {
	final class FreshRSS_ArticleSummaryCache {
		private PDO $pdo;

		public function __construct(string $path) {
			$directory = dirname($path);
			if (!is_dir($directory) && !mkdir($directory, 0700, true) && !is_dir($directory)) {
				throw new RuntimeException('Cannot create the shared article-summary directory.');
			}
			$this->pdo = new PDO('sqlite:' . $path, null, null, [
				PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
				PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
			]);
			$this->pdo->exec('PRAGMA busy_timeout = 5000');
			$this->pdo->exec('PRAGMA journal_mode = WAL');
			$this->pdo->exec('PRAGMA synchronous = NORMAL');
			$this->pdo->exec(<<<'SQL'
				CREATE TABLE IF NOT EXISTS summaries (
					entry_id TEXT NOT NULL,
					content_hash TEXT NOT NULL,
					summary_model TEXT NOT NULL,
					embedding_model TEXT NOT NULL,
					summary_text TEXT NOT NULL,
					source_text TEXT NOT NULL,
					embedding TEXT NOT NULL,
					event_title TEXT NOT NULL DEFAULT '',
					event_date TEXT NOT NULL DEFAULT '',
					origin TEXT NOT NULL,
					updated_at INTEGER NOT NULL,
					PRIMARY KEY(entry_id,content_hash,summary_model,embedding_model)
				);
				CREATE INDEX IF NOT EXISTS summaries_updated_idx ON summaries(updated_at);
				SQL);
		}

		/**
		 * @return array{summary_text:string,source_text:string,embedding:list<float>,
		 *     event_title:string,event_date:string,origin:string}|null
		 */
		public function find(string $entryId, string $contentHash, string $summaryModel, string $embeddingModel): ?array {
			$statement = $this->pdo->prepare('SELECT summary_text,source_text,embedding,event_title,event_date,origin '
				. 'FROM summaries WHERE entry_id=? AND content_hash=? AND summary_model=? AND embedding_model=?');
			$statement->execute([$entryId, $contentHash, $summaryModel, $embeddingModel]);
			$row = $statement->fetch();
			if (!is_array($row) || trim((string)$row['summary_text']) === '') {
				return null;
			}
			try {
				$values = json_decode((string)$row['embedding'], true, flags: JSON_THROW_ON_ERROR);
			} catch (JsonException) {
				return null;
			}
			if (!is_array($values) || $values === [] || count($values) > 8192) {
				return null;
			}
			$embedding = [];
			foreach ($values as $value) {
				if ((!is_int($value) && !is_float($value)) || !is_finite((float)$value)) {
					return null;
				}
				$embedding[] = (float)$value;
			}
			return [
				'summary_text' => (string)$row['summary_text'],
				'source_text' => (string)$row['source_text'],
				'embedding' => $embedding,
				'event_title' => (string)$row['event_title'],
				'event_date' => (string)$row['event_date'],
				'origin' => (string)$row['origin'],
			];
		}

		/** @param list<float> $embedding */
		public function save(string $entryId, string $contentHash, string $summaryModel, string $embeddingModel,
			string $summaryText, string $sourceText, array $embedding, string $eventTitle,
			string $eventDate, string $origin): void {
			if ($entryId === '' || $contentHash === '' || trim($summaryText) === ''
					|| $embedding === [] || count($embedding) > 8192) {
				throw new InvalidArgumentException('Shared article summary is incomplete.');
			}
			foreach ($embedding as $value) {
				if ((!is_int($value) && !is_float($value)) || !is_finite((float)$value)) {
					throw new InvalidArgumentException('Shared article embedding is invalid.');
				}
			}
			$statement = $this->pdo->prepare(<<<'SQL'
				INSERT INTO summaries(entry_id,content_hash,summary_model,embedding_model,summary_text,source_text,
					embedding,event_title,event_date,origin,updated_at)
				VALUES(?,?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(entry_id,content_hash,summary_model,embedding_model) DO UPDATE SET
					summary_text=excluded.summary_text,source_text=excluded.source_text,embedding=excluded.embedding,
					event_title=excluded.event_title,event_date=excluded.event_date,origin=excluded.origin,
					updated_at=excluded.updated_at
				SQL);
			$statement->execute([
				$entryId, $contentHash, mb_substr($summaryModel, 0, 200), mb_substr($embeddingModel, 0, 200),
				mb_substr($summaryText, 0, 10000), mb_substr($sourceText, 0, 50000),
				json_encode($embedding, JSON_THROW_ON_ERROR), mb_substr($eventTitle, 0, 1000),
				mb_substr($eventDate, 0, 100), mb_substr($origin, 0, 100), time(),
			]);
			$obsolete = $this->pdo->prepare('DELETE FROM summaries WHERE entry_id=? AND summary_model=? '
				. 'AND embedding_model=? AND content_hash<>?');
			$obsolete->execute([$entryId, $summaryModel, $embeddingModel, $contentHash]);
		}
	}
}
