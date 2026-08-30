<?php
declare(strict_types=1);

/** @phpstan-import-type TopicDigestConfig from TopicDigestExtension */
final class TopicDigestCoordinator {
	/** @var TopicDigestConfig */
	private array $config;

	public function __construct(private readonly TopicDigestExtension $extension, private readonly string $username) {
		$this->config = $extension->configuration();
	}

	public function parallelCloudEnabled(): bool {
		return self::isCloudPair((string)$this->config['summary_model'], (string)$this->config['judge_model'])
			&& PHP_OS_FAMILY !== 'Windows'
			&& function_exists('proc_open');
	}

	public static function isCloudPair(string $summaryModel, string $judgeModel): bool {
		return str_ends_with($summaryModel, ':cloud') && str_ends_with($judgeModel, ':cloud');
	}

	/** @return array{processed:int,failed:int,backfill_scanned:int,throttle_status:int,retry_after:int} */
	public function run(int $limit): array {
		if (!$this->parallelCloudEnabled()) {
			return (new TopicDigestProcessor($this->extension))->run($limit);
		}
		return $this->runParallel($limit);
	}

	/** @return array{processed:int,failed:int,backfill_scanned:int,throttle_status:int,retry_after:int} */
	private function runParallel(int $limit): array {
		$store = $this->extension->store();
		$pipelineHash = $this->extension->pipelineHash();
		$maximum = $this->maximumConcurrency();
		$controller = new TopicDigestCloudConcurrency($store->cloudConcurrencyState(), $maximum);
		$now = time();
		if ($store->isPaused() || $controller->target($now) === 0) {
			return ['processed' => 0, 'failed' => 0, 'backfill_scanned' => 0,
				'throttle_status' => 0, 'retry_after' => max(0, $controller->cooldownUntil() - $now)];
		}

		$processed = 0;
		$failed = 0;
		$scanned = $this->refillQueue(max(4, $controller->target($now) * 4));
		$launched = 0;
		$throttleStatus = 0;
		$retryAfter = -1;
		$activeStarted = null;
		$lastSuccessfulAt = null;
		/** @var array<int,array{process:resource,pipes:array<int,resource>,stdout:string,stderr:string,concurrency:int}> $active */
		$active = [];

		try {
			while ($launched < $limit || $active !== []) {
				$target = $controller->target(time());
				while (!$store->isPaused() && $target > 0 && count($active) < $target && $launched < $limit) {
					$status = $store->status();
					if ((int)$status['ready'] === 0) {
						$scanned += $this->refillQueue(max(4, $target * 4));
						$status = $store->status();
						if ((int)$status['ready'] === 0) {
							break;
						}
					}
					$worker = $this->launchWorker($target);
					$active[(int)$worker['process']] = $worker;
					$activeStarted ??= hrtime(true);
					$launched++;
				}

				if ($active === []) {
					break;
				}
				$completedAny = false;
				foreach ($active as $key => &$worker) {
					$worker['stdout'] .= stream_get_contents($worker['pipes'][1]) ?: '';
					$worker['stderr'] .= stream_get_contents($worker['pipes'][2]) ?: '';
					$status = proc_get_status($worker['process']);
					if ($status === false) {
						throw new RuntimeException('Cannot inspect a Topic Digest cloud worker.');
					}
					if ($status['running']) {
						continue;
					}
					$worker['stdout'] .= stream_get_contents($worker['pipes'][1]) ?: '';
					$worker['stderr'] .= stream_get_contents($worker['pipes'][2]) ?: '';
					fclose($worker['pipes'][1]);
					fclose($worker['pipes'][2]);
					proc_close($worker['process']);
					$stdout = $worker['stdout'];
					$stderr = $worker['stderr'];
					if (trim($stderr) !== '') {
						fwrite(STDERR, rtrim($stderr) . "\n");
					}
					$concurrency = $worker['concurrency'];
					unset($active[$key]);
					$result = $this->decodeWorkerResult($stdout, $stderr);
					$processed += $result['processed'];
					$failed += $result['failed'];
					if ($result['throttle_status'] !== 0) {
						$throttleStatus = $result['throttle_status'];
						$retryAfter = $controller->throttle($throttleStatus,
							$result['retry_after'] >= 0 ? $result['retry_after'] : null, time(),
							requestConcurrency: $concurrency);
					} elseif ($result['processed'] > 0) {
						$controller->success();
						$lastSuccessfulAt = hrtime(true);
					}
					$completedAny = true;
				}
				unset($worker);
				if ($completedAny) {
					$store->saveCloudConcurrencyState($controller->state());
				} else {
					usleep(20000);
				}
			}
		} finally {
			foreach ($active as $worker) {
				proc_terminate($worker['process']);
				foreach ($worker['pipes'] as $pipe) {
					if (is_resource($pipe)) {
						fclose($pipe);
					}
				}
				proc_close($worker['process']);
			}
		}

		if ($activeStarted !== null && $lastSuccessfulAt !== null && $processed > 0
				&& hash_equals($pipelineHash, $this->extension->pipelineHash())) {
			$store->recordParallelActivity($pipelineHash, $processed,
				max(0.000000001, ($lastSuccessfulAt - $activeStarted) / 1_000_000_000));
		}
		return ['processed' => $processed, 'failed' => $failed, 'backfill_scanned' => $scanned,
			'throttle_status' => $throttleStatus, 'retry_after' => $retryAfter];
	}

	private function maximumConcurrency(): int {
		$value = (string)$this->config['cloud_concurrency'];
		return $value === 'auto' ? TopicDigestCloudConcurrency::MAXIMUM : max(1, min(16, (int)$value));
	}

	private function refillQueue(int $target): int {
		$store = $this->extension->store();
		$status = $store->status();
		if ($store->isPaused() || !$store->backfill()['active'] || (int)$status['queued'] >= $target) {
			return 0;
		}
		return $this->extension->enqueueBackfillPage(min(100, max(1, $target - (int)$status['queued'])));
	}

	/** @return array{process:resource,pipes:array<int,resource>,stdout:string,stderr:string,concurrency:int} */
	private function launchWorker(int $concurrency): array {
		$command = [PHP_BINARY, __DIR__ . '/cli/worker.php', '--user', $this->username,
			'--coordinator-token', bin2hex(random_bytes(32))];
		$descriptors = [
			0 => ['file', '/dev/null', 'r'],
			1 => ['pipe', 'w'],
			2 => ['pipe', 'w'],
		];
		$pipes = [];
		$process = proc_open($command, $descriptors, $pipes);
		if (!is_resource($process) || !isset($pipes[1], $pipes[2])) {
			throw new RuntimeException('Cannot launch a Topic Digest cloud worker.');
		}
		stream_set_blocking($pipes[1], false);
		stream_set_blocking($pipes[2], false);
		return ['process' => $process, 'pipes' => $pipes, 'stdout' => '', 'stderr' => '', 'concurrency' => $concurrency];
	}

	/** @return array{processed:int,failed:int,throttle_status:int,retry_after:int} */
	private function decodeWorkerResult(string $stdout, string $stderr): array {
		$lines = preg_split('/\R/', trim($stdout)) ?: [];
		for ($index = count($lines) - 1; $index >= 0; $index--) {
			try {
				$result = json_decode($lines[$index], true, flags: JSON_THROW_ON_ERROR);
				if (is_array($result) && isset($result['processed'], $result['failed'],
						$result['throttle_status'], $result['retry_after'])) {
					return ['processed' => (int)$result['processed'], 'failed' => (int)$result['failed'],
						'throttle_status' => (int)$result['throttle_status'], 'retry_after' => (int)$result['retry_after']];
				}
			} catch (JsonException) {
				continue;
			}
		}
		throw new RuntimeException('Topic Digest cloud worker returned no valid result: ' . mb_substr(trim($stderr), 0, 1000));
	}
}
