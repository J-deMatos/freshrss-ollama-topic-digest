<?php
declare(strict_types=1);

/**
 * Runs the "Test connection" checks for all configured providers concurrently instead of one after another,
 * using PHP Fibers to suspend each provider's test() call at the point it would normally block on curl_exec(),
 * and a single shared curl_multi handle to drive every pending request together.
 *
 * Deliberately confined to the settings-page connectivity test: this is not the worker/queue path (which the
 * project intentionally keeps single-threaded), just a one-off diagnostic click that previously ran up to
 * three full HTTP round trips back to back, each allowed the full (often very large) configured processing
 * timeout. Requests made through this tester use a short, fixed timeout instead, since a connectivity check
 * has no reason to wait as long as real processing would.
 */
final class TopicDigestParallelTester implements TopicDigestTransportSource {
	private const CONNECT_TIMEOUT_SECONDS = 8;
	private const TOTAL_TIMEOUT_SECONDS = 15;
	private \CurlMultiHandle $multi;
	/** @var array<int,array{fiber:Fiber,handle:\CurlHandle}> Keyed by curl handle id. */
	private array $active = [];

	public function __construct() {
		$this->multi = curl_multi_init();
	}

	/**
	 * A transport closure matching TopicDigestOllama's contract: it is called with a *path* relative to a
	 * fixed base URL, no headers.
	 *
	 * @return Closure(string,string,array<string,mixed>|null):array<string,mixed>
	 */
	public function ollamaTransport(string $baseUrl): Closure {
		return fn(string $method, string $path, ?array $payload): array => $this->dispatch($method, $baseUrl . $path, $payload, []);
	}

	/**
	 * A transport closure matching TopicDigestOpenAICompatible's contract: it is called with a complete URL
	 * and any headers (e.g. Bearer authentication) already resolved by the caller.
	 *
	 * @return Closure(string,string,array<string,mixed>|null,array<string,string>):array<string,mixed>
	 */
	public function openAiTransport(): Closure {
		return fn(string $method, string $url, ?array $payload, array $headers): array => $this->dispatch($method, $url, $payload, $headers);
	}

	/**
	 * Registers one request with the shared curl_multi handle and suspends the current Fiber until run()'s
	 * pump() reports it finished. Must only be called from inside a Fiber started by run() below.
	 *
	 * @param array<string,mixed>|null $payload
	 * @param array<string,string> $headers
	 * @return array<string,mixed>
	 */
	private function dispatch(string $method, string $url, ?array $payload, array $headers): array {
		$fiber = Fiber::getCurrent();
		if ($fiber === null) {
			throw new RuntimeException('The connectivity-test transport must run inside run().');
		}
		$body = '';
		$handle = curl_init($url);
		if ($handle === false) {
			throw new RuntimeException('Cannot initialise the connectivity-test request.');
		}
		$httpHeaders = ['Accept: application/json', 'Content-Type: application/json'];
		foreach ($headers as $name => $value) {
			$httpHeaders[] = "{$name}: {$value}";
		}
		$options = [
			CURLOPT_CUSTOMREQUEST => $method, CURLOPT_RETURNTRANSFER => false,
			CURLOPT_CONNECTTIMEOUT => self::CONNECT_TIMEOUT_SECONDS, CURLOPT_TIMEOUT => self::TOTAL_TIMEOUT_SECONDS,
			CURLOPT_HTTPHEADER => $httpHeaders,
			CURLOPT_WRITEFUNCTION => static function ($curl, string $chunk) use (&$body): int {
				$body .= $chunk;
				return strlen($chunk);
			},
		];
		if ($payload !== null) {
			$options[CURLOPT_POSTFIELDS] = json_encode($payload, JSON_THROW_ON_ERROR);
		}
		curl_setopt_array($handle, $options);
		curl_multi_add_handle($this->multi, $handle);
		$this->active[(int)$handle] = ['fiber' => $fiber, 'handle' => $handle];
		/** @var array{0:int,1:string} $outcome [http status, curl error] */
		$outcome = Fiber::suspend();
		[$status, $error] = $outcome;
		if ($status < 200 || $status >= 300 || $error !== '') {
			$detail = $error !== '' ? $error : ($status > 0 ? "HTTP {$status}" : 'no response');
			throw new RuntimeException("Connectivity test request failed: {$method} {$url} ({$detail}).");
		}
		try {
			$result = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
		} catch (JsonException $e) {
			throw new RuntimeException('Connectivity test received invalid JSON.', previous: $e);
		}
		if (!is_array($result)) {
			throw new RuntimeException('Connectivity test received invalid JSON.');
		}
		return $result;
	}

	/**
	 * Runs each operation concurrently. Each is expected to internally call (only) a transport() closure
	 * obtained from this same instance, any number of times (including zero).
	 *
	 * @param list<Closure():void> $operations
	 * @return list<Throwable|null> One entry per operation, in the same order: null on success.
	 */
	public function run(array $operations): array {
		$results = array_fill(0, count($operations), null);
		$pending = [];
		foreach ($operations as $index => $operation) {
			$fiber = new Fiber(static function () use ($operation, $index, &$results): void {
				try {
					$operation();
				} catch (Throwable $e) {
					$results[$index] = $e;
				}
			});
			$pending[] = $fiber;
			$fiber->start();
		}
		$this->pump();
		foreach ($pending as $index => $fiber) {
			if (!$fiber->isTerminated()) {
				// Only reachable if pump() returned with work still outstanding, which it does not unless a
				// curl handle vanished from $this->active without ever finishing.
				$results[$index] = new RuntimeException('The connectivity test did not complete.');
			}
		}
		return $results;
	}

	private function pump(): void {
		if ($this->active === []) {
			return;
		}
		do {
			do {
				$status = curl_multi_exec($this->multi, $running);
			} while ($status === CURLM_CALL_MULTI_PERFORM);
			if ($running > 0) {
				curl_multi_select($this->multi, 1.0);
			}
			while (($info = curl_multi_info_read($this->multi)) !== false) {
				$handle = $info['handle'];
				$id = (int)$handle;
				$entry = $this->active[$id] ?? null;
				curl_multi_remove_handle($this->multi, $handle);
				if ($entry === null) {
					curl_close($handle);
					continue;
				}
				unset($this->active[$id]);
				$httpStatus = (int)curl_getinfo($handle, CURLINFO_HTTP_CODE);
				$error = curl_error($handle);
				curl_close($handle);
				$fiber = $entry['fiber'];
				if ($fiber->isSuspended()) {
					// The resumed transport() call reads the response body via its own by-reference local
					// variable (already fully populated by CURLOPT_WRITEFUNCTION); only status/error cross here.
					$fiber->resume([$httpStatus, $error]);
				}
			}
		} while ($running > 0 || $this->active !== []);
	}
}
