<?php
declare(strict_types=1);

final class TopicDigestScraper {
	private const MAX_BYTES = 2_000_000;

	public static function rssText(string $html): string {
		return self::normalise(html_entity_decode(strip_tags($html), ENT_QUOTES | ENT_HTML5, 'UTF-8'));
	}

	public static function isInsufficient(string $html): bool {
		$text = self::rssText($html);
		return mb_strlen($text, 'UTF-8') < 600 || preg_match(
			'/\b(continue reading|read (the )?(full|complete) (article|story)|read more|view original|'
			. 'ler mais|continuar a ler|lire la suite|leer m[aá]s|weiterlesen)\b/iu', mb_substr($text, -300)
		) === 1;
	}

	public static function fetch(string $url, int $timeout): ?string {
		$parts = parse_url($url);
		if (!is_array($parts) || !in_array(strtolower((string)($parts['scheme'] ?? '')), ['http', 'https'], true)
				|| empty($parts['host']) || isset($parts['user']) || isset($parts['pass'])) {
			return null;
		}
		try {
			$response = FreshRSS_http_Util::httpGet($url, type: 'html', attributes: ['timeout' => $timeout], curl_options: [
				CURLOPT_CONNECTTIMEOUT => min(10, $timeout), CURLOPT_TIMEOUT => $timeout,
				CURLOPT_MAXFILESIZE => self::MAX_BYTES, CURLOPT_MAXREDIRS => 5,
			]);
		} catch (Throwable $e) {
			Minz_Log::warning('Topic Digest article fetch failed: ' . $e->getMessage());
			return null;
		}
		$body = is_string($response['body'] ?? null) ? $response['body'] : '';
		if (($response['fail'] ?? true) || (int)($response['status'] ?? 0) < 200
				|| (int)($response['status'] ?? 0) >= 300 || $body === '' || strlen($body) > self::MAX_BYTES) {
			return null;
		}
		$document = new DOMDocument();
		if (!$document->loadHTML("\xEF\xBB\xBF" . $body, LIBXML_NONET | LIBXML_NOERROR | LIBXML_NOWARNING)) {
			return null;
		}
		$xpath = new DOMXPath($document);
		foreach (['//script', '//style', '//nav', '//aside', '//footer', '//form'] as $query) {
			foreach ($xpath->query($query) ?: [] as $node) {
				$node->parentNode?->removeChild($node);
			}
		}
		$best = null;
		$length = 0;
		foreach ($xpath->query('//article|//main|//body//div[count(.//p)>=2]') ?: [] as $node) {
			$candidate = self::normalise($node->textContent);
			if (mb_strlen($candidate, 'UTF-8') > $length) {
				$best = $candidate;
				$length = mb_strlen($candidate, 'UTF-8');
			}
		}
		return $length >= 400 ? mb_substr((string)$best, 0, 50000) : null;
	}

	private static function normalise(string $text): string {
		return trim(preg_replace('/[\p{Z}\s]+/u', ' ', $text) ?? $text);
	}
}
