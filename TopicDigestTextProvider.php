<?php
declare(strict_types=1);

/**
 * A structured-output text inference backend (e.g. local Ollama, Ollama Cloud, an OpenAI-compatible
 * endpoint) able to produce the summary/topic-match/event-match decisions TopicDigestProcessor needs.
 *
 * Implementations take the model name per call (rather than being bound to one model) so a single
 * instance can serve both the summary and judge roles, exactly like TopicDigestOllama already does.
 */
interface TopicDigestTextProvider {
	/** @param list<string> $models */
	public function test(array $models): void;

	/** @return array{summary:string,event_title:string,event_date:string} */
	public function summarise(string $model, string $title, string $text, int $publishedAt): array;

	/**
	 * @param array<string,mixed> $summary
	 * @param array<string,mixed> $topic
	 * @return array{matches:bool,confidence:float,reason:string,event_title:string}
	 */
	public function matchTopic(string $model, array $summary, array $topic): array;

	/**
	 * @param array<string,mixed> $summary
	 * @param list<array<string,mixed>> $topics
	 * @return array<int,array{matches:bool,confidence:float,reason:string,event_title:string}>
	 */
	public function matchTopics(string $model, array $summary, array $topics): array;

	/**
	 * @param array<string,mixed> $summary
	 * @param array<string,mixed> $event
	 * @return array{same_event:bool,confidence:float,reason:string}
	 */
	public function sameEvent(string $model, array $summary, array $event): array;

	/**
	 * @param array<string,mixed> $summary
	 * @param list<array<string,mixed>> $events
	 * @return array<string,array{same_event:bool,confidence:float,reason:string}>
	 */
	public function sameEvents(string $model, array $summary, array $events): array;
}

/** A backend able to turn text into an embedding vector, independent of which text provider is in use. */
interface TopicDigestEmbeddingProvider {
	/** @param list<string> $models */
	public function test(array $models): void;

	/** @return list<float> */
	public function embed(string $model, string $text): array;
}

/**
 * Something able to hand out Fiber+curl_multi-backed transport closures matching TopicDigestOllama's
 * (3-arg, path-relative-to-a-base-URL) and TopicDigestOpenAICompatible's (4-arg, complete URL + headers)
 * injectable `?Closure $transport` contract, so extension.php's provider-builder methods can route
 * construction through either the settings-page connectivity tester (TopicDigestParallelTester) or the
 * worker's real concurrent inference dispatcher (TopicDigestConcurrentDispatcher) without caring which.
 */
interface TopicDigestTransportSource {
	/** @return Closure(string,string,array<string,mixed>|null):array<string,mixed> */
	public function ollamaTransport(string $baseUrl): Closure;

	/** @return Closure(string,string,array<string,mixed>|null,array<string,string>):array<string,mixed> */
	public function openAiTransport(): Closure;
}
