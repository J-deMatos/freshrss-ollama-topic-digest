<?php
declare(strict_types=1);

final class FreshExtension_topicDigest_Controller extends Minz_ActionController {
	#[\Override]
	public function firstAction(): void {
		if (!FreshRSS_Auth::hasAccess()) {
			Minz_Error::error(403);
		}
	}

	public function restoreSourceAction(): void {
		if (!Minz_Request::isPost()) {
			Minz_Request::bad('Restore actions require POST.');
		}
		$this->extension()->restoreSource(Minz_Request::paramInt('topic_id'),
			Minz_Request::paramString('entry_id', plaintext: true));
		Minz_Request::good(_t('ext.topic_digest.source_restored'), ['c' => 'index', 'a' => 'index']);
	}

	public function restoreEventAction(): void {
		if (!Minz_Request::isPost()) {
			Minz_Request::bad('Restore actions require POST.');
		}
		$this->extension()->restoreEvent(Minz_Request::paramInt('topic_id'), Minz_Request::paramInt('event_id'));
		Minz_Request::good(_t('ext.topic_digest.event_restored'), ['c' => 'index', 'a' => 'index']);
	}

	private function extension(): TopicDigestExtension {
		$extension = Minz_ExtensionManager::findExtension('Topic Digest');
		if (!($extension instanceof TopicDigestExtension) || !$extension->isEnabled()) {
			throw new RuntimeException('Topic Digest is not enabled.');
		}
		return $extension;
	}
}
