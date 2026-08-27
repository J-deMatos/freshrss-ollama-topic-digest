(() => {
	'use strict';

	const initialise = () => {
		const panel = document.querySelector('[data-topic-digest-stats]');
		if (!panel) return;
		const toggle = panel.querySelector('[data-topic-digest-toggle]');
		const restart = panel.querySelector('[data-topic-digest-restart]');
		const controlStatus = panel.querySelector('[data-topic-digest-control-status]');

		const applyPayload = (payload) => {
			document.dispatchEvent(new CustomEvent('topic-digest:statistics', {detail: payload}));
			const summary = panel.querySelector('[data-topic-digest-stat-summary]');
			if (summary && typeof payload.summary === 'string') summary.textContent = payload.summary;
			if (payload.items && typeof payload.items === 'object') {
				for (const [key, item] of Object.entries(payload.items)) {
					const value = panel.querySelector(`[data-topic-digest-stat="${CSS.escape(key)}"]`);
					if (value && item && typeof item.value === 'string') value.textContent = item.value;
				}
			}
			if (toggle && payload.control && typeof payload.control.action === 'string'
					&& typeof payload.control.label === 'string') {
				toggle.dataset.action = payload.control.action;
				toggle.textContent = payload.control.label;
			}
		};

		const refresh = async () => {
			if (document.hidden || !panel.isConnected) return;
			const controller = new AbortController();
			const timeout = setTimeout(() => controller.abort(), 10000);
			try {
				const response = await fetch(panel.dataset.statsUrl, {
					credentials: 'same-origin',
					headers: {'Accept': 'application/json'},
					signal: controller.signal,
					cache: 'no-store',
				});
				if (response.ok) applyPayload(await response.json());
			} catch (error) {
				if (error instanceof Error && error.name !== 'AbortError') {
					console.debug('Topic Digest statistics refresh failed:', error.message);
				}
			} finally {
				clearTimeout(timeout);
			}
		};

		if (toggle) {
			toggle.addEventListener('click', async () => {
				const action = toggle.dataset.action;
				if (action !== 'pause' && action !== 'resume') return;
				toggle.disabled = true;
				if (controlStatus) controlStatus.textContent = '';
				try {
					const body = new URLSearchParams({
						_csrf: panel.dataset.csrf || '',
						_topic_digest_ajax: '1',
						topic_digest_action: action,
					});
					const response = await fetch(panel.dataset.controlUrl, {
						method: 'POST',
						credentials: 'same-origin',
						headers: {'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'},
						body: body.toString(),
					});
					if (!response.ok) throw new Error(`HTTP ${response.status}`);
					applyPayload(await response.json());
				} catch (error) {
					if (controlStatus) controlStatus.textContent = error instanceof Error ? error.message : String(error);
				} finally {
					toggle.disabled = false;
				}
			});
		}

		if (restart) {
			restart.addEventListener('click', async () => {
				restart.disabled = true;
				if (controlStatus) controlStatus.textContent = '';
				try {
					const body = new URLSearchParams({
						_csrf: panel.dataset.csrf || '',
						_topic_digest_ajax: '1',
						topic_digest_action: 'restart',
					});
					const response = await fetch(panel.dataset.controlUrl, {
						method: 'POST',
						credentials: 'same-origin',
						headers: {'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'},
						body: body.toString(),
					});
					if (!response.ok) throw new Error(`HTTP ${response.status}`);
					applyPayload(await response.json());
					if (controlStatus) controlStatus.textContent = restart.dataset.success || '';
				} catch (error) {
					if (controlStatus) controlStatus.textContent = error instanceof Error ? error.message : String(error);
				} finally {
					restart.disabled = false;
				}
			});
		}

		setInterval(refresh, 15000);
		document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
	};

	if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initialise, {once: true});
	else initialise();
})();
