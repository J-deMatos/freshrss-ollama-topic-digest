/* globals context */
(() => {
	'use strict';
	const ensureMarker = (title, emoji, label) => {
		if (!title) return;
		let marker = title.querySelector('[data-topic-digest-marker]');
		if (!marker) {
			marker = document.createElement('span');
			marker.className = 'topic-digest-sidebar-marker';
			marker.dataset.topicDigestMarker = '1';
			title.prepend(marker);
		}
		marker.textContent = emoji;
		marker.title = label;
		marker.setAttribute('role', 'img');
		marker.setAttribute('aria-label', label);
	};
	const applyTopicCounts = (counts) => {
		if (!counts || typeof counts !== 'object') return;
		for (const [feedId, rawCount] of Object.entries(counts)) {
			const count = Number(rawCount);
			const title = document.querySelector(`#f_${CSS.escape(feedId)} > .item-title > .title`);
			if (!title || !Number.isInteger(count) || count < 0) continue;
			let indicator = title.querySelector('[data-topic-digest-entry-count]');
			if (!indicator) {
				indicator = document.createElement('span');
				indicator.className = 'topic-digest-sidebar-count';
				indicator.dataset.topicDigestEntryCount = '1';
				title.append(indicator);
			}
			indicator.textContent = ` [ ${count} ]`;
		}
	};
	const configureCategoryLink = () => {
		if (!window.context) return;
		const settings = context.extensions?.topic_digest;
		const categoryId = Number(settings?.category_id || 0);
		const allState = Number(settings?.all_state || 0);
		const alwaysShowTopics = settings?.always_show_topics === true;
		const feedTypes = settings?.feed_types && typeof settings.feed_types === 'object'
			? settings.feed_types : {};
		if (categoryId < 1 || allState < 1) return;
		applyTopicCounts(settings.feed_counts);

		const category = document.getElementById(`c_${categoryId}`);
		const link = Array.from(category?.children || [])
			.find((child) => child.matches?.('a.tree-folder-title'));
		if (!link) return;
		ensureMarker(link.querySelector(':scope > .title'), '🧭', 'Topic Digests');
		const favorites = document.querySelector('#sidebar > .tree-folder.favorites');
		if (favorites?.parentElement === category.parentElement && favorites.nextElementSibling !== category) {
			favorites.parentElement.insertBefore(category, favorites.nextElementSibling);
		}

		const url = new URL(link.href, window.location.href);
		url.searchParams.set('state', String(allState));
		link.href = url.toString();
		category.classList.toggle('topic-digest-always-visible', alwaysShowTopics);

		let activeLowPriorityTopic = false;
		for (const feedId of Object.keys(settings.feed_counts || {})) {
			const feed = document.getElementById(`f_${feedId}`);
			if (!feed) continue;
			const feedType = feedTypes[feedId] || 'digest';
			const isLowPriority = feedType !== 'feed';
			const marker = feedType === 'feed' ? ['⚡', 'High-priority topic feed']
				: feedType === 'mark_read' ? ['✓', 'Mark-read verification feed']
				: ['🗂️', 'Low-priority living digest'];
			ensureMarker(feed.querySelector(':scope > .item-title > .title'), marker[0], marker[1]);
			feed.classList.toggle('topic-digest-always-visible', alwaysShowTopics);
			if (!alwaysShowTopics) continue;
			activeLowPriorityTopic ||= isLowPriority && feed.classList.contains('active');
			const feedLink = Array.from(feed.children).find((child) => child.matches?.('a.item-title'));
			if (feedLink && isLowPriority) {
				const feedUrl = new URL(feedLink.href, window.location.href);
				feedUrl.searchParams.set('state', String(allState));
				feedLink.href = feedUrl.toString();
			}
		}

		// A direct or bookmarked category URL without a state should behave like a click.
		const currentUrl = new URL(window.location.href);
		const categoryNeedsDefault = category.classList.contains('active') && !currentUrl.searchParams.has('state');
		const alwaysShowActive = alwaysShowTopics
			&& (category.classList.contains('active') || activeLowPriorityTopic)
			&& currentUrl.searchParams.get('state') !== String(allState);
		if (categoryNeedsDefault || alwaysShowActive) {
			currentUrl.searchParams.set('state', String(allState));
			window.location.replace(currentUrl.toString());
		}
	};
	document.addEventListener('topic-digest:statistics', (event) => {
		applyTopicCounts(event.detail?.topic_counts);
	});
	const initialise = () => {
		configureCategoryLink();
		for (const all of document.querySelectorAll('[data-topic-digest-editor] [data-select-all]')) {
			const update = () => {
				for (const item of document.querySelectorAll(`[data-topic-digest-editor] [name="${all.dataset.selectAll}"]`)) {
					item.disabled = all.checked;
				}
			};
			all.addEventListener('change', update);
			update();
		}
		const mode = document.getElementById('backfill_mode');
		const days = document.querySelector('[data-backfill-days]');
		const updateDays = () => { if (days) days.hidden = mode?.value !== 'days'; };
		mode?.addEventListener('change', updateDays);
		updateDays();
		const topicType = document.getElementById('topic_type');
		const verification = document.querySelector('[data-mark-read-verification]');
		const updateVerification = () => {
			if (verification) verification.hidden = topicType?.value !== 'mark_read';
		};
		topicType?.addEventListener('change', updateVerification);
		updateVerification();
		const profileRadios = document.querySelectorAll('[data-ollama-profile-radio]');
		const fallbackModeRadios = document.querySelectorAll('[data-fallback-mode-radio]');
		const updateProfile = () => {
			const selected = document.querySelector('[data-ollama-profile-radio]:checked')?.value;
			const fallbackSelected = document.querySelector('[data-fallback-mode-radio]:checked')?.value;
			for (const fieldset of document.querySelectorAll('[data-ollama-profile-fields]')) {
				const target = fieldset.dataset.ollamaProfileFields;
				// The OpenAI-compatible fieldset is one shared field set usable as the primary and/or the
				// fallback, so it stays visible whenever either role selects it.
				fieldset.hidden = fieldset.dataset.openaiFields === '1'
					? target !== selected && target !== fallbackSelected
					: target !== selected;
			}
		};
		for (const radio of profileRadios) radio.addEventListener('change', updateProfile);
		for (const radio of fallbackModeRadios) radio.addEventListener('change', updateProfile);
		updateProfile();
	};
	document.addEventListener('freshrss:globalContextLoaded', configureCategoryLink, {once: true});
	if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initialise, {once: true});
	else initialise();
})();
