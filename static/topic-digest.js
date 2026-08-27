/* globals context */
(() => {
	'use strict';
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
		if (categoryId < 1 || allState < 1) return;
		applyTopicCounts(settings.feed_counts);

		const category = document.getElementById(`c_${categoryId}`);
		const link = Array.from(category?.children || [])
			.find((child) => child.matches?.('a.tree-folder-title'));
		if (!link) return;

		const url = new URL(link.href, window.location.href);
		url.searchParams.set('state', String(allState));
		link.href = url.toString();
		category.classList.toggle('topic-digest-always-visible', alwaysShowTopics);

		let activeTopic = false;
		for (const feedId of Object.keys(settings.feed_counts || {})) {
			const feed = document.getElementById(`f_${feedId}`);
			if (!feed) continue;
			feed.classList.toggle('topic-digest-always-visible', alwaysShowTopics);
			if (!alwaysShowTopics) continue;
			activeTopic ||= feed.classList.contains('active');
			const feedLink = Array.from(feed.children).find((child) => child.matches?.('a.item-title'));
			if (feedLink) {
				const feedUrl = new URL(feedLink.href, window.location.href);
				feedUrl.searchParams.set('state', String(allState));
				feedLink.href = feedUrl.toString();
			}
		}

		// A direct or bookmarked category URL without a state should behave like a click.
		const currentUrl = new URL(window.location.href);
		const categoryNeedsDefault = category.classList.contains('active') && !currentUrl.searchParams.has('state');
		const alwaysShowActive = alwaysShowTopics && (category.classList.contains('active') || activeTopic)
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
	};
	document.addEventListener('freshrss:globalContextLoaded', configureCategoryLink, {once: true});
	if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initialise, {once: true});
	else initialise();
})();
