document.querySelectorAll('[data-confirm]').forEach((form) =>
  form.addEventListener('submit', (event) => {
    if (!window.confirm(form.dataset.confirm)) event.preventDefault();
  }),
);

// Cluster pod/HPA data is a server-rendered snapshot. Refresh only this page
// so a scaling event becomes visible without the presenter pressing reload.
const liveCluster = document.querySelector('[data-live-cluster]');
if (liveCluster) {
  window.setInterval(() => {
    if (!document.hidden) window.location.reload();
  }, Number(liveCluster.dataset.refreshMs || 5000));
}

const liveRebalancing = document.querySelector('[data-live-rebalancing]');
if (liveRebalancing) {
  const root = document;
  const distribution = root.querySelector('[data-distribution]');
  const events = root.querySelector('[data-events]');
  const migrations = root.querySelector('[data-migrations]');
  const summary = root.querySelector('[data-distribution-summary]');
  const migrationSummary = root.querySelector('[data-migration-summary]');
  const updated = root.querySelector('[data-last-updated]');
  const escape = (value) =>
    String(value ?? '').replace(
      /[&<>'"]/g,
      (character) =>
        ({
          '&': '&amp;',
          '<': '&lt;',
          '>': '&gt;',
          "'": '&#39;',
          '"': '&quot;',
        })[character],
    );
  const formatTime = (value) => new Date(value).toLocaleString();
  const render = (data) => {
    const workers = data.workers || [];
    const actors = data.actors || [];
    const total = actors.length;
    summary.textContent = `${workers.length} live worker(s) · ${total} assigned actor(s)`;
    if (!workers.length) {
      distribution.innerHTML =
        '<div class="empty">Waiting for worker pods to register.</div>';
    } else {
      distribution.innerHTML = workers
        .map((worker) => {
          const count = actors.filter(
            (actor) => actor.workerId === worker.workerId,
          ).length;
          const width = Math.max(4, total ? (count / total) * 100 : 0);
          return `<div class="dist-row"><div><b>${escape(worker.workerId)}</b><small>${escape(worker.host || worker.url)} · target about ${Math.ceil(total / workers.length)}</small></div><div class="dist-bar"><i style="width:${width}%"></i></div><strong>${count}</strong></div>`;
        })
        .join('');
    }
    events.innerHTML = (data.events || []).length
      ? data.events
          .map(
            (event) =>
              `<div class="event-row"><i class="status-dot amber"></i><span class="event-kind">${escape(event.type)}</span><span>${escape(event.message)}</span><time>${formatTime(event.createdAt)}</time></div>`,
          )
          .join('')
      : '<div class="empty">Waiting for the first coordinator event.</div>';
    migrationSummary.textContent = `${(data.migrations || []).length} recorded automatic transfer(s)`;
    migrations.innerHTML = (data.migrations || []).length
      ? `<table><thead><tr><th>ACTOR</th><th>SOURCE POD</th><th>DESTINATION POD</th><th>GENERATION</th><th>STATUS</th><th>STARTED</th></tr></thead><tbody>${data.migrations
          .map(
            (migration) =>
              `<tr><td>${escape(migration.actorId)}</td><td>${escape(migration.sourceWorkerId || 'UNASSIGNED')}</td><td>${escape(migration.destinationWorkerId)}</td><td>${migration.fromGeneration} → ${migration.toGeneration}</td><td>${escape(migration.status)}${migration.error ? `: ${escape(migration.error)}` : ''}</td><td>${formatTime(migration.startedAt)}</td></tr>`,
          )
          .join('')}</tbody></table>`
      : '<div class="empty">No automatic migrations recorded yet.</div>';
    updated.textContent = `updated ${new Date(data.observedAt).toLocaleTimeString()}`;
  };
  const refresh = async () => {
    try {
      const response = await fetch('/api/rebalancing/status', {
        headers: { Accept: 'application/json' },
      });
      if (!response.ok) throw new Error('Runtime status unavailable');
      render(await response.json());
    } catch (error) {
      updated.textContent = 'reconnecting…';
    }
  };
  refresh();
  window.setInterval(
    refresh,
    Number(liveRebalancing.dataset.refreshMs || 3000),
  );
}
