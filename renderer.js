window._bqFilePicked = false;

const $ = id => document.getElementById(id);

// selection and pairs state
window._selectedOracleColumn = null; // { table, name, type }
window._selectedBQColumn = null; // { table, name, type }
window._columnPairs = []; // array of { oracle: {...}, bq: {...}, id: string }

function uid() { return Math.random().toString(36).slice(2,10); }

function highlightSelectedColumns() {
  // Oracle
  document.querySelectorAll('#oracle-columns li').forEach(li => {
    const colText = li.dataset.colKey;
    li.style.background = (window._selectedOracleColumn && colText === `${window._selectedOracleColumn.table}|${window._selectedOracleColumn.name}`) ? '#e6f7ff' : '';
  });
  // BQ
  document.querySelectorAll('#bq-columns li').forEach(li => {
    const colText = li.dataset.colKey;
    li.style.background = (window._selectedBQColumn && colText === `${window._selectedBQColumn.table}|${window._selectedBQColumn.name}`) ? '#fff7e6' : '';
  });
}

function renderPairsList() {
  const ul = document.getElementById('pairs-list');
  ul.innerHTML = '';
  window._columnPairs.forEach(p => {
    const li = document.createElement('li');
    li.style.padding = '8px';
    li.style.borderBottom = '1px solid #f0f0f0';
    li.innerHTML = `<strong>Oracle:</strong> ${escapeHtml(p.oracle.table)}.${escapeHtml(p.oracle.name)} &nbsp; (<em>${escapeHtml(p.oracle.type)}</em>)
                    &nbsp; <strong>BQ:</strong> ${escapeHtml(p.bq.table)}.${escapeHtml(p.bq.name)} &nbsp; (<em>${escapeHtml(p.bq.type)}</em>)
                    <button data-remove-id="${p.id}" style="float:right">Remove</button>`;
    ul.appendChild(li);
  });
  // attach remove handlers
  ul.querySelectorAll('button[data-remove-id]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const id = e.currentTarget.getAttribute('data-remove-id');
      window._columnPairs = window._columnPairs.filter(x => x.id !== id);
      renderPairsList();
    });
  });
}

function escapeHtml(s) {
  if (!s && s !== 0) return '';
  return String(s).replace(/[&<>"']/g, (m) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
}

// convenience to clear current selections
function clearSelections() {
  window._selectedOracleColumn = null;
  window._selectedBQColumn = null;
  document.getElementById('oracle-status').textContent = '';
  document.getElementById('bq-status').textContent = '';
  highlightSelectedColumns();
}

// ---------- Oracle UI ----------
$('connect-oracle').addEventListener('click', async () => {
  setOracleState({ connecting: true });
  $('oracle-status').textContent = 'Connecting...';
  const cfg = {
    hostname: $('host').value.trim(),
    port: $('port').value.trim(),
    service: $('db').value.trim(),
    username: $('user').value.trim(),
    password: $('pass').value,
    libDir: $('libdir').value.trim() || undefined
  };
  const r = await window.api.connectOracle(cfg);
  if (!r.ok) {
    $('oracle-status').textContent = 'Error: ' + r.error;
    setOracleState({ connected: false, connecting: false });
    return;
  }
  $('oracle-status').textContent = 'Connected';
  setOracleState({ connected: true, connecting: false });
  await loadOracleTables();
});

$('disconnect-oracle').addEventListener('click', async () => {
  $('oracle-status').textContent = 'Disconnecting...';
  const r = await window.api.disconnectOracle();
  if (r.ok) {
    $('oracle-status').textContent = 'Disconnected';
    clearOracleUI();
    setOracleState({ connected: false, connecting: false });
  } else {
    $('oracle-status').textContent = 'Error: ' + r.error;
  }
});

function setOracleState({ connected, connecting }) {
  $('connect-oracle').disabled = !!connecting || !!connected;
  $('disconnect-oracle').disabled = !connected;
}

async function loadOracleTables() {
  const r = await window.api.listTables();
  const list = $('oracle-tables');
  list.innerHTML = '';
  if (!r.ok) { $('oracle-status').textContent = 'Error: ' + r.error; return; }
  window._oracleTables = r.tables.slice();
  renderOracleTableList(r.tables);
}

function renderOracleTableList(tables) {
  const list = $('oracle-tables');
  list.innerHTML = '';
  tables.forEach(t => {
    const li = document.createElement('li'); li.textContent = t;
    li.onclick = () => loadOracleColumns(t);
    list.appendChild(li);
  });
}

// Oracle: render columns with dataset attributes and click handler
async function loadOracleColumns(table) {
  const r = await window.api.listColumns(table);
  const list = $('oracle-columns');
  list.innerHTML = '';
  if (!r.ok) { list.textContent = 'Error: ' + r.error; return; }
  r.columns.forEach(c => {
    const li = document.createElement('li');
    li.textContent = c.name + ' — ' + c.type;
    li.dataset.colKey = `${table}|${c.name}`;
    li.style.cursor = 'pointer';
    li.addEventListener('click', () => {
      window._selectedOracleColumn = { table, name: c.name, type: c.type };
      highlightSelectedColumns();
      tryCreatePair();
    });
    list.appendChild(li);
  });
}

//async function loadOracleColumns(table) {
//  const r = await window.api.listColumns(table);
//  const list = $('oracle-columns');
//  list.innerHTML = '';
//  if (!r.ok) { list.textContent = 'Error: ' + r.error; return; }
//  r.columns.forEach(c => {
//    const li = document.createElement('li'); li.textContent = c.name + ' — ' + c.type;
//    list.appendChild(li);
//  });
//}

function clearOracleUI() {
  $('oracle-tables').innerHTML = '';
  $('oracle-columns').innerHTML = '';
  window._oracleTables = [];
  $('oracle-search').value = '';
}

// search filter
$('oracle-search').addEventListener('input', () => {
  const q = $('oracle-search').value.trim().toLowerCase();
  const src = window._oracleTables || [];
  renderOracleTableList(src.filter(t => t.toLowerCase().includes(q)));
});


// ---------- BigQuery UI ----------
// ensure flag not needed; use textarea presence
$('connect-bq').addEventListener('click', async () => {
  $('bq-status').textContent = 'Connecting to BigQuery...';
  setBQState({ connecting: true });
  const cfg = {
    projectId: $('bq-project').value.trim(),
    datasetId: $('bq-dataset').value.trim(),
    serviceAccountJson: $('bq-service-account').value.trim()
  };
  if (!cfg.projectId || !cfg.datasetId || !cfg.serviceAccountJson) {
    $('bq-status').textContent = 'Please provide projectId, datasetId and paste service account JSON.';
    setBQState({ connecting: false });
    return;
  }
  // optional: quick client-side JSON validation for better UX
  try { JSON.parse(cfg.serviceAccountJson); } catch(e) {
    $('bq-status').textContent = 'Service account JSON is invalid: ' + e.message;
    setBQState({ connecting: false });
    return;
  }
  const r = await window.api.connectBQ(cfg);
  $('bq-status').textContent = r.ok ? 'Connected to BigQuery' : ('Error: ' + r.error);
  if (r.ok) loadBQTables();
});

$('disconnect-bq').addEventListener('click', async () => {
  $('bq-status').textContent = 'Disconnecting...';
  const r = await window.api.disconnectBQ();
  if (r.ok) {
    $('bq-status').textContent = 'Disconnected';
    clearBQUI();
    setBQState({ connected: false, connecting: false });
  } else {
    $('bq-status').textContent = 'Error: ' + r.error;
  }
});

async function loadBQTables() {
  const r = await window.api.bqListTables();
  const list = $('bq-tables');
  list.innerHTML = '';
  if (!r.ok) { $('bq-status').textContent = 'Error: ' + r.error; return; }
  r.tables.forEach(t => {
    const li = document.createElement('li');
    li.textContent = t;
    li.onclick = () => loadBQColumns(t);
    list.appendChild(li);
  });
}

function renderBQTableList(tables) {
  const list = $('bq-tables');
  list.innerHTML = '';
  tables.forEach(t => {
    const li = document.createElement('li'); li.textContent = t;
    li.onclick = () => loadBQColumns(t);
    list.appendChild(li);
  });
}

// BQ: render columns with dataset attributes and click handler
async function loadBQColumns(table) {
  const r = await window.api.bqListColumns(table);
  const list = $('bq-columns');
  list.innerHTML = '';
  if (!r.ok) { list.textContent = 'Error: ' + r.error; return; }
  r.columns.forEach(c => {
    const li = document.createElement('li');
    li.textContent = c.name + ' — ' + c.type;
    li.dataset.colKey = `${table}|${c.name}`;
    li.style.cursor = 'pointer';
    li.addEventListener('click', () => {
      window._selectedBQColumn = { table, name: c.name, type: c.type };
      highlightSelectedColumns();
      tryCreatePair();
    });
    list.appendChild(li);
  });
}

//async function loadBQColumns(table) {
//  const r = await window.api.bqListColumns(table);
//  const list = $('bq-columns');
//  list.innerHTML = '';
//  if (!r.ok) { list.textContent = 'Error: ' + r.error; return; }
//  r.columns.forEach(c => {
//    const li = document.createElement('li'); 
//    li.textContent = c.name + ' - ' + c.type;
//    list.appendChild(li);
//  });
//}

function setBQState({ connected, connecting }) {
  $('connect-bq').disabled = !!connecting || !!connected;
  $('disconnect-bq').disabled = !connected;
  // keep textarea enabled so user can edit/paste as needed
}

function clearBQUI() {
  $('bq-tables').innerHTML = '';
  $('bq-columns').innerHTML = '';
  window._bqTables = [];
  $('bq-search').value = '';
}

// search filter
$('bq-search').addEventListener('input', () => {
  const q = $('bq-search').value.trim().toLowerCase();
  const src = window._bqTables || [];
  renderBQTableList(src.filter(t => t.toLowerCase().includes(q)));
});

function tryCreatePair() {
  if (window._selectedOracleColumn && window._selectedBQColumn) {
    const pair = {
      id: uid(),
      oracle: { ...window._selectedOracleColumn },
      bq: { ...window._selectedBQColumn }
    };
    window._columnPairs.push(pair);
    renderPairsList();
    // auto-clear selections so user can pick next pair
    clearSelections();
  }
}

// Compare button handler
$('compare-pairs').addEventListener('click', async () => {
  const pairs = window._columnPairs || [];
  if (!pairs.length) {
    alert('No pairs to compare.');
    return;
  }
  $('compare-results-meta').textContent = 'Running comparison...';
  $('compare-results-container').innerHTML = '';
  try {
    const r = await window.api.comparePairs(pairs);
    if (!r.ok) {
      $('compare-results-meta').textContent = 'Error: ' + r.error;
      return;
    }
    renderCompareResults(r.results);
  } catch (e) {
    $('compare-results-meta').textContent = 'Error: ' + (e.message || String(e));
  }
});

function renderCompareResults(results) {
  if (!results || results.length === 0) {
    $('compare-results-meta').textContent = 'No results returned.';
    return;
  }
  $('compare-results-meta').textContent = `Comparison completed for ${results.length} pair(s).`;
  const container = $('compare-results-container');
  container.innerHTML = '';
  for (const res of results) {
    if (!res) continue;                           // guard for undefined entries
    const box = document.createElement('div');
    box.style.border = '1px solid #e6e6e6';
    box.style.padding = '8px';
    box.style.marginBottom = '8px';
    // If the pair-level handler returned an error for this pair, show it and continue
    if (res.error) {
      const titleErr = document.createElement('div');
      titleErr.innerHTML = `<strong>Pair</strong>: ${escapeHtml(res.pairId || 'unknown')} <span style="color:red">Error</span>`;
      box.appendChild(titleErr);
      const err = document.createElement('div'); err.style.color = 'red'; err.textContent = res.error;
      box.appendChild(err);
      container.appendChild(box);
      continue;
    }
    // Ensure a valid pair object exists
    if (!res.pair || !res.pair.oracle || !res.pair.bq) {
      const titleUnknown = document.createElement('div');
      titleUnknown.innerHTML = `<strong>Pair</strong>: ${escapeHtml(res.pairId || 'unknown')} <span style="color:orange">Invalid result structure</span>`;
      box.appendChild(titleUnknown);
      const info = document.createElement('div'); info.style.color = '#666'; info.textContent = 'Result missing pair metadata.';
      box.appendChild(info);
      container.appendChild(box);
      continue;
    }
   
    const title = document.createElement('div');
    title.innerHTML = `<strong>Pair</strong>: Oracle ${escapeHtml(res.pair.oracle.table)}.${escapeHtml(res.pair.oracle.name)}
                       <strong>vs</strong> BQ ${escapeHtml(res.pair.bq.table)}.${escapeHtml(res.pair.bq.name)}`;
    box.appendChild(title);

    if (res.error) {
      const err = document.createElement('div'); err.style.color = 'red'; err.textContent = res.error;
      box.appendChild(err);
      container.appendChild(box);
      continue;
    }

    const counts = res.counts || {};
    const summary = document.createElement('div');
    summary.innerHTML = `<em>Matched:</em> ${counts.matched || 0} &nbsp; <em>Mismatched:</em> ${counts.mismatched || 0}
                         &nbsp; <em>In Oracle Not BQ:</em> ${counts.inOracleNotBQ || 0} &nbsp; <em>In BQ Not Oracle:</em> ${counts.inBQNotOracle || 0}`;
    box.appendChild(summary);

    // sample table
    const sample = res.sample || [];
    if (sample.length) {
      const table = document.createElement('table');
      table.style.width = '100%';
      table.style.borderCollapse = 'collapse';
      table.style.marginTop = '8px';
      const thead = document.createElement('thead');
      thead.innerHTML = `<tr style="background:#f3f3f3"><th style="border:1px solid #ddd;padding:6px">ID</th>
                         <th style="border:1px solid #ddd;padding:6px">Oracle Value</th>
                         <th style="border:1px solid #ddd;padding:6px">BQ Value</th>
                         <th style="border:1px solid #ddd;padding:6px">Status</th></tr>`;
      table.appendChild(thead);
      const tbody = document.createElement('tbody');
      for (const r of sample) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td style="border:1px solid #eee;padding:6px">${escapeHtml(r.id)}</td>
                        <td style="border:1px solid #eee;padding:6px">${escapeHtml(r.oracle)}</td>
                        <td style="border:1px solid #eee;padding:6px">${escapeHtml(r.bq)}</td>
                        <td style="border:1px solid #eee;padding:6px">${escapeHtml(r.status)}</td>`;
        tbody.appendChild(tr);
      }
      table.appendChild(tbody);
      box.appendChild(table);
    }
    container.appendChild(box);
  }
}
// Clear selection button handler
$('clear-selection').addEventListener('click', () => clearSelections());

// initialize pairs list (if any)
renderPairsList();





