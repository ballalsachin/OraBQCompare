const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const oracledb = require('oracledb');

let pool = null;

console.log("main.js has started executing!");
console.log("Current environment:", process.env.NODE_ENV); // Example with a variable
console.log("Timestamp:", new Date().toISOString());

// BigQuery client will be created on demand and cached per connection
// BigQuery state
let bigqueryClient = null;
let bigqueryConfig = null;
let bigqueryCreds = null;

function createWindow() {
  const win = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js')
    }
  });
  win.loadFile('index.html');
}

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('before-quit', async () => {
  try { if (pool) await pool.close(0); } catch(e) {}
});

ipcMain.handle('connect-oracle', async (event, cfg) => {
  try {
    // Optional: init thick client if libDir provided
    if (cfg.libDir && !oracledb.oracleClientVersion) {
      oracledb.initOracleClient({ libDir: cfg.libDir });
    }
    const connectString = `${cfg.hostname}:${cfg.port}/${cfg.service || cfg.sid || cfg.database || ''}`;
    pool = await oracledb.createPool({
      user: cfg.username,
      password: cfg.password,
      connectString,
      poolMin: 0,
      poolMax: 4
    });
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
});

ipcMain.handle('disconnect-oracle', async () => {
  try { if (pool) { await pool.close(0); pool = null; } return { ok: true }; }
  catch (err) { return { ok:false, error: err.message }; }
});

async function withConn(fn) {
  if (!pool) throw new Error('Not connected');
  const conn = await pool.getConnection();
  try {
    return await fn(conn);
  } finally {
    try { await conn.close(); } catch (e) {}
  }
}

ipcMain.handle('list-tables', async () => {
  try {
    const result = await withConn(async conn => {
      const sql = `SELECT table_name FROM user_tables ORDER BY table_name`;
      return await conn.execute(sql, [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
    });
    const tables = (result.rows || []).map(r => r.TABLE_NAME);
    return { ok: true, tables };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
});

ipcMain.handle('list-columns', async (event, tableName) => {
  try {
    const result = await withConn(async conn => {
      const sql = `SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :t ORDER BY column_id`;
      return await conn.execute(sql, { t: tableName.toUpperCase() }, { outFormat: oracledb.OUT_FORMAT_OBJECT });
    });
    const columns = (result.rows || []).map(r => ({ name: r.COLUMN_NAME, type: r.DATA_TYPE }));
    return { ok: true, columns };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
});

// ---------- BigQuery IPC handlers ----------
// ---------- BigQuery handlers (file-picker, connect, disconnect, list) ----------
ipcMain.handle('connect-bq', async (event, cfg) => {
  try {
    // cfg: { projectId, datasetId, serviceAccountJson }
    if (!cfg || !cfg.serviceAccountJson) throw new Error('Service account JSON not provided');
    let parsed;
    try { parsed = JSON.parse(cfg.serviceAccountJson); }
    catch (e) { throw new Error('Service account JSON is invalid: ' + e.message); }
    bigqueryCreds = parsed; // keep in memory only
    const { BigQuery } = require('@google-cloud/bigquery');
    bigqueryClient = new BigQuery({ projectId: cfg.projectId, credentials: bigqueryCreds });
    bigqueryConfig = { projectId: cfg.projectId, datasetId: cfg.datasetId };
    // validate dataset access
    const dataset = bigqueryClient.dataset(cfg.datasetId);
    await dataset.get({ autoCreate: false });
    return { ok: true };
  } catch (err) {
    bigqueryClient = null;
    bigqueryConfig = null;
    bigqueryCreds = null;
    console.error('connect-bq error:', err);
    return { ok: false, error: err.message || String(err) };
  }
});

ipcMain.handle('disconnect-bq', async () => {
  try {
    bigqueryClient = null;
    bigqueryConfig = null;
    bigqueryCreds = null;
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
});

ipcMain.handle('bq-list-tables', async () => {
  try {
    if (!bigqueryClient || !bigqueryConfig) throw new Error('Not connected to BigQuery');
    const dataset = bigqueryClient.dataset(bigqueryConfig.datasetId);
    const [tables] = await dataset.getTables();
    const tableNames = tables.map(t => t.id || (t.metadata && t.metadata.tableReference && t.metadata.tableReference.tableId)).filter(Boolean);
    return { ok: true, tables: tableNames };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
});

ipcMain.handle('bq-list-columns', async (event, tableId) => {
  try {
    if (!bigqueryClient || !bigqueryConfig) throw new Error('Not connected to BigQuery');
    const table = bigqueryClient.dataset(bigqueryConfig.datasetId).table(tableId);
    const [metadata] = await table.getMetadata();
    const schema = (metadata.schema && metadata.schema.fields) || [];
    const columns = schema.map(f => ({ name: f.name, type: f.type || '' }));
    return { ok: true, columns };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
});

// IPC handler: compare multiple column pairs
ipcMain.handle('compare-pairs', async (event, pairs) => {
  try {
    if (!Array.isArray(pairs) || pairs.length === 0) return { ok: false, error: 'No pairs provided' };

    const results = [];

    for (const p of pairs) {
      // p.oracle.table, p.oracle.name ; p.bq.table, p.bq.name
      const pairId = `${p.oracle.table}::${p.oracle.name}__${p.bq.table}::${p.bq.name}`;

      // Decide join key column name (try ID then id)
      const idCandidates = ['ID', 'id'];
      const oracleIdCol = idCandidates[0]; // change here if different key name needed
      const bqIdCol = idCandidates[0];

      // Fetch Oracle data: id -> value
      const oracleMap = new Map();
      try {
        if (!pool) throw new Error('Not connected to Oracle');
        const conn = await pool.getConnection();
        try {
          const oraSql = `SELECT ${oracleIdCol} AS IDX, "${p.oracle.name}" AS VAL FROM ${p.oracle.table}`;
          const oraRes = await conn.execute(oraSql, [], { outFormat: oracledb.OUT_FORMAT_OBJECT });
          for (const row of oraRes.rows || []) {
            const idv = row.IDX;
            const val = row.VAL;
            if (idv !== null && idv !== undefined) oracleMap.set(String(idv), val);
          }
        } finally { try { await conn.close(); } catch(_) {} }
      } catch (err) {
        results.push({ pairId, error: 'Oracle read error: ' + (err.message || String(err)) });
        continue;
      }

      // Fetch BQ data: id -> value
      const bqMap = new Map();
      try {
        if (!bigqueryClient || !bigqueryConfig) throw new Error('Not connected to BigQuery');
        const project = bigqueryConfig.projectId;
        const dataset = bigqueryConfig.datasetId;
        const tableId = p.bq.table;
        // Fully-qualified table reference
        const fullTable = `\`${project}.${dataset}.${tableId}\``;
        const bqQuery = `SELECT ${bqIdCol} AS IDX, ${p.bq.name} AS VAL FROM ${fullTable}`;
        const [job] = await bigqueryClient.createQueryJob({ query: bqQuery, useLegacySql: false });
        const [rows] = await job.getQueryResults();
        for (const r of rows || []) {
          const idv = r.IDX;
          const val = r.VAL;
          if (idv !== null && idv !== undefined) bqMap.set(String(idv), val);
        }
      } catch (err) {
        results.push({ pairId, error: 'BigQuery read error: ' + (err.message || String(err)) });
        continue;
      }

      // Compare maps
      const matched = [];
      const mismatched = [];
      const inOracleNotBQ = [];
      const inBQNotOracle = [];

      // iterate oracle keys
      for (const [id, oVal] of oracleMap.entries()) {
        if (bqMap.has(id)) {
          const bVal = bqMap.get(id);
          const oNorm = normalizeForCompare(oVal);
          const bNorm = normalizeForCompare(bVal);
          if (oNorm === bNorm) matched.push({ id, oracle: oVal, bq: bVal });
          else mismatched.push({ id, oracle: oVal, bq: bVal });
          bqMap.delete(id);
        } else {
          inOracleNotBQ.push({ id, oracle: oVal });
        }
      }
      // remaining in bqMap are not in Oracle
      for (const [id, bVal] of bqMap.entries()) inBQNotOracle.push({ id, bq: bVal });

      // prepare summary and sample rows (limit)
      const sampleLimit = 200;
      const sampleRows = [];
      for (const x of mismatched.slice(0, sampleLimit)) sampleRows.push({ id: x.id, oracle: safeString(x.oracle), bq: safeString(x.bq), status: 'MISMATCH' });
      for (const x of inOracleNotBQ.slice(0, Math.max(0, sampleLimit - sampleRows.length))) sampleRows.push({ id: x.id, oracle: safeString(x.oracle), bq: null, status: 'MISSING_IN_BQ' });
      for (const x of inBQNotOracle.slice(0, Math.max(0, sampleLimit - sampleRows.length))) sampleRows.push({ id: x.id, oracle: null, bq: safeString(x.bq), status: 'MISSING_IN_ORACLE' });

      results.push({
        pairId,
        pair: p,
        counts: { matched: matched.length, mismatched: mismatched.length, inOracleNotBQ: inOracleNotBQ.length, inBQNotOracle: inBQNotOracle.length },
        sample: sampleRows
      });
    } // end for pairs

    return { ok: true, results };
  } catch (err) {
    console.error('compare-pairs error', err);
    return { ok: false, error: err.message || String(err) };
  }

  // helpers
  function normalizeForCompare(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === 'string') return v.trim();
    if (typeof v === 'number' || typeof v === 'bigint') return String(v);
    if (v instanceof Date) return v.toISOString();
    return String(v).trim();
  }
  function safeString(v) {
    if (v === null || v === undefined) return null;
    return String(v);
  }
});

