/***** CONFIG *****/
const CONFIG = {
  PROJECT_ID: 'masterdata-470911',
  DATASET_ID: 'master_data',

  // Map when tableId differs from sheet name:
  TABLE_NAME_MAP: {
    // 'Sheet Name': 'table_id'
  },

  INCLUDE_HIDDEN_SHEETS: false,

  // Type inference: 'basic' or 'all_string'
  TYPE_INFERENCE: 'basic',

  WRITE_DISPOSITION: 'WRITE_TRUNCATE',
  FIELD_DELIMITER: ',',
  ALLOW_QUOTED_NEWLINES: true,

  JOB_POLL_SECONDS: 3,
  JOB_POLL_MAX_TRIES: 60
};

/***** MENU *****/
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('BigQuery Sync')
    .addItem('Populate (All sheets)', 'bqPopulateAll')
    .addItem('Populate (Current sheet)', 'bqPopulateCurrentSheet')
    .addSeparator()
    .addItem('Environment check', 'bqEnvironmentCheck')
    .addToUi();
}

/***** ENTRY POINTS *****/
function bqPopulateAll() {
  assertEnvReady_();
  const ss = SpreadsheetApp.getActive();
  const sheets = ss.getSheets().filter(sh => CONFIG.INCLUDE_HIDDEN_SHEETS || !sh.isSheetHidden());
  const out = [];

  for (const sh of sheets) {
    try {
      ss.toast(`Loading: ${sh.getName()}…`);
      const info = loadSheetToBigQuery_(sh);
      out.push(`✔ ${sh.getName()} → ${info.tableId}: ${info.loadedRows} rows (job: ${info.jobId})`);
    } catch (e) {
      out.push(`✖ ${sh.getName()} :: ${e && e.message ? e.message : e}`);
    }
  }
  SpreadsheetApp.getUi().alert(out.join('\n'));
}

function bqPopulateCurrentSheet() {
  assertEnvReady_();
  const sh = SpreadsheetApp.getActiveSheet();
  try {
    const info = loadSheetToBigQuery_(sh);
    SpreadsheetApp.getUi().alert(`✔ ${sh.getName()} → ${info.tableId}: ${info.loadedRows} rows (job: ${info.jobId})`);
  } catch (e) {
    SpreadsheetApp.getUi().alert(`✖ ${sh.getName()} :: ${e && e.message ? e.message : e}`);
  }
}

function bqEnvironmentCheck() {
  const messages = [];
  try {
    assertBigQueryNamespace_();
    messages.push('BigQuery Advanced Service: OK');
  } catch (e) {
    messages.push(`BigQuery Advanced Service: NOT READY → ${e.message}`);
  }
  messages.push(`Project ID: ${CONFIG.PROJECT_ID || '(missing)'}`);
  messages.push(`Dataset ID: ${CONFIG.DATASET_ID || '(missing)'}`);
  SpreadsheetApp.getUi().alert(messages.join('\n'));
}

/***** CORE LOAD *****/
function loadSheetToBigQuery_(sheet) {
  const projectId = CONFIG.PROJECT_ID;
  const datasetId = CONFIG.DATASET_ID;
  if (!projectId || !datasetId) throw new Error('Set CONFIG.PROJECT_ID and CONFIG.DATASET_ID first.');

  const rawName = sheet.getName();
  const tableId = (CONFIG.TABLE_NAME_MAP[rawName] || sanitizeTableId_(rawName));

  const values = sheet.getDataRange().getValues();
  if (!values || values.length === 0 || values[0].length === 0) throw new Error('No data found.');

  const header = values[0].map(v => String(v || '').trim());
  const dataRows = values.slice(1);

  // Build schema (from header + data)
  const schemaFields = (CONFIG.TYPE_INFERENCE === 'all_string')
    ? header.map((h, idx) => ({ name: sanitizeFieldName_(h, idx), type: 'STRING', mode: 'NULLABLE' }))
    : inferSchema_(header, dataRows);

  // Prepare CSV (include header as first row; job will skip it)
  const csv = arrayToCsvWithSchema_(values, CONFIG.FIELD_DELIMITER, schemaFields);
  const blob = Utilities.newBlob(csv, 'application/octet-stream', `${tableId}.csv`);

  // Build job config; attach schema only if table does NOT exist
  const exists = tableExists_(projectId, datasetId, tableId);
  const loadCfg = {
    destinationTable: { projectId, datasetId, tableId },
    writeDisposition: CONFIG.WRITE_DISPOSITION,
    sourceFormat: 'CSV',
    fieldDelimiter: CONFIG.FIELD_DELIMITER,
    skipLeadingRows: 1,
    allowQuotedNewlines: CONFIG.ALLOW_QUOTED_NEWLINES,
    encoding: 'UTF-8'
  };
  if (!exists) {
    loadCfg.schema = { fields: schemaFields };
  }

  const job = { configuration: { load: loadCfg } };

  // Run job
  const insertedJob = BigQuery.Jobs.insert(job, projectId, blob);
  const jobId = insertedJob.jobReference.jobId;
  const status = waitForJobDone_(projectId, jobId);

  if (status.errorResult) {
    const details = (status.errors || [])
      .map(e => `${e.message} [${e.reason}]${e.location ? ' @ '+e.location : ''}`)
      .join('; ');
    throw new Error(`BigQuery load failed: ${status.errorResult.message}${details ? ' | ' + details : ''}`);
  }

  const loadedRows = Math.max(0, values.length - 1);
  return { tableId, jobId, loadedRows };
}

/***** HELPERS *****/
function waitForJobDone_(projectId, jobId) {
  for (let i = 0; i < CONFIG.JOB_POLL_MAX_TRIES; i++) {
    const job = BigQuery.Jobs.get(projectId, jobId);
    if (job.status && job.status.state === 'DONE') return job.status;
    Utilities.sleep(CONFIG.JOB_POLL_SECONDS * 1000);
  }
  throw new Error('Timed out waiting for BigQuery job to complete.');
}

function tableExists_(projectId, datasetId, tableId) {
  try {
    BigQuery.Tables.get(projectId, datasetId, tableId);
    return true;
  } catch (e) {
    return false; // 404 or permission → treat as not existing
  }
}

function sanitizeTableId_(name) {
  return name.trim()
    .replace(/\s+/g, '_')
    .replace(/[^A-Za-z0-9_]/g, '_')
    .replace(/^_+/, '')
    .slice(0, 1024);
}

function sanitizeFieldName_(h, idx) {
  let n = (h || '').trim();
  if (!n) n = `col_${idx + 1}`;
  n = n.replace(/\s+/g, '_').replace(/[^A-Za-z0-9_]/g, '_');
  if (!/^[A-Za-z_]/.test(n)) n = '_' + n; // must start with letter/underscore
  return n.slice(0, 300);
}

function inferSchema_(header, rows) {
  const fields = [];
  for (let c = 0; c < header.length; c++) {
    const name = sanitizeFieldName_((header[c] || ''), c);
    let seenNumber = false, seenFloat = false, seenBool = false, seenDate = false, seenString = false;

    for (let r = 0; r < rows.length; r++) {
      const v = rows[r][c];
      if (v === '' || v === null || v === undefined) continue;

      if (typeof v === 'number') {
        seenNumber = true;
        if (Math.floor(v) !== v) seenFloat = true;
        continue;
      }
      if (typeof v === 'boolean') { seenBool = true; continue; }
      if (v instanceof Date) { seenDate = true; continue; }
      // Anything else as string
      const s = String(v).trim();
      if (s.length) seenString = true;
    }

    let type = 'STRING';
    if (!seenString && (seenNumber || seenBool || seenDate)) {
      if (seenBool && !seenNumber && !seenDate) type = 'BOOLEAN';
      else if (seenDate && !seenNumber && !seenBool) type = 'DATETIME'; // safe across timezones
      else if (seenNumber && !seenBool && !seenDate) type = seenFloat ? 'FLOAT' : 'INTEGER';
      else type = 'STRING'; // mixed types → string
    }

    fields.push({ name, type, mode: 'NULLABLE' });
  }
  return fields;
}

function arrayToCsvWithSchema_(values, delim, schemaFields) {
  // Row 0 = header (left as-is; load job skips it)
  const headerRow = values[0].map(v => csvEscape_(String(v ?? ''), delim)).join(delim);

  const dataLines = [];
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const cols = row.map((v, c) => {
      const t = schemaFields[c]?.type || 'STRING';
      const s = formatByType_(v, t);
      return csvEscape_(s, delim);
    });
    dataLines.push(cols.join(delim));
  }
  return [headerRow].concat(dataLines).join('\n');
}

function formatByType_(value, type) {
  if (value === null || value === undefined || value === '') return '';
  switch (type) {
    case 'INTEGER':
    case 'FLOAT':
      return (typeof value === 'number') ? String(value) : String(value);
    case 'BOOLEAN':
      if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
      const sv = String(value).trim().toLowerCase();
      if (sv === 'true' || sv === 'false') return sv.toUpperCase();
      return String(value);
    case 'DATETIME':
      if (value instanceof Date) {
        return Utilities.formatDate(value, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
      }
      return String(value);
    case 'STRING':
    default:
      return String(value);
  }
}

function csvEscape_(s, delim) {
  const mustQuote = s.includes('"') || s.includes('\n') || s.includes('\r') || s.includes(delim);
  const escaped = s.replace(/"/g, '""');
  return mustQuote ? `"${escaped}"` : escaped;
}

function assertEnvReady_() {
  assertBigQueryNamespace_();
  if (!CONFIG.PROJECT_ID || !CONFIG.DATASET_ID) {
    throw new Error('Missing CONFIG.PROJECT_ID or CONFIG.DATASET_ID.');
  }
}

function assertBigQueryNamespace_() {
  if (typeof BigQuery === 'undefined' || !BigQuery.Jobs || !BigQuery.Jobs.insert) {
    throw new Error(
      'BigQuery Advanced Service is not available. In the Apps Script editor: Services (puzzle icon) → + Add a service → BigQuery API. ' +
      'Also enable the BigQuery API in the linked GCP project.'
    );
  }
}
