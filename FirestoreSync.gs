/***** FirestoreSync.gs (UqudoBI) *****/
const TZ = 'Asia/Dubai';
const BATCH_LIMIT = 500; // Firestore batchWrite limit
const EXCLUDE_SHEETS = []; // e.g., ['Lookups','Notes'] if you want to skip any

/** Config from Script Properties **/
function cfg_() {
  const p = PropertiesService.getScriptProperties();
  const projectId = p.getProperty('FIREBASE_PROJECT_ID');
  const clientEmail = p.getProperty('SA_CLIENT_EMAIL');
  let privateKey = p.getProperty('SA_PRIVATE_KEY');
  if (!projectId || !clientEmail || !privateKey) {
    throw new Error('Missing FIREBASE_PROJECT_ID / SA_CLIENT_EMAIL / SA_PRIVATE_KEY in Script Properties.');
  }
  privateKey = privateKey.replace(/\\n/g, '\n');
  return { projectId, clientEmail, privateKey };
}

/** Get OAuth access token for Firestore using SA JWT **/
function getAccessToken_() {
  const { clientEmail, privateKey } = cfg_();
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: 'RS256', typ: 'JWT' };
  const payload = {
    iss: clientEmail,
    scope: 'https://www.googleapis.com/auth/datastore',
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600
  };
  const enc = (o) => Utilities.base64EncodeWebSafe(JSON.stringify(o)).replace(/=+$/, '');
  const toSign = enc(header) + '.' + enc(payload);
  const signature = Utilities.base64EncodeWebSafe(Utilities.computeRsaSha256Signature(toSign, privateKey));
  const jwt = toSign + '.' + signature;

  const res = UrlFetchApp.fetch('https://oauth2.googleapis.com/token', {
    method: 'post',
    contentType: 'application/x-www-form-urlencoded',
    payload: { grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: jwt },
    muteHttpExceptions: true
  });
  const data = JSON.parse(res.getContentText());
  if (!data.access_token) throw new Error('Token error: ' + res.getResponseCode() + ' ' + res.getContentText());
  return data.access_token;
}

/** Minimal type mapping: numbers, booleans, Date objects → timestamp; strings kept as strings **/
function toFsValue_(v) {
  if (v === '' || v === null || v === undefined) return { nullValue: null };
  if (v instanceof Date) return { timestampValue: new Date(v).toISOString() };
  const t = typeof v;
  if (t === 'number') return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (t === 'boolean') return { booleanValue: v };
  return { stringValue: String(v) };
}

function isEmptyRow_(arr) {
  return arr.every(v => v === '' || v === null || v === undefined);
}

function sanitizeId_(s) {
  return String(s).trim().replace(/[\/\s]+/g, '_').substring(0, 1500); // avoid slashes & overlong IDs
}

/** Push a single Sheet to Firestore (collection = sheet name) **/
function pushSheet_(sheet, token) {
  const { projectId } = cfg_();
  const name = sheet.getName();
  if (sheet.isSheetHidden() || EXCLUDE_SHEETS.includes(name)) return;

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) return; // header + at least 1 row
  const headers = values[0].map(h => String(h).trim() || '');
  const idIdx = headers.findIndex(h => h.toLowerCase() === 'id');

  const writes = [];
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    if (isEmptyRow_(row)) continue;

    const docId = sanitizeId_(idIdx >= 0 && row[idIdx] !== '' ? row[idIdx] : `ROW_${r + 1}`);
    const fields = {};
    headers.forEach((h, c) => {
      const key = h || `Col${c + 1}`;
      fields[key] = toFsValue_(row[c]);
    });

    writes.push({
      update: {
        name: `projects/${projectId}/databases/masterdata/documents/${encodeURIComponent(name)}/${encodeURIComponent(docId)}`,
        fields
      },
      updateMask: { fieldPaths: Object.keys(fields) }
    });
  }
  if (!writes.length) return;

  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/masterdata/documents:batchWrite`;
  for (let i = 0; i < writes.length; i += BATCH_LIMIT) {
    const chunk = writes.slice(i, i + BATCH_LIMIT);
    const res = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'application/json',
      headers: { Authorization: 'Bearer ' + token },
      payload: JSON.stringify({ writes: chunk }),
      muteHttpExceptions: true
    });
    const code = res.getResponseCode();
    if (code >= 300) {
      throw new Error(`batchWrite failed for sheet "${name}" (HTTP ${code}): ${res.getContentText()}`);
    }
  }
}

/** Public: push ALL visible tabs **/
function pushAllTabsToFirestore() {
  const ss = SpreadsheetApp.getActive();
  const token = getAccessToken_();
  ss.getSheets().forEach(sh => pushSheet_(sh, token));
  ss.toast('Firestore sync complete', 'Data Sync', 5);
}

/** Optional: push only active tab **/
function pushActiveSheetToFirestore() {
  const ss = SpreadsheetApp.getActive();
  const token = getAccessToken_();
  pushSheet_(ss.getActiveSheet(), token);
  ss.toast('Active sheet synced', 'Data Sync', 5);
}

/** Add menu **/
function buildDataSyncMenu_() {
  SpreadsheetApp.getUi()
    .createMenu('Data Sync')
    .addItem('Push all tabs to Firestore', 'pushAllTabsToFirestore')
    .addItem('Push active tab only', 'pushActiveSheetToFirestore')
    .addToUi();
}

/** Schedule daily sync at 23:55 (Asia/Dubai) **/
function createDailySyncTrigger() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction && t.getHandlerFunction() === 'pushAllTabsToFirestore')
    .forEach(ScriptApp.deleteTrigger);
  ScriptApp.newTrigger('pushAllTabsToFirestore')
    .timeBased()
    .atHour(23)      // project timeZone should be Asia/Dubai
    .nearMinute(55)
    .everyDays(1)
    .create();
}
