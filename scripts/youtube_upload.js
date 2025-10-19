// scripts/youtube_upload.js
// Queue から「最古順で max 本だけ」アップロード。
// 成功したファイルだけ sent/ へ移動。重複タイトルは dups/ へ回収。
// 使い方:
//   単発: node scripts/youtube_upload.js --file=videos/ja/queue/2025-10-18/0001.mp4 --lang=ja
//   連続: node scripts/youtube_upload.js --lang=es --max=3
//
// 必要環境変数:
//   YT_CLIENT_ID / YT_CLIENT_SECRET / (YT_REFRESH_TOKEN_{CC} または YT_REFRESH_TOKEN)
//
// 任意環境変数:
//   YT_SPACING_MS       ... 連投間隔 (ms) デフォ 2000
//   YT_DEDUP_TITLES=1   ... 直近50本のタイトル去重を有効化（既定OFF）

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const { google } = require("googleapis");

// ---- settings ----
const SETTINGS = {
  SPACING_MS: parseInt(process.env.YT_SPACING_MS || "2000", 10),
  DEDUP_TITLES: process.env.YT_DEDUP_TITLES === "1",
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clamp = (s, n) => (s && s.length > n ? s.slice(0, n) : s || "");
const uniq = (arr) => Array.from(new Set(arr || []));
const norm = (p) => p.split(path.sep).join("/");
function ensureArray(x){ return Array.isArray(x) ? x : x ? [x] : []; }
function normTitle(s){ return (s||"").toLowerCase().replace(/\s+/g," ").trim(); }

function detectLangFromPath(p) {
  const m = norm(p).match(/\/?videos\/([^/]+)\/queue\//);
  return m ? m[1] : null;
}
function detectDateDirFromPath(p) {
  const m = norm(p).match(/\/queue\/(\d{4}-\d{2}-\d{2})\//);
  return m ? m[1] : null;
}

// ---- channel meta ----
async function readChannelMeta(lang) {
  const p = path.join("data", "channel_meta", `${lang}.txt`);
  const out = {
    title_suffix: "",
    description: "📌 Daily 10s 'Small Success'. Save and try one today.",
    tags: ["small success", "mindset", "self help"],
    tags_extra: "",
  };
  if (!fs.existsSync(p)) return out;

  const txt = await fsp.readFile(p, "utf8");
  const lines = txt.split(/\r?\n/);
  let curKey = null;
  for (const raw of lines) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const m = line.match(/^([a-zA-Z_]+)\s*=\s*(.*)$/);
    if (m) {
      curKey = m[1];
      const val = m[2] ?? "";
      if (curKey === "title_suffix") out.title_suffix = val;
      else if (curKey === "description") out.description = val;
      else if (curKey === "tags")
        out.tags = val.split(",").map((s) => s.trim()).filter(Boolean);
      else if (curKey === "tags_extra") out.tags_extra = val;
      continue;
    }
    if (curKey === "description") out.description += `\n${line}`;
  }
  out.description = clamp(out.description, 4900);
  out.tags = (out.tags || []).slice(0, 10);
  return out;
}

// ---- auth ----
function ytClientForLang(lang) {
  const cc = (lang || "en").toUpperCase();
  const clientId = process.env.YT_CLIENT_ID;
  const clientSecret = process.env.YT_CLIENT_SECRET;
  const refreshToken =
    process.env[`YT_REFRESH_TOKEN_${cc}`] || process.env.YT_REFRESH_TOKEN;

  if (!clientId || !clientSecret || !refreshToken) {
    throw new Error(
      `YouTube creds missing. Need YT_CLIENT_ID / YT_CLIENT_SECRET / YT_REFRESH_TOKEN_${cc} (or YT_REFRESH_TOKEN)`
    );
  }

  const auth = new google.auth.OAuth2(clientId, clientSecret);
  auth.setCredentials({ refresh_token: refreshToken });
  const yt = google.youtube({ version: "v3", auth });

  // 取り違え検出用：mine チャンネル名を一度だけ出力
  yt.channels
    .list({ part: "snippet", mine: true })
    .then((r) => {
      const ch = r.data.items?.[0]?.snippet?.title || "unknown";
      const tokenName = process.env[`YT_REFRESH_TOKEN_${cc}`]
        ? `YT_REFRESH_TOKEN_${cc}`
        : "YT_REFRESH_TOKEN";
      console.log(`[yt auth] lang=${cc} channel="${ch}" token=${tokenName}`);
    })
    .catch(() => {});

  return yt;
}

// ---- sidecar ----
async function readSidecar(file) {
  const j = file.replace(/\.mp4$/i, ".json");
  if (fs.existsSync(j)) {
    try { return JSON.parse(await fsp.readFile(j, "utf8")) || {}; }
    catch { console.warn("[sidecar parse fail]", j); }
  }
  return {};
}

function buildSnippet(baseTitle, ch, sidecar) {
  const suffix = ch.title_suffix || "";
  const hasSuffix = suffix && (baseTitle.endsWith(suffix) || baseTitle.includes(suffix));
  const title = clamp(hasSuffix ? baseTitle : `${baseTitle}${suffix}`, 100);

  let description = sidecar.description || ch.description || "";
  if (ch.tags_extra) description = clamp(`${description}\n${ch.tags_extra}`, 4900);

  const sideTags = ensureArray(sidecar.tags).map((t)=>String(t||"").trim()).filter(Boolean);
  const tags = uniq(sideTags.length ? sideTags : ch.tags).slice(0,10);

  return { title, description, tags };
}

// ---- movers ----
async function safeMove(src, dest) {
  await fsp
    .rename(src, dest)
    .catch(async () => { await fsp.copyFile(src, dest); await fsp.unlink(src); })
    .catch((e) => { console.warn("[move fail]", src, "->", dest, e?.message || e); });
}

async function moveToSent(file) {
  const dateDir = detectDateDirFromPath(file) || "unknown-date";
  const lang = detectLangFromPath(file);
  if (!lang) throw new Error(`[moveToSent] cannot detect lang from ${file}`);
  const destDir = path.join("videos", lang, "sent", dateDir);
  await fsp.mkdir(destDir, { recursive: true });
  if (fs.existsSync(file)) await safeMove(file, path.join(destDir, path.basename(file)));
  const j = file.replace(/\.mp4$/i, ".json");
  if (fs.existsSync(j)) await safeMove(j, path.join(destDir, path.basename(j)));
  console.log("[moved to sent]", norm(path.join(destDir, path.basename(file))));
}

async function moveToFailed(file) {
  const dateDir = detectDateDirFromPath(file) || "unknown-date";
  const lang = detectLangFromPath(file);
  if (!lang) throw new Error(`[moveToFailed] cannot detect lang from ${file}`);
  const destDir = path.join("videos", lang, "failed", dateDir);
  await fsp.mkdir(destDir, { recursive: true });
  if (fs.existsSync(file)) await safeMove(file, path.join(destDir, path.basename(file)));
  const j = file.replace(/\.mp4$/i, ".json");
  if (fs.existsSync(j)) await safeMove(j, path.join(destDir, path.basename(j)));
  console.warn("[moved to failed]", norm(path.join(destDir, path.basename(file))));
}

async function moveToDups(file) {
  const dateDir = detectDateDirFromPath(file) || "unknown-date";
  const lang = detectLangFromPath(file);
  if (!lang) throw new Error(`[moveToDups] cannot detect lang from ${file}`);
  const destDir = path.join("videos", lang, "dups", dateDir);
  await fsp.mkdir(destDir, { recursive: true });
  if (fs.existsSync(file)) await safeMove(file, path.join(destDir, path.basename(file)));
  const j = file.replace(/\.mp4$/i, ".json");
  if (fs.existsSync(j)) await safeMove(j, path.join(destDir, path.basename(j)));
  console.warn("[moved to dups]", norm(path.join(destDir, path.basename(file))));
}

// ---- pickers ----
async function pickOldest(lang, max = 1) {
  const dir = path.join("videos", lang, "queue");
  if (!fs.existsSync(dir)) return [];
  const dates = fs.readdirSync(dir).filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d)).sort(); // oldest first

  const files = [];
  for (const d of dates) {
    const p = path.join(dir, d);
    const ls = fs.readdirSync(p).filter(f => f.endsWith(".mp4")).sort(); // 0001.mp4..順
    for (const f of ls) {
      files.push(path.join(p, f));
      if (files.length >= max) break;
    }
    if (files.length >= max) break;
  }
  console.log("[pick]", lang, files.map(norm));
  return files;
}

// ---- dedup (optional) ----
async function fetchRecentTitles(yt){
  try {
    const ch = await yt.channels.list({ part:"id", mine:true });
    const channelId = ch.data.items?.[0]?.id;
    if (!channelId) return new Set();
    const r = await yt.search.list({
      part: "snippet", channelId, order: "date", maxResults: 50, type: "video"
    });
    return new Set((r.data.items || []).map(i => normTitle(i.snippet?.title)));
  } catch { return new Set(); }
}

// ---- uploader ----
async function uploadOne(yt, file, lang, sidecar = {}) {
  const ch = await readChannelMeta(lang);
  const baseTitle = clamp(sidecar.title || path.basename(file, ".mp4"), 100);
  const { title, description, tags } = buildSnippet(baseTitle, ch, sidecar);

  const req = {
    part: "snippet,status",
    requestBody: {
      snippet: { title, description, tags, categoryId: "27" }, // Education
      status: { privacyStatus: "public", selfDeclaredMadeForKids: false },
    },
    media: { body: fs.createReadStream(file) },
  };

  // ★ 既存の yt クライアントをそのまま使う（auth 再注入しない）
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await yt.videos.insert(req);
      const vid = res?.data?.id;
      if (!vid) throw new Error("no video id in response");
      console.log("[uploaded]", path.basename(file), vid);
      return { vid, title };
    } catch (e) {
      const code = e?.code || e?.response?.status;
      const retriable = code === 429 || (code >= 500 && code < 600);
      console.warn(
        `[upload fail] ${path.basename(file)} (attempt ${attempt})`,
        code,
        e?.message || e
      );
      if (retriable && attempt < 3) {
        await sleep(1500 * attempt);
        continue;
      }
      throw e;
    }
  }
}

// ---- main ----
async function main() {
  const fileArg =
    (process.argv.find(a => a.startsWith("--file=")) || "").split("=")[1] || "";
  const langArg =
    (process.argv.find(a => a.startsWith("--lang=")) || "").split("=")[1] || "en";
  const maxArg = parseInt(
    (process.argv.find(a => a.startsWith("--max=")) || "").split("=")[1] || "1", 10
  );

  const yt = ytClientForLang(langArg);
  const recent = SETTINGS.DEDUP_TITLES ? await fetchRecentTitles(yt) : new Set();

  // ---- single file ----
  if (fileArg) {
    if (!fs.existsSync(fileArg)) {
      console.warn("[skip] not found:", norm(fileArg));
      return;
    }
    const sidecar = await readSidecar(fileArg);

    if (SETTINGS.DEDUP_TITLES) {
      const ch = await readChannelMeta(langArg);
      const baseTitle = clamp(sidecar.title || path.basename(fileArg, ".mp4"), 100);
      const title = clamp(
        (baseTitle.endsWith(ch.title_suffix) || baseTitle.includes(ch.title_suffix))
          ? baseTitle : `${baseTitle}${ch.title_suffix}`, 100);
      if (recent.has(normTitle(title))) {
        console.log("[skip dup-title]", title);
        await moveToDups(fileArg);
        return;
      }
    }

    try {
      await uploadOne(yt, fileArg, langArg, sidecar);
      await moveToSent(fileArg);
    } catch (e) {
      await moveToFailed(fileArg);
      throw e;
    }
    return;
  }

  // ---- batch ----
  const batch = await pickOldest(langArg, maxArg);
  if (!batch.length) {
    console.log("[skip] no files in queue");
    return;
  }

  let done = 0;
  for (const f of batch) {
    if (done >= maxArg) break;

    const sidecar = await readSidecar(f);

    if (SETTINGS.DEDUP_TITLES) {
      try {
        const ch = await readChannelMeta(langArg);
        const baseTitle = clamp(sidecar.title || path.basename(f, ".mp4"), 100);
        const title = clamp(
          (baseTitle.endsWith(ch.title_suffix) || baseTitle.includes(ch.title_suffix))
            ? baseTitle : `${baseTitle}${ch.title_suffix}`, 100);
        if (recent.has(normTitle(title))) {
          console.log("[skip dup-title]", title);
          await moveToDups(f);
          continue;
        }
      } catch {}
    }

    try {
      const { title } = await uploadOne(yt, f, langArg, sidecar);
      if (SETTINGS.DEDUP_TITLES) recent.add(normTitle(title));
      await moveToSent(f);
      done++;
    } catch (e) {
      await moveToFailed(f);
      console.warn("[upload fail]", path.basename(f), e?.message || e);
    }

    if (done < maxArg) await sleep(SETTINGS.SPACING_MS);
  }

  console.log(`[done] uploaded ${done} file(s) for ${langArg}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
