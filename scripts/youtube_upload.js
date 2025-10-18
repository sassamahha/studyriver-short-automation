// scripts/youtube_upload.js
// Upload from videos/{lang}/queue -> move to sent (or failed/dups)
// usage:
//   single: node scripts/youtube_upload.js --file=videos/fr/queue/2025-10-15/0001.mp4 --lang=fr
//   batch : node scripts/youtube_upload.js --lang=fr --max=2
//
// 必要な環境変数：
//   YT_CLIENT_ID / YT_CLIENT_SECRET / (YT_REFRESH_TOKEN_{CC} または YT_REFRESH_TOKEN)
//
// 主要機能：
// - queue だけを見る（単発でも queue 外はスキップ）
// - プリフライト内蔵（存在/サイズ/ストリーム/尺/解像度/mtimeクールダウン/任意blackdetect）
// - 失敗は failed/、直近50本とタイトル重複は dups/ へ退避
// - 成功時のみ sent/ へ移動（いずれも日付ディレクトリ維持）
// - 直近タイトルの集合を起動時取得し、同ラン内も去重
// - sidecar (.json) 併走、メタは言語別 channel_meta/{lang}.txt をマージ
// - ログ強化／タグ上限／説明長クリップ

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const { google } = require("googleapis");
const { spawnSync } = require("child_process");

// ---------------- preflight config (env overridable) ----------------
const PREFLIGHT = {
  MIN_SIZE: parseInt(process.env.PREFLIGHT_MIN_SIZE || "1000000", 10), // >=1MB
  MIN_DUR : parseFloat(process.env.PREFLIGHT_MIN_DUR  || "8"),         // 8s
  MAX_DUR : parseFloat(process.env.PREFLIGHT_MAX_DUR  || "60"),        // 60s
  MIN_W   : parseInt(process.env.PREFLIGHT_MIN_W      || "720", 10),
  MIN_H   : parseInt(process.env.PREFLIGHT_MIN_H      || "1280", 10),
  MTIME_COOLDOWN_S: parseInt(process.env.PREFLIGHT_MTIME_COOLDOWN_S || "30", 10),
  CHECK_BLACK: process.env.PREFLIGHT_CHECK_BLACK === "1",
};

// ---------------- utils ----------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clamp = (s, n) => (s && s.length > n ? s.slice(0, n) : s || "");
const uniq = (arr) => Array.from(new Set(arr || []));

function norm(p) { return p.split(path.sep).join("/"); }
function detectLangFromPath(p) {
  const m = norm(p).match(/\/?videos\/([^/]+)\/queue\//);
  return m ? m[1] : null;
}
function isInQueuePath(p){
  return /\/videos\/[^/]+\/queue\//.test(norm(p));
}
function detectDateDirFromPath(p) {
  const m = norm(p).match(/\/queue\/(\d{4}-\d{2}-\d{2})\//);
  return m ? m[1] : null;
}
function ensureArray(x) { return Array.isArray(x) ? x : x ? [x] : []; }
function normTitle(s){ return (s||"").toLowerCase().replace(/\s+/g," ").trim(); }

// ---------------- channel meta (title/desc/tags per language) ----------------
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

// ---------------- youtube auth (ID/SECRET + refresh_token_{CC}) ----------------
function ytClientForLang(lang) {
  const cc = (lang || "en").toUpperCase(); // en -> EN
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

// ---------------- sidecar meta (####.json) ----------------
async function readSidecar(file) {
  const j = file.replace(/\.mp4$/i, ".json");
  if (fs.existsSync(j)) {
    try {
      const obj = JSON.parse(await fsp.readFile(j, "utf8"));
      return obj || {};
    } catch (_) {
      console.warn("[sidecar parse fail]", j);
    }
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

// ---------------- move helpers ----------------
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

  await safeMove(file, path.join(destDir, path.basename(file)));

  const j = file.replace(/\.mp4$/i, ".json");
  if (fs.existsSync(j)) {
    await safeMove(j, path.join(destDir, path.basename(j)));
  }

  console.log("[moved to sent]", norm(path.join(destDir, path.basename(file))));
}

async function moveToFailed(file) {
  const dateDir = detectDateDirFromPath(file) || "unknown-date";
  const lang = detectLangFromPath(file);
  if (!lang) throw new Error(`[moveToFailed] cannot detect lang from ${file}`);

  const destDir = path.join("videos", lang, "failed", dateDir);
  await fsp.mkdir(destDir, { recursive: true });

  // ファイルが消えている場合は何もしない
  if (fs.existsSync(file)) {
    await safeMove(file, path.join(destDir, path.basename(file)));
  }

  const j = file.replace(/\.mp4$/i, ".json");
  if (fs.existsSync(j)) {
    await safeMove(j, path.join(destDir, path.basename(j)));
  }

  console.warn("[moved to failed]", norm(path.join(destDir, path.basename(file))));
}

async function moveToDups(file) {
  const dateDir = detectDateDirFromPath(file) || "unknown-date";
  const lang = detectLangFromPath(file);
  if (!lang) throw new Error(`[moveToDups] cannot detect lang from ${file}`);

  const destDir = path.join("videos", lang, "dups", dateDir);
  await fsp.mkdir(destDir, { recursive: true });

  if (fs.existsSync(file)) {
    await safeMove(file, path.join(destDir, path.basename(file)));
  }

  const j = file.replace(/\.mp4$/i, ".json");
  if (fs.existsSync(j)) {
    await safeMove(j, path.join(destDir, path.basename(j)));
  }

  console.warn("[moved to dups]", norm(path.join(destDir, path.basename(file))));
}

// ---------------- pick batch ----------------
async function pickBatch(lang, max = 1) {
  const dir = path.join("videos", lang, "queue");
  if (!fs.existsSync(dir)) return [];
  const dates = fs
    .readdirSync(dir)
    .filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d))
    .sort(); // oldest first

  const files = [];
  for (const d of dates) {
    const p = path.join(dir, d);
    const ls = fs.readdirSync(p).filter((f) => f.endsWith(".mp4")).sort();
    for (const f of ls) {
      files.push(path.join(p, f));
      if (files.length >= max) break;
    }
    if (files.length >= max) break;
  }
  console.log("[pick]", lang, files.map(norm));
  return files;
}

// ---------------- title dedup (YouTube recent 50) ----------------
async function recentTitlesSet(yt){
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

async function predictTitle(file, lang, sidecar){
  const ch = await readChannelMeta(lang);
  const baseTitle = clamp(sidecar.title || path.basename(file, ".mp4"), 100);
  const { title } = buildSnippet(baseTitle, ch, sidecar);
  return title;
}

// ---------------- preflight (built-in) ----------------
function preflightProbe(file){
  const p = spawnSync("ffprobe", [
    "-v","error","-print_format","json","-show_streams","-show_format", file
  ], { encoding:"utf8" });
  if (p.status !== 0) {
    throw new Error(`ffprobe failed: ${p.stderr || p.stdout || p.status}`);
  }
  const info = JSON.parse(p.stdout || "{}");
  return info;
}

function assertPreflight(file, cfg = PREFLIGHT){
  if (!fs.existsSync(file)) throw new Error("file not found");
  const st = fs.statSync(file);
  if (!st.isFile()) throw new Error("not a file");
  if (st.size < cfg.MIN_SIZE) throw new Error(`file too small: ${st.size} bytes`);
  const ageSec = (Date.now() - st.mtimeMs)/1000;
  if (ageSec < cfg.MTIME_COOLDOWN_S) {
    throw new Error(`file too fresh (mtime ${ageSec.toFixed(1)}s ago)`);
  }

  const info = preflightProbe(file);
  const vStreams = (info.streams||[]).filter(s=>s.codec_type==="video");
  if (vStreams.length === 0) throw new Error("no video stream");

  const fmt = info.format || {};
  const dur = parseFloat(fmt.duration || vStreams[0].duration || "0");
  if (!isFinite(dur) || dur < cfg.MIN_DUR || dur > cfg.MAX_DUR) {
    throw new Error(`bad duration: ${dur}s (expected ${cfg.MIN_DUR}..${cfg.MAX_DUR})`);
  }

  const w = vStreams[0].width || 0;
  const h = vStreams[0].height || 0;
  if (w < cfg.MIN_W || h < cfg.MIN_H) {
    throw new Error(`resolution too small: ${w}x${h} (>= ${cfg.MIN_W}x${cfg.MIN_H})`);
  }

  if (cfg.CHECK_BLACK){
    const r = spawnSync("ffmpeg", [
      "-hide_banner","-nostats","-v","error","-i", file,
      "-vf","blackdetect=d=0.2:pic_th=0.98","-f","null","-"
    ], { encoding:"utf8" });
    const hits = (r.stderr || "").match(/black_start/g);
    if (hits && hits.length > 0) throw new Error(`black frames detected: ${hits.length}`);
  }

  return { duration: dur, width: w, height: h, size: st.size };
}

// ---------------- uploader (with small retry) ----------------
async function uploadOne(yt, file, lang, sidecar = {}) {
  console.log("[try upload]", norm(file), "lang=", lang);

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

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await yt.videos.insert(req);
      const vid = res?.data?.id;
      if (!vid) throw new Error("no video id in response");
      console.log("[uploaded]", path.basename(file), vid);
      return { vid, title }; // titleも返す（去重用）
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

// ---------------- main ----------------
async function main() {
  const fileArg =
    (process.argv.find((a) => a.startsWith("--file=")) || "").split("=")[1] || "";
  const langArg =
    (process.argv.find((a) => a.startsWith("--lang=")) || "").split("=")[1] || "en";
  const maxArg = parseInt(
    (process.argv.find((a) => a.startsWith("--max=")) || "").split("=")[1] || "1",
    10
  );

  const yt = ytClientForLang(langArg);
  const recent = await recentTitlesSet(yt);

  // ---- single file mode ----
  if (fileArg) {
    if (!isInQueuePath(fileArg)) {
      console.warn("[skip] --file は queue 配下のみ許可:", norm(fileArg));
      return;
    }
    if (!fs.existsSync(fileArg)) {
      console.warn("[preflight FAIL] not found:", norm(fileArg));
      return;
    }

    // ① プリフライト（空投稿/壊れ防止）
    try {
      const pf = assertPreflight(fileArg);
      console.log(`[preflight OK] ${path.basename(fileArg)} ${pf.width}x${pf.height} ${pf.duration.toFixed(2)}s ${pf.size}B`);
    } catch (e) {
      console.warn("[preflight FAIL]", norm(fileArg), "-", e?.message || e);
      await moveToFailed(fileArg); // queue 配下なので failed へ
      return;
    }

    const sidecar = await readSidecar(fileArg);

    // ② タイトル去重（直近50本）
    try {
      const preTitle = await predictTitle(fileArg, langArg, sidecar);
      if (recent.has(normTitle(preTitle))) {
        console.log("[skip dup-title]", preTitle);
        await moveToDups(fileArg);
        return;
      }
    } catch(_) {}

    // ③ アップロード
    try {
      const { title } = await uploadOne(yt, fileArg, langArg, sidecar);
      recent.add(normTitle(title));
      await moveToSent(fileArg);
    } catch (e) {
      await moveToFailed(fileArg);
      throw e;
    }
    return;
  }

  // ---- batch mode ----
  const candidateN = Math.max(maxArg * 10, maxArg);
  const batch = await pickBatch(langArg, candidateN);
  if (!batch.length) {
    console.log("[skip] no files in queue");
    return;
  }

  let done = 0;
  for (const f of batch) {
    if (done >= maxArg) break;

    // ① プリフライト
    try {
      const pf = assertPreflight(f);
      console.log(`[preflight OK] ${path.basename(f)} ${pf.width}x${pf.height} ${pf.duration.toFixed(2)}s ${pf.size}B`);
    } catch (e) {
      console.warn("[preflight FAIL]", norm(f), "-", e?.message || e);
      await moveToFailed(f);
      continue;
    }

    const sidecar = await readSidecar(f);

    // ② タイトル去重
    try {
      const preTitle = await predictTitle(f, langArg, sidecar);
      if (recent.has(normTitle(preTitle))) {
        console.log("[skip dup-title]", preTitle);
        await moveToDups(f);
        continue;
      }
    } catch(_) {}

    // ③ アップロード
    try {
      const { title } = await uploadOne(yt, f, langArg, sidecar);
      recent.add(normTitle(title));
      await moveToSent(f);
      done++;
    } catch (e) {
      await moveToFailed(f);
      console.warn("[skip after fail]", path.basename(f), e?.message || e);
    }

    if (done < maxArg) await sleep(1200); // 連投間隔（調整可）
  }

  console.log(`[done] uploaded ${done} file(s) for ${langArg}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
