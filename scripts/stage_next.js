// scripts/stage_next.js
// usage:
//   node scripts/stage_next.js --lang=en --slot=morning --tz=America/New_York
//   node scripts/stage_next.js --lang=ja --slot=auto    --tz=Asia/Tokyo
const fs = require("fs");
const fsp = fs.promises;
const path = require("path");

const ARG = (k, d="") => {
  const m = process.argv.find(a => a.startsWith(`--${k}=`));
  return m ? m.split("=").slice(1).join("=") : d;
};

function nowInTZ(tz) {
  const s = new Date().toLocaleString("en-US", { timeZone: tz || "UTC" });
  const d = new Date(s);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  const w = ["sun","mon","tue","wed","thu","fri","sat"][d.getDay()];
  const hour = d.getHours();
  return { dateStr: `${y}-${m}-${day}`, weekday: w, hour };
}

function slotFromHour(h) {
  // シンプル判定：必要なら調整してOK
  if (h >= 5 && h < 11) return "morning";
  if (h >= 11 && h < 16) return "noon";
  return "night"; // それ以外は夜扱い
}

async function oldestMp4(dir) {
  try {
    const files = (await fsp.readdir(dir)).filter(f => f.toLowerCase().endsWith(".mp4"));
    if (!files.length) return "";
    const items = files.map(f => ({ f, stat: fs.statSync(path.join(dir,f)) }))
                       .sort((a,b) => a.stat.mtimeMs - b.stat.mtimeMs);
    return path.join(dir, items[0].f);
  } catch { return ""; }
}

(async () => {
  const LANG = ARG("lang");
  let SLOT = ARG("slot","morning");
  const TZ = ARG("tz","UTC");
  if (!LANG) { console.error("ERR: --lang required"); process.exit(1); }

  const { dateStr, weekday, hour } = nowInTZ(TZ);
  if (SLOT === "auto") SLOT = slotFromHour(hour);

  const invBase = path.join("videos", LANG, "inventory", SLOT);
  const candidates = [
    path.join(invBase, weekday),
    path.join(invBase, "_default"),
    invBase, // 直置きも許容（後方互換）
  ];

  let srcMp4 = "";
  for (const dir of candidates) {
    srcMp4 = await oldestMp4(dir);
    if (srcMp4) break;
  }

  if (!srcMp4) {
    console.log(`[${LANG}/${SLOT}] ${weekday} inventory empty. skip.`);
    process.exit(0);
  }

  const base = path.basename(srcMp4).replace(/\.mp4$/i,"");
  const qDir  = path.join("videos", LANG, "queue", dateStr);
  const dstMp4 = path.join(qDir, `${base}.mp4`);
  await fsp.mkdir(qDir, { recursive: true });

  try { await fsp.rename(srcMp4, dstMp4); }
  catch { await fsp.copyFile(srcMp4, dstMp4); await fsp.unlink(srcMp4); }

  // 同名のメタ（任意）も在庫側にあれば移動
  const srcJson = path.join(path.dirname(srcMp4), `${base}.json`);
  const dstJson = path.join(qDir, `${base}.json`);
  if (fs.existsSync(srcJson)) {
    try { await fsp.rename(srcJson, dstJson); }
    catch { await fsp.copyFile(srcJson, dstJson); await fsp.unlink(srcJson); }
  }

  console.log(`[${LANG}/${SLOT}] ${weekday} staged -> ${path.relative(process.cwd(), dstMp4)} (TZ=${TZ})`);
})();
