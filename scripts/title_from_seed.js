// scripts/title_from_seed.js
// usage:
//   LANG_DIR=videos/en/queue node scripts/title_from_seed.js --slot=morning --tz=America/New_York --max=1
//   LANG_DIR=videos/ja/queue node scripts/title_from_seed.js --slot=auto    --tz=Asia/Tokyo        --max=1
const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const crypto = require("crypto");

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
function slotFromHour(h){
  if (h >= 5 && h < 11) return "morning";
  if (h >= 11 && h < 16) return "noon";
  return "night";
}
function pickByHash(basename, arr) {
  const h = crypto.createHash("md5").update(basename).digest();
  const n = h.readUInt32BE(0);
  return arr[n % arr.length];
}

async function readSeed(slot, weekday) {
  const cand = [
    path.join("data","seeds","slots",slot,`${weekday}.txt`),
    path.join("data","seeds","slots",slot,`_default.txt`),
  ];
  for (const p of cand) {
    try {
      const lines = (await fsp.readFile(p,"utf8"))
        .split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
      if (lines.length) return lines;
    } catch {}
  }
  return [];
}

(async () => {
  let SLOT = ARG("slot","morning");
  const TZ = ARG("tz","UTC");
  const MAX = parseInt(ARG("max","1"),10);
  const { dateStr, weekday, hour } = nowInTZ(TZ);
  if (SLOT === "auto") SLOT = slotFromHour(hour);

  const LANG_DIR = process.env.LANG_DIR;
  if (!LANG_DIR) { console.error("ERR: set LANG_DIR to videos/{lang}/queue"); process.exit(1); }
  const todayDir = path.join(LANG_DIR, dateStr);

  let mp4s = [];
  try { mp4s = (await fsp.readdir(todayDir)).filter(f => f.toLowerCase().endsWith(".mp4")); }
  catch { console.log(`[seed] not found: ${todayDir}`); process.exit(0); }

  const targets = mp4s.map(f => f.replace(/\.mp4$/i,""))
    .filter(base => !fs.existsSync(path.join(todayDir, `${base}.json`)));

  if (!targets.length) { console.log("[seed] all have json. skip"); process.exit(0); }

  const seed = await readSeed(SLOT, weekday);
  if (!seed.length) { console.log(`[seed] empty for ${SLOT}/${weekday}. skip`); process.exit(0); }

  let count = 0;
  for (const base of targets) {
    const title = pickByHash(base, seed) || seed[0];
    const meta = { title, description: "", tags: [], privacyStatus: "public" };
    await fsp.writeFile(path.join(todayDir, `${base}.json`), JSON.stringify(meta,null,2));
    console.log(`[seed] ${SLOT}/${weekday}: "${title}" -> ${path.join(todayDir, base)}.json`);
    if (++count >= MAX) break; // その枠では1本だけ付与
  }
})();
