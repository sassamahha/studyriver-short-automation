// YAML + style -> videos/{lang}/queue/YYYY-MM-DD/####.mp4 (+ ####.json)
// 安全版: textfile=… を使い、UTF-8/BOM/不可視制御/フォント欠落に強い。
// - フォントは assets -> /usr/share/fonts の順で多段フォールバック
// - テキストは正規化＆クレンジング
// - drawtext は text_shaping=1:utf8=1 を明示

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const os = require("os");
const yaml = require("js-yaml");
const { spawnSync } = require("child_process");

// ---- args / env
const ARG = (k, def = "") => {
  const m = process.argv.find((a) => a.startsWith(`--${k}=`));
  return m ? m.split("=")[1] : def;
};
const LANG = ARG("lang", "en");
const DATE = ARG("date", new Date().toISOString().slice(0, 10));
const DUR = ARG("dur", process.env.DURATION || "10");
// ★ BG をディレクトリ指定にしてランダム選定（"assets/bg" がデフォ）
const BG = ARG("bg", "assets/bg");
const AUDIO = ARG("audio", "assets/bgm");

// ---- paths
const yamlPath = (d, lang) => path.join("data", lang, `${d}.yaml`);
const stylePath = () => path.join("data", "style.yaml");
const outDir = (d, lang) => path.join("videos", lang, "queue", d);
const chMetaTxt = (lang) => path.join("data", "channel_meta", `${lang}.txt`);

// ---- text utils
const toASCIIQuotes = (s) =>
  String(s || "").replace(/[“”]/g, '"').replace(/[‘’]/g, "'").replace(/\u00A0/g, " "); // NBSP -> space
const stripControls = (s) =>
  String(s || "").replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "");
const stripBOM = (s) => s.replace(/^\uFEFF/, "");
const normalize = (s) => stripControls(toASCIIQuotes(stripBOM(String(s || "")))).normalize("NFC");

// ---- simple wrapping（EN等=単語 / CJK=字数）
function wrapByLimit(text, limit, isCJK) {
  const t = normalize(text);
  if (!t) return [""];
  if (isCJK) {
    const lines = [];
    let cur = "";
    for (const ch of t) {
      if (cur.length >= limit) {
        lines.push(cur);
        cur = ch;
      } else cur += ch;
    }
    if (cur) lines.push(cur);
    return lines;
  } else {
    const words = t.split(/\s+/).filter(Boolean);
    const lines = [];
    let cur = "";
    for (const w of words) {
      const next = (cur ? cur + " " : "") + w;
      if (next.length > limit && cur) {
        lines.push(cur);
        cur = w;
      } else cur = next;
    }
    if (cur) lines.push(cur);
    return lines;
  }
}

// ---- channel meta (KV)
function readChannelMetaKV(lang) {
  const def = {
    title_suffix: "",
    description: "📌 Daily 10s ‘Small Wins’. Save and try one today.",
    tags: ["small wins", "mindset", "self help"],
    tags_extra: "",
  };
  const p = chMetaTxt(lang);
  if (!fs.existsSync(p)) return def;

  const raw = stripBOM(fs.readFileSync(p, "utf8"));
  const lines = raw.split(/\r?\n/);
  let curKey = null;
  for (let line of lines) {
    const s = line.trim();
    if (!s || s.startsWith("#")) continue;
    const m = s.match(/^([a-zA-Z_]+)\s*=\s*(.*)$/);
    if (m) {
      curKey = m[1];
      let v = m[2].replace(/^\s*(title_suffix|description|tags|tags_extra)\s*=\s*/i, "");
      if (curKey === "title_suffix") def.title_suffix = v;
      else if (curKey === "description") def.description = v;
      else if (curKey === "tags")
        def.tags = v.split(",").map((x) => x.trim()).filter(Boolean).slice(0, 10);
      else if (curKey === "tags_extra") def.tags_extra = v;
      continue;
    }
    if (curKey === "description") def.description += "\n" + line;
  }
  def.description = def.description.replace(/^\s*(title_suffix|description)\s*=\s*/i, "").trim();
  def.title_suffix = def.title_suffix.replace(/^\s*title_suffix\s*=\s*/i, "").trim();
  return def;
}

// ---- font resolver（多段フォールバック）
function firstExisting(paths) {
  for (const p of paths) {
    if (p && fs.existsSync(p)) return p;
  }
  return null;
}
function fontFor(lang, styleFont) {
  // 1) style.yaml 指定 2) assets 同梱 3) Ubuntu の Noto 4) DejaVu
  const assets = {
    ja: "assets/fonts/NotoSansJP-Regular.ttf",
    en: "assets/fonts/NotoSans-Regular.ttf",
    es: "assets/fonts/NotoSans-Regular.ttf",
    pt: "assets/fonts/NotoSans-Regular.ttf",
  };
  const sysNoto = [
    "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
    "/usr/share/fonts/truetype/noto/NotoSansDisplay-Regular.ttf",
    "/usr/share/fonts/truetype/noto/NotoSansJP-Regular.otf",
  ];
  const sysDejavu = ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"];

  return firstExisting([styleFont, assets[lang], assets.en, ...sysNoto, ...sysDejavu]);
}

// --- Random pick helpers (BG/BGM) ------------------------------------------
function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

// AUDIO: file / directory / wildcard / "random" -> assets/bgm
function pickAudioPath(audioSetting) {
  let p = String(audioSetting || "").trim();
  const exts = /\.(mp3|wav|m4a|aac|ogg)$/i;

  if (!p || p.toLowerCase() === "random") p = "assets/bgm";

  if (fs.existsSync(p) && fs.statSync(p).isDirectory()) {
    const files = fs.readdirSync(p).filter((f) => exts.test(f));
    if (files.length) return path.join(p, pickRandom(files));
  }

  if (/\*/.test(p)) {
    const dir = path.dirname(p);
    const pat = new RegExp(
      "^" +
        path
          .basename(p)
          .replace(/[.+^${}()|[\]\\]/g, "\\$&")
          .replace(/\*/g, ".*") +
        "$",
      "i"
    );
    const files = fs.readdirSync(dir).filter((f) => pat.test(f));
    if (files.length) return path.join(dir, pickRandom(files));
  }

  return p; // file or unknown
}

// BG: directory / wildcard / file / "random" -> assets/bg
function pickBgPath(bgSetting) {
  let p = String(bgSetting || "").trim();
  if (!p || p.toLowerCase() === "random") p = "assets/bg";

  if (fs.existsSync(p) && fs.statSync(p).isDirectory()) {
    const files = fs
      .readdirSync(p)
      .filter((f) => /\.(mp4|mov|mkv|webm|m4v|jpg|jpeg|png)$/i.test(f));
    if (files.length) return path.join(p, pickRandom(files));
    return null;
  }

  if (/\*/.test(p)) {
    const dir = path.dirname(p);
    const pat = new RegExp(
      "^" +
        path
          .basename(p)
          .replace(/[.+^${}()|[\]\\]/g, "\\$&")
          .replace(/\*/g, ".*") +
        "$",
      "i"
    );
    const files = fs.readdirSync(dir).filter((f) => pat.test(f));
    if (files.length) return path.join(dir, pickRandom(files));
    return null;
  }

  return p; // file or unknown
}

// ---- main
(async function main() {
  const yml = yamlPath(DATE, LANG);
  if (!fs.existsSync(yml)) throw new Error(`content not found: ${yml}`);

  const doc = yaml.load(await fsp.readFile(yml, "utf8")) || {};
  const st = yaml.load(await fsp.readFile(stylePath(), "utf8")) || {};
  const S0 = (st.styles && st.styles.default) || {};
  const S = Object.assign({}, S0, (st.styles && st.styles[LANG]) || {});

  // canvas
  const W = S.width ?? 1080;
  const H = S.height ?? 1920;

  // margins / paddings
  const mX = S.panel_margin_x ?? 0;
  const mY = S.panel_margin_y ?? 64;
  const pX = S.panel_padding_x ?? 64;
  const pY = S.panel_padding_y ?? 120;
  const panelAlpha = S.panel_alpha ?? 0.55;

  // typography
  const tSize = S.title_size ?? 88;
  const iSize = S.item_size ?? 54;
  const cSize = S.cta_size ?? 52;
  const gap = S.line_gap ?? 86;
  const titleGap = S.title_line_gap ?? 72;
  const titleBottomGap = S.title_bottom_gap ?? 64;

  const bullet = (S.bullet ?? "•") + " ";

  // ----- font
  const fontPath = fontFor(LANG, S.font);
  if (!fontPath)
    throw new Error(
      "No usable font found. Put NotoSans in assets/fonts or install Noto/DejaVu."
    );
  const isCJK = LANG === "ja" || LANG === "zh" || LANG === "ko";

  // wrap limits（per lang override → EN/JA の既定）
  const tLimit =
    S[`title_wrap_chars_${LANG}`] ??
    (isCJK ? S.title_wrap_chars_ja ?? 16 : S.title_wrap_chars_en ?? 28);
  const iLimit =
    S[`item_wrap_chars_${LANG}`] ??
    (isCJK ? S.item_wrap_chars_ja ?? 18 : S.item_wrap_chars_en ?? 36);

  // positions
  const px = mX,
    py = mY,
    pw = W - mX * 2,
    ph = H - mY * 2;
  const ix = px + pX;
  const iyTitle = py + pY;
  const iyItemsStart = iyTitle + tSize + titleGap + titleBottomGap;
  const iyCta = py + ph - pY - cSize - 12;

  // dirs
  const odir = outDir(DATE, LANG);
  await fsp.mkdir(odir, { recursive: true });
  const tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), "srshort-"));

  const CH = readChannelMetaKV(LANG);

  let idx = 0;
  for (const e of doc.entries || []) {
    const nonEmpty = (Array.isArray(e.items) ? e.items : []).some((s) =>
      String(s || "").trim()
    );
    if (!String(e.title || "").trim() && !nonEmpty) {
      console.log("[skip] empty entry");
      continue;
    }

    idx++;
    const outMp4 = path.join(odir, `${String(idx).padStart(4, "0")}.mp4`);
    const outJson = path.join(odir, `${String(idx).padStart(4, "0")}.json`);

    // ---- wrap/clean
    const titleLines = wrapByLimit(normalize(e.title || ""), tLimit, isCJK);
    const rawItems = (Array.isArray(e.items) ? e.items : [])
      .map((s) => normalize(s))
      .filter(Boolean)
      .slice(0, 12);
    const itemLines = [];
    const indent = isCJK ? "　" : "   ";
    for (const it of rawItems) {
      const arr = wrapByLimit(it, iLimit, isCJK);
      arr.forEach((line, li) => itemLines.push(li === 0 ? bullet + line : indent + line));
    }
    const ctaLine = normalize(e.cta || "Save and try one today");

    // ---- textfiles
    const textFiles = [];
    const makeTxt = async (base, txt) => {
      const p = path.join(tmpRoot, `${base}.txt`);
      // 明示的に LF & UTF-8 (BOMなし)
      await fsp.writeFile(p, txt.replace(/\r\n/g, "\n"), { encoding: "utf8", flag: "w" });
      textFiles.push(p);
      return p;
    };

    // ---- filtergraph
    const parts = [];
    // 背景+パネル
    parts.push(
      `[0:v]scale=${W}:${H},format=rgba,drawbox=x=${px}:y=${py}:w=${pw}:h=${ph}:color=black@${panelAlpha}:t=fill[v0]`
    );

    // タイトル
    const titleLineSpace = Math.max(0, titleGap - tSize + 10);
    let vi = 0;
    for (let k = 0; k < titleLines.length; k++) {
      const tf = await makeTxt(`title_${idx}_${k}`, titleLines[k]);
      const y = `${iyTitle}+${k}*(${tSize}+${titleLineSpace})`;
      parts.push(
        `[v${vi}]drawtext=fontfile=${fontPath}:textfile=${tf}:x=${ix}:y=${y}:fontsize=${tSize}:fontcolor=white:` +
          `shadowcolor=black@0.6:shadowx=2:shadowy=2:text_shaping=1[v${vi + 1}]`
      );
      vi++;
    }

    // 箇条書き
    for (let k = 0; k < itemLines.length; k++) {
      const tf = await makeTxt(`item_${idx}_${k}`, itemLines[k]);
      const y = `${iyItemsStart}+${k}*${gap}`;
      parts.push(
        `[v${vi}]drawtext=fontfile=${fontPath}:textfile=${tf}:x=${ix}:y=${y}:fontsize=${iSize}:fontcolor=white:` +
          `shadowcolor=black@0.5:shadowx=1:shadowy=1:text_shaping=1[v${vi + 1}]`
      );
      vi++;
    }

    // CTA
    {
      const tf = await makeTxt(`cta_${idx}`, ctaLine);
      parts.push(
        `[v${vi}]drawtext=fontfile=${fontPath}:textfile=${tf}:x=(w-text_w)/2:y=${iyCta}:fontsize=${cSize}:fontcolor=0xE0FFC8:` +
          `box=1:boxcolor=black@0.55:boxborderw=24:text_shaping=1[v]`
      );
    }

    const filtergraph = parts.join(";");

    // ---- inputs (BG / AUDIO)
    const chosenBg = pickBgPath(BG);
    let bgArgs;

    if (chosenBg && /\.(jpe?g|png)$/i.test(chosenBg)) {
      // 静止画：DUR秒ループ
      bgArgs = ["-loop", "1", "-t", String(DUR), "-i", chosenBg];
      console.log("[bg:image]", path.basename(chosenBg));
    } else if (chosenBg && /\.(mp4|mov|mkv|webm|m4v)$/i.test(chosenBg)) {
      // 動画：無限ループ入力 + 切り詰め
      bgArgs = ["-stream_loop", "-1", "-t", String(DUR), "-i", chosenBg];
      console.log("[bg:video]", path.basename(chosenBg));
    } else {
      // フォールバック：単色背景（生成）
      console.warn("[bg:fallback] using solid color");
      bgArgs = ["-f", "lavfi", "-t", String(DUR), "-i", `color=black:s=${W}x${H}:r=30`];
    }

    const chosenAudio = pickAudioPath(AUDIO);
    if (chosenAudio && fs.existsSync(chosenAudio)) {
      console.log("[bgm]", path.basename(chosenAudio));
    }
    const audioArgs =
      chosenAudio && fs.existsSync(chosenAudio)
        ? ["-i", chosenAudio]
        : ["-f", "lavfi", "-t", String(DUR), "-i", "anullsrc=cl=stereo:r=44100"];

    // ---- ffmpeg
    const args = [
      "-y",
      ...bgArgs,
      ...audioArgs,
      "-filter_complex",
      filtergraph,
      "-map",
      "[v]",
      "-map",
      "1:a?",
      "-shortest",
      "-r",
      "30",
      "-c:v",
      "libx264",
      "-pix_fmt",
      "yuv420p",
      "-c:a",
      "aac",
      outMp4,
    ];

    const r = spawnSync("ffmpeg", args, { stdio: "inherit" });
    if (r.status !== 0) {
      console.error("[font]", fontPath);
      console.error("[texts]", textFiles);
      throw new Error("ffmpeg failed");
    }

    // ---- sidecar
    const titleText = `${normalize(e.title || "Small Wins")}${CH.title_suffix || ""}`;
    const tags =
      Array.isArray(e.tags) && e.tags.length ? e.tags.slice(0, 10) : CH.tags;
    let desc = CH.description;
    if (CH.tags_extra) desc += `\n${CH.tags_extra}`;
    desc = normalize(desc).replace(/^\s*(title_suffix|description)\s*=\s*/i, "").trim();
    const sidecar = { title: titleText, description: desc, tags };
    await fsp.writeFile(outJson, JSON.stringify(sidecar, null, 2), "utf8");

    // cleanup
    for (const p of textFiles) {
      try {
        await fsp.unlink(p);
      } catch {}
    }
    console.log("[mp4]", outMp4);
    console.log("[meta]", outJson);
  }

  try {
    await fsp.rmdir(tmpRoot);
  } catch {}
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
