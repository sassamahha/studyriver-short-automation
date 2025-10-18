// scripts/seed_to_yaml.js
// Seeds (by categories/weights) -> EN master YAML (robust)
// - JSON strict 出力 + バリデーション + リトライ + フォールバック
// usage:
//   node scripts/seed_to_yaml.js --count=3
//   node scripts/seed_to_yaml.js --count=5 --cats=habits8,steady
//   node scripts/seed_to_yaml.js --count=5 --cats=habits8:2,steady:1
//
// env: OPENAI_API_KEY (required), OPENAI_MODEL (optional; default gpt-4o-mini)

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const yaml = require("js-yaml");
const { OpenAI } = require("openai");

const MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";
const TODAY = new Date().toISOString().slice(0,10);
const COUNT = parseInt((process.argv.find(a=>a.startsWith("--count="))||"").split("=")[1] || "3", 10);
const CATS_ARG = (process.argv.find(a=>a.startsWith("--cats="))||"").split("=")[1] || "";

const POOL_ROOT = path.join("data","seeds");
const STATE_DIR = path.join("data","_state");
const USED_FILE = path.join(STATE_DIR, "used_seeds.json");

function outPathEN(date){ return path.join("data","en",`${date}.yaml`); }
function unique(arr){ return [...new Set(arr)]; }
const stripCtrl = s => String(s||"").replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g,"");
const clean = s => stripCtrl(String(s||"").replace(/\u00A0/g," ")).trim();

const MIN_ITEMS = 3;
const MAX_ITEMS = 7;
const WORDS_MAX  = 10;

// 体を使う動詞（検出/補正用）
const PHYSICAL_VERBS = [
  "stand","walk","stretch","breathe","drink","move","sit","run","shake",
  "jump","squat","push","pull","hydrate","smile"
];
// 命令形の頭に付けるセーフ動詞
const IMPERATIVE_SEEDS = [
  "Start","Stop","Set","Keep","Limit","Cut","Open","Close",
  "Write","Plan","List","Clear","Tidy","Clean","Mute","Silence",
  "Stand","Walk","Stretch","Breathe","Drink","Focus","Move","Pause"
];

// 数字検出（digitを優先。簡易でOK）
const hasDigit = s => /\d/.test(String(s||""));
const hasBodyAction = s => new RegExp(`^(${PHYSICAL_VERBS.join("|")})\\b`,"i").test(String(s||"")) ||
                            new RegExp(`\\b(${PHYSICAL_VERBS.join("|")})\\b`,"i").test(String(s||""));

// 先頭を動詞化（雑でも確実に命令形に寄せる）
function imperativeize(line){
  const t = clean(line);
  if (!t) return t;
  const first = t.split(/\s+/)[0].replace(/[^A-Za-z\-]/g,"");
  const isVerbish = new RegExp(`^(${IMPERATIVE_SEEDS.join("|")})$`,"i").test(first);
  if (isVerbish) return t;
  // 動詞っぽくないなら安全に "Start " を頭に付ける
  return `Start ${t}`;
}

function wordTrim(line, max=WORDS_MAX){
  const parts = clean(line).split(/\s+/);
  if (parts.length <= max) return clean(line);
  return parts.slice(0, max).join(" ");
}

function normalizeItems(rawItems){
  // 1) clean + dedupe（大小無視）
  let items = (Array.isArray(rawItems) ? rawItems : [])
      .map(clean).filter(Boolean);
  const seen = new Set();
  items = items.filter(x=>{
    const key = x.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // 2) imperativeize + trim length
  items = items.map(x => wordTrim(imperativeize(x), WORDS_MAX));

  // 3) 体を使う行動と数字の存在チェック
  const hasAnyDigit = items.some(hasDigit);
  const hasAnyBody  = items.some(hasBodyAction);

  // 4) 不足を補う（優先的に注入）
  if (!hasAnyDigit) items.unshift("Set a 2-minute timer");
  if (!hasAnyBody)  items.unshift("Stand and stretch your back");

  // 5) 個数調整：>MAX → 優先順で間引き
  if (items.length > MAX_ITEMS){
    // 優先：体を使う / 数字入り / 先頭から
    const bodyItems  = items.filter(hasBodyAction);
    const digitItems = items.filter(hasDigit && (x=>!bodyItems.includes(x)));
    const rest = items.filter(x => !bodyItems.includes(x) && !digitItems.includes(x));
    const merged = unique([...bodyItems, ...digitItems, ...rest]);
    items = merged.slice(0, MAX_ITEMS);
  }

  // 6) <MIN → フォールバックで埋める
  const padPool = [
    "Write one line in a journal",
    "Drink a glass of water",
    "Breathe slowly for 30 seconds",
    "Tidy one small spot on desk",
    "Walk for two minutes",
    "Plan one tiny next step"
  ];
  while (items.length < MIN_ITEMS){
    const cand = padPool[items.length % padPool.length];
    items.push(wordTrim(imperativeize(cand), WORDS_MAX));
  }

  return items;
}

async function listCategories(){
  if (!fs.existsSync(POOL_ROOT)) return [];
  return fs.readdirSync(POOL_ROOT).filter(d => fs.statSync(path.join(POOL_ROOT,d)).isDirectory());
}

async function loadPoolByCategory(cat){
  const dir = path.join(POOL_ROOT, cat);
  if (!fs.existsSync(dir)) return [];
  const files = fs.readdirSync(dir).filter(f=>f.endsWith(".txt"));
  let lines = [];
  for (const f of files){
    const txt = await fsp.readFile(path.join(dir,f),"utf8");
    const arr = txt.split(/\r?\n/).map(s=>s.trim()).filter(s=>s && !s.startsWith("#"));
    lines.push(...arr.map(x => ({ text:x, cat })));
  }
  const seen = new Set(); const out=[];
  for (const it of lines){
    const key = it.text;
    if (seen.has(key)) continue;
    seen.add(key); out.push(it);
  }
  return out;
}

async function loadPoolFiltered(catsWeights){
  const cats = catsWeights ? Object.keys(catsWeights) : await listCategories();
  let pool = [];
  for (const c of cats){
    pool.push(...await loadPoolByCategory(c));
  }
  return pool;
}

async function loadUsed(){
  try { return JSON.parse(await fsp.readFile(USED_FILE,"utf8")); }
  catch { return []; }
}
async function saveUsed(used){
  await fsp.mkdir(STATE_DIR, { recursive:true });
  await fsp.writeFile(USED_FILE, JSON.stringify(used,null,2), "utf8");
}

function buildRemaining(pool, used){
  const usedSet = new Set(used.map(u => `${u.cat}::${u.text}`));
  return pool.filter(s => !usedSet.has(`${s.cat}::${s.text}`));
}

function parseCats(arg){
  if (!arg) return null;
  const m = {};
  arg.split(",").map(s=>s.trim()).filter(Boolean).forEach(tok=>{
    const [name,wRaw] = tok.split(":");
    const w = Math.max(1, parseInt(wRaw||"1",10));
    m[name] = w;
  });
  return m;
}

function weightedPickCategory(remaining, catsWeights){
  if (!catsWeights) return null;
  const byCat = {};
  for (const it of remaining){ (byCat[it.cat] ||= []).push(it); }
  const entries = Object.entries(byCat).filter(([_,arr])=>arr.length>0);
  if (!entries.length) return null;
  const weighted = entries.map(([cat]) => ({ cat, weight: catsWeights[cat] || 1 }));
  const sum = weighted.reduce((a,b)=>a+b.weight,0);
  let r = Math.random() * sum;
  for (const w of weighted){ if ((r -= w.weight) <= 0) return w.cat; }
  return weighted[weighted.length-1].cat;
}
function pickOneFromCategory(remaining, cat){
  const arr = remaining.filter(s => s.cat === cat);
  if (!arr.length) return null;
  return arr[Math.floor(Math.random()*arr.length)];
}
function sampleWithCategoryWeights(pool, count, catsWeights){
  const picks = []; let remaining = pool.slice();
  while (picks.length < Math.min(count, pool.length)){
    let chosenCat = weightedPickCategory(remaining, catsWeights);
    let pick = chosenCat
      ? (pickOneFromCategory(remaining, chosenCat) || remaining[Math.floor(Math.random()*remaining.length)])
      : remaining[Math.floor(Math.random()*remaining.length)];
    picks.push(pick);
    remaining = remaining.filter(s => !(s.cat===pick.cat && s.text===pick.text));
  }
  return picks;
}

// ---------- OpenAI ----------
async function askOpenAI_JSON(client, seed){
  const sys = "You generate concise, practical self-improvement content for 10–15s YouTube Shorts.";
  const user = `
Seed (title idea; may be JP/EN):
"${seed}"

Return STRICT JSON with keys:
- "title": <= 60 chars, clear and engaging
- "items": array of 3–7 bullets, each 4–10 words
  * Each bullet MUST start with an imperative verb (e.g., Start, Stop, Set, Keep, Limit, Open, Close, Write, Plan, List, Clear, Tidy, Stand, Walk, Stretch, Breathe, Drink)
  * Include at least ONE bullet with a numeric digit (e.g., 2, 30s, 9 tabs)
  * Include at least ONE bullet that explicitly uses the body (stand/walk/stretch/breathe/drink)
- "cta": very short imperative line
- "tags": 2–4 simple tags

Rules:
- Output MUST be English.
- No markdown or code fences. JSON object only.
- Avoid duplicates. Keep everyday language. Use digits for numbers.
  `.trim();

  const r = await client.chat.completions.create({
    model: MODEL,
    temperature: 0.3,
    messages: [{role:"system",content:sys},{role:"user",content:user}],
    response_format: { type: "json_object" }
  });
  return r.choices?.[0]?.message?.content || "{}";
}

function hasCJK(s){ return /[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}]/u.test(String(s||"")); }

function looksEnglishShort(s){
  if (!s || typeof s !== "string") return false;
  if (hasCJK(s)) return false;
  return true;
}

function validEnglishEntry(obj){
  if (!obj || typeof obj !== "object") return false;
  const title = clean(obj.title);
  const items = Array.isArray(obj.items) ? obj.items.map(clean).filter(Boolean) : [];
  if (!looksEnglishShort(title)) return false;
  if (!items.length) return false; // 数は後段で正規化
  if (items.some(hasCJK)) return false;
  return true;
}

// ---------- フォールバック（安全テンプレ） ----------
const FALLBACK_ACTIONS = [
  "Set a 2-minute timer",
  "Drink a glass of water",
  "Breathe slowly for 30 seconds",
  "Stand and stretch your back",
  "Write one line in a journal",
  "Tidy one small spot on desk",
  "Walk for two minutes",
  "Plan one tiny next step"
];
function fallbackEntryFrom(seed){
  const base = clean(seed) || "Tiny wins to reset your day";
  let title = base.length > 60 ? base.slice(0,57) + "..." : base;
  const items = normalizeItems([
    "Set a 2-minute timer",
    "Drink a glass of water",
    "Stand and stretch your back",
    "Breathe slowly for 30 seconds",
    "Write one line in a journal"
  ]);
  return {
    title,
    items,
    cta: "Save and try one today",
    tags: ["mindset","small wins"]
  };
}

async function generateOne(client, seed){
  // 最大2回リトライ → フォールバック
  for (let attempt=0; attempt<2; attempt++){
    try{
      const json = await askOpenAI_JSON(client, seed);
      let obj;
      try { obj = JSON.parse(json); } catch { obj = {}; }
      if (!validEnglishEntry(obj)) throw new Error("validation failed");
      // 正規化（ここで 3–7 / 動詞化 / 数字 / 体行動 / ≤10語 を担保）
      const items = normalizeItems(obj.items);
      const out = {
        title: clean(obj.title),
        items,
        cta: clean(obj.cta) || "Save and try one today",
        tags: (Array.isArray(obj.tags) && obj.tags.length ? obj.tags : ["mindset","small wins"])
                .map(x=>clean(x)).slice(0,4)
      };
      return out;
    }catch(e){
      // 次のループで再試行
    }
  }
  // だめならフォールバック
  return fallbackEntryFrom(seed);
}

async function main(){
  if (!process.env.OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  const catsWeights = parseCats(CATS_ARG);
  const poolAll = await loadPoolFiltered(catsWeights);
  if (!poolAll.length) throw new Error("no seeds found under data/seeds");

  let used = await loadUsed();
  let remaining = buildRemaining(poolAll, used);
  if (remaining.length < COUNT){ used = []; remaining = poolAll.slice(); }

  const picks = sampleWithCategoryWeights(remaining, COUNT, catsWeights || null);

  const entries = [];
  for (const s of picks){
    const e = await generateOne(client, s.text);
    entries.push(e);
  }

  await fsp.mkdir(path.join("data","en"), { recursive:true });
  await fsp.writeFile(outPathEN(TODAY), yaml.dump({ entries }, { lineWidth: 1000 }), "utf8");
  console.log(`[ok] wrote ${outPathEN(TODAY)} (${entries.length} entries)`);

  const newUsed = unique([...used.map(u=>`${u.cat}::${u.text}`), ...picks.map(u=>`${u.cat}::${u.text}`)])
    .map(key => { const [cat,text] = key.split("::"); return { cat, text }; });
  await saveUsed(newUsed);
  console.log(`[state] used ${newUsed.length}/${poolAll.length} seeds tracked`);
}

main().catch(e=>{ console.error(e); process.exit(1); });
