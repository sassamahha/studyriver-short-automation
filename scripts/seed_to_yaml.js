// scripts/seed_to_yaml.js
// EN master YAML generator for "Small Success Habits" (solo)
// - Exact N bullets (3–7) with imperative verbs
// - Must include ≥1 numeric digit & ≥1 physical action
// - Strict JSON, retries, local lint, self-repair, auto-pick hero
//
// usage:
//   node scripts/seed_to_yaml.js --count=5
//   node scripts/seed_to_yaml.js --count=5 --cats=habits8,steady
//   node scripts/seed_to_yaml.js --count=7 --cats=habits8:2,steady:1 --candidates=9 --refine=2 --auto_pick=1
//
// env:
//   OPENAI_API_KEY (required)
//   OPENAI_MODEL   (optional; default gpt-4o-mini)
//   USE_JSON_SCHEMA=1 (optional; use JSON Schema if supported)

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const yaml = require("js-yaml");
const { OpenAI } = require("openai");

// ==== Config / CLI ====
const MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";
const USE_JSON_SCHEMA = process.env.USE_JSON_SCHEMA === "1";
const TODAY = new Date().toISOString().slice(0,10);

const COUNT = intArg("--count", 3);
const CATS_ARG = strArg("--cats", "");
const CANDIDATES = intArg("--candidates", 7);   // 1日に作る候補数
const REFINE = intArg("--refine", 2);           // 自己修復ループ回数
const AUTO_PICK = boolArg("--auto_pick", true); // ヒーロー1本だけ出力

const POOL_ROOT = path.join("data","seeds");
const STATE_DIR = path.join("data","_state");
const USED_FILE = path.join(STATE_DIR, "used_seeds.json");

function intArg(flag, def){ const v=(process.argv.find(a=>a.startsWith(flag+"="))||"").split("=")[1]; return v?parseInt(v,10):def; }
function strArg(flag, def){ const v=(process.argv.find(a=>a.startsWith(flag+"="))||"").split("=")[1]; return v??def; }
function boolArg(flag, def){ const v=(process.argv.find(a=>a.startsWith(flag+"="))||"").split("=")[1]; return v===undefined?def:(v==="1"||v==="true"); }

function outPathEN(date){ return path.join("data","en",`${date}.yaml`); }
function unique(arr){ return [...new Set(arr)]; }
const stripCtrl = s => String(s||"").replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g,"");
const clean = s => stripCtrl(String(s||"").replace(/\u00A0/g," ")).trim();

const MIN_ITEMS = 3;
const MAX_ITEMS = 7;
const WORDS_MAX  = 10;

// ==== Lexicons / Rules ====
const PHYSICAL_VERBS = ["stand","walk","stretch","breathe","drink","move","sit","run","shake","clap","squat","push","pull","hydrate","smile"];
const IMPERATIVE_SEEDS = ["Start","Stop","Set","Keep","Limit","Cut","Open","Close","Write","Plan","List","Clear","Tidy","Clean","Mute","Silence","Stand","Walk","Stretch","Breathe","Drink","Focus","Move","Pause","Share"];
const DIGIT = /\d/;

const hasCJK = s => /[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}]/u.test(String(s||""));
const hasDigit = s => DIGIT.test(String(s||""));
const hasBody  = s => new RegExp(`\\b(${PHYSICAL_VERBS.join("|")})\\b`,"i").test(String(s||""));

function imperativeize(line){
  const t = clean(line);
  if (!t) return t;
  const first = t.split(/\s+/)[0].replace(/[^A-Za-z\-]/g,"");
  if (new RegExp(`^(${IMPERATIVE_SEEDS.join("|")})$`,"i").test(first)) return t;
  return `Start ${t}`;
}
function wordTrim(line, max=WORDS_MAX){
  const parts = clean(line).split(/\s+/);
  return parts.length <= max ? clean(line) : parts.slice(0, max).join(" ");
}

// ==== Desired count detection (多言語対応) ====
const numWord2int = { three:3,four:4,five:5,six:6,seven:7, Three:3,Four:4,Five:5,Six:6,Seven:7 };
const jpNumMap = { "三":3,"四":4,"五":5,"六":6,"七":7,"３":3,"４":4,"５":5,"６":6,"７":7 };
function detectDesiredCountFrom(text){
  const s = String(text||"");
  const m1 = s.match(/\b(three|four|five|six|seven)\b/ig);
  if (m1 && m1[0]) return numWord2int[m1[0]];
  const m2 = s.match(/([3-7]|[３-７])\s*(つ|手|ways?|moves?|steps?|items?)/i);
  if (m2 && m2[1]) { const d = m2[1]; return "３４５６７".includes(d) ? "３４５６７".indexOf(d)+3 : parseInt(d,10); }
  const m3 = s.match(/[三四五六七]/);
  if (m3) return jpNumMap[m3[0]];
  const m4 = s.match(/\b([3-7])\b/);
  if (m4) return parseInt(m4[1],10);
  return null;
}
function clampN(n){ if (!n) return null; return Math.max(MIN_ITEMS, Math.min(MAX_ITEMS, n|0)); }

// ==== Title number harmonization ====
function ensureTitleCount(title, N){
  if (!title) return title;
  let t = title;
  t = t.replace(/\b(three|four|five|six|seven)\b/gi, m => ({three:"3",four:"4",five:"5",six:"6",seven:"7"})[m.toLowerCase()]);
  t = t.replace(/[３-７]/g, ch => String("３４５６７".indexOf(ch)+3));
  t = t.replace(/[三四五六七]/g, ch => String(jpNumMap[ch]));
  t = t.replace(/\b([3-7])\s*(ways?|moves?|steps?|items?|つ|手)\b/i, `${N} $2`);
  if (!/\b[3-7]\b/.test(t)) t = `${N} ${t}`;
  return t;
}

// ==== Local lint & exact-N enforce ====
function normalizeAndEnforce(items, N){
  let arr = (Array.isArray(items)?items:[]).map(clean).filter(Boolean);
  // dedupe (case-insensitive)
  const seen=new Set(); arr=arr.filter(x=>{const k=x.toLowerCase(); if(seen.has(k)) return false; seen.add(k); return true;});
  // imperative + trim
  arr = arr.map(x => wordTrim(imperativeize(x), WORDS_MAX));
  // ensure required signals
  const inject = [];
  if (!arr.some(hasDigit)) inject.push("Set a 2-minute timer");
  if (!arr.some(hasBody))  inject.push("Stand and stretch your back");
  arr = [...inject, ...arr];

  // pad/trim to exact N
  const padPool = [
    "Drink a glass of water",
    "Walk for two minutes",
    "Breathe slowly for 30 seconds",
    "Tidy one small spot on desk",
    "Write one line in a journal",
    "Plan one tiny next step"
  ];
  if (arr.length > N){
    // keep: body > digit > others
    const keep = [];
    const push = v => { if(!keep.includes(v)) keep.push(v); };
    arr.filter(x=>hasBody(x)).forEach(push);
    arr.filter(x=>hasDigit(x) && !keep.includes(x)).forEach(push);
    arr.filter(x=>!keep.includes(x)).forEach(push);
    arr = keep.slice(0,N);
  }else{
    let i=0;
    while(arr.length<N){
      const cand = wordTrim(imperativeize(padPool[i++ % padPool.length]), WORDS_MAX);
      if(!arr.includes(cand)) arr.push(cand);
    }
  }
  return arr;
}

// ==== Seeds I/O ====
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
  // unique by text
  const seen = new Set(); const out=[];
  for (const it of lines){ const key = it.text; if (seen.has(key)) continue; seen.add(key); out.push(it); }
  return out;
}
async function loadPoolFiltered(catsWeights){
  const cats = catsWeights ? Object.keys(catsWeights) : await listCategories();
  let pool = [];
  for (const c of cats){ pool.push(...await loadPoolByCategory(c)); }
  return pool;
}
async function loadUsed(){ try { return JSON.parse(await fsp.readFile(USED_FILE,"utf8")); } catch { return []; } }
async function saveUsed(used){ await fsp.mkdir(STATE_DIR,{recursive:true}); await fsp.writeFile(USED_FILE, JSON.stringify(used,null,2), "utf8"); }
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

// ==== OpenAI Prompts ====
function buildUserPrompt(seed, N){
  return `
Seed (title idea; may be JP/EN):
"${seed}"

You write ultra-practical "Small Success Habits" for 10–15s YouTube Shorts.

Return STRICT JSON with keys:
- "title": <= 60 chars, engaging. Prefer pain→permission (e.g., "Tired Today? Go Slow.")
- "items": array of EXACTLY ${N} bullets, each 4–10 words
  * Every bullet MUST start with an imperative verb
  * Include at least ONE numeric digit (e.g., 2, 30s, 9 tabs)
  * Include at least ONE body-based action (stand/walk/stretch/breathe/drink)
  * Keep everyday language. No jargon. No emojis.
- "cta": very short imperative line (e.g., "Save for tomorrow")
- "tags": 2–4 simple tags (e.g., mindset, small wins)
Rules:
- Output MUST be English. JSON object only (no code fences).
- Avoid duplicates. Use digits for numbers. Do not append brand tails.
`.trim();
}
function buildJsonSchema(N){
  return {
    name: "shorts_entry",
    schema: {
      type: "object",
      additionalProperties: false,
      properties: {
        title: { type: "string", maxLength: 60 },
        items: { type: "array", minItems: N, maxItems: N, items: { type: "string", minLength: 4, maxLength: 80 }},
        cta:   { type: "string", maxLength: 60 },
        tags:  { type: "array", minItems: 2, maxItems: 4, items: { type: "string", minLength: 2, maxLength: 20 } }
      },
      required: ["title","items","cta","tags"]
    },
    strict: true
  };
}
async function askOpenAI_JSON(client, seed, N){
  const sys = "You generate concise, practical 'Small Success Habits' for Shorts.";
  const messages = [{role:"system",content:sys},{role:"user",content:buildUserPrompt(seed,N)}];
  if (USE_JSON_SCHEMA){
    const r = await client.chat.completions.create({
      model: MODEL,
      temperature: 0.2,
      messages,
      response_format: { type: "json_schema", json_schema: buildJsonSchema(N) }
    });
    return r.choices?.[0]?.message?.content || "{}";
  } else {
    const r = await client.chat.completions.create({
      model: MODEL,
      temperature: 0.2,
      messages,
      response_format: { type: "json_object" }
    });
    return r.choices?.[0]?.message?.content || "{}";
  }
}

function validEnglishEntry(obj){
  if (!obj || typeof obj !== "object") return false;
  const title = clean(obj.title);
  const items = Array.isArray(obj.items) ? obj.items.map(clean).filter(Boolean) : [];
  if (!title || hasCJK(title)) return false;
  if (!items.length || items.some(hasCJK)) return false;
  return true;
}

// ==== Self-repair (LLM judge) ====
async function critiqueAndRewrite(client, entry, N){
  const sys = "You are a strict editor for 'Small Success Habits' shorts. Output valid JSON only.";
  const user = `
ENTRY (JSON):
${JSON.stringify(entry)}

TASK:
1) Enforce hard rules:
   - Exactly ${N} bullets, 4–10 words, all imperative.
   - Include ≥1 numeric digit and ≥1 physical action.
2) If any rule fails, minimally REWRITE to satisfy all.
3) Keep everyday language. No emojis, no jargon.
4) Return STRICT JSON with keys: title, items, cta, tags. No code fences.
`.trim();
  const r = await client.chat.completions.create({
    model: MODEL,
    temperature: 0.1,
    messages: [{role:"system",content:sys},{role:"user",content:user}],
    response_format: { type: "json_object" }
  });
  let obj; try { obj = JSON.parse(r.choices?.[0]?.message?.content || "{}"); } catch { obj = {}; }
  return obj;
}

// ==== Fallbacks ====
function fallbackItems(N){
  const base = [
    "Set a 2-minute timer",
    "Drink a glass of water",
    "Stand and stretch your back",
    "Breathe slowly for 30 seconds",
    "Write one line in a journal",
    "Walk for two minutes",
    "Tidy one small spot on desk"
  ];
  const arr = [];
  let i=0;
  while(arr.length<N){
    const v = wordTrim(imperativeize(base[i++ % base.length]), WORDS_MAX);
    if(!arr.includes(v)) arr.push(v);
  }
  return arr;
}
function fallbackEntry(seed, N){
  const base = clean(seed) || "Tiny wins to reset your day";
  const title = ensureTitleCount(base.length>60?base.slice(0,57)+"...":base, N);
  return { title, items: normalizeAndEnforce(fallbackItems(N), N), cta: "Save and try one today", tags: ["mindset","small wins"] };
}

// ==== Scoring (auto-pick hero) ====
// 痛み・許可・時間/数字・体を使う に重み付け
function scoreEntry(entry){
  const t = (entry.title||"").toLowerCase();
  const body = (entry.items||[]).join(" ").toLowerCase();
  let s = 0;
  if (/\b(tired|heavy|anxious|stuck|overthinking|slump|no motivation)\b/.test(t)) s+=2; // Pain
  if (/\b(it'?s okay|allowed|normal|go slow|slow today)\b/.test(t)) s+=2;             // Permission/Normalize
  if (/\b(\d+\s?(s|min|minutes?)|before (noon|lunch|bed))\b/.test(t) || DIGIT.test(body)) s+=2; // Time/Number
  if (new RegExp(`\\b(${PHYSICAL_VERBS.join("|")})\\b`).test(body)) s+=2;             // Physical
  // Brevity bonus
  const avgWords = (entry.items||[]).reduce((a,x)=>a+x.split(/\s+/).length,0)/Math.max(1,(entry.items||[]).length);
  if (avgWords <= 7) s += 1;
  if ((entry.title||"").length <= 60) s += 1;
  return s;
}

// ==== Core generate ====
async function generateOne(client, seed){
  const detected = clampN(detectDesiredCountFrom(seed));
  const N = detected || 5; // デフォは中央値
  for (let attempt=0; attempt<2; attempt++){
    try{
      const json = await askOpenAI_JSON(client, seed, N);
      let obj; try { obj = JSON.parse(json); } catch { obj = {}; }
      if (!validEnglishEntry(obj)) throw new Error("validation failed");
      // local lint + exact N + title number harmonization
      const items = normalizeAndEnforce(obj.items, N);
      const fixed = {
        title: ensureTitleCount(clean(obj.title), N),
        items,
        cta: clean(obj.cta) || "Save and try one today",
        tags: (Array.isArray(obj.tags) && obj.tags.length ? obj.tags : ["mindset","small wins"]).map(x=>clean(x)).slice(0,4)
      };
      return fixed;
    }catch(e){ /* retry */ }
  }
  return fallbackEntry(seed, N);
}

// ==== Seeds workflow ====
async function main(){
  if (!process.env.OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  const catsWeights = parseCats(CATS_ARG);
  const poolAll = await loadPoolFiltered(catsWeights);
  if (!poolAll.length) throw new Error("no seeds found under data/seeds");

  let used = await loadUsed();
  let remaining = buildRemaining(poolAll, used);
  if (remaining.length < COUNT){ used = []; remaining = poolAll.slice(); }

  const picks = sampleWithCategoryWeights(remaining, Math.max(COUNT, CANDIDATES), catsWeights || null);

  // 生成→自己修復→スコア選抜（完全自動）
  const candidates = [];
  for (const s of picks){
    if (candidates.length >= CANDIDATES) break;
    // 1) 生成
    let entry = await generateOne(client, s.text);
    // 2) 自己修復（REFINEループ）
    const N = clampN(detectDesiredCountFrom(entry.title)) || clampN(detectDesiredCountFrom(s.text)) || 5;
    for (let i=0;i<REFINE;i++){
      const judged = await critiqueAndRewrite(client, entry, N);
      if (judged && judged.items) {
        // 再度ローカル矯正＆タイトル数字合わせ
        entry = {
          title: ensureTitleCount(clean(judged.title || entry.title), N),
          items: normalizeAndEnforce(judged.items, N),
          cta: clean(judged.cta || entry.cta || "Save and try one today"),
          tags: (Array.isArray(judged.tags) && judged.tags.length ? judged.tags : (entry.tags||["mindset","small wins"])).slice(0,4)
        };
      }
    }
    candidates.push(entry);
  }

  // スコアで並べてヒーロー選抜
  candidates.sort((a,b)=>scoreEntry(b)-scoreEntry(a));
  const hero   = candidates[0];
  const backup = candidates[1] || candidates[0];
  const outEntries = AUTO_PICK ? [hero] : candidates;

  await fsp.mkdir(path.join("data","en"), { recursive:true });
  await fsp.writeFile(outPathEN(TODAY), yaml.dump({ entries: outEntries }, { lineWidth: 1000 }), "utf8");
  await fsp.writeFile(outPathEN(TODAY + ".backup.yaml"), yaml.dump({ entries: [backup] }, { lineWidth: 1000 }), "utf8");
  console.log(`[ok] wrote ${outPathEN(TODAY)} entries=${outEntries.length} hero="${hero?.title}" schema=${USE_JSON_SCHEMA?"on":"off"}`);

  const newUsed = unique([...used.map(u=>`${u.cat}::${u.text}`), ...picks.slice(0, CANDIDATES).map(u=>`${u.cat}::${u.text}`)])
    .map(key => { const [cat,text] = key.split("::"); return { cat, text }; });
  await saveUsed(newUsed);
  console.log(`[state] used ${newUsed.length}/${poolAll.length} seeds tracked`);
}

main().catch(e=>{ console.error(e); process.exit(1); });
