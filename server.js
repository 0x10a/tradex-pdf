const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { v4: uuidv4 } = require("uuid");
const OpenAI = require("openai");
const AdmZip = require("adm-zip");

// ─── Configuration ─────────────────────────────────────────────────────────
const _MODEL = process.env.TRADEX_MODEL || "gpt-4.1-mini";
const _ORIGINAL_MODELS = new Set(["gpt-5.4", "gpt-5.4-pro", "gpt-5.5", "gpt-5.5-pro"]);
const _PROMPT = "Analyse cette image et trouve la valeur correspondant au 'Numero LC'. Renvoie uniquement le numero exact (majuscules, chiffres, parfois des espaces), sans aucun autre texte ni ponctuation. Exemple : 058ICD 2503674099";
const _INVALID_CHARS = /[\\/:*?"<>|]/g;
const _CONCURRENCY = parseInt(process.env.TRADEX_CONCURRENCY || "3", 10);
const _SESSION_TTL_MS = 30 * 60 * 1000; // 30 min

// ─── Dossier temp ───────────────────────────────────────────────────────────
const TMP_DIR = path.join(__dirname, "_tmp");
fs.mkdirSync(TMP_DIR, { recursive: true });

// ─── OpenAI (lit OPENAI_API_KEY automatiquement) ────────────────────────────
const openai = new OpenAI();

// ─── Multer ─────────────────────────────────────────────────────────────────
const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, TMP_DIR),
  filename: (_req, file, cb) => {
    const safe = Buffer.from(file.originalname, "latin1").toString("utf8");
    cb(null, `${uuidv4()}_${safe}`);
  },
});
const upload = multer({
  storage,
  limits: { fileSize: 20 * 1024 * 1024, files: 50 },
  fileFilter: (_req, file, cb) => {
    const ok = file.mimetype === "application/pdf" || file.originalname.toLowerCase().endsWith(".pdf");
    cb(null, ok);
  },
});

// ─── Sessions ───────────────────────────────────────────────────────────────
const sessions = {};

function scheduleSessionCleanup(sid) {
  setTimeout(() => {
    const sess = sessions[sid];
    if (!sess) return;
    for (const f of sess.files.values()) {
      fs.rm(f.path, { force: true }, () => {});
    }
    delete sessions[sid];
  }, _SESSION_TTL_MS);
}

// ─── PDF -> Base64 ──────────────────────────────────────────────────────────
// Préchargé une seule fois au démarrage pour éviter les imports concurrents
let _pdfModulePromise = null;
function getPdfModule() {
  if (!_pdfModulePromise) _pdfModulePromise = import("pdf-to-img");
  return _pdfModulePromise;
}

async function pdfToBase64(filePath) {
  const { pdf } = await getPdfModule();
  const doc = await pdf(filePath, { scale: 1.5 });
  for await (const page of doc) {
    return Buffer.from(page).toString("base64");
  }
  throw new Error("PDF vide");
}

// ─── Extraction LC ──────────────────────────────────────────────────────────
async function extractLC(b64) {
  const detail = _ORIGINAL_MODELS.has(_MODEL) ? "original" : "high";
  const r = await openai.responses.create({
    model: _MODEL,
    input: [{
      role: "user",
      content: [
        { type: "input_text", text: _PROMPT },
        { type: "input_image", image_url: `data:image/png;base64,${b64}`, detail },
      ],
    }],
    max_output_tokens: 64,
    temperature: 0,
  });
  const val = r.output_text.trim();
  if (!val) throw new Error("Reponse vide de l'API.");
  return val;
}

function sanitizeFilename(name) {
  return name.replace(_INVALID_CHARS, "_").trim() || "inconnu";
}

// ─── Traitement d'un fichier unique ─────────────────────────────────────────
async function processOne(f) {
  f.status = "processing";
  try {
    console.log(`[TRADEX] Traitement : ${f.original}`);
    const b64 = await pdfToBase64(f.path);
    const lc = await extractLC(b64);
    f.lc = lc;
    f.status = "ok";
    console.log(`[TRADEX] OK : ${f.original} -> ${lc}`);
  } catch (e) {
    console.error(`[TRADEX] ERREUR : ${f.original} — ${e.message}`);
    f.err = e.message;
    f.status = "error";
  }
}

// ─── Pool de concurrence ─────────────────────────────────────────────────────
async function runWithConcurrency(tasks, limit) {
  const queue = [...tasks];
  const workers = Array.from({ length: Math.min(limit, queue.length) }, async () => {
    while (queue.length) {
      const task = queue.shift();
      if (task) await task();
    }
  });
  await Promise.all(workers);
}

// ─── Processing asynchrone (parallèle) ──────────────────────────────────────
async function processFiles(sid) {
  const sess = sessions[sid];
  if (!sess) return;

  const tasks = [...sess.files.values()].map(f => () => processOne(f));
  await runWithConcurrency(tasks, _CONCURRENCY);

  sess.done = true;
  console.log(`[TRADEX] Session ${sid} terminee.`);
}

// ─── Express ────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ▸ Health check
app.get("/health", (_req, res) => res.json({ status: "ok" }));

// ▸ Upload
app.post("/api/upload", upload.array("pdfs", 50), (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: "Aucun fichier PDF recu." });
  }
  const sid = uuidv4();
  const files = new Map();
  for (const f of req.files) {
    const fid = uuidv4();
    // originalname déjà converti en UTF-8 par le filename Multer
    const original = Buffer.from(f.originalname, "latin1").toString("utf8");
    files.set(fid, { original, path: f.path, status: "pending", lc: null, err: null });
  }
  sessions[sid] = { files, done: false };
  scheduleSessionCleanup(sid);

  processFiles(sid);

  res.json({
    session_id: sid,
    count: req.files.length,
    files: [...files.entries()].map(([id, f]) => ({ id, original: f.original })),
  });
});

// ▸ SSE Stream
app.get("/api/stream/:sid", (req, res) => {
  const sid = req.params.sid;
  if (!sessions[sid]) return res.status(404).json({ error: "Session inconnue." });

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });

  const sess = sessions[sid];
  const total = sess.files.size;
  let lastSnapshot = "";

  const interval = setInterval(() => {
    let processed = 0, ok = 0, err = 0;
    const list = [];
    for (const [fid, f] of sess.files) {
      if (f.status === "ok" || f.status === "error") processed++;
      if (f.status === "ok") ok++;
      if (f.status === "error") err++;
      list.push({ id: fid, original: f.original, status: f.status, lc: f.lc, err: f.err });
    }

    const data = JSON.stringify({ type: "progress", processed, total, ok, err, files: list });
    if (data !== lastSnapshot) {
      res.write(`data: ${data}\n\n`);
      lastSnapshot = data;
    }

    if (sess.done && processed >= total) {
      res.write(`data: ${JSON.stringify({ type: "done", processed, total, ok, err, files: list })}\n\n`);
      clearInterval(interval);
      res.end();
    }
  }, 500);

  req.on("close", () => clearInterval(interval));
});

// ▸ Download individuel
app.get("/api/download/:sid/:fid", (req, res) => {
  const { sid, fid } = req.params;
  const sess = sessions[sid];
  if (!sess) return res.status(404).json({ error: "Session inconnue." });
  const f = sess.files.get(fid);
  if (!f) return res.status(404).json({ error: "Fichier inconnu." });
  if (f.status !== "ok") return res.status(400).json({ error: "Fichier non traite correctement." });

  const safe = sanitizeFilename(f.lc);
  res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(safe + ".pdf")}`);
  res.setHeader("Content-Type", "application/pdf");
  fs.createReadStream(f.path).pipe(res);
});

// ▸ Download batch ZIP
app.get("/api/download-all/:sid", (req, res) => {
  const sid = req.params.sid;
  const sess = sessions[sid];
  if (!sess) return res.status(404).json({ error: "Session inconnue." });

  const okFiles = [...sess.files.values()].filter(f => f.status === "ok");
  if (okFiles.length === 0) {
    return res.status(400).json({ error: "Aucun fichier traite avec succes." });
  }

  try {
    const zip = new AdmZip();
    const used = new Set();
    for (const f of okFiles) {
      let safe = sanitizeFilename(f.lc);
      let name = `${safe}.pdf`;
      let n = 1;
      while (used.has(name)) { name = `${safe}_(${n++}).pdf`; }
      used.add(name);
      zip.addLocalFile(f.path, "", name);
    }

    const buf = zip.toBuffer();
    res.setHeader("Content-Disposition", "attachment; filename=\"TRADEX_export.zip\"");
    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Length", buf.length);
    res.send(buf);
  } catch (e) {
    console.error("[TRADEX] Erreur ZIP:", e);
    res.status(500).json({ error: "Erreur creation ZIP." });
  }
});

// ─── Port libre ─────────────────────────────────────────────────────────────
function freePort() {
  const net = require("net");
  return new Promise(resolve => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => { const p = srv.address().port; srv.close(() => resolve(p)); });
    srv.on("error", () => resolve(5000));
  });
}

// ─── Lancement ──────────────────────────────────────────────────────────────
(async () => {
  const isProduction = !!process.env.PORT;
  const port = process.env.PORT || await freePort();
  const host = isProduction ? "0.0.0.0" : "127.0.0.1";
  const url = `http://${host === "0.0.0.0" ? "localhost" : host}:${port}`;

  // Préchargement du module PDF au démarrage
  getPdfModule().catch(() => {});

  app.listen(port, host, () => {
    console.log(`\n  TRADEX  --  ${url}\n  Concurrence : ${_CONCURRENCY} fichiers simultanes\n  Ctrl+C pour arreter.\n`);
    if (!isProduction) {
      const { exec } = require("child_process");
      exec(`start ${url}`);
    }
  });
})();
