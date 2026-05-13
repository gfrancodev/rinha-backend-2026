import { existsSync, unlinkSync } from "node:fs";
import { gunzipSync } from "node:zlib";

interface FraudRequest {
  id: string;
  transaction: {
    amount: number;
    installments: number;
    requested_at: string;
  };
  customer: {
    avg_amount: number;
    tx_count_24h: number;
    known_merchants: string[];
  };
  merchant: {
    id: string;
    mcc: string;
    avg_amount: number;
  };
  terminal: {
    is_online: boolean;
    card_present: boolean;
    km_from_home: number;
  };
  last_transaction: {
    timestamp: string;
    km_from_current: number;
  } | null;
}

interface NormalizationConfig {
  max_amount: number;
  max_installments: number;
  amount_vs_avg_ratio: number;
  max_minutes: number;
  max_km: number;
  max_tx_count_24h: number;
  max_merchant_avg_amount: number;
}

interface Neighbor {
  distance: number;
  isFraud: boolean;
}

type LeafN = { t: 1; ids: Int32Array };
type InN = {
  t: 0;
  vp: number;
  mu: number;
  mi: number;
  ma: number;
  L: TNode;
  R: TNode;
};
type TNode = InN | LeafN;

const LEAF = 32;
const SORT_THRESH = 384;
const K = 5;

let mccRiskMap: Map<string, number>;
let normalizationConfig: NormalizationConfig;
let points: Float32Array;
let labels: Uint8Array;
let nPts = 0;
let perm: Int32Array;
let distWork: Float32Array;
let scratchIx: Int32Array;
let root: TNode | null = null;

function euclIdxIdx(a: number, b: number): number {
  let sum = 0;
  const oa = a * 14;
  const ob = b * 14;
  for (let i = 0; i < 14; i++) {
    const d = points[oa + i] - points[ob + i];
    sum += d * d;
  }
  return Math.sqrt(sum);
}

function euclQueryIdx(q: Float32Array, b: number): number {
  let sum = 0;
  const ob = b * 14;
  for (let i = 0; i < 14; i++) {
    const d = q[i] - points[ob + i];
    sum += d * d;
  }
  return Math.sqrt(sum);
}

function swapPerm(a: number, b: number): void {
  const t = perm[a];
  perm[a] = perm[b];
  perm[b] = t;
}

function swapPair(b0: number, a: number, b: number): void {
  if (a === b) return;
  const pa = perm[b0 + a];
  perm[b0 + a] = perm[b0 + b];
  perm[b0 + b] = pa;
  const da = distWork[a];
  distWork[a] = distWork[b];
  distWork[b] = da;
}

function sortBlock(b0: number, L: number): void {
  for (let i = 0; i < L; i++) scratchIx[i] = i;
  scratchIx.subarray(0, L).sort((a, b) => {
    const da = distWork[a];
    const db = distWork[b];
    if (da !== db) return da - db;
    return perm[b0 + a] - perm[b0 + b];
  });
  const np = new Int32Array(L);
  const nd = new Float32Array(L);
  for (let i = 0; i < L; i++) {
    const j = scratchIx[i];
    np[i] = perm[b0 + j];
    nd[i] = distWork[j];
  }
  for (let i = 0; i < L; i++) {
    perm[b0 + i] = np[i];
    distWork[i] = nd[i];
  }
}

function qselect(b0: number, L: number, k: number): void {
  let lo = 0;
  let hi = L - 1;
  for (;;) {
    if (lo >= hi) return;
    const pivotD = distWork[hi];
    const pivotP = perm[b0 + hi];
    let i = lo;
    for (let j = lo; j < hi; j++) {
      const dj = distWork[j];
      const pj = perm[b0 + j];
      if (dj < pivotD || (dj === pivotD && pj < pivotP)) {
        swapPair(b0, i, j);
        i++;
      }
    }
    swapPair(b0, i, hi);
    if (i === k) return;
    if (k < i) hi = i - 1;
    else lo = i + 1;
  }
}

function buildRange(s: number, len: number): TNode {
  if (len <= LEAF) {
    const ids = new Int32Array(len);
    for (let i = 0; i < len; i++) ids[i] = perm[s + i];
    return { t: 1, ids };
  }
  const pick = ((s * 12582917) ^ (len * 2246822519)) >>> 0;
  swapPerm(s, s + (pick % len));
  const vp = perm[s];
  const L = len - 1;
  const b0 = s + 1;
  let mi = Infinity;
  let ma = 0;
  for (let i = 0; i < L; i++) {
    const d = euclIdxIdx(vp, perm[b0 + i]);
    distWork[i] = d;
    if (d < mi) mi = d;
    if (d > ma) ma = d;
  }
  const med = L >> 1;
  if (L < SORT_THRESH) sortBlock(b0, L);
  else qselect(b0, L, med);
  const mu = distWork[med];
  const leftLen = med;
  const rightLen = L - med;
  const Lsub = buildRange(b0, leftLen);
  const Rsub = buildRange(b0 + leftLen, rightLen);
  return { t: 0, vp, mu, mi: mi, ma: ma, L: Lsub, R: Rsub };
}

function pushNeighbor(heap: Neighbor[], d: number, isFraud: boolean): void {
  if (heap.length < K) {
    heap.push({ distance: d, isFraud });
    if (heap.length === K) heap.sort((a, b) => a.distance - b.distance);
    return;
  }
  if (d >= heap[K - 1].distance) return;
  heap[K - 1] = { distance: d, isFraud };
  heap.sort((a, b) => a.distance - b.distance);
}

function tauFrom(heap: Neighbor[]): number {
  return heap.length < K ? Infinity : heap[K - 1].distance;
}

function searchTree(node: TNode, q: Float32Array, heap: Neighbor[]): void {
  if (node.t === 1) {
    const { ids } = node;
    for (let i = 0; i < ids.length; i++) {
      const idx = ids[i]!;
      pushNeighbor(heap, euclQueryIdx(q, idx), labels[idx] === 1);
    }
    return;
  }
  const { vp, mu, mi, ma, L, R } = node;
  let tau = tauFrom(heap);
  const dist = euclQueryIdx(q, vp);
  pushNeighbor(heap, dist, labels[vp] === 1);
  tau = tauFrom(heap);
  if (dist < mu) {
    if (L && mi - tau < dist) searchTree(L, q, heap);
    tau = tauFrom(heap);
    if (R && mu - tau < dist && dist < ma + tau) searchTree(R, q, heap);
  } else {
    if (R && dist < ma + tau) searchTree(R, q, heap);
    tau = tauFrom(heap);
    if (L && mi - tau < dist && dist < mu + tau) searchTree(L, q, heap);
  }
}

function knnSearch(q: Float32Array): Neighbor[] {
  const heap: Neighbor[] = [];
  if (!root) return heap;
  searchTree(root, q, heap);
  return heap;
}

const clamp = (x: number): number => Math.max(0, Math.min(x, 1.0));

function vectorize(payload: FraudRequest): Float32Array {
  const v = new Float32Array(14);
  const tx = payload.transaction;
  const customer = payload.customer;
  const merchant = payload.merchant;
  const terminal = payload.terminal;
  const lastTx = payload.last_transaction;

  const requestedAt = new Date(tx.requested_at);
  const hour = requestedAt.getUTCHours();
  const dayOfWeek = ((requestedAt.getUTCDay() + 6) % 7) / 6;

  v[0] = clamp(tx.amount / normalizationConfig.max_amount);
  v[1] = clamp(tx.installments / normalizationConfig.max_installments);
  v[2] = clamp((tx.amount / customer.avg_amount) / normalizationConfig.amount_vs_avg_ratio);
  v[3] = hour / 23;
  v[4] = dayOfWeek;

  if (lastTx) {
    const lastAt = new Date(lastTx.timestamp);
    const minutesSince = (requestedAt.getTime() - lastAt.getTime()) / 60000;
    v[5] = clamp(minutesSince / normalizationConfig.max_minutes);
    v[6] = clamp(lastTx.km_from_current / normalizationConfig.max_km);
  } else {
    v[5] = -1;
    v[6] = -1;
  }

  v[7] = clamp(terminal.km_from_home / normalizationConfig.max_km);
  v[8] = clamp(customer.tx_count_24h / normalizationConfig.max_tx_count_24h);
  v[9] = terminal.is_online ? 1 : 0;
  v[10] = terminal.card_present ? 1 : 0;
  v[11] = customer.known_merchants.includes(merchant.id) ? 0 : 1;
  v[12] = mccRiskMap.get(merchant.mcc) ?? 0.5;
  v[13] = clamp(merchant.avg_amount / normalizationConfig.max_merchant_avg_amount);

  return v;
}

async function loadRefsFromBin(path: string): Promise<void> {
  const buf = await Bun.file(path).arrayBuffer();
  const dv = new DataView(buf);
  if (dv.byteLength < 8) throw new Error("short bin");
  const magic = dv.getUint32(0, false);
  if (magic !== 0x5242494e) throw new Error("bad magic");
  nPts = dv.getUint32(4, true);
  const fStart = 8;
  const fBytes = nPts * 14 * 4;
  const lStart = fStart + fBytes;
  if (dv.byteLength < lStart + nPts) throw new Error("short bin payload");
  points = new Float32Array(buf, fStart, nPts * 14);
  labels = new Uint8Array(buf, lStart, nPts);
}

async function loadRefsFromGzipJson(path: string): Promise<void> {
  const raw = await Bun.file(path).arrayBuffer();
  const text = gunzipSync(new Uint8Array(raw)).toString("utf8");
  const references = JSON.parse(text) as { vector: number[]; label: string }[];
  nPts = references.length;
  points = new Float32Array(nPts * 14);
  labels = new Uint8Array(nPts);
  for (let i = 0; i < nPts; i++) {
    points.set(references[i]!.vector, i * 14);
    labels[i] = references[i]!.label === "fraud" ? 1 : 0;
  }
}

async function loadRefsFromJsonArray(path: string): Promise<void> {
  const references = (await Bun.file(path).json()) as { vector: number[]; label: string }[];
  nPts = references.length;
  points = new Float32Array(nPts * 14);
  labels = new Uint8Array(nPts);
  for (let i = 0; i < nPts; i++) {
    points.set(references[i]!.vector, i * 14);
    labels[i] = references[i]!.label === "fraud" ? 1 : 0;
  }
}

async function loadData(): Promise<void> {
  const dataPath = process.env.DATA_PATH || "/data";
  const binPath = process.env.REFERENCES_BIN || "";

  const mccFile = Bun.file(`${dataPath}/mcc_risk.json`);
  const mccJson = (await mccFile.json()) as Record<string, number>;
  mccRiskMap = new Map(Object.entries(mccJson));

  const normFile = Bun.file(`${dataPath}/normalization.json`);
  normalizationConfig = (await normFile.json()) as NormalizationConfig;

  const refJson = process.env.REFERENCES_JSON || "";
  if (binPath) await loadRefsFromBin(binPath);
  else {
    const gzPath = `${dataPath}/references.json.gz`;
    const gzf = Bun.file(gzPath);
    if (await gzf.exists()) await loadRefsFromGzipJson(gzPath);
    else if (refJson && (await Bun.file(refJson).exists()))
      await loadRefsFromJsonArray(refJson);
    else await loadRefsFromJsonArray(`${dataPath}/example-references.json`);
  }

  perm = new Int32Array(nPts);
  for (let i = 0; i < nPts; i++) perm[i] = i;
  distWork = new Float32Array(Math.max(nPts, SORT_THRESH));
  scratchIx = new Int32Array(Math.max(SORT_THRESH, 512));
  console.time("build");
  root = buildRange(0, nPts);
  console.timeEnd("build");
}

interface PipelineCtx {
  request: Request;
  payload?: FraudRequest;
  vector?: Float32Array;
  neighbors?: Neighbor[];
  result?: { approved: boolean; fraud_score: number };
  aborted?: boolean;
}

type PipelineStep = (ctx: PipelineCtx) => "continue" | "abort";

const stepValidatePayload: PipelineStep = (ctx) => {
  const p = ctx.payload;
  if (!p?.transaction || typeof p.transaction.amount !== "number") return "abort";
  if (!p.customer || typeof p.customer.avg_amount !== "number" || p.customer.avg_amount <= 0) return "abort";
  if (!p.merchant?.mcc) return "abort";
  if (!p.terminal) return "abort";
  return "continue";
};

const stepVectorize: PipelineStep = (ctx) => {
  try {
    ctx.vector = vectorize(ctx.payload!);
    return "continue";
  } catch {
    return "abort";
  }
};

const stepSearchNeighbors: PipelineStep = (ctx) => {
  try {
    ctx.neighbors = knnSearch(ctx.vector!);
    return ctx.neighbors.length === K ? "continue" : "abort";
  } catch {
    return "abort";
  }
};

const stepCalculateScore: PipelineStep = (ctx) => {
  const fraudCount = ctx.neighbors!.filter((n) => n.isFraud).length;
  const fraud_score = fraudCount / K;
  ctx.result = {
    approved: fraud_score < 0.6,
    fraud_score: Math.round(fraud_score * 100) / 100,
  };
  return "continue";
};

const FRAUD_PIPELINE: ReadonlyArray<PipelineStep> = [
  stepValidatePayload,
  stepVectorize,
  stepSearchNeighbors,
  stepCalculateScore,
];

const runFraudPipeline = (ctx: PipelineCtx): PipelineCtx => {
  for (const step of FRAUD_PIPELINE) {
    if (step(ctx) === "abort") {
      ctx.aborted = true;
      break;
    }
  }
  return ctx;
};

const safeResp = (): Response =>
  new Response(JSON.stringify({ approved: true, fraud_score: 0.0 }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });

const handleHealthCheck = (): Response => {
  if (!root) return new Response("Loading", { status: 503 });
  return new Response("OK", { status: 200 });
};

const handleFraudScore = async (request: Request): Promise<Response> => {
  let payload: FraudRequest;
  try {
    payload = (await request.json()) as FraudRequest;
  } catch {
    return safeResp();
  }
  const ctx: PipelineCtx = { request, payload };
  runFraudPipeline(ctx);
  if (ctx.aborted || !ctx.result) return safeResp();
  return new Response(JSON.stringify(ctx.result), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
};

await loadData();

const listen = process.env.LISTEN || "/tmp/api.sock";
const fetchHandler = async (request: Request): Promise<Response> => {
  const url = new URL(request.url);
  if (url.pathname === "/ready" && request.method === "GET") return handleHealthCheck();
  if (url.pathname === "/fraud-score" && request.method === "POST") return handleFraudScore(request);
  return new Response("Not Found", { status: 404 });
};

if (/^\d+$/.test(listen)) {
  const port = Number(listen);
  Bun.serve({ hostname: "0.0.0.0", port, fetch: fetchHandler });
  console.log(`Server listening on tcp 0.0.0.0:${port}`);
} else {
  try {
    if (existsSync(listen)) unlinkSync(listen);
  } catch {}
  Bun.serve({ unix: listen, fetch: fetchHandler });
  console.log(`Server listening on ${listen}`);
}
