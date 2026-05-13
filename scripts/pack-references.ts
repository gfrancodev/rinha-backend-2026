import { gunzipSync } from "node:zlib";
import { readFileSync, writeFileSync } from "node:fs";

const outPath = "references.bin";
const gzPath = "references.json.gz";
const gz = readFileSync(gzPath);
const text = gunzipSync(gz).toString("utf8");
const arr = JSON.parse(text) as { vector: number[]; label: string }[];
const n = arr.length;
const floats = new Float32Array(n * 14);
const labels = new Uint8Array(n);
for (let i = 0; i < n; i++) {
  floats.set(arr[i].vector, i * 14);
  labels[i] = arr[i].label === "fraud" ? 1 : 0;
}
const b = Buffer.allocUnsafe(8 + floats.byteLength + labels.byteLength);
b.writeUInt32BE(0x5242494e, 0);
b.writeUInt32LE(n, 4);
Buffer.from(new Uint8Array(floats.buffer, floats.byteOffset, floats.byteLength)).copy(b, 8);
Buffer.from(labels.buffer, labels.byteOffset, labels.byteLength).copy(b, 8 + floats.byteLength);
writeFileSync(outPath, b);
