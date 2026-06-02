import { randomBytes } from "node:crypto";

// ULID-style id. Must match the Python SDK byte-for-byte in format (PROTOCOL.md):
// <prefix>_ + 26-char Crockford base32 of (48-bit ms timestamp << 80 | 80-bit random).
const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

export function newUlid(tsMs: number = Date.now()): string {
  let value = (BigInt(tsMs) << 80n) | BigInt("0x" + randomBytes(10).toString("hex"));
  const chars: string[] = [];
  for (let i = 0; i < 26; i++) {
    chars.push(CROCKFORD[Number(value & 31n)]);
    value >>= 5n;
  }
  return chars.reverse().join("");
}

export function newId(prefix = "task"): string {
  return `${prefix}_${newUlid()}`;
}

export function nowMs(): number {
  return Date.now();
}
