export type UuidCrypto = {
  randomUUID?: () => string;
  getRandomValues?: (values: Uint8Array) => Uint8Array;
};

let fallbackCounter = 0;

function browserCrypto(): UuidCrypto | null {
  try {
    return globalThis.crypto ?? null;
  } catch {
    return null;
  }
}

/**
 * Generate a UUID in secure and non-secure browser contexts.
 *
 * `crypto.randomUUID()` is unavailable on the production HTTP/IP origin. The
 * fallback is for record identity (not authentication): it prefers
 * `getRandomValues`, then uses timestamp/counter-mixed PRNG bytes.
 */
export function createUuid(source: UuidCrypto | null = browserCrypto()): string {
  try {
    if (typeof source?.randomUUID === 'function') {
      return source.randomUUID.call(source);
    }
  } catch {
    // Continue to the byte-based fallback; identity creation must not abort a tap.
  }

  const bytes = new Uint8Array(16);
  try {
    if (typeof source?.getRandomValues === 'function') {
      source.getRandomValues.call(source, bytes);
    } else {
      throw new Error('getRandomValues unavailable');
    }
  } catch {
    fallbackCounter = (fallbackCounter + 1) >>> 0;
    const now = Date.now();
    for (let index = 0; index < bytes.length; index += 1) {
      const timeByte = Math.floor(now / (2 ** ((index % 6) * 8))) & 0xff;
      const counterByte = (fallbackCounter >>> ((index % 4) * 8)) & 0xff;
      bytes[index] = Math.floor(Math.random() * 256) ^ timeByte ^ counterByte;
    }
  }

  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, value => value.toString(16).padStart(2, '0'));
  return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex.slice(6, 8).join('')}-${hex.slice(8, 10).join('')}-${hex.slice(10).join('')}`;
}
