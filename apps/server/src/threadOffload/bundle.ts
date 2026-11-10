// @effect-diagnostics nodeBuiltinImport:off - zstd with a long window and streaming hashes are only in node:zlib and node:crypto.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeStream from "node:stream";
import * as NodeZlib from "node:zlib";

/**
 * A bundle is zstd-compressed lines of JSON: a header, the rows, and a trailer holding the SHA-256
 * and count of every line before it. Each update of a turn item repeats its whole payload (often
 * the same screenshots), so a 128 MiB window with long-distance matching finds those repeats.
 */
const ZSTD_WINDOW_LOG = 27;
const ZSTD_LEVEL = 3;

export const BUNDLE_HEADER = "$header";
export const BUNDLE_TRAILER = "$end";

export interface BundleWriter {
  readonly writeLine: (line: string) => Promise<void>;
  /** Writes the trailer, flushes the file to disk and moves it to its final path. */
  readonly finish: (trailer: (summary: BundleSummary) => string) => Promise<BundleSummary>;
  readonly abort: () => Promise<void>;
}

export interface BundleSummary {
  readonly sha256: string;
  readonly lineCount: number;
}

export const openBundleWriter = async (path: string): Promise<BundleWriter> => {
  const partialPath = `${path}.partial`;
  await NodeFS.promises.rm(partialPath, { force: true });
  const file = NodeFS.createWriteStream(partialPath, { flags: "wx" });
  const zstd = NodeZlib.createZstdCompress({
    params: {
      [NodeZlib.constants.ZSTD_c_compressionLevel]: ZSTD_LEVEL,
      [NodeZlib.constants.ZSTD_c_windowLog]: ZSTD_WINDOW_LOG,
      [NodeZlib.constants.ZSTD_c_enableLongDistanceMatching]: 1,
    },
  });
  const done = NodeStream.promises.pipeline(zstd, file);
  // A failed pipeline must also end a write that waits for drain.
  const failed = done.then(
    () => new Promise<never>(() => {}),
    (cause: unknown) => Promise.reject(cause),
  );
  void failed.catch(() => undefined);
  const hash = NodeCrypto.createHash("sha256");
  let lineCount = 0;

  const push = async (text: string) => {
    const bytes = Buffer.from(`${text}\n`, "utf8");
    if (!zstd.write(bytes)) {
      await Promise.race([new Promise((resolve) => zstd.once("drain", resolve)), failed]);
    }
    return bytes;
  };

  return {
    writeLine: async (line) => {
      hash.update(await push(line));
      lineCount += 1;
    },
    finish: async (trailer) => {
      const summary = { sha256: hash.digest("hex"), lineCount };
      await push(trailer(summary));
      zstd.end();
      await done;
      const handle = await NodeFS.promises.open(partialPath, "r+");
      try {
        await handle.sync();
      } finally {
        await handle.close();
      }
      await NodeFS.promises.rename(partialPath, path);
      return summary;
    },
    abort: async () => {
      zstd.destroy();
      await done.catch(() => undefined);
      await NodeFS.promises.rm(partialPath, { force: true });
    },
  };
};

/** Lines of a bundle, decompressed as they are read. The trailer comes last, unhashed. */
export interface BundleReader {
  readonly next: () => Promise<string | undefined>;
  readonly close: () => void;
  /** SHA-256 and count of the lines read so far, before the trailer. */
  readonly summary: () => BundleSummary;
}

export const openBundleReader = (path: string): BundleReader => {
  const source = NodeFS.createReadStream(path);
  const zstd = NodeZlib.createZstdDecompress({
    params: { [NodeZlib.constants.ZSTD_d_windowLogMax]: ZSTD_WINDOW_LOG },
  });
  const decompressed = source.pipe(zstd);
  source.on("error", (cause) => zstd.destroy(cause));
  const chunks = decompressed[Symbol.asyncIterator]();
  const hash = NodeCrypto.createHash("sha256");
  let pending: Array<Buffer> = [];
  let lineCount = 0;
  let ended = false;
  let previous: Buffer | undefined;

  const takeLine = (): Buffer | undefined => {
    for (let index = 0; index < pending.length; index += 1) {
      const chunk = pending[index]!;
      const newline = chunk.indexOf(0x0a);
      if (newline === -1) continue;
      const parts = [...pending.slice(0, index), chunk.subarray(0, newline + 1)];
      const rest = chunk.subarray(newline + 1);
      pending = rest.length > 0 ? [rest, ...pending.slice(index + 1)] : pending.slice(index + 1);
      return parts.length === 1 ? parts[0]! : Buffer.concat(parts);
    }
    return undefined;
  };

  return {
    next: async () => {
      while (true) {
        const line = takeLine();
        if (line !== undefined) {
          // Hash a line once the next one shows it was not the trailer.
          if (previous !== undefined) {
            hash.update(previous);
            lineCount += 1;
          }
          previous = line;
          return line.toString("utf8", 0, line.length - 1);
        }
        if (ended) {
          if (pending.some((chunk) => chunk.length > 0)) {
            throw new Error("The bundle ends in the middle of a line.");
          }
          return undefined;
        }
        const chunk = await chunks.next();
        if (chunk.done === true) ended = true;
        else pending.push(chunk.value as Buffer);
      }
    },
    close: () => {
      source.destroy();
      zstd.destroy();
    },
    summary: () => ({ sha256: hash.copy().digest("hex"), lineCount }),
  };
};

export const fileSize = async (path: string) => (await NodeFS.promises.stat(path)).size;

/** A file name for a thread id, which can be long and hold characters paths refuse. */
export const threadKey = (threadId: string) =>
  NodeCrypto.createHash("sha256").update(threadId).digest("hex").slice(0, 32);

export const writeFileAtomically = async (path: string, text: string) => {
  const partialPath = `${path}.partial`;
  await NodeFS.promises.writeFile(partialPath, text);
  await NodeFS.promises.rename(partialPath, path);
};
