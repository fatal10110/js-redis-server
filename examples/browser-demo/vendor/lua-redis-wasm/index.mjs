import { createHash } from 'node:crypto';

/**
 * @fileoverview Binary codec for the Redis Lua WASM ABI.
 *
 * This module handles serialization and deserialization of data between the
 * JavaScript host and the WASM Lua runtime. All data is binary-safe - null bytes
 * and arbitrary binary content are fully supported.
 *
 * ## Wire Format
 *
 * ### Reply Format
 * Each reply is encoded as:
 * ```
 * [type: u8][count_or_len: u32le][payload: bytes...]
 * ```
 *
 * Type tags:
 * - 0x00: NULL - no payload
 * - 0x01: INTEGER - 8-byte int64le payload
 * - 0x02: BULK STRING - raw bytes payload
 * - 0x03: ARRAY - count of nested Reply items
 * - 0x04: STATUS - raw bytes payload (Redis +OK style)
 * - 0x05: ERROR - raw bytes payload (Redis -ERR style)
 *
 * ### Argument Array Format
 * ```
 * [count: u32le][entry1][entry2]...
 * ```
 * Each entry:
 * ```
 * [len: u32le][bytes...]
 * ```
 *
 * @module codec
 */
/** Reply type tag: null/nil value. Wire format: [0x00][0x00000000] */
const REPLY_NULL = 0x00;
/** Reply type tag: 64-bit signed integer. Wire format: [0x01][0x00000008][int64le] */
const REPLY_INT = 0x01;
/** Reply type tag: bulk string (binary-safe bytes). Wire format: [0x02][length: u32le][bytes...] */
const REPLY_BULK = 0x02;
/** Reply type tag: array of nested replies. Wire format: [0x03][count: u32le][reply1][reply2]... */
const REPLY_ARRAY = 0x03;
/** Reply type tag: status reply (Redis +OK style). Wire format: [0x04][length: u32le][bytes...] */
const REPLY_STATUS = 0x04;
/** Reply type tag: error reply (Redis -ERR style). Wire format: [0x05][length: u32le][bytes...] */
const REPLY_ERROR = 0x05;
/**
 * Reply type tag: script-aborting error. Same wire payload as REPLY_ERROR, but
 * signals that the error aborted the script (uncaught runtime error or an error
 * that propagated out of redis.call) and should be decorated with the script
 * sha / source context by the engine. Returned error *values* (e.g.
 * `return redis.pcall(...)`) use REPLY_ERROR and are left undecorated.
 * Wire format: [0x06][length: u32le][bytes...]
 */
const REPLY_SCRIPT_ERROR = 0x06;
/**
 * Splits a raw error payload (`CODE message`) into a structured error reply.
 *
 * The leading token is treated as the error code only when it matches
 * `/^[A-Z][A-Z0-9]*$/` (Redis error-code convention); otherwise the whole
 * payload is the message and `code` is omitted. Binary-safe: the message bytes
 * are preserved verbatim.
 */
function splitErrorPayload(payload) {
    const space = payload.indexOf(0x20);
    if (space > 0 && isErrorCode(payload, space)) {
        return {
            err: Buffer.from(payload.subarray(space + 1)),
            code: Buffer.from(payload.subarray(0, space)),
        };
    }
    return { err: Buffer.from(payload) };
}
/** Tests whether `buffer[0, end)` matches the Redis error-code shape `[A-Z][A-Z0-9]*`. */
function isErrorCode(buffer, end) {
    if (buffer[0] < 0x41 || buffer[0] > 0x5a) {
        return false;
    }
    for (let i = 1; i < end; i += 1) {
        const c = buffer[i];
        const isUpper = c >= 0x41 && c <= 0x5a;
        const isDigit = c >= 0x30 && c <= 0x39;
        if (!isUpper && !isDigit) {
            return false;
        }
    }
    return true;
}
/**
 * Converts various input types to a Buffer for binary-safe processing.
 *
 * This function is the foundation of binary safety in the codec - it ensures
 * all data is handled as raw bytes without any string coercion or encoding
 * transformation (except for strings, which are UTF-8 encoded).
 *
 * @param value - The value to convert (Buffer, Uint8Array, or string)
 * @param label - Descriptive label for error messages
 * @returns A Buffer containing the binary data
 * @throws TypeError if value is not a supported type
 *
 * @example
 * ```typescript
 * ensureBuffer(Buffer.from([0x00, 0x01]), "key");  // Returns same Buffer
 * ensureBuffer(new Uint8Array([1, 2]), "key");    // Converts to Buffer
 * ensureBuffer("hello", "key");                    // UTF-8 encodes to Buffer
 * ```
 */
function ensureBuffer(value, label) {
    if (Buffer.isBuffer(value)) {
        return value;
    }
    if (value instanceof Uint8Array) {
        return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    }
    if (typeof value === "string") {
        return Buffer.from(value, "utf8");
    }
    throw new TypeError(`${label} must be a Buffer, Uint8Array, or string`);
}
/**
 * Writes a number or bigint as a little-endian 64-bit signed integer.
 *
 * @param value - The integer value to encode
 * @returns 8-byte Buffer containing the int64le representation
 */
function writeInt64LE(value) {
    const buf = Buffer.alloc(8);
    const big = typeof value === "bigint" ? value : BigInt(value);
    buf.writeBigInt64LE(big, 0);
    return buf;
}
/**
 * Encodes a ReplyValue into the ABI wire format for transmission to WASM.
 *
 * This is the primary serialization function for sending Redis-compatible
 * reply values to the Lua runtime. The encoding is recursive for arrays
 * and handles all Redis reply types.
 *
 * @param value - The value to encode
 * @returns Buffer containing the encoded wire format
 *
 * @example
 * ```typescript
 * encodeReplyValue(null);                          // NULL reply
 * encodeReplyValue(42);                            // INTEGER reply
 * encodeReplyValue(Buffer.from("hello"));          // BULK STRING reply
 * encodeReplyValue({ ok: Buffer.from("OK") });     // STATUS reply
 * encodeReplyValue({ err: Buffer.from("ERR") });   // ERROR reply
 * encodeReplyValue([1, 2, 3]);                     // ARRAY reply
 * ```
 */
function encodeReplyValue(value) {
    // Handle null/undefined -> NULL reply
    if (value === null || value === undefined) {
        return Buffer.from([REPLY_NULL, 0, 0, 0, 0]);
    }
    // Handle numbers and bigints -> INTEGER reply
    if (typeof value === "number" || typeof value === "bigint") {
        const payload = writeInt64LE(value);
        const header = Buffer.alloc(5);
        header[0] = REPLY_INT;
        header.writeUInt32LE(payload.length, 1);
        return Buffer.concat([header, payload]);
    }
    // Handle arrays -> ARRAY reply (recursive encoding)
    if (Array.isArray(value)) {
        const items = value.map(encodeReplyValue);
        const header = Buffer.alloc(5);
        header[0] = REPLY_ARRAY;
        header.writeUInt32LE(items.length, 1);
        return Buffer.concat([header, ...items]);
    }
    // Handle objects with 'ok' or 'err' properties -> STATUS/ERROR reply
    if (typeof value === "object") {
        if (Object.prototype.hasOwnProperty.call(value, "ok")) {
            const payload = ensureBuffer(value.ok, "status reply");
            const header = Buffer.alloc(5);
            header[0] = REPLY_STATUS;
            header.writeUInt32LE(payload.length, 1);
            return Buffer.concat([header, payload]);
        }
        if (Object.prototype.hasOwnProperty.call(value, "err")) {
            const errValue = value;
            const message = ensureBuffer(errValue.err, "error reply");
            // Prepend the code so the wire payload is the Redis "CODE message" form.
            const payload = errValue.code
                ? Buffer.concat([
                    ensureBuffer(errValue.code, "error code"),
                    Buffer.from(" "),
                    message,
                ])
                : message;
            const header = Buffer.alloc(5);
            header[0] = REPLY_ERROR;
            header.writeUInt32LE(payload.length, 1);
            return Buffer.concat([header, payload]);
        }
    }
    // Default: treat as bulk string
    const payload = ensureBuffer(value, "bulk reply");
    const header = Buffer.alloc(5);
    header[0] = REPLY_BULK;
    header.writeUInt32LE(payload.length, 1);
    return Buffer.concat([header, payload]);
}
/**
 * Decodes the ABI wire format into a ReplyValue tree.
 *
 * This is the primary deserialization function for receiving reply values
 * from the Lua runtime. It recursively decodes nested arrays and returns
 * both the decoded value and the new buffer offset.
 *
 * @param buffer - The buffer containing encoded reply data
 * @param offset - Starting offset in the buffer (default: 0)
 * @returns Object containing the decoded value and new offset position
 * @throws Error if the buffer is truncated or contains unknown types
 *
 * @example
 * ```typescript
 * const { value, offset } = decodeReply(buffer);
 * // value is the decoded ReplyValue
 * // offset is the position after the decoded data
 * ```
 */
function decodeReply(buffer, offset = 0) {
    // Validate minimum header size (1 byte type + 4 bytes count/len)
    if (offset + 5 > buffer.length) {
        throw new Error("ERR reply decoding failed");
    }
    const type = buffer.readUInt8(offset);
    const countOrLen = buffer.readUInt32LE(offset + 1);
    let cursor = offset + 5;
    // Decode based on type tag
    if (type === REPLY_NULL) {
        return { value: null, offset: cursor };
    }
    if (type === REPLY_INT) {
        if (cursor + 8 > buffer.length) {
            throw new Error("ERR reply decoding failed");
        }
        const big = buffer.readBigInt64LE(cursor);
        cursor += 8;
        // Return number if within safe integer range, otherwise bigint
        const value = big >= BigInt(Number.MIN_SAFE_INTEGER) &&
            big <= BigInt(Number.MAX_SAFE_INTEGER)
            ? Number(big)
            : big;
        return { value, offset: cursor };
    }
    if (type === REPLY_BULK) {
        const payload = buffer.subarray(cursor, cursor + countOrLen);
        cursor += countOrLen;
        return { value: Buffer.from(payload), offset: cursor };
    }
    if (type === REPLY_STATUS) {
        const payload = buffer.subarray(cursor, cursor + countOrLen);
        cursor += countOrLen;
        return { value: { ok: Buffer.from(payload) }, offset: cursor };
    }
    if (type === REPLY_ERROR || type === REPLY_SCRIPT_ERROR) {
        const payload = buffer.subarray(cursor, cursor + countOrLen);
        cursor += countOrLen;
        return { value: splitErrorPayload(payload), offset: cursor };
    }
    if (type === REPLY_ARRAY) {
        const items = [];
        for (let i = 0; i < countOrLen; i += 1) {
            const decoded = decodeReply(buffer, cursor);
            items.push(decoded.value);
            cursor = decoded.offset;
        }
        return { value: items, offset: cursor };
    }
    throw new Error("ERR unknown reply type");
}
/**
 * Encodes an array of arguments into the ArgArray ABI format.
 *
 * This format is used for passing KEYS and ARGV to Lua scripts, as well as
 * for encoding redis.call/redis.pcall arguments. All arguments are converted
 * to binary-safe buffers.
 *
 * Wire format:
 * ```
 * [count: u32le][len1: u32le][bytes1...][len2: u32le][bytes2...]...
 * ```
 *
 * @param args - Array of arguments (Buffer, Uint8Array, or string)
 * @returns Buffer containing the encoded argument array
 *
 * @example
 * ```typescript
 * encodeArgArray([Buffer.from("GET"), Buffer.from("key:1")]);
 * encodeArgArray(["SET", "key", "value"]);  // Strings are UTF-8 encoded
 * ```
 */
function encodeArgArray(args) {
    const parts = [];
    // Write argument count
    const header = Buffer.alloc(4);
    header.writeUInt32LE(args.length, 0);
    parts.push(header);
    // Write each argument as [length][bytes]
    for (const arg of args) {
        const buf = ensureBuffer(arg, "arg");
        const len = Buffer.alloc(4);
        len.writeUInt32LE(buf.length, 0);
        parts.push(len, buf);
    }
    return Buffer.concat(parts);
}
/**
 * Packs a pointer and length into a single bigint for non-sret ABI paths.
 *
 * Some WASM ABI calling conventions return pointer+length pairs as a single
 * 64-bit value. This function creates such a packed value with the pointer
 * in the lower 32 bits and length in the upper 32 bits.
 *
 * @param ptr - Memory pointer (32-bit unsigned)
 * @param len - Data length (32-bit unsigned)
 * @returns Packed bigint: (len << 32) | ptr
 *
 * @example
 * ```typescript
 * const packed = packPtrLen(0x1000, 256);
 * // packed = 0x0000010000001000n
 * ```
 */
function packPtrLen(ptr, len) {
    return (BigInt(len) << 32n) | BigInt(ptr >>> 0);
}
/**
 * Unpacks a PtrLen from various ABI return shapes.
 *
 * Different WASM runtimes and calling conventions may return pointer+length
 * pairs in different formats. This function handles:
 * - bigint: packed format from packPtrLen
 * - number[]: array of [ptr, len]
 * - { ptr, len }: explicit object format
 *
 * @param result - The return value from a WASM function
 * @returns Object containing ptr and len as numbers
 * @throws Error if the input format is not recognized
 *
 * @example
 * ```typescript
 * unpackPtrLen(0x0000010000001000n);      // { ptr: 0x1000, len: 256 }
 * unpackPtrLen([0x1000, 256]);            // { ptr: 0x1000, len: 256 }
 * unpackPtrLen({ ptr: 0x1000, len: 256 }); // { ptr: 0x1000, len: 256 }
 * ```
 */
function unpackPtrLen(result) {
    if (typeof result === "bigint") {
        const ptr = Number(result & 0xffffffffn);
        const len = Number(result >> 32n);
        return { ptr, len };
    }
    if (Array.isArray(result)) {
        return { ptr: Number(result[0]), len: Number(result[1]) };
    }
    if (result &&
        typeof result === "object" &&
        "ptr" in result &&
        "len" in result) {
        return { ptr: Number(result.ptr), len: Number(result.len) };
    }
    throw new Error("Unexpected PtrLen return type");
}

/**
 * @fileoverview WASM module loader for the Redis Lua engine.
 *
 * This module handles loading and instantiating the Emscripten-compiled
 * WASM module. It provides:
 * - Path resolution for bundled WASM and JS glue files
 * - Host import injection for redis.call/pcall/log callbacks
 * - Module instantiation with custom configuration
 *
 * ## Architecture
 *
 * The loader bridges between Emscripten's module system and our host:
 *
 * ```
 * ┌─────────────────────┐
 * │   Host (Node.js)    │
 * │  - hostImports      │
 * │  - readBytes/write  │
 * └──────────┬──────────┘
 *            │ instantiateWasm
 *            ▼
 * ┌─────────────────────┐
 * │  Emscripten Glue    │
 * │  (redis_lua.mjs)    │
 * └──────────┬──────────┘
 *            │
 *            ▼
 * ┌─────────────────────┐
 * │    WASM Module      │
 * │  (redis_lua.wasm)   │
 * │  - Lua 5.1 VM       │
 * │  - Redis API layer  │
 * └─────────────────────┘
 * ```
 *
 * @module loader
 */
// Browser-safe by construction: NO top-level `node:*` imports. The Node-only
// path resolution and filesystem reads are loaded via dynamic `import("node:*")`
// inside `isNode` branches, so a browser bundler (Vite/Rollup) never has to
// resolve a Node builtin to put this module in the graph. The browser branch
// instead resolves the co-located Emscripten glue + `.wasm` via `import.meta.url`
// so the bundler emits them as assets.
const isNode = typeof process !== "undefined" &&
    process.versions != null &&
    process.versions.node != null;
/**
 * Node-only: resolve a co-located asset, preferring the built dist/ layout and
 * falling back to the dev wasm/build/ layout. Dynamic-imports node builtins so
 * this module stays browser-safe.
 *
 * @param file - Bare asset filename, e.g. "redis_lua.wasm"
 * @returns Absolute filesystem path to the first existing candidate
 */
async function nodeAssetPath(file) {
    const fs = await import('node:fs');
    const path = await import('node:path');
    const { fileURLToPath } = await import('node:url');
    const here = path.dirname(fileURLToPath(import.meta.url));
    for (const rel of [`./${file}`, `../wasm/build/${file}`]) {
        const candidate = path.resolve(here, rel);
        if (fs.existsSync(candidate)) {
            return candidate;
        }
    }
    return path.resolve(here, `./${file}`);
}
/**
 * Returns the default location of the WASM binary as a URL href co-located with
 * this module (e.g. `file://.../dist/redis_lua.wasm` in Node, the served asset
 * URL in a browser bundle). String-returning and browser-safe.
 *
 * @returns URL href of the bundled WASM binary
 */
function defaultWasmPath() {
    return new URL("./redis_lua.wasm", import.meta.url).href;
}
/**
 * Returns the default location of the Emscripten JS glue module as a URL href
 * co-located with this module. String-returning and browser-safe.
 *
 * @returns URL href of the bundled JS glue module
 */
function defaultModulePath() {
    return new URL("./redis_lua.mjs", import.meta.url).href;
}
/**
 * Loads and instantiates the Emscripten WASM module with host imports.
 *
 * This is the core module loading function. It:
 * 1. Loads the Emscripten JS glue module
 * 2. Reads the WASM binary (from file or provided bytes)
 * 3. Injects host callback functions into the WASM imports
 * 4. Instantiates the WebAssembly module
 *
 * The host imports are injected into both `env` and `wasi_snapshot_preview1`
 * namespaces for compatibility with different Emscripten configurations.
 *
 * @param options - Engine or standalone options with optional custom paths
 * @param hostImports - Map of host callback functions to inject
 * @returns Object containing the instantiated module and exports
 *
 * @example
 * ```typescript
 * const hostImports = {
 *   host_redis_call: (retPtr, ptr, len) => { ... },
 *   host_redis_pcall: (retPtr, ptr, len) => { ... },
 *   host_redis_log: (level, ptr, len) => { ... },
 *   host_sha1hex: (retPtr, ptr, len) => { ... }
 * };
 *
 * const { module, exports } = await loadModule(options, hostImports);
 * ```
 */
async function loadModule(options, hostImports) {
    const moduleFactory = await loadGlueFactory(options);
    const wasmBinary = await loadWasmBinary(options);
    // Instantiate the Emscripten module with custom WASM instantiation
    const module = await moduleFactory({
        // wasmBinary + the custom instantiateWasm below fully drive instantiation,
        // so locateFile is never consulted for the .wasm — pass other files through.
        locateFile: (file) => file,
        wasmBinary,
        // Custom instantiation to inject host imports
        instantiateWasm(imports, successCallback) {
            // Merge host callbacks into the env namespace
            const env = imports.env || {};
            imports.env = { ...env, ...hostImports };
            // Also add to WASI namespace for compatibility
            imports.wasi_snapshot_preview1 = imports.env;
            // Perform async instantiation
            WebAssembly.instantiate(wasmBinary, imports).then((result) => {
                const instantiated = result;
                successCallback(instantiated.instance, instantiated.module);
            });
            // Return empty object to signal async instantiation
            return {};
        }
    });
    return { module, exports: module };
}
/**
 * Load the Emscripten glue module factory.
 * - Browser: literal `import("./redis_lua.mjs")` so the bundler statically emits
 *   and resolves the glue as an asset.
 * - Node: dynamic import of the resolved `file://` URL (dist/ or dev wasm/build/),
 *   honoring an explicit `options.modulePath`.
 */
async function loadGlueFactory(options) {
    if (!isNode) {
        if (options.modulePath) {
            // Explicit URL (e.g. a jsdelivr CDN URL). Fully dynamic so the bundler
            // doesn't try to resolve/emit it; @vite-ignore silences the warning.
            const imported = await import(/* @vite-ignore */ options.modulePath);
            return (imported.default ?? imported);
        }
        // Bundled default: literal specifier so the bundler emits + resolves the glue
        // as a co-located asset.
        // @ts-ignore - Emscripten glue has no type declarations; resolved by the bundler.
        const imported = await import('./redis_lua.mjs');
        return (imported.default ?? imported);
    }
    const { pathToFileURL } = await import('node:url');
    const modulePath = options.modulePath ?? (await nodeAssetPath("redis_lua.mjs"));
    const moduleUrl = /^[a-z]+:\/\//i.test(modulePath)
        ? modulePath
        : pathToFileURL(modulePath).href;
    const imported = await import(moduleUrl);
    return (imported.default ?? imported);
}
/**
 * Load the WASM binary bytes.
 * - `options.wasmBytes` always wins (lets callers bypass any file/network read).
 * - Browser: fetch the co-located asset resolved via `import.meta.url`.
 * - Node: read the resolved file (`options.wasmPath`, else dist/ or dev wasm/build/).
 */
async function loadWasmBinary(options) {
    if (options.wasmBytes) {
        return options.wasmBytes;
    }
    if (!isNode) {
        // Explicit URL (e.g. jsdelivr) wins; otherwise the co-located bundled asset.
        const wasmUrl = options.wasmPath ?? new URL("./redis_lua.wasm", import.meta.url);
        const response = await fetch(wasmUrl);
        if (!response.ok) {
            throw new Error(`Failed to fetch redis_lua.wasm: ${response.status} ${response.statusText}`);
        }
        return new Uint8Array(await response.arrayBuffer());
    }
    const { readFile } = await import('node:fs/promises');
    const wasmPath = options.wasmPath ?? (await nodeAssetPath("redis_lua.wasm"));
    return new Uint8Array(await readFile(wasmPath));
}

/**
 * @fileoverview Shared helper functions for WASM memory operations and ABI handling.
 * @module helpers
 */
// =============================================================================
// Memory Helpers
// =============================================================================
/**
 * Reads bytes from WASM linear memory into a Buffer.
 * Binary-safe - no string coercion or encoding transformation.
 */
function readBytes(heap, ptr, len) {
    return Buffer.from(heap.subarray(ptr, ptr + len));
}
/**
 * Writes a Buffer into WASM linear memory at the given pointer.
 */
function writeBytes(heap, ptr, data) {
    heap.set(data, ptr);
}
/**
 * Allocates memory and writes data in one operation.
 * Returns the pointer to the allocated memory.
 */
function allocAndWrite(exports$1, data) {
    const ptr = exports$1._alloc(data.length);
    writeBytes(exports$1.HEAPU8, ptr, data);
    return ptr;
}
/**
 * Encodes a ReplyValue and writes it to WASM memory.
 * Returns the pointer and length for passing back to WASM.
 */
function encodeReplyToPtrLen(exports$1, value) {
    const encoded = encodeReplyValue(value);
    const ptr = allocAndWrite(exports$1, encoded);
    return { ptr, len: encoded.length };
}
/**
 * Writes a PtrLen struct to WASM memory for sret-style returns.
 * Layout: [ptr: u32le][len: u32le] = 8 bytes total
 */
function writePtrLen(heap, retPtr, ptrLen) {
    heap[retPtr] = ptrLen.ptr & 0xff;
    heap[retPtr + 1] = (ptrLen.ptr >> 8) & 0xff;
    heap[retPtr + 2] = (ptrLen.ptr >> 16) & 0xff;
    heap[retPtr + 3] = (ptrLen.ptr >> 24) & 0xff;
    heap[retPtr + 4] = ptrLen.len & 0xff;
    heap[retPtr + 5] = (ptrLen.len >> 8) & 0xff;
    heap[retPtr + 6] = (ptrLen.len >> 16) & 0xff;
    heap[retPtr + 7] = (ptrLen.len >> 24) & 0xff;
}
/**
 * Parses ABI arguments to extract return pointer, data pointer, and length.
 * Handles both sret (3+ args) and direct return (2 args) ABI conventions.
 */
function parseAbiArgs(args) {
    const hasRet = args.length >= 3;
    return {
        hasRet,
        retPtr: hasRet ? args[0] : 0,
        ptr: hasRet ? args[1] : args[0],
        len: hasRet ? args[2] : args[1]
    };
}
/**
 * Returns PtrLen result using the appropriate ABI convention.
 * For sret: writes to retPtr and returns void.
 * For direct: returns packed bigint.
 */
function returnPtrLen(heap, abiArgs, ptrLen) {
    if (abiArgs.hasRet) {
        writePtrLen(heap, abiArgs.retPtr, ptrLen);
        return;
    }
    return packPtrLen(ptrLen.ptr, ptrLen.len);
}
// =============================================================================
// Argument Decoding
// =============================================================================
/**
 * Decodes an ArgArray payload from a Buffer into Buffer arguments.
 * Wire format: [count: u32le][len: u32le][bytes]...
 */
function decodeArgs(buf) {
    if (buf.length < 4) {
        throw new Error("ERR invalid argument encoding");
    }
    const count = buf.readUInt32LE(0);
    const out = [];
    let offset = 4;
    for (let i = 0; i < count; i += 1) {
        if (offset + 4 > buf.length) {
            throw new Error("ERR invalid argument encoding");
        }
        const argLen = buf.readUInt32LE(offset);
        offset += 4;
        if (offset + argLen > buf.length) {
            throw new Error("ERR invalid argument encoding");
        }
        out.push(Buffer.from(buf.subarray(offset, offset + argLen)));
        offset += argLen;
    }
    return out;
}
// =============================================================================
// SHA1 Helper
// =============================================================================
/**
 * Computes SHA1 hex digest from input data.
 * Returns 40-char hex string as Buffer.
 */
function computeSha1Hex(data) {
    const hex = createHash("sha1").update(data).digest("hex");
    return Buffer.from(hex, "utf8");
}

/**
 * @fileoverview Main API for executing Redis Lua scripts in WebAssembly.
 *
 * This module provides the primary API for the redis-lua-wasm package:
 * - `load()` - Async function to load the WASM module
 * - `LuaWasmModule` - Factory for creating engine instances
 * - `LuaEngine` - Executes Lua scripts
 * - `LuaWasmEngine` - Convenience API (combines load and create)
 *
 * ## Architecture
 *
 * The API separates async loading from sync execution:
 *
 * ```
 * ┌─────────────────────────────────────────────────────────────┐
 * │                      load(options)                          │
 * │  - Async WASM loading                                       │
 * │  - Returns LuaWasmModule                                    │
 * └─────────────────────┬───────────────────────────────────────┘
 *                       │
 *                       ▼
 * ┌─────────────────────────────────────────────────────────────┐
 * │                    LuaWasmModule                            │
 * │  - create(host) → LuaEngine                                 │
 * │  - createStandalone() → LuaEngine                           │
 * │  - One-time use (consumed after create)                     │
 * └─────────────────────┬───────────────────────────────────────┘
 *                       │
 *                       ▼
 * ┌─────────────────────────────────────────────────────────────┐
 * │                      LuaEngine                              │
 * │  - eval(script)                                             │
 * │  - evalWithArgs(script, keys, args)                         │
 * └─────────────────────────────────────────────────────────────┘
 * ```
 *
 * ## Host Callbacks
 *
 * When Lua code calls `redis.call()`, `redis.pcall()`, `redis.log()`, or
 * `redis.sha1hex()`, the WASM module invokes host-provided callbacks:
 *
 * - `host_redis_call` - Handles redis.call() (may throw)
 * - `host_redis_pcall` - Handles redis.pcall() (returns errors)
 * - `host_redis_log` - Handles redis.log() messages
 * - `host_sha1hex` - Computes SHA1 hex digest
 *
 * @module engine
 */
/**
 * Lua script execution engine.
 *
 * This class provides methods to evaluate Lua scripts. Instances are created
 * via `LuaWasmModule` or `LuaWasmEngine`.
 *
 * ## Evaluating Scripts
 *
 * ```typescript
 * // Simple evaluation
 * engine.eval("return 1 + 1");  // Returns: 2
 *
 * // With KEYS and ARGV
 * engine.evalWithArgs(
 *   "return {KEYS[1], ARGV[1]}",
 *   [Buffer.from("key")],
 *   [Buffer.from("arg")]
 * );
 * ```
 */
class LuaEngine {
    exports;
    limits;
    /**
     * @internal
     */
    constructor(exports$1, limits) {
        this.exports = exports$1;
        this.limits = limits;
    }
    /**
     * Returns the configured resource limits, if any.
     * @returns EngineLimits object or undefined if no limits configured
     */
    getLimits() {
        return this.limits;
    }
    /**
     * Evaluates a Lua script and returns the result.
     *
     * The script is executed in a fresh Lua environment. Return values
     * are converted to JavaScript types:
     * - Lua numbers -> JavaScript number or bigint
     * - Lua strings -> Buffer (binary-safe)
     * - Lua tables -> Array
     * - Lua nil -> null
     *
     * @param script - Lua source code as string, Buffer, or Uint8Array
     * @returns The script's return value as a ReplyValue
     *
     * @example
     * ```typescript
     * engine.eval("return 1 + 1");           // 2
     * engine.eval("return 'hello'");         // Buffer.from("hello")
     * engine.eval("return {1, 2, 3}");       // [1, 2, 3]
     * engine.eval("return redis.call('PING')"); // {ok: Buffer.from("PONG")}
     * ```
     */
    eval(script) {
        const scriptBuf = ensureBuffer(script, "script");
        const sha = computeSha1Hex(scriptBuf).toString("utf8");
        const ptr = this.exports._alloc(scriptBuf.length);
        this.exports.HEAPU8.set(scriptBuf, ptr);
        const result = this.callEval(ptr, scriptBuf.length);
        this.exports._free_mem(ptr);
        return this.decodeResult(result, sha);
    }
    /**
     * Evaluates a Lua script with KEYS and ARGV arrays injected.
     *
     * This matches Redis's EVALSHA/EVAL interface. The KEYS and ARGV
     * globals are populated before script execution and are binary-safe.
     *
     * @param script - Lua source code
     * @param keys - Array of KEYS values (typically key names)
     * @param args - Array of ARGV values (additional arguments)
     * @returns The script's return value as a ReplyValue
     *
     * @example
     * ```typescript
     * engine.evalWithArgs(
     *   "return {KEYS[1], ARGV[1]}",
     *   [Buffer.from("user:1")],
     *   [Buffer.from("active")]
     * );
     * // Returns: [Buffer.from("user:1"), Buffer.from("active")]
     * ```
     */
    evalWithArgs(script, keys = [], args = []) {
        const scriptBuf = ensureBuffer(script, "script");
        const sha = computeSha1Hex(scriptBuf).toString("utf8");
        const argBuf = encodeArgArray([...keys, ...args]);
        // Enforce maxArgBytes limit on host side
        if (this.limits?.maxArgBytes && argBuf.length > this.limits.maxArgBytes) {
            return {
                err: Buffer.from("ERR KEYS/ARGV exceeds configured limit", "utf8"),
            };
        }
        const scriptPtr = this.exports._alloc(scriptBuf.length);
        const argsPtr = this.exports._alloc(argBuf.length);
        this.exports.HEAPU8.set(scriptBuf, scriptPtr);
        this.exports.HEAPU8.set(argBuf, argsPtr);
        const result = this.callEvalWithArgs(scriptPtr, scriptBuf.length, argsPtr, argBuf.length, keys.length);
        this.exports._free_mem(scriptPtr);
        this.exports._free_mem(argsPtr);
        return this.decodeResult(result, sha);
    }
    /**
     * Calls the WASM _eval function, handling different ABI conventions.
     * @private
     */
    callEval(ptr, len) {
        if (this.exports._eval.length >= 3) {
            const retPtr = this.exports._alloc(8);
            this.exports._eval(retPtr, ptr, len);
            const ptrLen = this.readPtrLen(retPtr);
            this.exports._free_mem(retPtr);
            return ptrLen;
        }
        const result = this.exports._eval(ptr, len);
        if (result === undefined) {
            throw new Error("Unexpected PtrLen return type");
        }
        return result;
    }
    /**
     * Calls the WASM _eval_with_args function with KEYS/ARGV.
     * @private
     */
    callEvalWithArgs(scriptPtr, scriptLen, argsPtr, argsLen, keysCount) {
        if (this.exports._eval_with_args.length >= 6) {
            const retPtr = this.exports._alloc(8);
            this.exports._eval_with_args(retPtr, scriptPtr, scriptLen, argsPtr, argsLen, keysCount);
            const ptrLen = this.readPtrLen(retPtr);
            this.exports._free_mem(retPtr);
            return ptrLen;
        }
        const result = this.exports._eval_with_args(scriptPtr, scriptLen, argsPtr, argsLen, keysCount);
        if (result === undefined) {
            throw new Error("Unexpected PtrLen return type");
        }
        return result;
    }
    /**
     * Reads a PtrLen struct from WASM memory.
     * @private
     */
    readPtrLen(base) {
        const heap = this.exports.HEAPU8;
        if (base + 8 > heap.length) {
            throw new Error("Unexpected PtrLen return type");
        }
        const ptr = heap[base] |
            (heap[base + 1] << 8) |
            (heap[base + 2] << 16) |
            (heap[base + 3] << 24);
        const len = heap[base + 4] |
            (heap[base + 5] << 8) |
            (heap[base + 6] << 16) |
            (heap[base + 7] << 24);
        return { ptr, len };
    }
    /**
     * Decodes a PtrLen result from WASM into a ReplyValue.
     * @private
     */
    decodeResult(result, sha) {
        let ptrLen;
        if (typeof result === "number") {
            if (this.exports.getTempRet0) {
                const len = this.exports.getTempRet0();
                if (!len) {
                    throw new Error("Unexpected PtrLen return type");
                }
                ptrLen = { ptr: result >>> 0, len };
            }
            else {
                ptrLen = this.readPtrLen(result >>> 0);
            }
        }
        else {
            ptrLen = unpackPtrLen(result);
        }
        const { ptr, len } = ptrLen;
        if (!ptr || !len) {
            return null;
        }
        if (this.limits?.maxReplyBytes && len > this.limits.maxReplyBytes) {
            this.exports._free_mem(ptr);
            return { err: Buffer.from("ERR reply exceeds configured limit", "utf8") };
        }
        const buffer = Buffer.from(this.exports.HEAPU8.subarray(ptr, ptr + len));
        this.exports._free_mem(ptr);
        const topTag = len > 0 ? buffer.readUInt8(0) : -1;
        const value = decodeReply(buffer).value;
        // Decorate only errors that aborted the script (REPLY_SCRIPT_ERROR): an
        // uncaught Lua runtime error or an error that propagated out of redis.call.
        // Error values the script returns (REPLY_ERROR, e.g. `return redis.pcall`)
        // are passed through untouched, matching Redis.
        if (topTag === REPLY_SCRIPT_ERROR &&
            value &&
            typeof value === "object" &&
            "err" in value) {
            return decorateScriptError(value, sha);
        }
        return value;
    }
}
/**
 * Appends the Redis script source context to a script-aborting error message.
 *
 * Lua runtime errors carry a `user_script:N:` prefix (N is the line); command
 * errors propagated out of redis.call have no prefix and are reported at line 1.
 * The error `code` (if any) is preserved.
 */
function decorateScriptError(value, sha) {
    const errStr = value.err.toString("utf8");
    let line = "1";
    if (errStr.startsWith("user_script:")) {
        const colonIdx = errStr.indexOf(":", 12); // after "user_script:"
        line = colonIdx > 12 ? errStr.substring(12, colonIdx) : "1";
    }
    const formatted = `${errStr} script: ${sha}, on @user_script:${line}.`;
    return { err: Buffer.from(formatted, "utf8"), code: value.code };
}
/**
 * Loaded WASM module that can create engine instances.
 *
 * This class holds a loaded WASM module and provides factory methods
 * to create `LuaEngine` instances. It can only be used once - after
 * calling `create()` or `createStandalone()`, subsequent calls will throw.
 *
 * @example
 * ```typescript
 * const module = await load({ limits: { maxFuel: 1_000_000 } });
 *
 * // Create with Redis host
 * const engine = module.create(myRedisHost);
 *
 * // OR create standalone (no redis.call support)
 * const standalone = module.createStandalone();
 * ```
 */
class LuaWasmModule {
    exports;
    handlers;
    options;
    consumed = false;
    /**
     * @internal
     */
    constructor(exports$1, handlers, options) {
        this.exports = exports$1;
        this.handlers = handlers;
        this.options = options;
    }
    /**
     * Creates an engine with full Redis host integration.
     *
     * This binds the host callbacks to the WASM module. The host provides
     * implementations for `redis.call()`, `redis.pcall()`, and `redis.log()`.
     *
     * This method can only be called once per module instance.
     *
     * @param host - Redis host implementation
     * @returns Configured LuaEngine instance
     * @throws Error if module has already been used
     *
     * @example
     * ```typescript
     * const engine = module.create({
     *   redisCall(args) {
     *     const cmd = args[0].toString().toUpperCase();
     *     if (cmd === "PING") return { ok: Buffer.from("PONG") };
     *     throw new Error("ERR unknown command");
     *   },
     *   redisPcall(args) {
     *     try { return this.redisCall(args); }
     *     catch (e) { return { err: Buffer.from(e.message) }; }
     *   },
     *   log(level, msg) { console.log(msg.toString()); }
     * });
     * ```
     */
    create(host) {
        this.ensureNotConsumed();
        this.consumed = true;
        this.wireHostCallbacks(host);
        this.initializeLua();
        return new LuaEngine(this.exports, this.options.limits);
    }
    /**
     * Creates a standalone engine without Redis host integration.
     *
     * In standalone mode, `redis.call()` and `redis.pcall()` return errors.
     * This is useful for running pure Lua computations or testing.
     *
     * This method can only be called once per module instance.
     *
     * @returns Configured LuaEngine instance
     * @throws Error if module has already been used
     *
     * @example
     * ```typescript
     * const engine = module.createStandalone();
     *
     * engine.eval("return math.sqrt(16)");  // Returns: 4
     * engine.eval("redis.call('PING')");    // Returns: {err: "ERR..."}
     * ```
     */
    createStandalone() {
        this.ensureNotConsumed();
        this.consumed = true;
        this.wireStandaloneCallbacks();
        this.initializeLua();
        return new LuaEngine(this.exports, this.options.limits);
    }
    /**
     * Returns the default path to the bundled WASM binary.
     */
    static defaultWasmPath() {
        return defaultWasmPath();
    }
    /**
     * Returns the default path to the bundled Emscripten JS module.
     */
    static defaultModulePath() {
        return defaultModulePath();
    }
    ensureNotConsumed() {
        if (this.consumed) {
            throw new Error("LuaWasmModule has already been used. Load a new module with load().");
        }
    }
    initializeLua() {
        if (this.exports._set_limits && this.options.limits) {
            this.exports._set_limits(this.options.limits.maxFuel ?? 0, this.options.limits.maxReplyBytes ?? 0, this.options.limits.maxArgBytes ?? 0);
        }
        const initResult = this.exports._init();
        if (typeof initResult === "number" && initResult !== 0) {
            throw new Error("Failed to initialize Lua WASM engine");
        }
    }
    wireHostCallbacks(host) {
        const exports$1 = this.exports;
        const callHandler = (args, isPcall) => {
            try {
                return isPcall
                    ? host.redisPcall.call(host, args)
                    : host.redisCall.call(host, args);
            }
            catch (err) {
                const message = err instanceof Error ? err.message : String(err);
                return { err: Buffer.from(message, "utf8") };
            }
        };
        this.handlers.log = (level, ptr, len) => {
            const msg = readBytes(exports$1.HEAPU8, ptr, len);
            host.log(level, msg);
        };
        this.handlers.sha1hex = (...args) => {
            const abiArgs = parseAbiArgs(args);
            const data = readBytes(exports$1.HEAPU8, abiArgs.ptr, abiArgs.len);
            const bytes = computeSha1Hex(data);
            const ptrLen = { ptr: allocAndWrite(exports$1, bytes), len: bytes.length };
            return returnPtrLen(exports$1.HEAPU8, abiArgs, ptrLen);
        };
        this.handlers.call = (...args) => {
            const abiArgs = parseAbiArgs(args);
            const decoded = decodeArgs(readBytes(exports$1.HEAPU8, abiArgs.ptr, abiArgs.len));
            const ptrLen = encodeReplyToPtrLen(exports$1, callHandler(decoded, false));
            return returnPtrLen(exports$1.HEAPU8, abiArgs, ptrLen);
        };
        this.handlers.pcall = (...args) => {
            const abiArgs = parseAbiArgs(args);
            const decoded = decodeArgs(readBytes(exports$1.HEAPU8, abiArgs.ptr, abiArgs.len));
            const ptrLen = encodeReplyToPtrLen(exports$1, callHandler(decoded, true));
            return returnPtrLen(exports$1.HEAPU8, abiArgs, ptrLen);
        };
    }
    wireStandaloneCallbacks() {
        const exports$1 = this.exports;
        const notSupported = (action) => ({
            err: Buffer.from(`ERR ${action} is not available in standalone mode`, "utf8"),
        });
        this.handlers.log = () => { };
        this.handlers.sha1hex = (...args) => {
            const abiArgs = parseAbiArgs(args);
            const data = readBytes(exports$1.HEAPU8, abiArgs.ptr, abiArgs.len);
            const bytes = computeSha1Hex(data);
            const ptrLen = { ptr: allocAndWrite(exports$1, bytes), len: bytes.length };
            return returnPtrLen(exports$1.HEAPU8, abiArgs, ptrLen);
        };
        this.handlers.call = (...args) => {
            const abiArgs = parseAbiArgs(args);
            const ptrLen = encodeReplyToPtrLen(exports$1, notSupported("redis.call"));
            return returnPtrLen(exports$1.HEAPU8, abiArgs, ptrLen);
        };
        this.handlers.pcall = (...args) => {
            const abiArgs = parseAbiArgs(args);
            const ptrLen = encodeReplyToPtrLen(exports$1, notSupported("redis.pcall"));
            return returnPtrLen(exports$1.HEAPU8, abiArgs, ptrLen);
        };
    }
}
/**
 * Loads the WASM module and returns a LuaWasmModule for creating engines.
 *
 * This is the main entry point for the package. It handles async WASM loading
 * and returns a module that can be used to create engine instances synchronously.
 *
 * @param options - Optional configuration for paths and limits
 * @returns Promise resolving to a LuaWasmModule
 *
 * @example
 * ```typescript
 * // Basic usage
 * const module = await load();
 * const engine = module.create(myRedisHost);
 *
 * // With options
 * const module = await load({
 *   limits: { maxFuel: 10_000_000 },
 *   wasmPath: "/custom/path/to/redis_lua.wasm"
 * });
 * ```
 */
async function load(options = {}) {
    // Mutable handlers - these will be set by wireHostCallbacks/wireStandaloneCallbacks
    const handlers = {
        log: () => { },
        sha1hex: () => BigInt(0),
        call: () => BigInt(0),
        pcall: () => BigInt(0),
    };
    // Create wrapper imports that delegate to mutable handlers
    // These wrappers are captured by WASM at instantiation, but they call handlers which can be swapped
    const hostImports = {
        host_redis_log: (level, ptr, len) => handlers.log(level, ptr, len),
        host_sha1hex: (...args) => handlers.sha1hex(...args),
        host_redis_call: (...args) => handlers.call(...args),
        host_redis_pcall: (...args) => handlers.pcall(...args),
    };
    const { exports: exports$1 } = await loadModule(options, hostImports);
    return new LuaWasmModule(exports$1, handlers, options);
}
/**
 * This class provides a convenience API
 * where `create()` and `createStandalone()` are static async methods.
 *
 * @example
 * ```typescript
 * // Convenience API
 * const engine = await LuaWasmEngine.create({ host: myHost });
 *
 * // Modular API
 * const module = await load();
 * const engine = module.create(myHost);
 * ```
 */
class LuaWasmEngine {
    engine;
    constructor(engine) {
        this.engine = engine;
    }
    static async create(options) {
        const module = await load(options);
        const engine = module.create(options.host);
        return new LuaWasmEngine(engine);
    }
    static async createStandalone(options = {}) {
        const module = await load(options);
        const engine = module.createStandalone();
        return new LuaWasmEngine(engine);
    }
    static defaultWasmPath() {
        return defaultWasmPath();
    }
    static defaultModulePath() {
        return defaultModulePath();
    }
    eval(script) {
        return this.engine.eval(script);
    }
    evalWithArgs(script, keys = [], args = []) {
        return this.engine.evalWithArgs(script, keys, args);
    }
    getLimits() {
        return this.engine.getLimits();
    }
}

function encodeReply(value) {
    return encodeReplyValue(value);
}
function decodeReplyBuffer(buffer) {
    return decodeReply(buffer).value;
}
function encodeArgs(args) {
    return encodeArgArray(args);
}

export { LuaEngine, LuaWasmEngine, LuaWasmModule, decodeReplyBuffer, encodeArgs, encodeReply, load };
//# sourceMappingURL=index.mjs.map
