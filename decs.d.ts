/// <reference types="node" />

declare module 'respjs' {
  interface Resp {
    new (options?: { bufBulk?: boolean }): Writable
    encodeError(error: Error): Buffer
    encodeString(str: string): Buffer
    encodeNull(): Buffer
    encodeInteger(i: number): Buffer
    encodeArray(arr: Iterable): Buffer
    encodeBufBulk(buff: Buffer): Buffer
    encodeNullArray(): Buffer
  }

  declare const resp: Resp
  export default resp
}
