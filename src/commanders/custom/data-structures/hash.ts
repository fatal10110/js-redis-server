export class HashDataType {
  private readonly data: Map<Buffer, Buffer>

  constructor() {
    this.data = new Map<Buffer, Buffer>()
  }
}
