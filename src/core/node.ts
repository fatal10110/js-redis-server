import { DB } from './db'
import { RequestHandler } from './request'
import commands from './commands'

export class Node {
  private readonly db = new DB()
  public readonly requestHandler: RequestHandler

  constructor() {
    this.requestHandler = new RequestHandler(this.db, commands)
  }
}
