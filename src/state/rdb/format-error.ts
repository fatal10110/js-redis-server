/** A payload that does not parse; RESTORE answers `Bad data format`. */
export class RdbFormatError extends Error {
  constructor() {
    super('Bad data format')
  }
}
