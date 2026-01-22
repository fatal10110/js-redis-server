export type SchemaType =
  | { type: 'string' }
  | { type: 'integer'; min?: number; max?: number }
  | { type: 'key' }
  | { type: 'flag'; name: string }
  | { type: 'literal'; value: string }
  | { type: 'sequence'; name: string; item: SchemaType }
  | { type: 'union'; options: SchemaType[] }
  | { type: 'xor'; options: SchemaType[] }
  | { type: 'options'; fields: Record<string, SchemaType> }
  | { type: 'tuple'; items: SchemaType[] }
  | { type: 'variadic'; item: SchemaType }
  | { type: 'optional'; item: SchemaType }
