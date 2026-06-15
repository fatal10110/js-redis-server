declare module 'respjs' {
  // Minimal typing to satisfy usages in adapter without pulling in external types.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const Resp: any
  export default Resp
}
