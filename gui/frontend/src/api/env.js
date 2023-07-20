function env(mode = '', opts = { isOnly: false }) {
  const yamls = {}
  const list = []

  if (mode === '*') {
    return yamls
  }

  if (opts.isOnly) {
    return Object.assign({}, yamls[`${mode}.yaml`])
  } else {
    list.push(...['.env.yaml', '.env.local.yaml'])
    if (mode) {
      list.push(
        ...[
          `.env.${mode}.yaml`,
          `.env.${mode}.local.yaml`,
          `${mode}.yaml`,
          `${mode}.local.yaml`,
        ]
      )
    }

    return Object.assign({}, ...list.map((key) => yamls[key] || {}))
  }
}

export default env
