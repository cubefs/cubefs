module.exports = {
  root: true,
  env: {
    node: true
  },
  globals: {
    CONFIG: true
  },
  extends: ['plugin:vue/recommended', '@vue/standard'],
  rules: {
    'no-console': process.env.NODE_ENV === 'production' ? 'error' : 'off',
    'no-debugger': process.env.NODE_ENV === 'production' ? 'error' : 'off',
    'no-var': 1,
    'object-curly-spacing': 1,
    'comma-dangle': [1, 'always-multiline'],
    'prefer-const': 1,
    'space-before-function-paren': 0,
    'prefer-promise-reject-errors': 0,
    'vue/singleline-html-element-content-newline': 0,
    'vue/arrow-spacing': 1,
    'vue/block-spacing': 1,
    'vue/v-on-function-call': 1,
    'vue/html-self-closing': 0,
    'vue/eqeqeq': 1,
    'vue/object-curly-spacing': ['warn', 'always'],
    'vue/multiline-html-element-content-newline': 0,
    'vue/max-attributes-per-line': [
      'error',
      {
        singleline: 5,
        multiline: {
          max: 1,
        }
      }
    ]
  },
  parserOptions: {
    parser: 'babel-eslint'
  }
}
