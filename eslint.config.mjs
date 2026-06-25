import typescriptEslint from '@typescript-eslint/eslint-plugin'
import eslintConfigPrettier from 'eslint-config-prettier'
import globals from 'globals'
import tsParser from '@typescript-eslint/parser'
import js from '@eslint/js'

export default [
  {
    ignores: ['**/dist/**', '**/node_modules/**', '**/coverage/**', '.grepai/**'],
  },
  js.configs.recommended,
  ...typescriptEslint.configs['flat/recommended'],
  eslintConfigPrettier,
  {
    languageOptions: {
      globals: {
        ...globals.node,
      },

      parser: tsParser,
      ecmaVersion: 'latest',
      sourceType: 'module',
    },

    rules: {
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
      '@typescript-eslint/no-explicit-any': 'warn',
      'prefer-const': ['error', { ignoreReadBeforeAssign: true }],
    },
  },
]
