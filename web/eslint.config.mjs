import path from 'node:path'
import js from '@eslint/js'
import { defineConfig, includeIgnoreFile } from 'eslint/config'
import globals from 'globals'
import tseslint from 'typescript-eslint'

export default defineConfig(
  includeIgnoreFile(path.join(import.meta.dirname, '.gitignore')),
  { linterOptions: { noInlineConfig: true } },
  js.configs.recommended,
  tseslint.configs.strictTypeChecked,
  tseslint.configs.stylisticTypeChecked,
  {
    languageOptions: {
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
    },
    rules: {
      'no-inline-comments': 'error',
      '@typescript-eslint/consistent-type-definitions': ['error', 'type'],
      '@typescript-eslint/no-confusing-void-expression': ['error', { ignoreArrowShorthand: true }],
      '@typescript-eslint/no-unused-vars': ['error', { ignoreRestSiblings: true }],
      '@typescript-eslint/restrict-template-expressions': [
        'error',
        { allowAny: false, allowBoolean: false, allowNever: false, allowNullish: false, allowNumber: true, allowRegExp: false },
      ],
    },
  },
  {
    files: ['**/*.mjs'],
    extends: [tseslint.configs.disableTypeChecked],
    languageOptions: { globals: globals.nodeBuiltin },
  },
)
