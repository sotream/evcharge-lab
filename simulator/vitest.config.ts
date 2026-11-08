import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      { test: { name: 'unit', include: ['test/unit/**/*.test.ts'] } },
      {
        test: {
          name: 'int',
          include: ['test/int/**/*.test.ts'],
          testTimeout: 90_000,
          hookTimeout: 90_000,
          // Each file uses its own topics / station ids; keep broker load predictable.
          fileParallelism: false,
        },
      },
    ],
  },
});
