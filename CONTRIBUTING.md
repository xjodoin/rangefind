# Contributing to Rangefind

Thanks for your interest. Rangefind is a small, focused project — issues,
patches, and benchmark reports are all welcome.

## Scope

Rangefind is a static, range-fetched search engine. Changes that fit the
project well:

- index format improvements (smaller transfers, fewer requests, better top-k),
- runtime improvements that stay dependency-free and work on any static host
  with HTTP `Range` support,
- new analyzers, scorers, or facet/filter primitives that keep the static
  hosting story intact,
- benchmarks against representative corpora,
- documentation and examples.

Changes that are usually out of scope:

- features that require a live server, a hosted service, or a runtime
  dependency,
- index format changes without a benchmark showing the tradeoff,
- broad rewrites that mix unrelated concerns in a single PR.

If you're not sure, open an issue first to discuss the approach.

## Development

```bash
npm install
npm run check        # syntax check every JS file
npm test             # unit tests
npm run test:smoke   # build the example, run a browser-runtime query
npm run test:all     # browser bundle + check + tests + smoke
```

Useful benchmarks:

```bash
npm run bench:quality
npm run bench:performance
npm run bench:directories -- --index=/path/to/public/rangefind
npm run bench:frwiki                      # streams the live Wikipedia dump
```

The benchmark scripts are dependency-free and run against the example static
site.

## Pull Requests

- Keep PRs small and focused on a single change.
- Run `npm run test:all` before pushing.
- For format or scoring changes, include benchmark numbers (before/after) in
  the PR description.
- New runtime dependencies are very unlikely to be accepted — open an issue
  first if you think one is necessary.
- The project has no formatter configured; match the surrounding code style.
- Commit messages: short imperative subject (`feat(runtime): ...`,
  `perf(build): ...`, `fix(...): ...`, `docs: ...`, `test: ...`).

## Reporting Issues

Useful issues include:

- Rangefind version (or commit hash),
- Node.js version,
- a minimal config + sample documents that reproduce the problem,
- expected vs observed behavior.

For performance or quality regressions, the output of `bench:performance` or
`bench:quality` against your corpus is the most useful signal.

## License

By contributing, you agree that your contributions will be licensed under the
project's [MIT License](LICENSE).
