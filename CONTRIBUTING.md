# Contributing

Thanks for helping. This file covers how changes get in. For how the repository is organized and how to work on it, see [docs/CONNECTOR_GUIDE.md](docs/CONNECTOR_GUIDE.md).

## Before you start

- Check [docs/ROADMAP.md](docs/ROADMAP.md) and the open issues. If what you want is not there, open an issue first.
- One topic per pull request. Small ones get reviewed faster.

## Branches

Work on a short lived branch from `main` with a plain name. Rebase on `main` before asking for review.

## Commits

Keep them short and plain, written like a person explaining the change to a colleague.

- Subject line in the imperative, under 60 characters, no period at the end. For example `Add NIF check digit validation` or `Fix stable id on cluster split`.
- If the change needs context, leave a blank line and write two or three sentences about why. Skip the body when the subject says it all.
- No prefixes, tags, emojis or generated summaries. No trailers except `Signed-off-by`.
- One logical change per commit. Squash fixup commits before merging.

## Checks

Run these before pushing a branch. CI runs the same.

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Pull requests

The description answers three things in plain sentences. What changes, why, and how you tested it. A pull request merges when CI is green and a maintainer approves it. Only maintainers push to `main`.

Sign off your commits (`git commit -s`) under the Developer Certificate of Origin (https://developercertificate.org).

## Data

Test data is synthetic. Never commit real company names, tax ids, emails, phones, addresses or credentials.
