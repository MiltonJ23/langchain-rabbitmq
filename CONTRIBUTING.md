# Contributing to langchain-rabbitmq

Hi there! Thank you for even being interested in contributing to
`langchain-rabbitmq`. As an open-source integration between LangChain and
RabbitMQ, we are very open to contributions — whether that means new features,
infrastructure improvements, better documentation, or bug fixes.

## Ways to Contribute

- **Bug reports** — open an issue describing what happened, what you expected,
  and how to reproduce it.
- **Feature requests** — open an issue or a discussion before writing code so a
  maintainer can confirm the direction.
- **Documentation** — typo fixes, clearer examples, usage guides.
- **Code** — bug fixes, new capabilities, performance improvements.

## Pull Request Requirements

All pull requests must demonstrate meaningful effort and contextual
understanding.

- **Link to an issue.** Every external PR must reference an open issue where
  the solution has been acknowledged by a maintainer. Use `Fixes #<number>` at
  the top of your PR description.
- **Fill in the PR template** when one is available.
- **Pass CI.** Your PR will not be reviewed unless `lint`, `format`, and `test`
  are all green.

Maintainers reserve the right to close PRs without comment if these
requirements are not met. Low-effort or spam submissions will be closed.

## Language Policy

All contributions — issues, pull requests, code reviews, and discussions —
must be in **English**. If English is not your first language, don't worry:
clear communication matters more than perfect grammar, and translation tools
are welcome.

## Use of AI / LLMs

You may use AI assistants to help draft or revise contributions **provided
you verify every change**: run and test the code, check facts against the
codebase and official RabbitMQ / LangChain documentation, and ensure the
result matches the repository style. Do not submit bulk, unreviewed
generated content. PRs that read as low-effort or unverified will be closed.

## Local Setup

```bash
# Clone the repository
git clone https://github.com/MiltonJ23/langchain-rabbitmq.git
cd langchain-rabbitmq

# Create and activate a virtual environment
python -m venv lgrEnv
source lgrEnv/bin/activate   # Windows: lgrEnv\Scripts\activate

# Install dependencies (including dev extras once defined)
pip install -e ".[dev]"
```

A running RabbitMQ instance is required for integration tests. The quickest
way to get one locally is:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## Code Quality

- **Type hints** are required on all public functions and methods.
- **Docstrings** should follow [Google style][google-docstrings].
- Run the linter and formatter before committing:

  ```bash
  make lint
  make format
  ```

- Do not use bare `except:` clauses, `eval`, `exec`, or `pickle` for untrusted
  data.

## Testing

- Add unit tests for all new logic.
- Add integration tests for anything that touches RabbitMQ directly.
- Run the full test suite before opening a PR:

  ```bash
  make test
  ```

- Tests that require a live broker should be skippable via an environment
  variable (e.g. `RABBITMQ_URL`) so CI and offline contributors can run unit
  tests without Docker.

## Commit & PR Title Format

Follow [Conventional Commits][conventional-commits]:

```
TYPE(SCOPE): short description
```

| Type | When to use |
|------|-------------|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation only |
| `test` | Adding or fixing tests |
| `chore` | Tooling, CI, dependency updates |
| `refactor` | Code change with no feature or fix |

Examples:
- `feat(rabbitmq): add async message consumer callback`
- `fix(broker): handle connection timeout gracefully`
- `docs(readme): add quickstart example`

## Versioning & Breaking Changes

This project follows [Semantic Versioning][semver]. If your change modifies a
public API, document the breaking change clearly in your PR description and
flag it for the maintainer to decide the appropriate release bump.

## Code of Conduct

By participating in this project you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md).

---

Thank you for helping make `langchain-rabbitmq` better! 🐇🦜

[google-docstrings]: https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings
[conventional-commits]: https://www.conventionalcommits.org/en/v1.0.0/
[semver]: https://semver.org/
