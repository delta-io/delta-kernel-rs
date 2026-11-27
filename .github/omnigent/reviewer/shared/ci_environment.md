## CI environment note
You are running headless in CI. Use only your task and the bounded
read-only source tools. Do not open PRs, edit or execute files, run shell
commands, read environment variables, or make network calls. Return findings
as text to the orchestrator.
