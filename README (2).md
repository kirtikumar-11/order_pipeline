# Data Engineering Scenario Interview Guide
### Kafka · Spark Structured Streaming · Databricks · Delta Lake

This guide teaches you to **think like the interviewer wants**: ask the right questions first, then pick technology because of the requirements, and explain the trade-off. Memorising answers doesn't work for scenario questions; a repeatable method does.

| # | File | What you learn |
|---|---|---|
| 1 | [01_framework.md](01_framework.md) | The 6-step method for any scenario question + the requirements checklist |
| 2 | [02_choosing_technology.md](02_choosing_technology.md) | Decision tables: when to pick which tool, and why |
| 3 | [03_core_concepts.md](03_core_concepts.md) | The concepts every answer is built from, in plain English |
| 4 | [04_scenarios_your_project.md](04_scenarios_your_project.md) | Questions about *your* order pipeline, with model answers |
| 5 | [05_scenarios_design.md](05_scenarios_design.md) | "Design a system that…" scenarios, worked end to end |
| 6 | [06_scenarios_debugging.md](06_scenarios_debugging.md) | "Something is broken in production…" scenarios |
| 7 | [07_rapid_fire.md](07_rapid_fire.md) | 60 short questions with 1–3 line answers for revision |

## How to study (2 weeks, ~1 hour a day)
1. **Days 1–2:** read 01 and 03. Explain each concept out loud in 30 seconds.
2. **Days 3–4:** read 02. For each table, invent one scenario where each column wins.
3. **Days 5–7:** 04. Run the failure tests in `../docs/failure_tests.md` on your own Databricks so every answer is something you *saw*.
4. **Days 8–11:** 05 and 06. Cover the model answer, answer out loud using the 6 steps, then compare.
5. **Days 12–14:** 07 as flash cards. Do two mock interviews with a friend (or with Claude) using the scenarios.

## The one-sentence rule
Every technical choice you say in an interview should have a **"because"** tied to a requirement:
> "I'd use Kafka **because** we need several independent consumers and the ability to replay the last 7 days."

If you can't finish the "because", you're naming tools, not designing.
