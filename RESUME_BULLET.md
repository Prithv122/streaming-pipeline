# Resume Bullets — streaming-pipeline

Form: **action → technical specifics → measured outcome.**

---

## Bullets

- Built a streaming aggregation pipeline (Redpanda / Kafka API → hand-written tumbling-window core
  → WebSocket dashboard). It compares two late-data policies under one lateness budget and scores
  every window against a batch recompute. At 5 % late events, answering early cut first-result
  latency from 31.6 s to 1.5 s (p95), but only 39.9 % of early results were exact, versus 100 % for
  waiting.
- Replaced a pre-registered success criterion that a pre-run review showed could not fail with a
  falsifiable analytic model (Poisson thinning of the generator). It predicted drop rate, accuracy and
  latency with exact acceptance regions, was frozen and hash-pinned before the run, and passed
  369/369 checks across 450 simulation runs.

## Which roles this supports

- [ ] Data Scientist / ML
- [ ] AI Engineer (LLM/NLP/CV)
- [x] Data Engineer
- [x] Data Analyst / Python Developer

## Keywords this project earns

Stream processing · event time vs processing time · watermarks · allowed lateness · tumbling
windows · Kafka / Redpanda · confluent-kafka · pre-registration · Poisson processes · simulation ·
pytest property testing · Docker Compose
