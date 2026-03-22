## Summary
<!-- 1-3 bullet points describing the change -->

## Guardrails Checklist
<!-- See SYSTEM_GUARDRAILS.md for details -->

- [ ] Follows pipeline boundaries (ingestion/enrichment/decision/delivery/analytics)
- [ ] Shadow mode first (if affects signal quality)
- [ ] Has feature flag (default OFF)
- [ ] Has CLI report command
- [ ] Has Grafana query or panel
- [ ] No business logic in delivery/transport layer
- [ ] No duplication of existing service logic
- [ ] Handles missing data gracefully (NULL-safe)
- [ ] Uses existing tables or adds columns additively
- [ ] All new env vars documented in config dataclass
- [ ] Tests cover happy path + NULL/missing data + failure modes

## Test plan
<!-- How to verify this change works -->
