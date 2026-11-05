# 1. Record architecture decisions

Status: accepted

## Context

The SDK's structural choices (which packages exist, what a worker must and must not do, how the
SDK tracks the cairn-core contract) are easy to lose when they live only in prose and commit
messages.

## Decision

Significant architecture decisions are recorded here as numbered ADRs (context, decision,
consequences). An ADR is never rewritten after acceptance; a later ADR supersedes it.

## Consequences

Structural changes come with an ADR in the same pull request. The first substantive decision,
releasing in lockstep with cairn-core, is ADR 0002.
