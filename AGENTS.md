# Agent instructions

## Flaky tests

A test that fails only sometimes means something is fundamentally wrong, in the code or in the test. Never rerun it until it passes, mark it ignored, or call it unrelated. Stop, diagnose the cause, and fix it before doing anything else.
