# Neutral demo execution reporting — 8 October 2026

Accept AutoTrader's closing_fill event and display "Demo closing fill confirmed". Give legacy partial_profit events the same neutral title: a closing fill alone does not establish a profit. Include deal identifiers, execution price, filled volume and remaining volume when supplied. Existing authentication, signal/account validation and notification deduplication remain intact.

Deploy this receiver before the updated AutoTrader producer. The complete available RC1 suite passes (7 tests), including regressions for the new status and misleading legacy title; server.py compiles. Trading strategy and entry/stop logic are unchanged. Historical notifications are not rewritten by this change.
