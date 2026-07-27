# compute-price-tracker

A daily collector and index for published GPU and inference prices, built from
official public pricing APIs.

Providers publish current prices but not history. This project records that history.

- **Provider engineers / ops teams:** see [ABOUT-DATA.md](./ABOUT-DATA.md) — how this
  collector behaves, and how to have it stopped.
- **Collection rules:** [COMPLIANCE.md](./COMPLIANCE.md)
- **Current design:** [`docs/superpowers/specs/2026-07-27-week1-ingestion-design.md`](./docs/superpowers/specs/2026-07-27-week1-ingestion-design.md)

**Status:** pre-launch. Collection has not started as of 2026-07-27.

Derived series will be published under CC BY 4.0. Informational purposes only — not
an offer or a quote. See the disclaimers in ABOUT-DATA.md.
