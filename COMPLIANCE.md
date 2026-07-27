# COMPLIANCE — hard rules

These are non-negotiable and are enforced **in code**, not just documented here.
They are reviewed as sources are added, and tightened rather than relaxed.

Public summary for provider operators: [ABOUT-DATA.md](./ABOUT-DATA.md).

---

1. **Official APIs only.** No HTML scraping, no headless browsers, no parsing of
   rendered pricing pages. If a source has no documented API, it does not go in the
   MVP — add it to `docs/backlog-sources.md` instead.

2. **Never authenticate to a third-party service we don't own an account
   relationship with.** Do not create accounts. Do not accept clickwrap terms.
   AWS/GCP credentials are fine — they are our own cloud accounts, used as
   documented. Never sign up for a neocloud account to reach pricing.

3. **Never bypass anti-bot measures.** No CAPTCHA solving, no proxy rotation, no
   user-agent spoofing to look like a browser.

4. **Identify ourselves.** Every outbound request sets:
   ```
   User-Agent: compute-price-tracker/0.1 (+https://github.com/mrmonroe90/compute-price-tracker; mrmonroe12@gmail.com)
   ```

5. **Respect robots.txt** even for API endpoints. The fetcher implements a robots
   check and logs the decision.

6. **Rate limit conservatively.** Default 1 req/sec per host, exponential backoff
   on 429/5xx, hard daily request cap per source in config. We collect daily
   snapshots, not real-time data — there is no reason to be aggressive.

7. **Tier enforcement.** `config/sources.yaml` carries a `tier` field
   (`safe` | `medium` | `avoid`). The fetcher **refuses to run** any source not
   marked `safe`, with a loud error. Adding a `medium` source requires a deliberate
   config flag AND a note in `docs/compliance-log.md`.

8. **Never add these sources.** CoreWeave and RunPod both explicitly prohibit
   scraping/automated access in their AUP/ToS. They are hard-coded into a denylist
   in `src/compliance.py` — not read from config — so a future careless commit
   cannot add them.

9. **Publish derived metrics, not mirrors.** The public site shows normalized
   indices, spreads, medians, and percentiles. It must not present itself as a
   verbatim mirror of any provider's price list. Raw per-provider values may appear
   in charts, but the product is the *derived* series.

10. **Provenance on every row.** Every record carries `source_id`, `source_url`,
    `fetched_at_utc`, `raw_hash`. If we can't say where a number came from, we
    don't publish it.

11. **Disclaimers on the site.** "Informational purposes only; not an offer; prices
    are as published by providers and may be stale." Mirrors the language AWS and
    Azure use on their own price APIs.

12. **No secrets in the repo.** All keys via environment variables / GitHub Actions
    secrets. A CI check guards against accidental key commits.

---

## Implementation notes

Rules 1–8 and 12 are enforced by `src/compliance.py` and `src/fetchers/base.py`,
and are covered by `tests/test_compliance.py`. Those tests assert, among other
things, that the denylist blocks CoreWeave and RunPod **even when they are
deliberately injected into `sources.yaml`**, and that a non-`safe` tier refuses to
run.

**robots.txt semantics (rule 5).** Per RFC 9309, an absent robots.txt means
unrestricted crawling. The checker therefore **fails open on HTTP 404** and fails
closed only on an explicit `Disallow` match against our path. As of 2026-07-27 both
`prices.azure.com` and `pricing.us-east-1.amazonaws.com` return 404 for
`/robots.txt`; `openrouter.ai` returns 200 and permits our path.

**Opt-outs.** Any provider requesting exclusion is added to the denylist and the
decision recorded in `docs/compliance-log.md`, with the request honoured before any
discussion of it.
