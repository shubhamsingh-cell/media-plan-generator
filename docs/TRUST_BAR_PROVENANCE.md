# Trust-bar provenance — hub.html marquee

**Standard (owner's rule):** every proof element traces to a real artifact or
carries an explicit "illustrative" label, or it doesn't ship.

**Label:** "Built on the Joveo platform powering recruitment at" — chosen
2026-07-21 to state the true relationship. The listed companies are **Joveo
platform clients** (Nova is built on Joveo's platform and data); the previous
label "Powering recruitment intelligence at" implied Nova itself powers these
companies, which overstated. Do not revert the label without re-checking this
distinction.

**Protocol:** adding a name to the marquee requires an evidence row FIRST.
Public silence is not disqualifying (Joveo anonymizes most of its public case
studies), but *some* real artifact — public case study, domain-matched client
correspondence, or production telemetry — is mandatory. This repository is
public, so the table below records only the artifact TYPE and date; the
detailed evidence (contacts, references, channels) is held privately by the
owner and is never committed here.

## Current names (8) — verified 2026-07-17/21

| Name | Tier | Artifact type |
|---|---|---|
| HealthTrust | PUBLIC | Named, quoted case study on joveo.com (customer stories) |
| HCA Healthcare | PUBLIC (indirect) | Same case study names HealthTrust as the staffing arm of HCA Healthcare |
| Wells Fargo | STRONG | Client correspondence + internal delivery records, 2024–2026 |
| Randstad | STRONG | Client correspondence (2021–2022) + internal delivery channel (2025) |
| Adecco Group | STRONG | Client correspondence on a live job-feed integration (2022) |
| Barclays | STRONG | Production conversion telemetry (2026) + campaign target record (2025) |
| Pizza Hut | STRONG | Production job-ad pipeline records (2023–2024) + internal confirmation (2025) |
| ING | MODERATE | Campaign CPA target record + publisher enablement record (2025) |

## Removed (do not re-add without new evidence)

| Name | Removed | Reason |
|---|---|---|
| Johnson & Johnson | 447a42c (2026-07-17) | Zero corroboration anywhere — orphaned logo file on Joveo CDN only |
| Circle K | 447a42c | Same — orphaned CDN logo, no page/press/internal evidence |
| Korn Ferry | 447a42c | Wrong relationship: the artifact is a co-marketing webinar, not a customer |
| Philips | ca04142 (2026-07-17) | No public page content, no client correspondence; internal hits were a name collision |

## History

- Marquee introduced e873be4 (2026-03-22) by scraping joveo.com's homepage
  logo wall. Images later replaced with text (0f6b70a); the commit message
  claimed "real Joveo customer logos" but cited no source — this document is
  that missing source, reconstructed.
- Public-web check 2026-07-17: joveo.com's `customers` post type is an
  abandoned, unlinked stub list — treated as weak signal only. Real case
  studies live under `customer_stories` (mostly anonymized).
- Internal evidence sweep 2026-07-21 produced the table above (details held
  privately).
- 2026-10-16: commit b029de8 (an unrelated change) had restored the old label
  and deleted this file in July; the design-judge trust lens caught the silent
  revert. Label and ledger restored, names unchanged. The evidence rows were
  redacted to artifact type + date because the repository is public (the
  ddfbae1 version in git history still holds the details — purging history is
  the owner's call). `tests/test_proof_claims.py::test_trust_bar_names_have_an_evidence_row`
  fails if a marquee name has no row here or the label drifts.
- Alternative considered and rejected (2026-10-16): driving the strip from
  clients with Nova-generated plans (the notifier log). It would have put
  client and prospect names from an internal log onto a public page and into
  this repository.
