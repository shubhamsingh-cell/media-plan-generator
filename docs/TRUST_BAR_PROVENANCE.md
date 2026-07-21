# Trust-bar provenance — hub.html marquee

**Standard (owner's rule):** every proof element traces to a real artifact or
carries an explicit "illustrative" label, or it doesn't ship.

**Label:** "Built on the Joveo platform powering recruitment at" — chosen
2026-07-21 to state the true relationship. The listed companies are **Joveo
platform clients** (Nova is built on Joveo's platform and data); the previous
label "Powering recruitment intelligence at" implied Nova itself powers these
companies, which overstated. Do not revert the label without re-checking this
distinction.

**Protocol:** adding a name to the marquee requires an evidence row in the
table below FIRST. Public silence is not disqualifying (Joveo anonymizes ~70%
of its public case studies), but *some* real artifact — public case study,
domain-matched client correspondence, or production telemetry — is mandatory.

## Current names (8) — verified 2026-07-17/21

| Name | Tier | Artifact |
|---|---|---|
| HealthTrust | PUBLIC | Named, quoted case study: joveo.com/customer-stories/healthtrust-tracks-all-recruitment-media-in-one-place/ (Michael Swift, Dir. Recruitment Marketing) |
| HCA Healthcare | PUBLIC (indirect) | Same case study names HealthTrust as "the staffing arm of HCA Healthcare" |
| Wells Fargo | STRONG (email + Slack) | Gmail thread w/ @wellsfargo.com (Dennis.Tupper, Daniel.Allwright, Ryan.D.Stene; "we are the AOR for them", 17 Dec 2024, "Invoice INV-1508..."); Slack #wells-fargo-internal (active Jul 2026); Jira JCS-14932/8555/9288 |
| Randstad | STRONG (email + Slack) | Gmail: george.ackerman@randstadusa.com (Mar 2022), david.beardshaw@randstad.com security-monitoring thread (Sep 2021); Slack #temp-scale-ai-delivery CPA goals (Apr 2025) |
| Adecco Group | STRONG (email) | 2022 Gmail thread w/ @adeccogroup.com (quitterie.vanneaud.eyraud, sergio.bartolome, Julien.PERRIER) on live XML job-feed issue ("CAS-2294505-F9Y2Q2") |
| Barclays | STRONG (telemetry) | Slack #trk-pixel-alerts live conversion-pixel counts for client_name "Barclays - Exchange" (Jun–Jul 2026); feed-management SLA alert (AgencyId rsremea — runs via Randstad Sourceright EMEA, not direct); CPA target in #cg-communications (Aug 2025) |
| Pizza Hut | STRONG (telemetry) | Slack #cg-posting-errors PROD pipeline carrying real Pizza Hut job ads (Dec 2023, Mar 2024); internal confirmation in #ext-joveo-breakout (Nov 2025) |
| ING | MODERATE | Slack #cg-communications dated CPA target "ING-NL: CPA (30–40)" + Marktplaatz publisher enablement "for Netherlands(ING)" (Aug 2025). Thinnest entry — upgrade or drop if challenged |

## Removed (do not re-add without new evidence)

| Name | Removed | Reason |
|---|---|---|
| Johnson & Johnson | 447a42c (2026-07-17) | Zero corroboration anywhere — not even Joveo's own unlinked customer stub list; orphaned logo file on Joveo CDN only |
| Circle K | 447a42c | Same — orphaned CDN logo, no page/press/internal evidence |
| Korn Ferry | 447a42c | Wrong relationship: real artifact is a co-marketing webinar (GlobeNewswire, Apr 2026), not a customer |
| Philips | ca04142 (2026-07-17) | No public page content, no @philips.com email, Slack hits were a name collision with a person named Philip |

## History

- Marquee introduced e873be4 (2026-03-22) by scraping joveo.com's homepage
  logo wall (hotlinked wp-content logo files). Images later replaced with
  text (0f6b70a); commit message claimed "real Joveo customer logos" but
  cited no source — this document is that missing source, reconstructed.
- Public-web check 2026-07-17: joveo.com REST API `customers` post type is an
  abandoned, unlinked stub list (29 title-only entries) — treated as weak
  signal only. Real case studies live under `customer_stories` (30, mostly
  anonymized).
- Internal check 2026-07-21: Slack + Gmail operational-evidence sweep
  produced the table above.
