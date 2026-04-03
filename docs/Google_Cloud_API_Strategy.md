# Google Cloud API Strategy for Nova AI Suite

**Project ID**: gen-lang-client-0117143859 | **Date**: 2026-04-03
**39 APIs Enabled | 54 Total Items Tracked | 9 Modules Built This Session**

## Quick Reference: What to Enable Next

| Priority | API | Free Tier | Enable URL |
|----------|-----|-----------|------------|
| **1** | BigQuery | 1 TB queries/mo | [Enable](https://console.cloud.google.com/apis/library/bigquery.googleapis.com?project=gen-lang-client-0117143859) |
| **2** | Knowledge Graph | 100K calls/day | [Enable](https://console.cloud.google.com/apis/library/kgsearch.googleapis.com?project=gen-lang-client-0117143859) |
| **3** | PageSpeed Insights | 25K queries/day | [Enable](https://console.cloud.google.com/apis/library/pagespeedonline.googleapis.com?project=gen-lang-client-0117143859) |
| **4** | Cloud Natural Language | 5K units/mo | [Enable](https://console.cloud.google.com/apis/library/language.googleapis.com?project=gen-lang-client-0117143859) |
| **5** | Cloud Translation | 500K chars/mo | [Enable](https://console.cloud.google.com/apis/library/translate.googleapis.com?project=gen-lang-client-0117143859) |
| **6** | reCAPTCHA Enterprise | 1M assessments/mo | [Enable](https://console.cloud.google.com/apis/library/recaptchaenterprise.googleapis.com?project=gen-lang-client-0117143859) |

## Modules Built This Session (S39)

| Module | Lines | Functions | APIs Used |
|--------|-------|-----------|-----------|
| `google_maps_integration.py` | 300 | 6 | Geocoding, Places |
| `google_vision_integration.py` | 239 | 4 | Cloud Vision |
| `google_ads_analytics.py` | 346 | 4 | Analytics, YouTube |
| `google_workspace_integration.py` | 293 | 4 | Calendar, Gmail |
| `google_cloud_storage.py` | 270 | 6 | Cloud Storage |
| `sheets_export.py` | 1062 | existing | Google Sheets (DEPLOYED) |
| `deck_generator.py` | 619 | existing | Google Slides (DEPLOYED) |

## Ad Tech Strategy

**Google Ads API** is the highest-value ad tech integration. Requires MCC account.
- Live CPC/CPA by keyword + geography via Keyword Planner
- Seasonal trend data for recruitment verticals
- No enterprise license needed (unlike CM360/SA360/DV360)

## APIs Safe to Disable (reduce attack surface ~60%)

Unused infrastructure: Compute Engine, Cloud Functions, Cloud Build, Kubernetes Engine, Container Registry, Cloud SQL, Datastore, Pub/Sub, Cloud Trace, Cloud Debugger, Cloud Profiler, Cloud Scheduler, Cloud Tasks, Ad Exchange Seller, Real-time Bidding.
