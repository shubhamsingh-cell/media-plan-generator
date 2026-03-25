# SOC 2 Type II Compliance Roadmap -- Nova AI Suite

**Document Classification:** Confidential -- Internal Use Only
**Prepared for:** Joveo Leadership Team
**Perspective:** CTO + CISO
**Date:** March 2026
**Version:** 1.0

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current State Assessment](#2-current-state-assessment)
3. [Gap Analysis: SOC 2 Trust Service Criteria](#3-gap-analysis-soc-2-trust-service-criteria)
4. [Phase 1: Foundation (Months 0-3)](#4-phase-1-foundation-months-0-3)
5. [Phase 2: Policies & Procedures (Months 3-6)](#5-phase-2-policies--procedures-months-3-6)
6. [Phase 3: Audit Readiness (Months 6-12)](#6-phase-3-audit-readiness-months-6-12)
7. [Cost Estimates](#7-cost-estimates)
8. [Tool Recommendations](#8-tool-recommendations)
9. [Risk Register](#9-risk-register)
10. [Appendices](#10-appendices)

---

## 1. Executive Summary

Nova AI Suite processes sensitive recruitment data -- job postings, candidate pipelines, media spend allocations, and employer brand assets -- for mid-market and enterprise clients. As we move upmarket, SOC 2 Type II compliance is no longer optional. Enterprise procurement teams (100+ employee companies) require it as a gating criterion, and our competitors (Appcast, PandoLogic, Joveo's own enterprise arm) already hold it.

**Current posture:** We have a solid engineering-led security foundation (auth, rate limiting, audit logging, input sanitization) but lack the formal policies, procedures, and evidence collection required for a SOC 2 audit. We estimate 40-50% of controls are already in place technically; the gap is primarily in documentation, process formalization, and continuous monitoring.

**Timeline:** 12 months to Type II readiness, with a Type I report achievable at month 8.
**Estimated total investment:** $85,000 - $145,000 (detailed breakdown in Section 7).
**Expected ROI:** Unlocks enterprise deals with 6-12 month sales cycles; removes the #1 objection from procurement.

---

## 2. Current State Assessment

### 2.1 Existing Security Controls

The Nova AI Suite already implements meaningful security controls, largely as a result of Session 11's security hardening. Below is an honest inventory.

| Control Area | Implementation | Status | Evidence |
|---|---|---|---|
| **Authentication** | `auth.py` -- NOVA_ADMIN_KEY + NOVA_API_KEYS, key-based auth on `/api/admin/*` | Partial | No MFA, no user-level auth, no session management |
| **Rate Limiting** | RateLimiter class with DDoS protection on 16 endpoints | Strong | Configurable per-endpoint, IP-based |
| **Audit Logging** | `audit_logger.py` -- structured logging of admin actions, API calls | Moderate | Race condition fixed in Session 11; no log immutability or SIEM integration |
| **Input Sanitization** | Null byte stripping, HTML escaping, JSON recursion limits on all chat endpoints | Strong | Applied to all user-facing inputs |
| **CORS** | Configured for production domain | Basic | Needs review for overly permissive origins |
| **CSP Headers** | Content Security Policy headers on HTML responses | Moderate | Needs tightening (inline scripts still allowed) |
| **Encryption in Transit** | TLS via Render.com (managed SSL) | Strong | Automatic HTTPS enforcement |
| **Error Handling** | Error isolation per data collector, try/except with logging | Strong | 649+ tests, fault injection tested |
| **Vulnerability Management** | No formal program | Gap | No dependency scanning, no CVE tracking |
| **Access Control** | Render.com dashboard (team-based), environment variables for secrets | Basic | No principle of least privilege, no access reviews |
| **Backup & Recovery** | Supabase managed backups (daily) | Partial | No tested restore procedure, no RTO/RPO defined |
| **Incident Response** | Self-healing module, health dashboard | Partial | No formal IR plan, no communication templates |

### 2.2 Architecture Security Profile

```
[Client Browser] --HTTPS--> [Render.com CDN/LB] --HTTPS--> [Python HTTP Server]
                                                                    |
                                    +-------------------------------+-------------------------------+
                                    |                               |                               |
                              [Supabase]                     [External APIs]                  [LLM Providers]
                              (PostgreSQL)                   (8 data sources)                (24 providers)
                              Row-Level N/A                  API key auth                    API key auth
                              Managed backups                Rate limited                    Fallback chains
```

**Key observations:**
- Single-server architecture on Render.com Standard instance (no horizontal scaling)
- 48 environment variables, including API keys for 24 LLM providers and 8 data APIs
- No secrets management solution (env vars stored in Render dashboard)
- No WAF (Web Application Firewall)
- No network segmentation (all services accessible from application server)

---

## 3. Gap Analysis: SOC 2 Trust Service Criteria

### 3.1 Security (Common Criteria -- CC)

The Security category is mandatory for all SOC 2 reports. It spans 9 sub-categories (CC1-CC9).

| Criteria | Requirement | Current State | Gap Severity | Remediation |
|---|---|---|---|---|
| **CC1: Control Environment** | Tone at the top, security awareness | No formal security policy | HIGH | Write Information Security Policy, define roles |
| **CC2: Communication & Information** | Internal/external security communication | Ad-hoc (Slack) | HIGH | Establish security communication procedures |
| **CC3: Risk Assessment** | Formal risk identification process | No formal risk register | HIGH | Create risk assessment framework |
| **CC4: Monitoring Activities** | Continuous monitoring of controls | Health dashboard, PostHog analytics | MEDIUM | Formalize monitoring, add alerting SLAs |
| **CC5: Control Activities** | Logical access, change management | Auth.py for API, Render for deploy | MEDIUM | Formalize change management, code review policy |
| **CC6: Logical & Physical Access** | User provisioning, MFA, access reviews | No MFA, no access reviews | HIGH | Implement MFA, quarterly access reviews |
| **CC7: System Operations** | Vulnerability management, incident detection | Self-healing module exists | MEDIUM | Add vulnerability scanning, formalize operations |
| **CC8: Change Management** | Controlled deployment process | Git-based, auto-deploy from main | MEDIUM | Add approval gates, rollback procedures |
| **CC9: Risk Mitigation** | Vendor risk management | No vendor assessments | HIGH | Assess all 32+ third-party integrations |

### 3.2 Availability (A)

| Criteria | Requirement | Current State | Gap Severity | Remediation |
|---|---|---|---|---|
| **A1.1** | Processing capacity management | Single Render Standard instance | HIGH | Define capacity thresholds, scaling plan |
| **A1.2** | Recovery objectives (RTO/RPO) | Not defined | HIGH | Define RTO < 4hr, RPO < 1hr |
| **A1.3** | Backup and restoration testing | Supabase daily backups, untested | HIGH | Quarterly restore tests, documented procedure |
| **A1.4** | Business continuity plan | None | HIGH | Write BCP, define failover procedures |
| **A1.5** | Environmental safeguards | Render.com managed (SOC 2 certified) | LOW | Obtain Render's SOC 2 report |

### 3.3 Processing Integrity (PI)

| Criteria | Requirement | Current State | Gap Severity | Remediation |
|---|---|---|---|---|
| **PI1.1** | Data validation | Input sanitization on all endpoints | LOW | Document validation rules |
| **PI1.2** | Processing completeness | Error isolation per collector | LOW | Add end-to-end transaction monitoring |
| **PI1.3** | Accuracy of outputs | LLM hallucination controls (grounding, RAG) | MEDIUM | Document AI output quality controls |
| **PI1.4** | Data integrity monitoring | Basic health checks | MEDIUM | Add data integrity checksums |
| **PI1.5** | Error correction | Self-healing module | LOW | Document error correction procedures |

### 3.4 Confidentiality (C)

| Criteria | Requirement | Current State | Gap Severity | Remediation |
|---|---|---|---|---|
| **C1.1** | Data classification | Not implemented | HIGH | Create data classification policy (4 tiers) |
| **C1.2** | Encryption at rest | Supabase (encrypted), local data (not encrypted) | MEDIUM | Encrypt local data stores, verify Supabase encryption |
| **C1.3** | Encryption in transit | TLS everywhere | LOW | Verify all internal connections use TLS |
| **C1.4** | Data disposal | No data retention policy | HIGH | Define retention periods, implement auto-purge |
| **C1.5** | Confidentiality agreements | No vendor NDAs on file | HIGH | Execute NDAs with all data processors |

### 3.5 Privacy (P)

| Criteria | Requirement | Current State | Gap Severity | Remediation |
|---|---|---|---|---|
| **P1.1** | Privacy notice | No privacy policy page | HIGH | Draft and publish privacy policy |
| **P1.2** | Consent management | No consent collection | HIGH | Implement consent workflows |
| **P2.1** | Data collection limitations | No PII minimization | MEDIUM | Audit PII collection, minimize |
| **P3.1** | Data retention & disposal | No retention schedule | HIGH | Implement retention policies |
| **P4.1** | Data subject rights (GDPR) | No self-service data export/deletion | HIGH | Build data subject request workflow |
| **P5.1** | Third-party data sharing | No data processing agreements | HIGH | Execute DPAs with LLM providers |

### 3.6 Gap Severity Summary

| Severity | Count | Categories |
|---|---|---|
| HIGH | 18 | Policies, MFA, vendor mgmt, data classification, privacy, BCP |
| MEDIUM | 8 | Monitoring formalization, change mgmt, encryption verification |
| LOW | 5 | Existing controls needing documentation |
| **Total gaps** | **31** | |

---

## 4. Phase 1: Foundation (Months 0-3)

**Objective:** Close quick-win gaps and establish the security program foundation.
**Theme:** "Stop the bleeding, start the paperwork."

### 4.1 Logging & Monitoring Hardening (Weeks 1-4)

| Task | Owner | Effort | Priority |
|---|---|---|---|
| Upgrade audit_logger.py to append-only log storage (S3 bucket with object lock) | Backend | 3 days | P0 |
| Integrate structured logs with a SIEM solution (Datadog or Axiom) | DevOps | 5 days | P0 |
| Define log retention policy: 90 days hot, 1 year cold, 7 years archive | CTO | 1 day | P0 |
| Add authentication event logging (login attempts, key usage, failures) | Backend | 2 days | P0 |
| Configure alerts for: failed auth > 5/min, error rate > 5%, latency > 5s | DevOps | 2 days | P1 |
| Create security event dashboard (separate from health dashboard) | Frontend | 3 days | P1 |

**Deliverables:**
- Immutable audit log pipeline
- SIEM integration with 5+ alert rules
- Log retention policy document

### 4.2 MFA for Admin Access (Weeks 2-4)

| Task | Owner | Effort | Priority |
|---|---|---|---|
| Implement TOTP-based MFA for `/api/admin/*` endpoints | Backend | 5 days | P0 |
| Add MFA to Render.com dashboard (already supported, enforce for team) | DevOps | 1 hour | P0 |
| Add MFA to Supabase dashboard | DevOps | 1 hour | P0 |
| Add MFA to GitHub organization | DevOps | 1 hour | P0 |
| Document MFA enrollment procedure | Security | 1 day | P0 |
| Create MFA recovery procedure (backup codes) | Security | 1 day | P1 |

**Deliverables:**
- MFA enforced on all administrative access
- MFA policy document
- Recovery procedure

### 4.3 Data Encryption (Weeks 3-6)

| Task | Owner | Effort | Priority |
|---|---|---|---|
| Verify Supabase encryption at rest (AES-256, document it) | DevOps | 1 day | P0 |
| Encrypt local knowledge base files at rest (age or gpg) | Backend | 3 days | P1 |
| Implement field-level encryption for PII in Supabase (email, names) | Backend | 5 days | P1 |
| Migrate secrets from Render env vars to a secrets manager (Doppler or HashiCorp Vault) | DevOps | 5 days | P1 |
| Document encryption standards (algorithms, key lengths, rotation schedule) | Security | 2 days | P1 |
| Implement API key rotation procedure (quarterly) | Backend | 3 days | P2 |

**Deliverables:**
- Encryption-at-rest verified and documented
- Secrets management solution deployed
- Encryption standards document

### 4.4 Backup & Recovery (Weeks 4-8)

| Task | Owner | Effort | Priority |
|---|---|---|---|
| Document current Supabase backup configuration | DevOps | 1 day | P0 |
| Define RTO (4 hours) and RPO (1 hour) targets | CTO | 1 day | P0 |
| Implement automated backup verification (weekly restore test to staging) | DevOps | 3 days | P0 |
| Create application-level backup for knowledge base and config files | Backend | 2 days | P1 |
| Write disaster recovery runbook (step-by-step) | DevOps | 3 days | P1 |
| Conduct first tabletop DR exercise | Team | 4 hours | P1 |

**Deliverables:**
- RTO/RPO document
- Automated backup verification pipeline
- DR runbook
- First tabletop exercise completed

### 4.5 Core Security Policies (Weeks 6-12)

| Policy | Pages | Owner | Priority |
|---|---|---|---|
| Information Security Policy (master) | 8-12 | CTO/CISO | P0 |
| Acceptable Use Policy | 3-5 | HR/Legal | P0 |
| Access Control Policy | 5-8 | Security | P0 |
| Data Classification Policy (4 tiers: Public, Internal, Confidential, Restricted) | 4-6 | Security | P0 |
| Incident Response Policy | 6-10 | Security | P0 |
| Password & Authentication Policy | 3-4 | Security | P1 |
| Encryption Policy | 3-5 | Security | P1 |
| Change Management Policy | 4-6 | Engineering | P1 |

**Deliverables:**
- 8 security policies drafted, reviewed, and approved
- Policy acknowledgment process established

### 4.6 Phase 1 Milestones

| Week | Milestone | Success Criteria |
|---|---|---|
| Week 2 | SIEM integration live | Logs flowing, 3+ alert rules active |
| Week 4 | MFA enforced everywhere | 100% admin access requires MFA |
| Week 6 | Encryption verified | At-rest + in-transit documented, secrets manager live |
| Week 8 | First DR test passed | Successful restore from backup within RTO |
| Week 12 | All Phase 1 policies approved | 8 policies signed by leadership |

---

## 5. Phase 2: Policies & Procedures (Months 3-6)

**Objective:** Formalize operational procedures, assess vendors, and establish continuous compliance monitoring.
**Theme:** "Build the muscle memory."

### 5.1 Vendor Risk Management (Weeks 13-18)

Nova AI Suite depends on 32+ third-party services. Each must be assessed.

| Vendor Category | Vendors | Risk Level | Required Actions |
|---|---|---|---|
| **Infrastructure** | Render.com | High | Obtain SOC 2 report, review annually |
| **Database** | Supabase | High | Obtain SOC 2 report, execute DPA |
| **LLM Providers** | OpenAI, Anthropic, Google, OpenRouter, +20 others | High | Execute DPAs, verify data handling policies |
| **Data APIs** | Adzuna, BLS, FRED, O*NET, Jooble, BEA, Census, USAJobs | Medium | Verify data usage terms, check for PII exposure |
| **Analytics** | PostHog, Sentry | Medium | Obtain SOC 2 reports, verify data residency |
| **Communication** | Resend (email) | Medium | Execute DPA, verify encryption |
| **Vector DB** | Chroma | Low | Self-hosted component, document architecture |

**Process:**
1. Create vendor inventory spreadsheet (all 32+ vendors)
2. Classify by risk tier (Critical / High / Medium / Low)
3. Collect SOC 2 reports or equivalent certifications
4. Execute Data Processing Agreements where PII is shared
5. Establish annual review cadence

**Deliverables:**
- Vendor risk assessment matrix
- 10+ DPAs executed
- Vendor review schedule (annual for High, biannual for Critical)

### 5.2 Incident Response Plan (Weeks 13-16)

| Component | Description |
|---|---|
| **Severity Classification** | SEV1 (data breach, full outage), SEV2 (partial outage, security event), SEV3 (degraded service), SEV4 (minor issue) |
| **Response Team** | Incident Commander (CTO), Technical Lead, Communications Lead |
| **Communication Templates** | Customer notification (SEV1/2), internal escalation, regulatory notification |
| **Escalation Matrix** | SEV1: 15 min response, SEV2: 1 hour, SEV3: 4 hours, SEV4: next business day |
| **Post-Incident Review** | Blameless postmortem within 48 hours, documented in shared repository |
| **Regulatory Notification** | GDPR: 72 hours, state breach laws: varies (document per-state requirements) |

**Deliverables:**
- Incident Response Plan document (10-15 pages)
- Communication templates (4 templates)
- Tabletop exercise completed (simulate SEV1 data breach)
- On-call rotation established

### 5.3 Change Management Formalization (Weeks 14-18)

| Current State | Target State |
|---|---|
| Direct push to main, auto-deploy | PR required, 1 approval minimum |
| No staging environment | Staging environment on Render (Preview Deploys) |
| No rollback procedure | Documented rollback within 5 minutes |
| No change log | Automated changelog from PR titles |
| No change risk assessment | Risk classification (Low/Medium/High) per change |

**Implementation:**
1. Enable GitHub branch protection on `main` (require PR, 1 approval, status checks)
2. Configure Render Preview Deployments for staging
3. Create change request template (risk level, rollback plan, testing evidence)
4. Implement automated changelog generation
5. Define emergency change procedure (hotfix process)

**Deliverables:**
- Branch protection enabled
- Change management policy enforced
- Staging environment live
- Emergency change procedure documented

### 5.4 Access Control Hardening (Weeks 16-20)

| Task | Description | Effort |
|---|---|---|
| Inventory all access | Document who has access to what (Render, Supabase, GitHub, LLM providers) | 2 days |
| Principle of least privilege | Reduce permissions to minimum required per role | 3 days |
| Quarterly access reviews | Calendar recurring review, documented sign-off | 1 day setup |
| Offboarding checklist | Immediate revocation procedure for departing team members | 1 day |
| Service account inventory | Document all API keys, their purpose, and rotation schedule | 2 days |
| Implement RBAC in Nova | Role-based access for platform users (Admin, Editor, Viewer) | 10 days |

**Deliverables:**
- Access control matrix (who has what)
- Quarterly access review process
- Offboarding checklist
- RBAC implementation in application

### 5.5 Security Awareness Training (Weeks 18-22)

| Training Module | Audience | Frequency | Duration |
|---|---|---|---|
| Security fundamentals | All employees | Annual + onboarding | 1 hour |
| Secure coding practices | Engineering team | Quarterly | 2 hours |
| Phishing awareness | All employees | Monthly simulations | 15 min |
| Incident response | Response team | Quarterly tabletop | 2 hours |
| Data handling procedures | Data team | Annual | 1 hour |

**Deliverables:**
- Training curriculum documented
- First training cycle completed
- Training completion tracking

### 5.6 Phase 2 Milestones

| Week | Milestone | Success Criteria |
|---|---|---|
| Week 16 | Vendor assessments complete | 100% of Critical/High vendors assessed |
| Week 18 | IR plan tested | Tabletop exercise completed, plan refined |
| Week 20 | Change management enforced | 100% of changes go through PR process |
| Week 22 | Access reviews initiated | First quarterly review completed |
| Week 24 | Training cycle 1 complete | 100% team trained, certificates on file |

---

## 6. Phase 3: Audit Readiness (Months 6-12)

**Objective:** Prepare evidence, close remaining gaps, engage auditors, and achieve Type II readiness.
**Theme:** "Prove it."

### 6.1 Evidence Collection & Continuous Compliance (Weeks 25-36)

SOC 2 Type II requires evidence that controls operated effectively over a period (minimum 3 months, typically 6-12 months). Evidence collection must begin no later than Month 6.

| Evidence Category | Examples | Collection Method |
|---|---|---|
| **Access Control** | User provisioning tickets, access review sign-offs, MFA enrollment records | Compliance platform (Vanta/Drata) auto-collection |
| **Change Management** | PR approvals, deployment logs, rollback records | GitHub + Render API integration |
| **Incident Response** | Incident tickets, postmortem documents, communication logs | Ticketing system + compliance platform |
| **Monitoring** | Alert configurations, alert response times, uptime reports | SIEM + Render metrics |
| **Vulnerability Management** | Scan results, remediation timelines, patch records | Snyk/Dependabot + GitHub |
| **Backup & Recovery** | Backup logs, restore test results, DR exercise records | Automated scripts + compliance platform |
| **Training** | Completion certificates, training materials, attendance records | LMS or compliance platform |
| **Policy Reviews** | Annual review sign-offs, version history, approval records | Git-tracked policy repository |

### 6.2 Vulnerability Management Program (Weeks 25-30)

| Component | Tool | Frequency | SLA |
|---|---|---|---|
| Dependency scanning | Snyk or GitHub Dependabot | Continuous (every PR) | Critical: 24hr, High: 7 days, Medium: 30 days |
| Container/image scanning | Snyk Container | Weekly | Critical: 48hr, High: 14 days |
| SAST (static analysis) | Semgrep or CodeQL | Every PR | Block merge on Critical |
| DAST (dynamic testing) | OWASP ZAP | Monthly | Report reviewed within 5 days |
| Third-party penetration test | External firm | Annual (before audit) | All Critical/High fixed before audit |
| Bug bounty program | HackerOne or Bugcrowd | Continuous (optional, Phase 3+) | Per severity SLAs |

**Deliverables:**
- Vulnerability management policy
- Scanning tools integrated into CI/CD
- Remediation SLAs defined and tracked
- Monthly vulnerability report

### 6.3 Third-Party Penetration Testing (Weeks 32-38)

| Phase | Activity | Duration | Cost Estimate |
|---|---|---|---|
| Scoping | Define test boundaries, provide documentation | 1 week | Included |
| External network test | Test internet-facing services | 1 week | $8,000 - $15,000 |
| Web application test | Test all endpoints, auth, injection, business logic | 2 weeks | $12,000 - $25,000 |
| API security test | Test all 33 API endpoints | 1 week | $8,000 - $12,000 |
| AI/ML security test | Test prompt injection, data leakage, model abuse | 1 week | $5,000 - $10,000 |
| Remediation verification | Re-test after fixes | 1 week | $3,000 - $5,000 |
| **Total** | | **7 weeks** | **$36,000 - $67,000** |

**Recommended firms:**
- NCC Group (strong AI/ML security practice)
- Bishop Fox (excellent web app testing)
- Trail of Bits (if deeper AI security review needed)

### 6.4 Audit Firm Selection & Engagement (Weeks 28-32)

| Criteria | Weight | Notes |
|---|---|---|
| SaaS/startup experience | 30% | Must understand cloud-native, serverless |
| AI/ML audit experience | 25% | LLM-specific risks are unique |
| Cost | 20% | Type II range: $30,000 - $60,000 |
| Timeline flexibility | 15% | Must accommodate our evidence window |
| Reputation | 10% | Top 20 SOC 2 audit firms preferred |

**Recommended audit firms:**
- Johanson Group (startup-friendly, $30-40K)
- Prescient Assurance (tech-focused, $35-50K)
- A-LIGN (larger, $40-60K, strong brand recognition)

**Engagement timeline:**
- Week 28: Issue RFPs to 3 firms
- Week 30: Select firm, sign engagement letter
- Week 32: Readiness assessment (auditor reviews controls)
- Week 36: Begin Type II observation period (if not already started)
- Week 48+: Type II audit report issued

### 6.5 Pre-Audit Readiness Assessment (Weeks 36-40)

| Activity | Description | Owner |
|---|---|---|
| Internal audit | Self-assessment against all TSC criteria | Security team |
| Gap remediation sprint | Fix all identified gaps from internal audit | Engineering |
| Evidence completeness check | Verify evidence exists for every control | Compliance |
| Policy currency check | Ensure all policies are current (reviewed within 12 months) | Security |
| Mock audit | Simulate auditor walkthroughs with team | External consultant or compliance platform |
| Management assertion | CTO/CEO sign management assertion letter | Leadership |

### 6.6 Privacy Compliance (GDPR/CCPA) (Weeks 30-40)

| Requirement | Implementation | Effort |
|---|---|---|
| Privacy policy page | Publish at `/privacy` with full GDPR-compliant disclosure | 3 days |
| Cookie consent banner | Implement consent management (PostHog, analytics cookies) | 2 days |
| Data subject access requests (DSAR) | Build `/api/admin/dsar` endpoint for data export | 5 days |
| Right to deletion | Build `/api/admin/delete-user-data` with cascade logic | 5 days |
| Data processing records (Article 30) | Document all processing activities | 3 days |
| DPAs with LLM providers | Ensure OpenAI, Anthropic, etc. have signed DPAs | 5 days |
| Data residency documentation | Map where data is stored geographically | 2 days |
| Consent for AI processing | Explicit consent for data used in AI/ML processing | 3 days |

### 6.7 Phase 3 Milestones

| Week | Milestone | Success Criteria |
|---|---|---|
| Week 28 | Audit firm RFPs issued | 3 firms contacted |
| Week 30 | Vulnerability scanning live | All scans running, SLAs defined |
| Week 32 | Audit firm selected | Engagement letter signed |
| Week 36 | Evidence collection verified | 90%+ controls have evidence |
| Week 38 | Penetration test complete | All Critical/High findings remediated |
| Week 40 | Pre-audit readiness passed | Internal audit shows < 5 gaps |
| Week 48 | Type II observation period ends | 6+ months of evidence collected |
| Week 50 | SOC 2 Type II report issued | Unqualified opinion |

---

## 7. Cost Estimates

### 7.1 Phase 1: Foundation (Months 0-3)

| Item | Low Estimate | High Estimate | Notes |
|---|---|---|---|
| SIEM solution (Datadog/Axiom) | $200/mo | $500/mo | 12 months = $2,400 - $6,000 |
| Secrets manager (Doppler) | $0 | $150/mo | Free tier may suffice initially |
| Compliance automation platform | $500/mo | $1,500/mo | Vanta/Drata/Secureframe |
| Engineering time (internal) | $15,000 | $25,000 | ~2 FTE-months of security work |
| Policy writing (external consultant) | $5,000 | $10,000 | If using consultant; $0 if internal |
| **Phase 1 Total** | **$24,000** | **$47,000** | |

### 7.2 Phase 2: Policies & Procedures (Months 3-6)

| Item | Low Estimate | High Estimate | Notes |
|---|---|---|---|
| Compliance platform (continued) | $1,500 | $4,500 | 3 months |
| Security awareness training platform | $0 | $2,000 | KnowBe4 or similar |
| Engineering time (internal) | $10,000 | $20,000 | RBAC, staging, hardening |
| Legal (DPAs, NDAs, privacy policy) | $3,000 | $8,000 | External counsel |
| Staging environment (Render) | $75/mo | $150/mo | Preview deploys or second instance |
| **Phase 2 Total** | **$15,000** | **$35,000** | |

### 7.3 Phase 3: Audit Readiness (Months 6-12)

| Item | Low Estimate | High Estimate | Notes |
|---|---|---|---|
| Compliance platform (continued) | $3,000 | $9,000 | 6 months |
| Penetration testing | $15,000 | $35,000 | Web app + API + AI/ML |
| SOC 2 Type II audit | $30,000 | $60,000 | Audit firm fees |
| Engineering time (internal) | $10,000 | $15,000 | Remediation, evidence |
| Vulnerability scanning tools | $0 | $3,000 | Snyk free tier or paid |
| **Phase 3 Total** | **$58,000** | **$122,000** | |

### 7.4 Total Investment Summary

| Category | Low Estimate | High Estimate |
|---|---|---|
| Phase 1 (Months 0-3) | $24,000 | $47,000 |
| Phase 2 (Months 3-6) | $15,000 | $35,000 |
| Phase 3 (Months 6-12) | $58,000 | $122,000 |
| **Total** | **$97,000** | **$204,000** |
| **Ongoing annual (post-audit)** | **$25,000** | **$50,000** |

**Note:** Internal engineering time is estimated at $75/hr blended rate. Actual costs depend on whether dedicated security hire is made vs. distributed across existing team.

### 7.5 ROI Justification

| Factor | Value |
|---|---|
| Enterprise deals requiring SOC 2 | 60-80% of prospects > 500 employees |
| Average enterprise deal size | $50,000 - $200,000 ARR |
| Deals lost to SOC 2 gap (estimated) | 3-5 per year |
| Revenue at risk | $150,000 - $1,000,000 ARR |
| SOC 2 investment | $97,000 - $204,000 (one-time) + $25-50K annual |
| **Payback period** | **< 12 months from first enterprise deal closed** |

---

## 8. Tool Recommendations

### 8.1 Compliance Automation Platform (Pick One)

| Platform | Monthly Cost | Strengths | Weaknesses | Recommendation |
|---|---|---|---|---|
| **Vanta** | $500 - $1,200/mo | Market leader, 200+ integrations, strong auditor network | Higher cost, can be opinionated | Best for teams wanting white-glove experience |
| **Drata** | $400 - $1,000/mo | Modern UI, good automation, competitive pricing | Newer, fewer integrations | Best value for startups |
| **Secureframe** | $500 - $1,100/mo | Strong policy templates, good AI features | Smaller team, fewer auditor relationships | Best for AI-first companies |

**Our recommendation: Drata**
Reasoning: Best price-to-value ratio for a small team. Integrates with Render, GitHub, Supabase (via API). Modern interface that engineering teams actually enjoy using. Growing fast with good feature velocity.

### 8.2 Supporting Tools

| Category | Tool | Cost | Purpose |
|---|---|---|---|
| **SIEM/Logging** | Axiom | Free - $25/mo (startup plan) | Log aggregation, alerting, dashboards |
| **Vulnerability Scanning** | Snyk | Free (OSS) - $98/mo | Dependency scanning, container scanning |
| **SAST** | Semgrep | Free (OSS) | Static code analysis in CI/CD |
| **Secrets Management** | Doppler | Free - $18/user/mo | Centralized secrets, rotation |
| **Training** | KnowBe4 | $10-25/user/mo | Security awareness, phishing simulations |
| **Policy Management** | Git repo + Drata | $0 (included) | Version-controlled policies |
| **Endpoint Security** | Kolide/Fleet | $3-6/device/mo | Device compliance (if SOC 2 scope includes endpoints) |

### 8.3 Implementation Priority

```
Month 1:  Drata (compliance platform) + Axiom (SIEM) + Doppler (secrets)
Month 2:  Snyk (vulnerability scanning) + Semgrep (SAST)
Month 3:  KnowBe4 (training) -- or defer if budget-constrained
Month 6:  Penetration testing firm engagement
Month 8:  Audit firm engagement
```

---

## 9. Risk Register

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| Engineering bandwidth insufficient | High | High | Prioritize security work in sprint planning; consider fractional CISO hire |
| Vendor refuses to sign DPA | Medium | Medium | Evaluate alternative vendors; document risk acceptance for non-critical vendors |
| Penetration test reveals critical vulnerability | Medium | High | Budget 4 weeks for remediation before audit; maintain security sprint capacity |
| Audit observation period too short | Medium | Medium | Start evidence collection in Month 6 at latest; target 6-month window |
| LLM provider data handling non-compliant | Medium | High | Use zero-retention API plans (OpenAI, Anthropic offer these); document in DPAs |
| Cost overrun | Medium | Medium | Phase spending; defer optional items (bug bounty, endpoint security) |
| Key person dependency | High | Medium | Document all procedures; cross-train on security operations |
| Regulatory landscape changes (AI Act, state privacy laws) | Low | High | Monitor regulatory updates quarterly; engage privacy counsel |

---

## 10. Appendices

### Appendix A: SOC 2 Trust Service Criteria Mapping to Nova AI Suite

```
CC1.1  -> Information Security Policy (Phase 1)
CC1.2  -> Security roles defined in org chart (Phase 1)
CC1.3  -> Board/leadership oversight (Phase 2)
CC2.1  -> Security communication procedures (Phase 1)
CC2.2  -> External communication policy (Phase 2)
CC3.1  -> Risk assessment process (Phase 2)
CC3.2  -> Risk register maintained (Phase 2)
CC4.1  -> Monitoring of controls (Phase 1 -- SIEM)
CC4.2  -> Evaluation and remediation of deficiencies (Phase 2)
CC5.1  -> Change management process (Phase 2)
CC5.2  -> Segregation of duties (Phase 2)
CC6.1  -> Logical access controls (Phase 1 -- MFA, auth.py)
CC6.2  -> User provisioning/deprovisioning (Phase 2)
CC6.3  -> Access reviews (Phase 2 -- quarterly)
CC6.6  -> Authentication mechanisms (Phase 1 -- MFA)
CC6.7  -> Encryption (Phase 1)
CC7.1  -> Vulnerability management (Phase 3)
CC7.2  -> System monitoring (Phase 1 -- health dashboard, SIEM)
CC7.3  -> Change detection (Phase 2)
CC7.4  -> Incident response (Phase 2)
CC8.1  -> Change management (Phase 2)
CC9.1  -> Risk mitigation (Phase 2 -- vendor assessments)
A1.1   -> Capacity planning (Phase 2)
A1.2   -> Disaster recovery (Phase 1 -- backup/restore)
PI1.1  -> Data validation (exists -- document)
PI1.2  -> Processing monitoring (Phase 2)
C1.1   -> Data classification (Phase 1)
C1.2   -> Encryption at rest (Phase 1)
P1.1   -> Privacy notice (Phase 3)
P3.1   -> Data retention (Phase 3)
P4.1   -> Data subject rights (Phase 3)
```

### Appendix B: Key Contacts

| Role | Responsibility | Escalation |
|---|---|---|
| CTO (Security Owner) | Overall security program, policy approval | CEO |
| Lead Engineer (Security Champion) | Technical implementation, code reviews | CTO |
| Compliance Manager (or fractional CISO) | Evidence collection, vendor management, audit coordination | CTO |
| Legal Counsel (external) | DPAs, privacy policy, regulatory compliance | CTO |
| Audit Firm Partner | Audit execution, report issuance | Compliance Manager |

### Appendix C: Decision Log

| Date | Decision | Rationale | Decided By |
|---|---|---|---|
| TBD | Compliance platform selection | Cost, integrations, team fit | CTO |
| TBD | Audit firm selection | Experience, cost, timeline | CTO + CEO |
| TBD | SOC 2 scope (which TSC categories) | Minimum: Security; recommended: Security + Availability + Confidentiality | CTO |
| TBD | Type I vs. direct to Type II | Type I first if enterprise deal imminent; otherwise direct to Type II | CTO + CEO |

### Appendix D: Recommended SOC 2 Scope

**For Nova AI Suite v1 audit, we recommend including:**
- Security (CC) -- **Required** (always included)
- Availability (A) -- **Recommended** (SaaS customers expect uptime commitments)
- Confidentiality (C) -- **Recommended** (we handle client media spend data)

**Defer to v2 audit:**
- Processing Integrity (PI) -- Add once AI output quality controls are more mature
- Privacy (P) -- Add once GDPR/CCPA compliance program is fully operational

This scoping reduces audit complexity and cost by ~30% while covering the criteria that enterprise buyers actually request.

---

*This document should be reviewed quarterly and updated as controls mature. The next review is scheduled for June 2026.*

*Prepared by the Nova AI Suite Security Team. For questions, contact the CTO.*
