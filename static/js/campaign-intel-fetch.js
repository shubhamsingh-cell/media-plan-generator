/**
 * Campaign Intelligence — live data fetcher.
 *
 * Drop-in replacement for the Google Sheet fetch that was CORS-blocked. Pulls
 * Meta + Google Ads campaign performance from the Nova backend's
 * /api/campaign-intel/metrics endpoint, which reads from the
 * social_campaign_metrics Supabase table (synced every few hours by the
 * social_metrics_sync background job).
 *
 * Three call shapes:
 *   1. fetchAsCSV()    -> string  (drop-in for Sheet "Publish to web" CSV)
 *   2. fetchAsFile()   -> File    (drop-in for the existing file uploader)
 *   3. fetchAsJSON()   -> object  (aggregated totals + top campaigns)
 *
 * Auth: defaults to credentialed cookies (works when same-origin or when
 * cookies set with SameSite=None on a Joveo subdomain). Cross-origin
 * deployments should pass an apiKey that matches one entry in NOVA_API_KEYS.
 *
 * Usage (vanilla, no build step):
 *   <script src="https://media-plan-generator.onrender.com/static/js/campaign-intel-fetch.js"></script>
 *   <script>
 *     const csv = await CampaignIntel.fetchAsCSV({ platform: 'both', days: 7 });
 *     // Feed `csv` directly into the existing CSV parser.
 *
 *     // Or, if the tool's upload pipeline expects a File:
 *     const file = await CampaignIntel.fetchAsFile({ days: 30 });
 *     existingUploadHandler(file);
 *   </script>
 */
(function (global) {
  "use strict";

  const DEFAULT_BASE = "https://media-plan-generator.onrender.com";

  function buildQuery(opts) {
    const params = new URLSearchParams();
    if (opts.platform) params.set("platform", opts.platform);
    if (opts.startDate) params.set("start_date", opts.startDate);
    if (opts.endDate) params.set("end_date", opts.endDate);
    if (opts.campaignFilter) params.set("campaign_filter", opts.campaignFilter);
    if (opts.topN != null) params.set("top_n", String(opts.topN));
    if (opts.format) params.set("format", opts.format);

    // Convenience: opts.days N -> end_date=yesterday, start_date=N-1 days before
    if (!opts.startDate && !opts.endDate && opts.days) {
      const end = new Date();
      end.setUTCDate(end.getUTCDate() - 1);
      const start = new Date(end);
      start.setUTCDate(start.getUTCDate() - (opts.days - 1));
      const fmt = (d) => d.toISOString().slice(0, 10);
      params.set("start_date", fmt(start));
      params.set("end_date", fmt(end));
    }
    return params.toString();
  }

  function buildHeaders(opts) {
    const headers = { Accept: "application/json, text/csv" };
    if (opts.apiKey) {
      headers["X-Nova-Api-Key"] = opts.apiKey;
    }
    return headers;
  }

  async function fetchRaw(opts) {
    const base = (opts.baseUrl || DEFAULT_BASE).replace(/\/+$/, "");
    const qs = buildQuery(opts);
    const url = `${base}/api/campaign-intel/metrics${qs ? "?" + qs : ""}`;
    const resp = await fetch(url, {
      method: "GET",
      headers: buildHeaders(opts),
      // Send cookies on same-origin / subdomain deployments. Cross-origin
      // calls require both this AND an Access-Control-Allow-Credentials
      // header on the server, which the Nova backend grants only to origins
      // in _ALLOWED_ORIGINS. If you're on a different origin, use apiKey.
      credentials: opts.apiKey ? "omit" : "include",
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      const err = new Error(
        `Campaign Intel fetch failed: HTTP ${resp.status} — ${text.slice(0, 200)}`,
      );
      err.status = resp.status;
      err.body = text;
      throw err;
    }
    return resp;
  }

  /**
   * Returns aggregated metrics as JSON: per-platform totals + top campaigns.
   *
   * @param {Object} opts
   * @param {('meta'|'google_ads'|'both')} [opts.platform='both']
   * @param {string} [opts.startDate]      YYYY-MM-DD
   * @param {string} [opts.endDate]        YYYY-MM-DD
   * @param {number} [opts.days]           shortcut: ignore startDate/endDate
   * @param {string} [opts.campaignFilter] substring (case-insensitive)
   * @param {number} [opts.topN=10]
   * @param {string} [opts.apiKey]         X-Nova-Api-Key for cross-origin
   * @param {string} [opts.baseUrl]        override the Nova backend URL
   * @returns {Promise<Object>}
   */
  async function fetchAsJSON(opts) {
    const o = Object.assign({}, opts || {}, { format: "json" });
    const resp = await fetchRaw(o);
    return resp.json();
  }

  /**
   * Returns the raw daily campaign rows as a CSV string. Drop-in for the
   * Google Sheet CSV that the Campaign Intelligence tool used to fetch.
   *
   * Columns: platform, account_id, campaign_id, campaign_name, objective,
   *          date, spend, impressions, clicks, conversions, ctr, cpc, cpa,
   *          cpm, currency
   *
   * @param {Object} opts (same shape as fetchAsJSON)
   * @returns {Promise<string>}
   */
  async function fetchAsCSV(opts) {
    const o = Object.assign({}, opts || {}, { format: "csv" });
    const resp = await fetchRaw(o);
    return resp.text();
  }

  /**
   * Returns the same CSV wrapped in a File object so it can be fed straight
   * into the Campaign Intelligence tool's existing file-upload pipeline
   * (CSV/TSV/Excel/JSON/ODS uploader).
   *
   * @param {Object} opts (same shape as fetchAsJSON)
   * @returns {Promise<File>}
   */
  async function fetchAsFile(opts) {
    const csv = await fetchAsCSV(opts);
    const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    return new File([csv], `campaign_metrics_${stamp}.csv`, {
      type: "text/csv",
      lastModified: Date.now(),
    });
  }

  /**
   * Convenience helper: fetch as File and dispatch a synthetic 'change' event
   * on the given <input type="file"> element. Useful if the tool wires its
   * upload logic to the input's change event. Returns the File for chaining.
   *
   * @param {HTMLInputElement} inputEl
   * @param {Object} opts
   * @returns {Promise<File>}
   */
  async function fetchAndAttachToInput(inputEl, opts) {
    if (!inputEl || inputEl.tagName !== "INPUT" || inputEl.type !== "file") {
      throw new Error('fetchAndAttachToInput requires an <input type="file">');
    }
    const file = await fetchAsFile(opts);
    const dt = new DataTransfer();
    dt.items.add(file);
    inputEl.files = dt.files;
    inputEl.dispatchEvent(new Event("change", { bubbles: true }));
    return file;
  }

  global.CampaignIntel = {
    fetchAsJSON,
    fetchAsCSV,
    fetchAsFile,
    fetchAndAttachToInput,
    _version: "1.0.0",
  };
})(typeof window !== "undefined" ? window : globalThis);
