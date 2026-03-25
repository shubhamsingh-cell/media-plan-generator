/**
 * Nova ATS Widget v1.0.0 -- Embeddable AI Plan Suggestions
 * Usage: <script src="/static/nova-ats-widget.js"></script>
 *        <script>NovaATS.init({ apiEndpoint, jobTitle, location, budget, theme, position });</script>
 */
(function () {
  "use strict";
  var BV = "#5A54BD",
    DT = "#6BB3CD",
    PG = "#202058",
    F = 'Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif';
  var TH = {
    light: {
      bg: "#FFF",
      bp: "#F8F9FB",
      tx: "#1A1A2E",
      tm: "#6B7280",
      bd: "#E5E7EB",
      sh: "rgba(0,0,0,.12)",
      hg: "linear-gradient(135deg," + PG + "," + BV + ")",
      bb: "#E5E7EB",
      bf: BV,
      ba: "rgba(90,84,189,.1)",
      bt: BV,
    },
    dark: {
      bg: "#111827",
      bp: "#1F2937",
      tx: "#E5E7EB",
      tm: "#9CA3AF",
      bd: "#374151",
      sh: "rgba(0,0,0,.4)",
      hg: "linear-gradient(135deg," + PG + "," + BV + ")",
      bb: "#374151",
      bf: DT,
      ba: "rgba(107,179,205,.15)",
      bt: DT,
    },
  };
  var _c = {},
    _h = null,
    _s = null,
    _o = false,
    _d = null,
    _l = false,
    _e = null;

  function mg(a, b) {
    var o = {},
      k;
    for (k in a) if (a.hasOwnProperty(k)) o[k] = a[k];
    for (k in b) if (b.hasOwnProperty(k) && b[k] !== undefined) o[k] = b[k];
    return o;
  }
  function esc(s) {
    if (!s) return "";
    var d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }
  function fmt(n) {
    return n == null
      ? "--"
      : typeof n === "number"
        ? n.toLocaleString("en-US")
        : String(n);
  }
  function money(n) {
    return n == null
      ? "--"
      : "$" +
          Number(n).toLocaleString("en-US", {
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
          });
  }
  function pct(n) {
    return n == null ? "--" : Math.round(n) + "%";
  }

  function fetchData() {
    if (_l) return;
    _l = true;
    _e = null;
    render();
    var base = (_c.apiEndpoint || "").replace(/\/+$/, "");
    var url =
      base +
      "/api/channels?job_title=" +
      encodeURIComponent(_c.jobTitle || "") +
      "&location=" +
      encodeURIComponent(_c.location || "") +
      "&budget=" +
      encodeURIComponent(_c.budget || 5000);
    try {
      var x = new XMLHttpRequest();
      x.open("GET", url, true);
      x.timeout = 15000;
      x.onload = function () {
        _l = false;
        if (x.status >= 200 && x.status < 300) {
          try {
            _d = JSON.parse(x.responseText);
          } catch (e) {
            _e = "Invalid response";
          }
        } else {
          _e = "Server returned " + x.status;
        }
        render();
      };
      x.onerror = function () {
        _l = false;
        _e = "Network error";
        render();
      };
      x.ontimeout = function () {
        _l = false;
        _e = "Request timed out";
        render();
      };
      x.send();
    } catch (err) {
      _l = false;
      _e = "Failed: " + err.message;
      render();
    }
  }

  function buildRec(raw) {
    var budget = Number(_c.budget) || 5000,
      all = [],
      seen = {},
      uniq = [],
      channels = [];
    if (raw && typeof raw === "object") {
      var ks = Object.keys(raw);
      for (var i = 0; i < ks.length; i++) {
        var v = raw[ks[i]];
        if (Array.isArray(v))
          for (var j = 0; j < v.length; j++)
            if (v[j] && v[j].name) all.push(v[j]);
      }
    }
    for (var u = 0; u < all.length; u++) {
      var nm = (all[u].name || "").toLowerCase();
      if (!seen[nm]) {
        seen[nm] = true;
        uniq.push(all[u]);
      }
    }
    var top = uniq.slice(0, 4);
    if (!top.length)
      top = [
        { name: "Indeed", allocation: 35 },
        { name: "LinkedIn", allocation: 25 },
        { name: "ZipRecruiter", allocation: 20 },
        { name: "Google Jobs", allocation: 20 },
      ];
    var totalApps = 0;
    for (var c = 0; c < top.length; c++) {
      var sh =
          top[c].allocation ||
          (c === 0 ? 35 : c === 1 ? 25 : c === 2 ? 22 : 18),
        sp = Math.round(budget * (sh / 100)),
        cpa = top[c].cpa || top[c].cost_per_apply || 12 + c * 3;
      channels.push({ name: top[c].name, allocation: sh, spend: sp, cpa: cpa });
      totalApps += Math.round(sp / cpa);
    }
    return {
      channels: channels,
      totalBudget: budget,
      estApps: totalApps,
      avgCpa: totalApps > 0 ? Math.round(budget / totalApps) : 15,
    };
  }

  function css(t) {
    return (
      "*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}" +
      ":host{all:initial;font-family:" +
      F +
      ";font-size:14px;line-height:1.5}" +
      ".fab{position:fixed;z-index:2147483646;display:flex;align-items:center;gap:8px;padding:10px 18px;border-radius:28px;border:none;cursor:pointer;background:" +
      BV +
      ";color:#FFF;font:600 13px/1 " +
      F +
      ";box-shadow:0 4px 20px " +
      t.sh +
      ";transition:transform .2s,box-shadow .2s}" +
      ".fab:hover{transform:translateY(-2px);box-shadow:0 6px 28px " +
      t.sh +
      "}.fab:active{transform:translateY(0)}" +
      ".fab svg{width:18px;height:18px;flex-shrink:0}" +
      ".pbr{bottom:24px;right:24px}.pbl{bottom:24px;left:24px}" +
      ".pan{position:fixed;z-index:2147483647;width:380px;max-width:calc(100vw - 32px);max-height:min(560px,calc(100vh - 100px));border-radius:16px;overflow:hidden;background:" +
      t.bg +
      ";border:1px solid " +
      t.bd +
      ";box-shadow:0 8px 40px " +
      t.sh +
      ";display:flex;flex-direction:column;animation:si .25s ease-out}" +
      ".nbr{bottom:80px;right:24px}.nbl{bottom:80px;left:24px}" +
      "@keyframes si{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}" +
      ".hd{background:" +
      t.hg +
      ";color:#FFF;padding:16px 20px;display:flex;justify-content:space-between;align-items:center}" +
      ".hd h3{font:600 15px/1.3 " +
      F +
      ";margin:0}.hd p{font:400 12px/1.4 " +
      F +
      ";margin:4px 0 0;opacity:.85}" +
      ".cb{background:none;border:none;color:inherit;cursor:pointer;padding:4px;border-radius:6px;opacity:.7;transition:opacity .15s}.cb:hover{opacity:1}" +
      ".bd{padding:16px 20px;overflow-y:auto;flex:1}" +
      ".sc{margin-bottom:16px}" +
      ".st{font:600 12px/1 " +
      F +
      ";text-transform:uppercase;letter-spacing:.06em;color:" +
      t.tm +
      ";margin-bottom:10px}" +
      ".ch{display:flex;align-items:center;gap:10px;padding:10px 12px;background:" +
      t.bp +
      ";border-radius:10px;margin-bottom:6px}" +
      ".cn{font:500 13px/1.3 " +
      F +
      ";color:" +
      t.tx +
      ";flex:1}.cs{font:600 13px/1 " +
      F +
      ";color:" +
      t.tx +
      "}.cp{font:500 11px/1 " +
      F +
      ";color:" +
      t.tm +
      ";min-width:36px;text-align:right}" +
      ".br{height:6px;border-radius:3px;background:" +
      t.bb +
      ";margin-top:4px;overflow:hidden}" +
      ".bf{height:100%;border-radius:3px;background:" +
      t.bf +
      ";transition:width .6s ease}" +
      ".og{display:grid;grid-template-columns:1fr 1fr;gap:8px}" +
      ".sv{background:" +
      t.bp +
      ";border-radius:10px;padding:12px;text-align:center}" +
      ".vl{font:700 20px/1.2 " +
      F +
      ";color:" +
      t.tx +
      "}.lb{font:400 11px/1.3 " +
      F +
      ";color:" +
      t.tm +
      ";margin-top:2px}" +
      ".ct{display:block;width:100%;padding:12px;border:none;border-radius:10px;background:" +
      BV +
      ";color:#FFF;font:600 14px/1 " +
      F +
      ";cursor:pointer;text-align:center;text-decoration:none;transition:opacity .15s;margin-top:8px}.ct:hover{opacity:.9}" +
      ".ft{padding:10px 20px;border-top:1px solid " +
      t.bd +
      ";display:flex;align-items:center;justify-content:center;gap:6px}" +
      ".ft span{font:400 11px/1 " +
      F +
      ";color:" +
      t.tm +
      "}.ft a{color:" +
      BV +
      ";text-decoration:none;font-weight:500}" +
      ".ld{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px 20px;gap:12px}" +
      ".sp{width:28px;height:28px;border:3px solid " +
      t.bb +
      ";border-top-color:" +
      BV +
      ";border-radius:50%;animation:sn .7s linear infinite}" +
      "@keyframes sn{to{transform:rotate(360deg)}}" +
      ".ld p{font:400 13px/1.4 " +
      F +
      ";color:" +
      t.tm +
      "}" +
      ".er{padding:24px 20px;text-align:center}.er p{font:400 13px/1.4 " +
      F +
      ";color:" +
      t.tm +
      ";margin-bottom:12px}" +
      ".rt{padding:8px 16px;border:1px solid " +
      t.bd +
      ";border-radius:8px;background:transparent;color:" +
      t.tx +
      ";cursor:pointer;font:500 13px/1 " +
      F +
      "}" +
      "@media(max-width:480px){.pan{width:calc(100vw - 16px);left:8px !important;right:8px !important;bottom:72px !important;border-radius:12px}.fab{padding:8px 14px;font-size:12px}}"
    );
  }

  var svgLayers =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>';
  var svgX =
    '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>';

  function render() {
    if (!_s) return;
    var t = TH[_c.theme] || TH.light,
      pp = _c.position === "bottom-left" ? "bl" : "br";
    _s.innerHTML = "";
    var sty = document.createElement("style");
    sty.textContent = css(t);
    _s.appendChild(sty);

    var fab = document.createElement("button");
    fab.className = "fab p" + pp;
    fab.setAttribute(
      "aria-label",
      _o ? "Close AI Plan Suggestions" : "Open AI Plan Suggestions",
    );
    fab.innerHTML = _o
      ? svgX + "<span>Close</span>"
      : svgLayers + "<span>AI Plan Suggestions</span>";
    fab.addEventListener("click", function () {
      _o = !_o;
      if (_o && !_d && !_l) fetchData();
      render();
    });
    _s.appendChild(fab);
    if (!_o) return;

    var pan = document.createElement("div");
    pan.className = "pan n" + pp;
    pan.setAttribute("role", "dialog");
    pan.setAttribute("aria-label", "AI Plan Suggestions");

    var hdr = document.createElement("div");
    hdr.className = "hdr";
    hdr.style.cssText =
      "background:" +
      t.hg +
      ";color:#FFF;padding:16px 20px;display:flex;justify-content:space-between;align-items:center";
    var hl = document.createElement("div");
    hl.innerHTML =
      '<h3 style="font:600 15px/1.3 ' +
      F +
      ';margin:0">AI Plan Suggestions</h3><p style="font:400 12px/1.4 ' +
      F +
      ';margin:4px 0 0;opacity:.85">' +
      esc(_c.jobTitle || "Role") +
      " &middot; " +
      esc(_c.location || "Location") +
      "</p>";
    var cb = document.createElement("button");
    cb.className = "cb";
    cb.setAttribute("aria-label", "Close");
    cb.innerHTML = svgX;
    cb.addEventListener("click", function () {
      _o = false;
      render();
    });
    hdr.appendChild(hl);
    hdr.appendChild(cb);
    pan.appendChild(hdr);

    var bd = document.createElement("div");
    bd.className = "bd";
    if (_l) {
      bd.innerHTML =
        '<div class="ld"><div class="sp"></div><p>Analyzing channels...</p></div>';
    } else if (_e) {
      var ev = document.createElement("div");
      ev.className = "er";
      ev.innerHTML = "<p>" + esc(_e) + "</p>";
      var rb = document.createElement("button");
      rb.className = "rt";
      rb.textContent = "Retry";
      rb.addEventListener("click", function () {
        fetchData();
      });
      ev.appendChild(rb);
      bd.appendChild(ev);
    } else if (_d) {
      bd.appendChild(renderRec(buildRec(_d), t));
    }
    pan.appendChild(bd);

    var ft = document.createElement("div");
    ft.className = "ft";
    ft.innerHTML =
      '<span>Powered by</span><a href="https://media-plan-generator.onrender.com" target="_blank" rel="noopener">Nova AI Suite</a>';
    pan.appendChild(ft);
    _s.appendChild(pan);
  }

  function renderRec(rec, t) {
    var f = document.createDocumentFragment();
    var sc = document.createElement("div");
    sc.className = "sc";
    sc.innerHTML = '<div class="st">Recommended Channels</div>';
    for (var i = 0; i < rec.channels.length; i++) {
      var c = rec.channels[i],
        cd = document.createElement("div");
      cd.className = "ch";
      cd.innerHTML =
        '<div class="cn">' +
        esc(c.name) +
        '</div><div class="cs">' +
        money(c.spend) +
        '</div><div class="cp">' +
        pct(c.allocation) +
        "</div>";
      sc.appendChild(cd);
    }
    var bw = document.createElement("div");
    bw.className = "br";
    bw.setAttribute("role", "progressbar");
    var bi = document.createElement("div");
    bi.className = "bf";
    bi.style.width = "100%";
    bw.appendChild(bi);
    sc.appendChild(bw);
    f.appendChild(sc);

    var so = document.createElement("div");
    so.className = "sc";
    so.innerHTML = '<div class="st">Expected Outcomes</div>';
    var gr = document.createElement("div");
    gr.className = "og";
    gr.innerHTML =
      '<div class="sv"><div class="vl">' +
      fmt(rec.estApps) +
      '</div><div class="lb">Est. Applications</div></div>' +
      '<div class="sv"><div class="vl">' +
      money(rec.avgCpa) +
      '</div><div class="lb">Avg Cost/Apply</div></div>' +
      '<div class="sv"><div class="vl">' +
      money(rec.totalBudget) +
      '</div><div class="lb">Total Budget</div></div>' +
      '<div class="sv"><div class="vl">' +
      fmt(rec.channels.length) +
      '</div><div class="lb">Channels</div></div>';
    so.appendChild(gr);
    f.appendChild(so);

    var base = (_c.apiEndpoint || "").replace(/\/+$/, "");
    var ctaUrl =
      base +
      "/platform/plan?job_title=" +
      encodeURIComponent(_c.jobTitle || "") +
      "&location=" +
      encodeURIComponent(_c.location || "") +
      "&budget=" +
      encodeURIComponent(_c.budget || 5000);
    var cta = document.createElement("a");
    cta.className = "ct";
    cta.href = ctaUrl;
    cta.target = "_blank";
    cta.rel = "noopener";
    cta.textContent = "Generate Full Plan";
    f.appendChild(cta);
    return f;
  }

  function init(cfg) {
    _c = mg(
      {
        apiEndpoint: "",
        jobTitle: "",
        location: "",
        budget: 5000,
        theme: "light",
        position: "bottom-right",
      },
      cfg || {},
    );
    if (!TH[_c.theme]) _c.theme = "light";
    if (_c.position !== "bottom-left") _c.position = "bottom-right";
    if (_h && _h.parentNode) _h.parentNode.removeChild(_h);
    _h = document.createElement("div");
    _h.id = "nova-ats-widget";
    _h.style.cssText =
      "position:fixed;z-index:2147483646;pointer-events:none;top:0;left:0;width:0;height:0";
    document.body.appendChild(_h);
    _s = _h.attachShadow({ mode: "closed" });
    var ws = document.createElement("style");
    ws.textContent = ":host{pointer-events:none}.fab,.pan{pointer-events:auto}";
    _s.appendChild(ws);
    _o = false;
    _d = null;
    _l = false;
    _e = null;
    render();
  }

  window.NovaATS = {
    init: init,
    destroy: function () {
      if (_h && _h.parentNode) _h.parentNode.removeChild(_h);
      _h = null;
      _s = null;
      _o = false;
      _d = null;
    },
    setTheme: function (th) {
      if (TH[th]) {
        _c.theme = th;
        render();
      }
    },
    version: "1.0.0",
  };
})();
