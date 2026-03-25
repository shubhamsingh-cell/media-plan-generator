/**
 * Sankey Flow Budget Visualizer for Nova AI Suite.
 * Renders animated SVG diagrams showing budget flow from source through channels to outcomes.
 * Pure JavaScript -- zero dependencies.
 *
 * Usage:
 *   SankeyViz.render(containerEl, {
 *     budget: 50000,
 *     channels: [
 *       { name: 'Indeed', allocation_pct: 35, budget: 17500, projected_clicks: 4200 },
 *       { name: 'LinkedIn', allocation_pct: 25, budget: 12500, projected_clicks: 1800 },
 *       ...
 *     ]
 *   }, { width: 900, height: 420, animate: true, interactive: true });
 */
var SankeyViz = (function () {
  "use strict";

  /* ── Brand palette ── */
  var BRAND = {
    PORT_GORE: "#202058",
    BLUE_VIOLET: "#5A54BD",
    DOWNY_TEAL: "#6BB3CD",
  };

  var CHANNEL_COLORS = [
    "#5A54BD",
    "#6BB3CD",
    "#8b85e0",
    "#4ecdc4",
    "#a78bfa",
    "#f59e0b",
    "#22c55e",
    "#ef4444",
    "#06b6d4",
    "#f43f5e",
    "#34d399",
    "#e879f9",
  ];

  var NS = "http://www.w3.org/2000/svg";

  /* ──────────────────────────────────────────────
   * Helpers
   * ────────────────────────────────────────────── */

  /** Create an SVG element with attributes. */
  function svgEl(tag, attrs) {
    var el = document.createElementNS(NS, tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) {
        el.setAttribute(k, attrs[k]);
      });
    }
    return el;
  }

  /** Format large numbers: 1200000 -> "1.2M", 45000 -> "45K" */
  function fmtNum(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e4 ? 0 : 1) + "K";
    return String(n);
  }

  /** Escape HTML entities for safe text insertion. */
  function esc(s) {
    var d = document.createElement("div");
    d.appendChild(document.createTextNode(s));
    return d.innerHTML;
  }

  /** Build a cubic-bezier Sankey band path between two vertical segments. */
  function bandPath(x1, y1, h1, x2, y2, h2) {
    var cx = (x1 + x2) / 2;
    return (
      "M" +
      x1 +
      "," +
      y1 +
      " C" +
      cx +
      "," +
      y1 +
      " " +
      cx +
      "," +
      y2 +
      " " +
      x2 +
      "," +
      y2 +
      " L" +
      x2 +
      "," +
      (y2 + h2) +
      " C" +
      cx +
      "," +
      (y2 + h2) +
      " " +
      cx +
      "," +
      (y1 + h1) +
      " " +
      x1 +
      "," +
      (y1 + h1) +
      " Z"
    );
  }

  /* ──────────────────────────────────────────────
   * Drawing primitives
   * ────────────────────────────────────────────── */

  /** Add a rounded-rect node with optional glow. */
  function drawNode(svg, x, y, w, h, color, opacity) {
    var rect = svgEl("rect", {
      x: x,
      y: y,
      width: w,
      height: Math.max(h, 14),
      rx: 4,
      ry: 4,
      fill: color,
      opacity: opacity || 0.9,
    });
    // Subtle glow filter
    rect.style.filter = "drop-shadow(0 0 6px " + color + "44)";
    svg.appendChild(rect);
    return rect;
  }

  /** Add a flow band between two segments with staggered animation. */
  function drawFlow(svg, x1, y1, h1, x2, y2, h2, color, animate, idx) {
    var d = bandPath(x1, y1, h1, x2, y2, h2);
    var path = svgEl("path", {
      d: d,
      fill: color,
      opacity: "0",
      stroke: color,
      "stroke-width": "0.5",
      "stroke-opacity": "0.4",
    });

    // Gradient fill for depth
    var gradId = "_skGrad" + idx + "_" + Date.now();
    var defs = svg.querySelector("defs") || svgEl("defs");
    if (!defs.parentNode) svg.insertBefore(defs, svg.firstChild);

    var grad = svgEl("linearGradient", {
      id: gradId,
      x1: "0%",
      y1: "0%",
      x2: "100%",
      y2: "0%",
    });
    grad.appendChild(
      svgEl("stop", {
        offset: "0%",
        "stop-color": color,
        "stop-opacity": "0.35",
      }),
    );
    grad.appendChild(
      svgEl("stop", {
        offset: "50%",
        "stop-color": color,
        "stop-opacity": "0.18",
      }),
    );
    grad.appendChild(
      svgEl("stop", {
        offset: "100%",
        "stop-color": color,
        "stop-opacity": "0.35",
      }),
    );
    defs.appendChild(grad);
    path.setAttribute("fill", "url(#" + gradId + ")");

    if (animate) {
      path.style.transition =
        "opacity 0.7s cubic-bezier(0.22,1,0.36,1) " + idx * 0.12 + "s";
      setTimeout(function () {
        path.style.opacity = "1";
      }, 60);
    } else {
      path.style.opacity = "1";
    }

    svg.appendChild(path);
    return path;
  }

  /** Add a text label. */
  function drawLabel(svg, x, y, text, opts) {
    opts = opts || {};
    var el = svgEl("text", {
      x: x,
      y: y,
      fill: opts.color || "#a1a1a1",
      "font-size": opts.size || "10",
      "font-family": "Inter, system-ui, sans-serif",
      "font-weight": opts.weight || "700",
      "text-anchor": opts.anchor || "start",
      "letter-spacing": opts.spacing || "1",
    });
    el.textContent = text;
    svg.appendChild(el);
    return el;
  }

  /** Add a value + sublabel pair next to a node. */
  function drawNodeLabel(svg, x, y, h, label, value, color) {
    // Value text (bold)
    drawLabel(svg, x, y + Math.max(h, 14) / 2 - 1, value, {
      color: "#e5e5e5",
      size: "12",
      weight: "700",
      spacing: "0",
    });
    // Sublabel (lighter, smaller)
    drawLabel(svg, x, y + Math.max(h, 14) / 2 + 12, label, {
      color: color || "#888",
      size: "9",
      weight: "500",
      spacing: "0.5",
    });
  }

  /* ──────────────────────────────────────────────
   * Tooltip
   * ────────────────────────────────────────────── */

  function createTooltip(container) {
    var tip = document.createElement("div");
    tip.className = "sankey-tooltip";
    tip.style.cssText =
      "position:absolute;pointer-events:none;background:rgba(32,32,88,0.95);" +
      "color:#e5e5e5;padding:8px 12px;border-radius:8px;font-size:11px;" +
      "font-family:Inter,sans-serif;opacity:0;transition:opacity 0.2s;z-index:10;" +
      "border:1px solid rgba(90,84,189,0.3);backdrop-filter:blur(8px);" +
      "box-shadow:0 4px 20px rgba(0,0,0,0.3);max-width:220px;line-height:1.5;";
    container.appendChild(tip);
    return tip;
  }

  /* ──────────────────────────────────────────────
   * Animated flow particles (optional enhancement)
   * ────────────────────────────────────────────── */

  function addFlowParticles(svg, x1, y1, h1, x2, y2, h2, color, idx) {
    // Animated dot traveling along the flow center-line
    var cx = (x1 + x2) / 2;
    var my1 = y1 + h1 / 2;
    var my2 = y2 + h2 / 2;
    var pathD =
      "M" +
      x1 +
      "," +
      my1 +
      " C" +
      cx +
      "," +
      my1 +
      " " +
      cx +
      "," +
      my2 +
      " " +
      x2 +
      "," +
      my2;

    var motionPath = svgEl("path", { d: pathD, fill: "none", stroke: "none" });
    motionPath.id = "_skMotion" + idx + "_" + Date.now();
    svg.appendChild(motionPath);

    var circle = svgEl("circle", {
      r: "2.5",
      fill: color,
      opacity: "0.7",
    });
    circle.style.filter = "drop-shadow(0 0 3px " + color + ")";

    var animMotion = svgEl("animateMotion", {
      dur: 2 + Math.random() * 1.5 + "s",
      repeatCount: "indefinite",
      begin: idx * 0.3 + "s",
    });
    var mpath = svgEl("mpath");
    mpath.setAttributeNS(
      "http://www.w3.org/1999/xlink",
      "href",
      "#" + motionPath.id,
    );
    animMotion.appendChild(mpath);
    circle.appendChild(animMotion);

    svg.appendChild(circle);
  }

  /* ──────────────────────────────────────────────
   * Main render function
   * ────────────────────────────────────────────── */

  /**
   * Render a Sankey diagram into a container element.
   *
   * @param {HTMLElement} container - Target container (will be cleared)
   * @param {Object} data - Plan data:
   *   { budget: number,
   *     channels: [{ name, allocation_pct, budget, projected_clicks,
   *                   outcomes?: { impressions, clicks, applies } }] }
   * @param {Object} [options] - Rendering options:
   *   { width: 900, height: 420, animate: true, interactive: true }
   */
  function render(container, data, options) {
    var opts = {
      width: 900,
      height: 420,
      animate: true,
      interactive: true,
    };
    if (options) {
      Object.keys(options).forEach(function (k) {
        opts[k] = options[k];
      });
    }

    var W = opts.width;
    var H = opts.height;
    var channels = (data && data.channels) || [];
    var totalBudget = (data && data.budget) || 0;

    // If no channels, bail with a message
    if (channels.length === 0) {
      container.innerHTML =
        '<div style="color:#666;text-align:center;padding:40px;font-family:Inter,sans-serif;">' +
        "No channel data available for budget flow visualization.</div>";
      return;
    }

    // Compute total budget from channels if not provided
    if (!totalBudget) {
      totalBudget = channels.reduce(function (s, c) {
        return s + (Number(c.budget) || 0);
      }, 0);
    }

    // Wrapper for relative positioning (tooltips)
    var wrapper = document.createElement("div");
    wrapper.style.cssText = "position:relative;width:100%;";

    // Create SVG
    var svg = svgEl("svg", {
      viewBox: "0 0 " + W + " " + H,
      width: "100%",
      role: "img",
      "aria-label":
        "Sankey budget flow diagram showing money flowing from total budget through channels to expected outcomes",
    });
    svg.style.maxWidth = W + "px";
    svg.style.display = "block";
    svg.style.margin = "0 auto";

    // Transparent background for event handling
    svg.appendChild(
      svgEl("rect", {
        width: W,
        height: H,
        fill: "transparent",
      }),
    );

    // ── Layout constants ──
    var PAD_TOP = 40;
    var PAD_BOTTOM = 20;
    var NODE_W = 22;
    var USABLE_H = H - PAD_TOP - PAD_BOTTOM;

    var srcX = 70; // Source node left edge
    var chX = Math.round(W * 0.38); // Channel nodes left edge
    var outX = Math.round(W * 0.76); // Outcome nodes left edge

    // ── Column headers ──
    drawLabel(svg, srcX + NODE_W / 2, PAD_TOP - 16, "BUDGET", {
      color: "#666",
      size: "9",
      anchor: "middle",
      weight: "800",
      spacing: "2",
    });
    drawLabel(svg, chX + NODE_W / 2, PAD_TOP - 16, "CHANNELS", {
      color: "#666",
      size: "9",
      anchor: "middle",
      weight: "800",
      spacing: "2",
    });
    drawLabel(svg, outX + NODE_W / 2, PAD_TOP - 16, "OUTCOMES", {
      color: "#666",
      size: "9",
      anchor: "middle",
      weight: "800",
      spacing: "2",
    });

    // ── Source node (total budget) ──
    var srcH = USABLE_H * 0.85;
    var srcY = PAD_TOP + (USABLE_H - srcH) / 2;
    drawNode(svg, srcX, srcY, NODE_W, srcH, BRAND.PORT_GORE, 0.95);
    drawNodeLabel(
      svg,
      srcX - 55,
      srcY,
      srcH,
      "Total",
      "$" + fmtNum(totalBudget),
      "#888",
    );

    // ── Compute channel heights proportional to allocation ──
    var GAP = Math.max(6, Math.round(60 / channels.length));
    var totalGap = GAP * (channels.length - 1);
    var availH = srcH - totalGap;

    // Normalize percentages
    var totalPct = channels.reduce(function (s, c) {
      return s + (Number(c.allocation_pct) || 100 / channels.length);
    }, 0);

    var channelLayouts = [];
    var curY = srcY;

    channels.forEach(function (ch, i) {
      var pct = Number(ch.allocation_pct) || 100 / channels.length;
      var normalPct = pct / totalPct;
      var h = Math.max(18, Math.round(normalPct * availH));
      channelLayouts.push({
        ch: ch,
        y: curY,
        h: h,
        pct: pct,
        color: CHANNEL_COLORS[i % CHANNEL_COLORS.length],
      });
      curY += h + GAP;
    });

    // ── Source-to-channel flow offset tracking ──
    var srcOffset = 0;

    channelLayouts.forEach(function (cl, i) {
      var ch = cl.ch;
      var chY = cl.y;
      var chH = cl.h;
      var color = cl.color;
      var spend = Number(ch.budget) || Math.round((totalBudget * cl.pct) / 100);

      // Proportional source band height
      var srcBandH = Math.max(14, Math.round((cl.pct / totalPct) * srcH));
      var srcBandY = srcY + srcOffset;
      srcOffset += srcBandH;

      // Flow: source -> channel
      drawFlow(
        svg,
        srcX + NODE_W,
        srcBandY,
        srcBandH,
        chX,
        chY,
        chH,
        color,
        opts.animate,
        i,
      );

      // Animated particles along the flow
      if (opts.animate) {
        addFlowParticles(
          svg,
          srcX + NODE_W,
          srcBandY,
          srcBandH,
          chX,
          chY,
          chH,
          color,
          i,
        );
      }

      // Channel node
      var chNode = drawNode(svg, chX, chY, NODE_W, chH, color, 0.9);

      // Channel label (right side)
      drawNodeLabel(
        svg,
        chX + NODE_W + 8,
        chY,
        chH,
        "$" + fmtNum(spend) + " (" + cl.pct.toFixed(0) + "%)",
        esc(ch.name || "Channel " + (i + 1)),
        color,
      );

      // ── Channel -> Outcome flow ──
      var clicks = Number(ch.projected_clicks) || 0;
      var applies =
        (ch.outcomes && ch.outcomes.applies) ||
        Math.round(clicks * 0.08) ||
        Math.round(spend / 50);
      var outH = Math.max(14, chH * 0.65);
      var outY = chY + (chH - outH) / 2;

      drawFlow(
        svg,
        chX + NODE_W,
        chY,
        chH,
        outX,
        outY,
        outH,
        color,
        opts.animate,
        i + channels.length,
      );

      if (opts.animate) {
        addFlowParticles(
          svg,
          chX + NODE_W,
          chY,
          chH,
          outX,
          outY,
          outH,
          color,
          i + channels.length,
        );
      }

      // Outcome node
      drawNode(svg, outX, outY, NODE_W, outH, color, 0.75);

      // Outcome label
      var outcomeVal =
        clicks > 0 ? fmtNum(clicks) + " clicks" : fmtNum(applies) + " applies";
      drawNodeLabel(
        svg,
        outX + NODE_W + 8,
        outY,
        outH,
        outcomeVal,
        "Est. ROI",
        "#888",
      );

      // ── Interactive tooltip on hover ──
      if (opts.interactive) {
        (function (channelData, nodeEl, cColor, cSpend, cClicks, cApplies) {
          var hoverGroup = svgEl("rect", {
            x: chX - 10,
            y: chY - 4,
            width: outX + NODE_W + 100 - chX + 20,
            height: Math.max(chH, 18) + 8,
            fill: "transparent",
            style: "cursor:pointer",
          });

          hoverGroup.addEventListener("mouseenter", function (e) {
            nodeEl.setAttribute("opacity", "1");
            nodeEl.style.filter = "drop-shadow(0 0 10px " + cColor + "88)";

            var tip = container.querySelector(".sankey-tooltip");
            if (tip) {
              tip.innerHTML =
                '<div style="font-weight:700;color:white;margin-bottom:4px;">' +
                esc(channelData.name || "Channel") +
                "</div>" +
                "<div>Budget: <strong>$" +
                Number(cSpend).toLocaleString() +
                "</strong></div>" +
                "<div>Allocation: <strong>" +
                (channelData.allocation_pct || 0).toFixed(1) +
                "%</strong></div>" +
                (cClicks > 0
                  ? "<div>Projected clicks: <strong>" +
                    Number(cClicks).toLocaleString() +
                    "</strong></div>"
                  : "") +
                (cApplies > 0
                  ? "<div>Est. applies: <strong>" +
                    Number(cApplies).toLocaleString() +
                    "</strong></div>"
                  : "") +
                (channelData.cpc_range
                  ? "<div>CPC: <strong>" +
                    esc(String(channelData.cpc_range)) +
                    "</strong></div>"
                  : "");
              tip.style.opacity = "1";
            }
          });

          hoverGroup.addEventListener("mousemove", function (e) {
            var tip = container.querySelector(".sankey-tooltip");
            if (tip) {
              var rect = container.getBoundingClientRect();
              tip.style.left = e.clientX - rect.left + 14 + "px";
              tip.style.top = e.clientY - rect.top - 10 + "px";
            }
          });

          hoverGroup.addEventListener("mouseleave", function () {
            nodeEl.setAttribute("opacity", "0.9");
            nodeEl.style.filter = "drop-shadow(0 0 6px " + cColor + "44)";
            var tip = container.querySelector(".sankey-tooltip");
            if (tip) tip.style.opacity = "0";
          });

          svg.appendChild(hoverGroup);
        })(ch, chNode, color, spend, clicks, applies);
      }
    });

    // ── Assemble ──
    wrapper.appendChild(svg);
    var tooltip = createTooltip(wrapper);

    container.innerHTML = "";
    container.appendChild(wrapper);

    // Respect reduced-motion preference
    if (
      window.matchMedia &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches
    ) {
      svg.querySelectorAll("animateMotion").forEach(function (el) {
        el.remove();
      });
      svg.querySelectorAll("[style]").forEach(function (el) {
        el.style.transition = "none";
        if (el.style.opacity === "0") el.style.opacity = "1";
      });
    }
  }

  /* ── Public API ── */
  return { render: render };
})();

// CommonJS export for testing
if (typeof module !== "undefined" && module.exports) {
  module.exports = SankeyViz;
}
