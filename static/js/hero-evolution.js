/**
 * hero-evolution.js — "Data to Plan" animated hero visualization
 *
 * A looping animation that shows Nova's value proposition visually:
 *   Phase 1 (Chaos):        Scattered floating elements — job boards, costs, cities
 *   Phase 2 (Intelligence): Elements flow toward center, connections appear
 *   Phase 3 (Plan):         Elements snap into an organized dashboard layout
 *   Phase 4 (Hold + Reset): Hold the plan, then fade and restart
 *
 * Pure CSS + SVG + requestAnimationFrame. No Three.js. No GSAP dependency
 * (uses its own animation system so it works even if GSAP CDN fails).
 *
 * Respects prefers-reduced-motion. Falls back to static layout.
 */
(function () {
  "use strict";

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  var hero = document.querySelector(".hero");
  if (!hero) return;

  /* ── Create container ── */
  var container = document.createElement("div");
  container.className = "hero-evolution";
  container.setAttribute("aria-hidden", "true");

  /* Insert after hero-content so it's behind text (z-index layering) */
  hero.appendChild(container);

  /* ── Inject styles ── */
  var style = document.createElement("style");
  style.textContent = [
    ".hero-evolution {",
    "  position: absolute;",
    "  inset: 0;",
    "  z-index: 1;",
    "  pointer-events: none;",
    "  overflow: hidden;",
    "  opacity: 0.8;",
    "}",

    ".evo-node {",
    "  position: absolute;",
    "  font-family: var(--font-mono, 'SF Mono', monospace);",
    "  font-size: 13px;",
    "  font-weight: 600;",
    "  color: rgba(255, 255, 255, 0.75);",
    "  background: rgba(90, 84, 189, 0.12);",
    "  border: 1px solid rgba(90, 84, 189, 0.25);",
    "  border-radius: 8px;",
    "  padding: 8px 16px;",
    "  white-space: nowrap;",
    "  transition: none;",
    "  will-change: transform, opacity;",
    "}",

    ".evo-node.is-channel {",
    "  border-color: rgba(90, 84, 189, 0.25);",
    "  color: rgba(123, 117, 212, 0.8);",
    "}",
    ".evo-node.is-metric {",
    "  border-color: rgba(107, 179, 205, 0.2);",
    "  color: rgba(107, 179, 205, 0.8);",
    "  font-variant-numeric: tabular-nums;",
    "}",
    ".evo-node.is-location {",
    "  border-color: rgba(34, 197, 94, 0.15);",
    "  color: rgba(34, 197, 94, 0.7);",
    "}",

    "/* Plan phase: organized dashboard */",
    ".evo-node.in-plan {",
    "  background: rgba(17, 17, 34, 0.9);",
    "  border-color: rgba(90, 84, 189, 0.3);",
    "}",

    "/* Connection lines SVG */",
    ".evo-lines {",
    "  position: absolute;",
    "  inset: 0;",
    "  width: 100%;",
    "  height: 100%;",
    "}",
    ".evo-line {",
    "  stroke: rgba(90, 84, 189, 0.15);",
    "  stroke-width: 1;",
    "  fill: none;",
    "}",
    ".evo-line.active {",
    "  stroke: rgba(107, 179, 205, 0.3);",
    "  stroke-width: 1.5;",
    "}",

    "/* Center pulse (Nova processing) */",
    ".evo-center {",
    "  position: absolute;",
    "  left: 50%;",
    "  top: 50%;",
    "  transform: translate(-50%, -50%);",
    "  width: 60px;",
    "  height: 60px;",
    "  border-radius: 50%;",
    "  background: radial-gradient(circle, rgba(90,84,189,0.3) 0%, transparent 70%);",
    "  opacity: 0;",
    "}",

    "/* Plan dashboard frame */",
    ".evo-dashboard {",
    "  position: absolute;",
    "  left: 50%;",
    "  top: 50%;",
    "  transform: translate(-50%, -50%);",
    "  width: 320px;",
    "  height: 200px;",
    "  border: 1px solid rgba(90, 84, 189, 0.2);",
    "  border-radius: 12px;",
    "  background: rgba(8, 8, 15, 0.8);",
    "  opacity: 0;",
    "  backdrop-filter: blur(8px);",
    "}",
    ".evo-dash-bar {",
    "  position: absolute;",
    "  height: 4px;",
    "  border-radius: 2px;",
    "  left: 20px;",
    "  background: rgba(90, 84, 189, 0.5);",
    "  width: 0;",
    "}",

    "@media (max-width: 768px) {",
    "  .hero-evolution { opacity: 0.3; }",
    "  .evo-dashboard { width: 200px; height: 140px; }",
    "}",
  ].join("\n");
  document.head.appendChild(style);

  /* ── Data nodes ── */
  var nodeData = [
    { text: "Indeed", type: "channel" },
    { text: "LinkedIn", type: "channel" },
    { text: "ZipRecruiter", type: "channel" },
    { text: "Glassdoor", type: "channel" },
    { text: "$2.41 CPC", type: "metric" },
    { text: "$24 CPA", type: "metric" },
    { text: "847 apps", type: "metric" },
    { text: "$50,000", type: "metric" },
    { text: "Houston, TX", type: "location" },
    { text: "RN", type: "location" },
    { text: "91+ boards", type: "metric" },
    { text: "30s", type: "metric" },
  ];

  /* ── Create SVG for connection lines ── */
  var svgNS = "http://www.w3.org/2000/svg";
  var svg = document.createElementNS(svgNS, "svg");
  svg.setAttribute("class", "evo-lines");
  container.appendChild(svg);

  /* ── Create center pulse ── */
  var centerEl = document.createElement("div");
  centerEl.className = "evo-center";
  container.appendChild(centerEl);

  /* ── Create dashboard frame ── */
  var dashEl = document.createElement("div");
  dashEl.className = "evo-dashboard";
  /* Add bars inside dashboard */
  var barWidths = [72, 55, 38, 25];
  var barColors = ["#5A54BD", "#06b6d4", "#22c55e", "#f59e0b"];
  for (var b = 0; b < barWidths.length; b++) {
    var bar = document.createElement("div");
    bar.className = "evo-dash-bar";
    bar.style.top = 60 + b * 28 + "px";
    bar.dataset.targetWidth = barWidths[b];
    bar.style.background = barColors[b];
    dashEl.appendChild(bar);
  }
  container.appendChild(dashEl);

  /* ── Create node elements ── */
  var nodes = [];
  var w, h;

  function updateDimensions() {
    w = container.offsetWidth || window.innerWidth;
    h = container.offsetHeight || window.innerHeight;
  }
  updateDimensions();

  for (var i = 0; i < nodeData.length; i++) {
    var el = document.createElement("div");
    el.className = "evo-node is-" + nodeData[i].type;
    el.textContent = nodeData[i].text;
    container.appendChild(el);

    /* Random starting position (scattered) */
    var angle = (i / nodeData.length) * Math.PI * 2;
    var radius = 0.25 + Math.random() * 0.2;
    var startX = 0.5 + Math.cos(angle) * radius;
    var startY = 0.5 + Math.sin(angle) * radius;

    nodes.push({
      el: el,
      data: nodeData[i],
      /* Chaos positions (% of container) */
      chaosX: startX,
      chaosY: startY,
      chaosVx: (Math.random() - 0.5) * 0.0003,
      chaosVy: (Math.random() - 0.5) * 0.0003,
      /* Current render position */
      x: startX,
      y: startY,
      opacity: 0,
      /* Plan positions (organized grid) */
      planX: 0,
      planY: 0,
    });
  }

  /* Assign plan positions (organized layout around dashboard) */
  var planPositions = [
    /* Channels on left */
    { x: 0.18, y: 0.35 },
    { x: 0.18, y: 0.45 },
    { x: 0.18, y: 0.55 },
    { x: 0.18, y: 0.65 },
    /* Metrics on right */
    { x: 0.82, y: 0.35 },
    { x: 0.82, y: 0.45 },
    { x: 0.82, y: 0.55 },
    { x: 0.82, y: 0.65 },
    /* Location/role at top */
    { x: 0.42, y: 0.25 },
    { x: 0.58, y: 0.25 },
    /* Stats at bottom */
    { x: 0.42, y: 0.75 },
    { x: 0.58, y: 0.75 },
  ];

  for (var j = 0; j < nodes.length; j++) {
    nodes[j].planX = planPositions[j].x;
    nodes[j].planY = planPositions[j].y;
  }

  /* ── Connection lines data ── */
  var lines = [];
  var maxLines = 15;
  for (var li = 0; li < maxLines; li++) {
    var line = document.createElementNS(svgNS, "line");
    line.setAttribute("class", "evo-line");
    svg.appendChild(line);
    lines.push({ el: line, opacity: 0 });
  }

  /* ── Animation state ── */
  var PHASE_CHAOS = 0;
  var PHASE_CONVERGE = 1;
  var PHASE_PLAN = 2;
  var PHASE_HOLD = 3;

  var phase = PHASE_CHAOS;
  var phaseTime = 0;
  var totalTime = 0;

  /* Phase durations (seconds) */
  var CHAOS_DURATION = 4;
  var CONVERGE_DURATION = 2.5;
  var PLAN_DURATION = 1.5;
  var HOLD_DURATION = 3;
  var CYCLE_DURATION =
    CHAOS_DURATION + CONVERGE_DURATION + PLAN_DURATION + HOLD_DURATION;

  /* ── Easing ── */
  function easeOutExpo(t) {
    return t >= 1 ? 1 : 1 - Math.pow(2, -10 * t);
  }

  function easeInOutCubic(t) {
    return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
  }

  /* ── lerp ── */
  function lerp(a, b, t) {
    return a + (b - a) * t;
  }

  /* ── Render loop ── */
  var lastTime = 0;

  function animate(timestamp) {
    if (heroRunning) requestAnimationFrame(animate);

    if (!lastTime) lastTime = timestamp;
    var dt = (timestamp - lastTime) / 1000;
    lastTime = timestamp;

    /* Cap delta to prevent jumps after tab switch */
    if (dt > 0.1) dt = 0.016;

    totalTime += dt;
    phaseTime += dt;

    /* Phase transitions */
    if (phase === PHASE_CHAOS && phaseTime >= CHAOS_DURATION) {
      phase = PHASE_CONVERGE;
      phaseTime = 0;
    } else if (phase === PHASE_CONVERGE && phaseTime >= CONVERGE_DURATION) {
      phase = PHASE_PLAN;
      phaseTime = 0;
    } else if (phase === PHASE_PLAN && phaseTime >= PLAN_DURATION) {
      phase = PHASE_HOLD;
      phaseTime = 0;
    } else if (phase === PHASE_HOLD && phaseTime >= HOLD_DURATION) {
      phase = PHASE_CHAOS;
      phaseTime = 0;
      /* Reset chaos positions */
      for (var r = 0; r < nodes.length; r++) {
        var angle2 = (r / nodes.length) * Math.PI * 2 + totalTime * 0.1;
        var rad2 = 0.25 + Math.random() * 0.2;
        nodes[r].chaosX = 0.5 + Math.cos(angle2) * rad2;
        nodes[r].chaosY = 0.5 + Math.sin(angle2) * rad2;
        nodes[r].chaosVx = (Math.random() - 0.5) * 0.0003;
        nodes[r].chaosVy = (Math.random() - 0.5) * 0.0003;
      }
    }

    updateDimensions();
    var centerX = w * 0.5;
    var centerY = h * 0.5;

    /* ── Update nodes ── */
    for (var n = 0; n < nodes.length; n++) {
      var node = nodes[n];
      var targetX, targetY, targetOpacity;

      if (phase === PHASE_CHAOS) {
        /* Float randomly */
        node.chaosX += node.chaosVx;
        node.chaosY += node.chaosVy;
        /* Gentle boundary bounce */
        if (node.chaosX < 0.1 || node.chaosX > 0.9) node.chaosVx *= -1;
        if (node.chaosY < 0.2 || node.chaosY > 0.8) node.chaosVy *= -1;
        /* Add wave motion */
        var wave = Math.sin(totalTime * 0.5 + n * 0.8) * 0.01;
        targetX = node.chaosX + wave;
        targetY = node.chaosY + Math.cos(totalTime * 0.3 + n * 1.2) * 0.008;
        targetOpacity = 0.4 + Math.sin(totalTime + n) * 0.15;

        /* Fade in during first second */
        if (phaseTime < 1) targetOpacity *= phaseTime;
      } else if (phase === PHASE_CONVERGE) {
        /* Move toward center */
        var t = easeInOutCubic(Math.min(phaseTime / CONVERGE_DURATION, 1));
        targetX = lerp(node.chaosX, 0.5, t * 0.6);
        targetY = lerp(node.chaosY, 0.5, t * 0.6);
        targetOpacity = 0.6 + t * 0.3;
      } else if (phase === PHASE_PLAN) {
        /* Snap to organized positions */
        var t2 = easeOutExpo(Math.min(phaseTime / PLAN_DURATION, 1));
        targetX = lerp(0.5, node.planX, t2);
        targetY = lerp(0.5, node.planY, t2);
        targetOpacity = 0.7 + t2 * 0.3;
        if (t2 > 0.5) node.el.classList.add("in-plan");
      } else {
        /* Hold in plan position */
        targetX = node.planX;
        targetY = node.planY;
        /* Fade out in last second of hold */
        targetOpacity =
          phaseTime > HOLD_DURATION - 1
            ? 1 - (phaseTime - (HOLD_DURATION - 1))
            : 1;
        if (phaseTime > HOLD_DURATION - 1.5)
          node.el.classList.remove("in-plan");
      }

      /* Smooth interpolation */
      node.x += (targetX - node.x) * 0.08;
      node.y += (targetY - node.y) * 0.08;
      node.opacity += (targetOpacity - node.opacity) * 0.1;

      /* Apply to DOM */
      var px = node.x * w;
      var py = node.y * h;
      node.el.style.transform =
        "translate3d(" + (px - 40) + "px, " + (py - 12) + "px, 0)";
      node.el.style.opacity = Math.max(0, Math.min(1, node.opacity));
    }

    /* ── Update connection lines ── */
    var lineIdx = 0;
    var showLines = phase === PHASE_CONVERGE || phase === PHASE_PLAN;

    for (var li2 = 0; li2 < lines.length; li2++) {
      if (showLines && li2 < nodes.length - 1) {
        var fromNode = nodes[li2];
        var toIdx = (li2 + 1) % nodes.length;
        /* Connect some nodes to center during converge */
        var fx = fromNode.x * w;
        var fy = fromNode.y * h;
        var tx, ty;

        if (phase === PHASE_CONVERGE) {
          tx = centerX;
          ty = centerY;
        } else {
          tx = nodes[toIdx].x * w;
          ty = nodes[toIdx].y * h;
        }

        lines[li2].el.setAttribute("x1", fx);
        lines[li2].el.setAttribute("y1", fy);
        lines[li2].el.setAttribute("x2", tx);
        lines[li2].el.setAttribute("y2", ty);

        var lineOpacity =
          phase === PHASE_CONVERGE
            ? Math.min(phaseTime / 1, 0.4)
            : phase === PHASE_PLAN
              ? 0.3 * (1 - phaseTime / PLAN_DURATION)
              : 0;
        lines[li2].el.style.opacity = lineOpacity;
        lines[li2].el.classList.toggle("active", phase === PHASE_CONVERGE);
      } else {
        lines[li2].el.style.opacity = 0;
      }
    }

    /* ── Center pulse ── */
    if (phase === PHASE_CONVERGE) {
      var pulseT = Math.min(phaseTime / CONVERGE_DURATION, 1);
      centerEl.style.opacity = pulseT * 0.8;
      var pulseScale = 1 + Math.sin(phaseTime * 4) * 0.2;
      centerEl.style.transform =
        "translate(-50%, -50%) scale(" + pulseScale + ")";
    } else {
      centerEl.style.opacity = Math.max(
        0,
        parseFloat(centerEl.style.opacity || 0) - dt * 2,
      );
    }

    /* ── Dashboard frame ── */
    if (phase === PHASE_PLAN || phase === PHASE_HOLD) {
      var dashT = easeOutExpo(Math.min(phaseTime / 0.8, 1));
      if (phase === PHASE_PLAN) {
        dashEl.style.opacity = dashT * 0.9;
        dashEl.style.transform =
          "translate(-50%, -50%) scale(" + lerp(0.9, 1, dashT) + ")";
        /* Animate bars */
        var bars = dashEl.querySelectorAll(".evo-dash-bar");
        for (var bi = 0; bi < bars.length; bi++) {
          var barTarget = parseInt(bars[bi].dataset.targetWidth);
          var barDelay = bi * 0.2;
          var barProgress = Math.max(
            0,
            Math.min(1, (phaseTime - barDelay) / 0.8),
          );
          bars[bi].style.width =
            easeOutExpo(barProgress) * barTarget * 0.8 + "%";
        }
      } else if (phase === PHASE_HOLD) {
        /* Fade out during hold end */
        if (phaseTime > HOLD_DURATION - 1) {
          var fadeOut = 1 - (phaseTime - (HOLD_DURATION - 1));
          dashEl.style.opacity = fadeOut * 0.9;
        }
      }
    } else {
      dashEl.style.opacity = 0;
      /* Reset bars */
      var bars2 = dashEl.querySelectorAll(".evo-dash-bar");
      for (var bi2 = 0; bi2 < bars2.length; bi2++) {
        bars2[bi2].style.width = "0";
      }
    }
  }

  /* ── Visibility-gated animation (stop rAF when off-screen) ── */
  var heroRunning = false;
  var heroObserver = new IntersectionObserver(
    function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting && !heroRunning) {
          heroRunning = true;
          lastTime = 0;
          requestAnimationFrame(animate);
        } else if (!entry.isIntersecting) {
          heroRunning = false;
        }
      });
    },
    { threshold: 0.05 },
  );
  heroObserver.observe(hero);

  /* Override animate to check heroRunning */
  var origAnimate = animate;
  animate = function (timestamp) {
    if (!heroRunning) return;
    origAnimate(timestamp);
  };

  /* ── Resize handler ── */
  window.addEventListener("resize", updateDimensions, { passive: true });
})();
