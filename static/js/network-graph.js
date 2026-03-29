/**
 * network-graph.js — Orbital network graph for Moment 3 ("Nova analyzes everything")
 *
 * 12 readable channel nodes orbiting a central Nova AI hub with pill-shaped labels.
 * Sized for readability on 14" laptops. Two orbits only.
 * Pure SVG + requestAnimationFrame. No dependencies.
 * Respects prefers-reduced-motion.
 */
(function () {
  "use strict";

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  var moment3 = document.querySelector(".moment-3 .moment-visual");
  if (!moment3) return;

  /* Remove the static SVG */
  var oldSvg = moment3.querySelector(".intelligence-viz");
  if (oldSvg) oldSvg.style.display = "none";

  /* ── Channel data (12 nodes, 2 orbits — readable, not cluttered) ── */
  var channels = [
    /* Inner orbit (6 major channels) */
    { name: "Indeed", orbit: 1, color: "#7B75D4" },
    { name: "LinkedIn", orbit: 1, color: "#06b6d4" },
    { name: "Google Ads", orbit: 1, color: "#22c55e" },
    { name: "Facebook", orbit: 1, color: "#60a5fa" },
    { name: "ZipRecruiter", orbit: 1, color: "#a78bfa" },
    { name: "Glassdoor", orbit: 1, color: "#f59e0b" },
    /* Outer orbit (6 secondary channels) */
    { name: "Programmatic", orbit: 2, color: "#6BB3CD" },
    { name: "TikTok", orbit: 2, color: "#ef4444" },
    { name: "Career Sites", orbit: 2, color: "#c084fc" },
    { name: "Niche Boards", orbit: 2, color: "#34d399" },
    { name: "Social", orbit: 2, color: "#f472b6" },
    { name: "Job Aggregators", orbit: 2, color: "#fb923c" },
  ];

  var orbitRadii = { 1: 120, 2: 195 };
  var orbitSpeeds = { 1: 0.0003, 2: -0.0002 };
  var CX = 300,
    CY = 210;

  /* ── Create SVG ── */
  var svgNS = "http://www.w3.org/2000/svg";
  var svg = document.createElementNS(svgNS, "svg");
  svg.setAttribute("viewBox", "0 0 600 420");
  svg.setAttribute("class", "network-graph");
  svg.setAttribute("aria-hidden", "true");
  svg.style.cssText = "width:100%;max-width:600px;overflow:visible;";

  /* ── Orbit rings (subtle dashed) ── */
  [1, 2].forEach(function (o) {
    var ring = document.createElementNS(svgNS, "ellipse");
    ring.setAttribute("cx", CX);
    ring.setAttribute("cy", CY);
    ring.setAttribute("rx", orbitRadii[o]);
    ring.setAttribute("ry", orbitRadii[o] * 0.6);
    ring.setAttribute("fill", "none");
    ring.setAttribute("stroke", "rgba(90, 84, 189, 0.1)");
    ring.setAttribute("stroke-width", "1");
    ring.setAttribute("stroke-dasharray", "6 8");
    svg.appendChild(ring);
  });

  /* ── Connection lines group ── */
  var linesGroup = document.createElementNS(svgNS, "g");
  svg.appendChild(linesGroup);

  /* ── Center hub ── */
  var hubGlow = document.createElementNS(svgNS, "circle");
  hubGlow.setAttribute("cx", CX);
  hubGlow.setAttribute("cy", CY);
  hubGlow.setAttribute("r", "52");
  hubGlow.setAttribute("fill", "rgba(90, 84, 189, 0.12)");
  svg.appendChild(hubGlow);

  var hub = document.createElementNS(svgNS, "circle");
  hub.setAttribute("cx", CX);
  hub.setAttribute("cy", CY);
  hub.setAttribute("r", "42");
  hub.setAttribute("fill", "rgba(20, 18, 50, 0.9)");
  hub.setAttribute("stroke", "#5A54BD");
  hub.setAttribute("stroke-width", "2");
  svg.appendChild(hub);

  var hubLabel = document.createElementNS(svgNS, "text");
  hubLabel.setAttribute("x", CX);
  hubLabel.setAttribute("y", CY - 2);
  hubLabel.setAttribute("text-anchor", "middle");
  hubLabel.setAttribute("fill", "#b4b0f0");
  hubLabel.setAttribute("font-size", "16");
  hubLabel.setAttribute("font-weight", "700");
  hubLabel.setAttribute("font-family", "Inter, system-ui, sans-serif");
  hubLabel.textContent = "Nova AI";
  svg.appendChild(hubLabel);

  var hubSub = document.createElementNS(svgNS, "text");
  hubSub.setAttribute("x", CX);
  hubSub.setAttribute("y", CY + 16);
  hubSub.setAttribute("text-anchor", "middle");
  hubSub.setAttribute("fill", "rgba(107, 179, 205, 0.7)");
  hubSub.setAttribute("font-size", "10");
  hubSub.setAttribute("font-family", "Inter, system-ui, sans-serif");
  hubSub.textContent = "91+ sources";
  svg.appendChild(hubSub);

  /* ── Create channel nodes as pill-shaped labels ── */
  var nodeEls = [];
  var orbitGroups = { 1: [], 2: [] };

  channels.forEach(function (ch, i) {
    orbitGroups[ch.orbit].push(i);
  });

  channels.forEach(function (ch, i) {
    var group = orbitGroups[ch.orbit];
    var posInGroup = group.indexOf(i);
    var angleOffset = (posInGroup / group.length) * Math.PI * 2;

    /* Connection line */
    var line = document.createElementNS(svgNS, "line");
    line.setAttribute("stroke", ch.color);
    line.setAttribute("stroke-width", "1");
    line.setAttribute("opacity", "0.15");
    linesGroup.appendChild(line);

    /* Node group */
    var g = document.createElementNS(svgNS, "g");

    /* Pill background */
    var textLen = ch.name.length * 6.5 + 20;
    var pill = document.createElementNS(svgNS, "rect");
    pill.setAttribute("x", -textLen / 2);
    pill.setAttribute("y", -12);
    pill.setAttribute("width", textLen);
    pill.setAttribute("height", 24);
    pill.setAttribute("rx", 12);
    pill.setAttribute("fill", "rgba(10, 10, 20, 0.85)");
    pill.setAttribute("stroke", ch.color);
    pill.setAttribute("stroke-width", "1.2");
    pill.setAttribute("stroke-opacity", "0.6");
    g.appendChild(pill);

    /* Label text */
    var label = document.createElementNS(svgNS, "text");
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("dy", "0.35em");
    label.setAttribute("fill", ch.color);
    label.setAttribute("font-size", "12");
    label.setAttribute("font-family", "Inter, system-ui, sans-serif");
    label.setAttribute("font-weight", "500");
    label.textContent = ch.name;
    g.appendChild(label);

    svg.appendChild(g);

    nodeEls.push({
      g: g,
      line: line,
      angle: angleOffset,
      orbit: ch.orbit,
      data: ch,
    });
  });

  moment3.appendChild(svg);

  /* ── Animation ── */
  var running = false;
  var lastTime = 0;

  function animate(timestamp) {
    if (!running) return;
    requestAnimationFrame(animate);

    if (!lastTime) lastTime = timestamp;
    var dt = timestamp - lastTime;
    lastTime = timestamp;
    if (dt > 100) dt = 16;

    nodeEls.forEach(function (node) {
      var speed = orbitSpeeds[node.orbit];
      node.angle += speed * dt;

      var r = orbitRadii[node.orbit];
      var wobble = Math.sin(timestamp * 0.0008 + node.angle * 2) * 5;
      var x = CX + Math.cos(node.angle) * (r + wobble);
      var y = CY + Math.sin(node.angle) * (r * 0.6 + wobble * 0.4);

      node.g.setAttribute("transform", "translate(" + x + "," + y + ")");

      node.line.setAttribute("x1", CX);
      node.line.setAttribute("y1", CY);
      node.line.setAttribute("x2", x);
      node.line.setAttribute("y2", y);

      /* Subtle opacity variation */
      var op = 0.1 + 0.1 * Math.sin(timestamp * 0.001 + node.angle);
      node.line.setAttribute("opacity", op);
    });

    /* Hub pulse */
    var pulse = 52 + Math.sin(timestamp * 0.002) * 3;
    hubGlow.setAttribute("r", pulse);
  }

  /* Start/stop based on visibility */
  var observer = new IntersectionObserver(
    function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting && !running) {
          running = true;
          lastTime = 0;
          requestAnimationFrame(animate);
        } else if (!entry.isIntersecting) {
          running = false;
        }
      });
    },
    { threshold: 0.1 },
  );

  observer.observe(moment3);
})();
