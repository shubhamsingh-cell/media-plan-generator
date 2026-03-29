/**
 * custom-cursor.js — Premium custom cursor for Nova AI Suite
 * Dot + ring cursor with magnetic hover effects on interactive elements.
 * Hides on touch devices. Respects prefers-reduced-motion.
 */
(function () {
  "use strict";

  /* ── Skip on touch / reduced motion ── */
  if ("ontouchstart" in window || navigator.maxTouchPoints > 0) return;
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  if (window.innerWidth < 768) return;

  /* ── Create cursor elements ── */
  var dot = document.createElement("div");
  dot.className = "nova-cursor-dot";
  var ring = document.createElement("div");
  ring.className = "nova-cursor-ring";

  document.body.appendChild(dot);
  document.body.appendChild(ring);

  /* ── Inject styles ── */
  var style = document.createElement("style");
  style.textContent = [
    "* { cursor: none !important; }",

    ".nova-cursor-dot {",
    "  position: fixed;",
    "  top: 0; left: 0;",
    "  width: 8px; height: 8px;",
    "  background: #fff;",
    "  border-radius: 50%;",
    "  pointer-events: none;",
    "  z-index: 99999;",
    "  mix-blend-mode: difference;",
    "  transform: translate(-50%, -50%);",
    "  transition: width 0.2s, height 0.2s, background 0.2s;",
    "  will-change: transform;",
    "}",

    ".nova-cursor-ring {",
    "  position: fixed;",
    "  top: 0; left: 0;",
    "  width: 40px; height: 40px;",
    "  border: 1.5px solid rgba(255, 255, 255, 0.4);",
    "  border-radius: 50%;",
    "  pointer-events: none;",
    "  z-index: 99998;",
    "  mix-blend-mode: difference;",
    "  transform: translate(-50%, -50%);",
    "  transition: width 0.25s cubic-bezier(0.16,1,0.3,1), height 0.25s cubic-bezier(0.16,1,0.3,1), border-color 0.25s, opacity 0.25s;",
    "  will-change: transform;",
    "}",

    "/* Hover states */",
    ".nova-cursor-dot.is-hovering {",
    "  width: 12px; height: 12px;",
    "  background: var(--accent, #5A54BD);",
    "}",
    ".nova-cursor-ring.is-hovering {",
    "  width: 56px; height: 56px;",
    "  border-color: rgba(90, 84, 189, 0.6);",
    "}",

    "/* Click state */",
    ".nova-cursor-dot.is-clicking {",
    "  width: 6px; height: 6px;",
    "}",
    ".nova-cursor-ring.is-clicking {",
    "  width: 32px; height: 32px;",
    "  border-color: rgba(107, 179, 205, 0.8);",
    "}",

    "/* Text cursor */",
    ".nova-cursor-dot.is-text {",
    "  width: 3px; height: 20px;",
    "  border-radius: 1px;",
    "}",
    ".nova-cursor-ring.is-text {",
    "  width: 28px; height: 28px;",
    "  opacity: 0.3;",
    "}",

    "/* CTA magnetic */",
    ".nova-cursor-dot.is-magnetic {",
    "  width: 14px; height: 14px;",
    "  background: var(--accent, #5A54BD);",
    "}",
    ".nova-cursor-ring.is-magnetic {",
    "  width: 64px; height: 64px;",
    "  border-color: rgba(90, 84, 189, 0.8);",
    "  border-width: 2px;",
    "}",

    "/* Hidden when off-screen */",
    ".nova-cursor-dot.is-hidden, .nova-cursor-ring.is-hidden {",
    "  opacity: 0;",
    "}",

    "/* Mobile: revert to normal cursor */",
    "@media (max-width: 767px) {",
    "  * { cursor: auto !important; }",
    "  .nova-cursor-dot, .nova-cursor-ring { display: none !important; }",
    "}",
  ].join("\n");
  document.head.appendChild(style);

  /* ── State ── */
  var mouse = { x: -100, y: -100 };
  var dotPos = { x: -100, y: -100 };
  var ringPos = { x: -100, y: -100 };
  var currentMagnetic = null;

  /* ── Mouse tracking ── */
  document.addEventListener(
    "mousemove",
    function (e) {
      mouse.x = e.clientX;
      mouse.y = e.clientY;

      dot.classList.remove("is-hidden");
      ring.classList.remove("is-hidden");
    },
    { passive: true },
  );

  document.addEventListener("mouseleave", function () {
    dot.classList.add("is-hidden");
    ring.classList.add("is-hidden");
  });

  /* ── Click feedback ── */
  document.addEventListener("mousedown", function () {
    dot.classList.add("is-clicking");
    ring.classList.add("is-clicking");
  });

  document.addEventListener("mouseup", function () {
    dot.classList.remove("is-clicking");
    ring.classList.remove("is-clicking");
  });

  /* ── Hover detection ── */
  var interactiveSelector =
    'a, button, [role="button"], input, select, textarea, .product-hero-card, .demo-btn, .nav-cta, .cta-btn, .btn-primary, .btn-ghost, .hero-secondary-link';
  var textSelector = "input, textarea, [contenteditable]";
  var magneticSelector = ".cta-btn, .btn-primary.btn-lg, .nav-cta, .demo-btn";

  document.addEventListener(
    "mouseover",
    function (e) {
      var target = e.target;

      /* Check magnetic first */
      var magneticEl = target.closest(magneticSelector);
      if (magneticEl) {
        dot.classList.add("is-magnetic");
        ring.classList.add("is-magnetic");
        dot.classList.remove("is-hovering", "is-text");
        ring.classList.remove("is-hovering", "is-text");
        currentMagnetic = magneticEl;
        return;
      }

      /* Check text input */
      var textEl = target.closest(textSelector);
      if (textEl) {
        dot.classList.add("is-text");
        ring.classList.add("is-text");
        dot.classList.remove("is-hovering", "is-magnetic");
        ring.classList.remove("is-hovering", "is-magnetic");
        currentMagnetic = null;
        return;
      }

      /* Check interactive */
      var interactiveEl = target.closest(interactiveSelector);
      if (interactiveEl) {
        dot.classList.add("is-hovering");
        ring.classList.add("is-hovering");
        dot.classList.remove("is-text", "is-magnetic");
        ring.classList.remove("is-text", "is-magnetic");
        currentMagnetic = null;
        return;
      }

      /* Default */
      dot.classList.remove("is-hovering", "is-text", "is-magnetic");
      ring.classList.remove("is-hovering", "is-text", "is-magnetic");
      currentMagnetic = null;
    },
    { passive: true },
  );

  /* ── Animation loop ── */
  function updateCursor() {
    var targetX = mouse.x;
    var targetY = mouse.y;

    /* Magnetic pull toward CTA center */
    if (currentMagnetic) {
      var rect = currentMagnetic.getBoundingClientRect();
      var centerX = rect.left + rect.width / 2;
      var centerY = rect.top + rect.height / 2;
      var dx = mouse.x - centerX;
      var dy = mouse.y - centerY;
      var dist = Math.sqrt(dx * dx + dy * dy);
      var maxDist = Math.max(rect.width, rect.height);

      if (dist < maxDist) {
        var pull = 0.35;
        targetX = mouse.x - dx * pull;
        targetY = mouse.y - dy * pull;

        /* Slight element shift toward cursor */
        var shiftX = dx * 0.1;
        var shiftY = dy * 0.1;
        currentMagnetic.style.transform =
          "translate(" + shiftX + "px, " + shiftY + "px)";
      }
    }

    /* Dot follows instantly */
    dotPos.x += (targetX - dotPos.x) * 0.9;
    dotPos.y += (targetY - dotPos.y) * 0.9;

    /* Ring follows with lag */
    ringPos.x += (targetX - ringPos.x) * 0.15;
    ringPos.y += (targetY - ringPos.y) * 0.15;

    dot.style.transform =
      "translate3d(" +
      dotPos.x +
      "px, " +
      dotPos.y +
      "px, 0) translate(-50%, -50%)";
    ring.style.transform =
      "translate3d(" +
      ringPos.x +
      "px, " +
      ringPos.y +
      "px, 0) translate(-50%, -50%)";

    requestAnimationFrame(updateCursor);
  }

  updateCursor();

  /* ── Reset magnetic element transforms ── */
  document.addEventListener(
    "mouseout",
    function (e) {
      var target = e.target;
      var magneticEl = target.closest(magneticSelector);
      if (magneticEl) {
        magneticEl.style.transform = "";
      }
    },
    { passive: true },
  );
})();
