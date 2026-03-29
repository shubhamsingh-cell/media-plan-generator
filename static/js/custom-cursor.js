/**
 * custom-cursor.js v2.0 — Subtle dot+ring cursor for Nova AI Suite
 * Simplified: no magnetic pull, just smooth follow + expand on hover.
 * Hides on touch devices. Respects prefers-reduced-motion.
 */
(function () {
  "use strict";

  if ("ontouchstart" in window || navigator.maxTouchPoints > 0) return;
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  if (window.innerWidth < 768) return;

  /* Custom cursor disabled — distracting, use native cursor */
  return;

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
    "  position: fixed; top: 0; left: 0;",
    "  width: 6px; height: 6px;",
    "  background: #fff;",
    "  border-radius: 50%;",
    "  pointer-events: none;",
    "  z-index: 99999;",
    "  mix-blend-mode: difference;",
    "  transform: translate(-50%, -50%);",
    "  transition: width 0.25s cubic-bezier(0.16,1,0.3,1), height 0.25s cubic-bezier(0.16,1,0.3,1);",
    "  will-change: transform;",
    "}",

    ".nova-cursor-ring {",
    "  position: fixed; top: 0; left: 0;",
    "  width: 40px; height: 40px;",
    "  border: 1.5px solid rgba(255, 255, 255, 0.35);",
    "  border-radius: 50%;",
    "  pointer-events: none;",
    "  z-index: 99998;",
    "  mix-blend-mode: difference;",
    "  transform: translate(-50%, -50%);",
    "  transition: width 0.3s cubic-bezier(0.16,1,0.3,1), height 0.3s cubic-bezier(0.16,1,0.3,1), opacity 0.3s;",
    "  will-change: transform;",
    "}",

    ".nova-cursor-dot.is-active { width: 10px; height: 10px; }",
    ".nova-cursor-ring.is-active { width: 56px; height: 56px; border-color: rgba(90, 84, 189, 0.5); }",

    ".nova-cursor-dot.is-hidden, .nova-cursor-ring.is-hidden { opacity: 0; }",

    "@media (max-width: 767px), (hover: none) {",
    "  * { cursor: auto !important; }",
    "  .nova-cursor-dot, .nova-cursor-ring { display: none !important; }",
    "}",
  ].join("\n");
  document.head.appendChild(style);

  /* ── State ── */
  var mouse = { x: -100, y: -100 };
  var dotPos = { x: -100, y: -100 };
  var ringPos = { x: -100, y: -100 };

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

  /* ── Hover detection (expand ring on interactive elements) ── */
  var interactiveSelector =
    "a, button, [role='button'], input, select, textarea, .product-hero-card, .demo-btn, .nav-cta, .cta-btn, .btn-primary, .btn-ghost";

  document.addEventListener(
    "mouseover",
    function (e) {
      if (e.target.closest(interactiveSelector)) {
        dot.classList.add("is-active");
        ring.classList.add("is-active");
      } else {
        dot.classList.remove("is-active");
        ring.classList.remove("is-active");
      }
    },
    { passive: true },
  );

  /* ── Animation loop ── */
  function animate() {
    dotPos.x += (mouse.x - dotPos.x) * 0.9;
    dotPos.y += (mouse.y - dotPos.y) * 0.9;
    ringPos.x += (mouse.x - ringPos.x) * 0.15;
    ringPos.y += (mouse.y - ringPos.y) * 0.15;

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

    requestAnimationFrame(animate);
  }
  animate();
})();
