/**
 * Nova Ripple Effect v1.0
 *
 * Material-style ripple with spring easing on all interactive elements.
 * Works on touch devices via pointer events.
 * Respects prefers-reduced-motion.
 *
 * Load as: <script src="/static/js/ripple.js" defer></script>
 */
(function () {
  "use strict";

  // Respect reduced motion
  var reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  if (reducedMotion.matches) return;

  // Re-check if media query changes during session
  reducedMotion.addEventListener("change", function (e) {
    if (e.matches) {
      document.removeEventListener("pointerdown", handleRipple, true);
    } else {
      document.addEventListener("pointerdown", handleRipple, true);
    }
  });

  /**
   * Selectors that should receive ripple effects.
   * Broad enough to catch all interactive elements, narrow enough
   * to skip things like text nodes inside buttons.
   */
  var RIPPLE_SELECTOR = [
    "button",
    ".btn",
    '[role="button"]',
    "a.cta-btn",
    "a.btn-primary",
    "a.btn-ghost",
    ".suggestion-chip",
    ".topbar-btn",
    ".new-chat-btn",
    ".sidebar-toggle",
    ".send-btn",
    ".attach-btn",
    ".voice-btn",
    ".stop-btn",
    ".demo-btn",
    ".predict-btn",
    ".export-btn",
    ".tab-btn",
    ".nova-suggestion-btn",
    ".nova-send-btn",
    ".nova-close-btn",
    ".nova-theme-btn",
    ".nova-regen-btn",
  ].join(", ");

  function handleRipple(e) {
    var target = e.target.closest(RIPPLE_SELECTOR);
    if (!target) return;
    if (target.disabled || target.getAttribute("aria-disabled") === "true")
      return;

    // Ensure parent is positioned for absolute ripple
    var pos = getComputedStyle(target).position;
    if (pos === "static") {
      target.style.position = "relative";
    }
    // Ensure overflow hidden
    if (getComputedStyle(target).overflow !== "hidden") {
      target.style.overflow = "hidden";
    }

    var rect = target.getBoundingClientRect();

    // Calculate ripple position relative to the target
    var x = e.clientX - rect.left;
    var y = e.clientY - rect.top;

    // For keyboard or synthetic events, center the ripple
    if (x === 0 && y === 0) {
      x = rect.width / 2;
      y = rect.height / 2;
    }

    // Ripple diameter should cover the entire button from the click point
    var maxDim = Math.max(rect.width, rect.height);
    var size = maxDim * 2;

    // Create ripple element
    var ripple = document.createElement("span");
    ripple.className = "nova-ripple";
    ripple.style.width = size + "px";
    ripple.style.height = size + "px";
    ripple.style.left = x - size / 2 + "px";
    ripple.style.top = y - size / 2 + "px";

    target.appendChild(ripple);

    // Force reflow to ensure animation starts
    ripple.offsetHeight;

    // Start animation
    ripple.classList.add("animate");

    // Cleanup after animation
    ripple.addEventListener("animationend", function () {
      if (ripple.parentNode) {
        ripple.parentNode.removeChild(ripple);
      }
    });

    // Fallback cleanup in case animationend doesn't fire
    setTimeout(function () {
      if (ripple.parentNode) {
        ripple.parentNode.removeChild(ripple);
      }
    }, 700);
  }

  document.addEventListener("pointerdown", handleRipple, true);
})();
