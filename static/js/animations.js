/**
 * animations.js — Shared GSAP animation module for Nova AI Suite
 * Loaded via CDN (gsap + ScrollTrigger), NOT npm import.
 * Requires: gsap.min.js and ScrollTrigger.min.js loaded before this script.
 */

(function () {
  "use strict";

  // Bail early if GSAP not loaded
  if (typeof gsap === "undefined") {
    console.warn("[Nova] GSAP not loaded — skipping animations");
    return;
  }

  // Register ScrollTrigger plugin
  if (typeof ScrollTrigger !== "undefined") {
    gsap.registerPlugin(ScrollTrigger);
  }

  // Check reduced motion preference
  const prefersReducedMotion = window.matchMedia(
    "(prefers-reduced-motion: reduce)",
  ).matches;

  /**
   * Initialize all scroll-triggered reveal animations.
   * Add class="reveal" to any element that should fade-in on scroll.
   */
  function initRevealAnimations() {
    if (prefersReducedMotion) return;
    if (typeof ScrollTrigger === "undefined") return;

    gsap.utils.toArray(".reveal").forEach(function (el) {
      gsap.from(el, {
        opacity: 0,
        y: 24,
        duration: 0.6,
        ease: "power2.out",
        scrollTrigger: {
          trigger: el,
          start: "top 88%",
          once: true,
        },
      });
    });
  }

  /**
   * Initialize counter animations.
   * Usage: <span data-counter data-target="5400" data-prefix="" data-suffix="+">0</span>
   */
  function initCounterAnimations() {
    if (prefersReducedMotion) {
      // Show final values immediately
      gsap.utils.toArray("[data-counter]").forEach(function (el) {
        var target = parseFloat(el.dataset.target) || 0;
        var prefix = el.dataset.prefix || "";
        var suffix = el.dataset.suffix || "";
        el.textContent = prefix + Math.round(target) + suffix;
      });
      return;
    }
    if (typeof ScrollTrigger === "undefined") return;

    gsap.utils.toArray("[data-counter]").forEach(function (el) {
      var target = parseFloat(el.dataset.target) || 0;
      var prefix = el.dataset.prefix || "";
      var suffix = el.dataset.suffix || "";

      ScrollTrigger.create({
        trigger: el,
        start: "top 88%",
        once: true,
        onEnter: function () {
          var obj = { val: 0 };
          gsap.to(obj, {
            val: target,
            duration: 1.2,
            ease: "power2.out",
            onUpdate: function () {
              el.textContent = prefix + Math.round(obj.val) + suffix;
            },
          });
        },
      });
    });
  }

  /**
   * Initialize stagger animations for groups.
   * Usage: <div class="stagger-group"> <div class="stagger-item">...</div> ... </div>
   */
  function initStaggerAnimations() {
    if (prefersReducedMotion) return;
    if (typeof ScrollTrigger === "undefined") return;

    gsap.utils.toArray(".stagger-group").forEach(function (group) {
      var items = group.querySelectorAll(".stagger-item");
      if (!items.length) return;

      gsap.from(items, {
        opacity: 0,
        y: 20,
        duration: 0.4,
        stagger: 0.08,
        ease: "power2.out",
        scrollTrigger: {
          trigger: group,
          start: "top 88%",
          once: true,
        },
      });
    });
  }

  /**
   * Master init — call on DOMContentLoaded
   */
  function initAnimations() {
    initRevealAnimations();
    initCounterAnimations();
    initStaggerAnimations();
  }

  // Auto-init when DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAnimations);
  } else {
    initAnimations();
  }

  // Expose for manual calls from other scripts
  window.NovaAnimations = {
    init: initAnimations,
    initReveals: initRevealAnimations,
    initCounters: initCounterAnimations,
    initStaggers: initStaggerAnimations,
    prefersReducedMotion: prefersReducedMotion,
  };
})();
