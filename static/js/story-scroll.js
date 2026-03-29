/**
 * Story Scroll v2.0 — Awwwards-level scroll choreography for "How Nova Works"
 * Features: progress bar, text split reveals, parallax visuals, dramatic transitions,
 * step counter, and smooth moment-to-moment animations.
 * Requires: GSAP 3.12+ with ScrollTrigger plugin.
 * Respects prefers-reduced-motion and falls back to vertical stack on mobile.
 */
function initStoryScroll() {
  "use strict";

  var storySection = document.querySelector(".story-scroll");
  if (!storySection) return;

  var moments = document.querySelectorAll(".story-moment");
  if (!moments.length) return;

  /* ── Reduced motion: show all moments stacked vertically ── */
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
    moments.forEach(function (m) {
      m.style.position = "relative";
      m.style.opacity = "1";
      m.style.pointerEvents = "auto";
      m.style.marginBottom = "120px";
    });
    var inner = storySection.querySelector(".story-scroll-inner");
    if (inner) inner.style.height = "auto";
    storySection.style.minHeight = "auto";
    return;
  }

  /* ── Mobile: vertical stack with IntersectionObserver fade-in ── */
  if (window.innerWidth < 768) {
    var innerEl = storySection.querySelector(".story-scroll-inner");
    if (innerEl) innerEl.style.height = "auto";
    storySection.style.minHeight = "auto";

    moments.forEach(function (m) {
      m.style.position = "relative";
      m.style.opacity = "0";
      m.style.transform = "translateY(30px)";
      m.style.pointerEvents = "auto";
      m.style.marginBottom = "80px";
      m.style.transition = "opacity 0.6s ease, transform 0.6s ease";
    });

    var mobileObserver = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (e) {
          if (e.isIntersecting) {
            e.target.style.opacity = "1";
            e.target.style.transform = "translateY(0)";
          }
        });
      },
      { threshold: 0.3 },
    );

    moments.forEach(function (m) {
      mobileObserver.observe(m);
    });
    return;
  }

  /* ── Desktop: GSAP not loaded fallback ── */
  if (typeof gsap === "undefined" || typeof ScrollTrigger === "undefined") {
    moments.forEach(function (m) {
      m.style.position = "relative";
      m.style.opacity = "1";
      m.style.pointerEvents = "auto";
      m.style.marginBottom = "120px";
    });
    var innerFB = storySection.querySelector(".story-scroll-inner");
    if (innerFB) innerFB.style.height = "auto";
    storySection.style.minHeight = "auto";
    return;
  }

  gsap.registerPlugin(ScrollTrigger);

  var momentArray = gsap.utils.toArray(".story-moment");
  var totalMoments = momentArray.length;

  /* ── Create progress bar ── */
  var progressWrap = document.createElement("div");
  progressWrap.className = "story-progress";
  progressWrap.setAttribute("aria-hidden", "true");

  var progressFill = document.createElement("div");
  progressFill.className = "story-progress-fill";
  progressWrap.appendChild(progressFill);

  /* Step dots */
  for (var d = 0; d < totalMoments; d++) {
    var stepDot = document.createElement("div");
    stepDot.className = "story-progress-dot";
    stepDot.style.top = (d / (totalMoments - 1)) * 100 + "%";
    stepDot.dataset.step = d + 1;
    progressWrap.appendChild(stepDot);
  }

  storySection.appendChild(progressWrap);

  /* ── Step counter ── */
  var stepCounter = document.createElement("div");
  stepCounter.className = "story-step-counter";
  stepCounter.innerHTML =
    '<span class="step-current">01</span><span class="step-sep">/</span><span class="step-total">' +
    String(totalMoments).padStart(2, "0") +
    "</span>";
  storySection.appendChild(stepCounter);

  /* ── Inject progress + counter styles ── */
  var progressStyle = document.createElement("style");
  progressStyle.textContent = [
    ".story-progress {",
    "  position: absolute; right: 32px; top: 50%; transform: translateY(-50%);",
    "  width: 2px; height: 200px; background: rgba(255,255,255,0.08);",
    "  border-radius: 1px; z-index: 10;",
    "}",
    ".story-progress-fill {",
    "  position: absolute; top: 0; left: 0; width: 100%; height: 0%;",
    "  background: linear-gradient(180deg, var(--accent, #5A54BD), var(--teal, #6BB3CD));",
    "  border-radius: 1px; transition: height 0.3s ease;",
    "}",
    ".story-progress-dot {",
    "  position: absolute; left: 50%; width: 8px; height: 8px;",
    "  background: rgba(255,255,255,0.15); border-radius: 50%;",
    "  transform: translate(-50%, -50%); transition: background 0.3s, transform 0.3s, box-shadow 0.3s;",
    "}",
    ".story-progress-dot.active {",
    "  background: var(--accent, #5A54BD);",
    "  transform: translate(-50%, -50%) scale(1.5);",
    "  box-shadow: 0 0 12px rgba(90, 84, 189, 0.5);",
    "}",
    ".story-step-counter {",
    "  position: absolute; right: 24px; bottom: 32px;",
    "  font-family: var(--font-mono, monospace); font-size: 14px;",
    "  color: var(--text-muted, #71717a); z-index: 10;",
    "  letter-spacing: 0.05em;",
    "}",
    ".step-current { color: var(--text-primary, #e4e4e7); font-weight: 600; }",
    ".step-sep { opacity: 0.4; margin: 0 2px; }",
    "@media (max-width: 1024px) {",
    "  .story-progress, .story-step-counter { display: none; }",
    "}",
  ].join("\n");
  document.head.appendChild(progressStyle);

  /* ── Text split animation helper ── */
  function splitTextReveal(selector, tl, position) {
    var el = document.querySelector(selector);
    if (!el) return;

    var text = el.textContent;
    var words = text.split(/\s+/);
    el.innerHTML = words
      .map(function (w) {
        return (
          '<span class="word-reveal" style="display:inline-block;opacity:0;transform:translateY(20px)">' +
          w +
          "</span>"
        );
      })
      .join(" ");

    var wordEls = el.querySelectorAll(".word-reveal");
    tl.to(
      wordEls,
      {
        opacity: 1,
        y: 0,
        duration: 0.3,
        stagger: 0.04,
        ease: "power3.out",
      },
      position,
    );
  }

  /* ── Main pinned timeline ── */
  var scrollLength = totalMoments * 120; /* % of viewport per moment */

  var tl = gsap.timeline({
    scrollTrigger: {
      trigger: storySection,
      start: "top top",
      end: "+=" + scrollLength + "%",
      pin: true,
      scrub: 0.6,
      anticipatePin: 1,
      onUpdate: function (self) {
        /* Update progress bar */
        var progress = self.progress;
        progressFill.style.height = progress * 100 + "%";

        /* Update step counter + dots */
        var currentStep = Math.min(
          Math.floor(progress * totalMoments),
          totalMoments - 1,
        );
        var currentEl = stepCounter.querySelector(".step-current");
        if (currentEl) {
          currentEl.textContent = String(currentStep + 1).padStart(2, "0");
        }

        var dots = progressWrap.querySelectorAll(".story-progress-dot");
        dots.forEach(function (dot, idx) {
          if (idx <= currentStep) {
            dot.classList.add("active");
          } else {
            dot.classList.remove("active");
          }
        });
      },
    },
  });

  /* ── Build moment transitions ── */
  momentArray.forEach(function (moment, i) {
    var visual = moment.querySelector(".moment-visual");
    var textBlock = moment.querySelector(".moment-text");
    var heading = moment.querySelector("h2");

    if (i === 0) {
      /* First moment: entrance */
      tl.set(moment, { opacity: 1, pointerEvents: "auto" });

      if (visual) {
        tl.from(
          visual,
          {
            scale: 0.8,
            opacity: 0,
            duration: 0.5,
            ease: "power3.out",
          },
          0,
        );
      }
      if (textBlock) {
        tl.from(
          textBlock,
          {
            x: -40,
            opacity: 0,
            duration: 0.4,
            ease: "power2.out",
          },
          0.15,
        );
      }

      /* Hold */
      tl.to({}, { duration: 0.5 });
    }

    if (i < totalMoments - 1) {
      /* ── Exit current moment ── */

      /* Visual scales down + fades */
      if (visual) {
        tl.to(visual, {
          scale: 0.85,
          opacity: 0,
          duration: 0.35,
          ease: "power2.in",
        });
      }

      /* Text slides up + fades */
      if (textBlock) {
        tl.to(
          textBlock,
          {
            y: -60,
            opacity: 0,
            duration: 0.35,
            ease: "power2.in",
          },
          "<0.05",
        );
      }

      tl.set(moment, { pointerEvents: "none" });

      /* ── Enter next moment ── */
      var nextMoment = momentArray[i + 1];
      var nextVisual = nextMoment.querySelector(".moment-visual");
      var nextText = nextMoment.querySelector(".moment-text");

      tl.set(nextMoment, { opacity: 1, pointerEvents: "auto" });

      /* Visual enters from below with scale */
      if (nextVisual) {
        tl.fromTo(
          nextVisual,
          { scale: 1.1, opacity: 0, y: 60 },
          {
            scale: 1,
            opacity: 1,
            y: 0,
            duration: 0.5,
            ease: "power3.out",
          },
          "-=0.1",
        );
      }

      /* Text slides in from right */
      if (nextText) {
        tl.fromTo(
          nextText,
          { x: 60, opacity: 0 },
          {
            x: 0,
            opacity: 1,
            duration: 0.4,
            ease: "power2.out",
          },
          "-=0.3",
        );
      }

      /* Hold on each moment */
      tl.to({}, { duration: 0.5 });
    }
  });

  /* ── Moment 3: Animate SVG intelligence lines ── */
  ScrollTrigger.create({
    trigger: storySection,
    start: "top+=" + 2 * 120 + "% top",
    end: "top+=" + 2.8 * 120 + "% top",
    onEnter: function () {
      gsap.to(".intel-line", {
        strokeDashoffset: 0,
        duration: 1.2,
        stagger: 0.15,
        ease: "power2.out",
      });

      /* Pulse the center node */
      gsap.to(".intelligence-viz circle:first-child", {
        scale: 1.2,
        duration: 0.8,
        yoyo: true,
        repeat: 2,
        ease: "sine.inOut",
        transformOrigin: "center center",
      });
    },
  });

  /* ── Moment 4: Animate dashboard build ── */
  ScrollTrigger.create({
    trigger: storySection,
    start: "top+=" + 3 * 120 + "% top",
    onEnter: function () {
      /* Window entrance */
      gsap.from(".dash-window", {
        y: 30,
        opacity: 0,
        duration: 0.6,
        ease: "power3.out",
      });

      /* Metrics count up */
      gsap.from(".dash-metric-value", {
        textContent: 0,
        duration: 1,
        stagger: 0.15,
        ease: "power2.out",
      });

      /* Bars grow */
      gsap.to(".moment-4 .dash-bar-fill", {
        width: "var(--bar-width)",
        duration: 1.2,
        stagger: 0.12,
        ease: "power2.out",
        delay: 0.3,
      });

      /* Table rows slide in */
      gsap.fromTo(
        ".moment-4 .dash-table-row",
        { opacity: 0, x: -20 },
        {
          opacity: 1,
          x: 0,
          duration: 0.4,
          stagger: 0.1,
          ease: "power2.out",
          delay: 0.8,
        },
      );
    },
  });

  /* ── Moment 5: Success celebration ── */
  ScrollTrigger.create({
    trigger: storySection,
    start: "top+=" + 4 * 120 + "% top",
    onEnter: function () {
      /* Checkmark draw */
      gsap.from(".outcome-badge svg path", {
        strokeDasharray: 50,
        strokeDashoffset: 50,
        duration: 0.8,
        ease: "power2.out",
      });

      /* Badge scale bounce */
      gsap.from(".outcome-badge", {
        scale: 0,
        duration: 0.6,
        ease: "back.out(1.7)",
      });

      /* CTAs stagger in */
      gsap.from(".moment-ctas a", {
        y: 20,
        opacity: 0,
        stagger: 0.15,
        duration: 0.4,
        ease: "power2.out",
        delay: 0.5,
      });
    },
  });
}

/* ── Section reveal animations for non-story sections ── */
function initSectionReveals() {
  "use strict";

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  if (typeof gsap === "undefined" || typeof ScrollTrigger === "undefined")
    return;

  gsap.registerPlugin(ScrollTrigger);

  /* Demo section: staggered card entrance */
  var demoSection = document.querySelector(".demo-section");
  if (demoSection) {
    gsap.from(".demo-card", {
      scrollTrigger: {
        trigger: demoSection,
        start: "top 80%",
        once: true,
      },
      y: 60,
      opacity: 0,
      duration: 0.8,
      ease: "power3.out",
    });
  }

  /* Products section: cards slide in from sides */
  var productsSection = document.querySelector(".products-section");
  if (productsSection) {
    gsap.from(".product-plan", {
      scrollTrigger: {
        trigger: productsSection,
        start: "top 75%",
        once: true,
      },
      x: -60,
      opacity: 0,
      duration: 0.7,
      ease: "power3.out",
    });

    gsap.from(".product-nova", {
      scrollTrigger: {
        trigger: productsSection,
        start: "top 75%",
        once: true,
      },
      x: 60,
      opacity: 0,
      duration: 0.7,
      ease: "power3.out",
      delay: 0.15,
    });
  }

  /* Proof section: stats scale up */
  var proofSection = document.querySelector(".proof-section");
  if (proofSection) {
    gsap.from(".proof-stats > div", {
      scrollTrigger: {
        trigger: proofSection,
        start: "top 80%",
        once: true,
      },
      scale: 0.8,
      opacity: 0,
      stagger: 0.12,
      duration: 0.5,
      ease: "back.out(1.4)",
    });
  }

  /* CTA section: dramatic entrance */
  var ctaSection = document.querySelector(".cta-section");
  if (ctaSection) {
    gsap.from(".cta-card", {
      scrollTrigger: {
        trigger: ctaSection,
        start: "top 85%",
        once: true,
      },
      y: 80,
      scale: 0.95,
      opacity: 0,
      duration: 0.8,
      ease: "power3.out",
    });
  }

  /* Trust bar: slide in */
  var trustBar = document.querySelector(".trust-bar");
  if (trustBar) {
    gsap.from(trustBar, {
      scrollTrigger: {
        trigger: trustBar,
        start: "top 90%",
        once: true,
      },
      y: 20,
      opacity: 0,
      duration: 0.5,
      ease: "power2.out",
    });
  }
}

/* Initialize when DOM is ready */
document.addEventListener("DOMContentLoaded", function () {
  initStoryScroll();
  initSectionReveals();
});
