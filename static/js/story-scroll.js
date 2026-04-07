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

  /* Step dots (clickable) */
  for (var d = 0; d < totalMoments; d++) {
    var stepDot = document.createElement("div");
    stepDot.className = "story-progress-dot";
    stepDot.style.top = (d / (totalMoments - 1)) * 100 + "%";
    stepDot.dataset.step = d;
    stepDot.setAttribute("role", "button");
    stepDot.setAttribute("tabindex", "0");
    stepDot.setAttribute("aria-label", "Go to moment " + (d + 1));
    progressWrap.appendChild(stepDot);
  }

  /* Wire vertical dot clicks */
  progressWrap.addEventListener("click", function (e) {
    var dot = e.target.closest(".story-progress-dot");
    if (dot) scrollToMoment(parseInt(dot.dataset.step, 10));
  });

  storySection.appendChild(progressWrap);

  /* ── Step counter ── */
  var stepCounter = document.createElement("div");
  stepCounter.className = "story-step-counter";
  stepCounter.innerHTML =
    '<span class="step-current">01</span><span class="step-sep">/</span><span class="step-total">' +
    String(totalMoments).padStart(2, "0") +
    "</span>";
  storySection.appendChild(stepCounter);

  /* ── Bottom navigation: dots + arrows + horizontal progress bar ── */
  var bottomNav = document.createElement("nav");
  bottomNav.className = "story-bottom-nav";
  bottomNav.setAttribute("aria-label", "Story navigation");

  /* Left arrow */
  var arrowLeft = document.createElement("button");
  arrowLeft.className = "story-arrow story-arrow--left";
  arrowLeft.setAttribute("aria-label", "Previous moment");
  arrowLeft.innerHTML =
    '<svg width="20" height="20" viewBox="0 0 20 20" fill="none"><path d="M12.5 15L7.5 10L12.5 5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  bottomNav.appendChild(arrowLeft);

  /* Dot row */
  var dotRow = document.createElement("div");
  dotRow.className = "story-dot-row";
  dotRow.setAttribute("role", "tablist");
  dotRow.setAttribute("aria-label", "Story moments");
  for (var bd = 0; bd < totalMoments; bd++) {
    var bDot = document.createElement("button");
    bDot.className = "story-nav-dot" + (bd === 0 ? " active" : "");
    bDot.setAttribute("role", "tab");
    bDot.setAttribute("aria-selected", bd === 0 ? "true" : "false");
    bDot.setAttribute("aria-label", "Go to moment " + (bd + 1));
    bDot.dataset.index = bd;
    dotRow.appendChild(bDot);
  }
  bottomNav.appendChild(dotRow);

  /* Right arrow */
  var arrowRight = document.createElement("button");
  arrowRight.className = "story-arrow story-arrow--right";
  arrowRight.setAttribute("aria-label", "Next moment");
  arrowRight.innerHTML =
    '<svg width="20" height="20" viewBox="0 0 20 20" fill="none"><path d="M7.5 5L12.5 10L7.5 15" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  bottomNav.appendChild(arrowRight);

  storySection.appendChild(bottomNav);

  /* Horizontal progress bar at very bottom */
  var hProgressWrap = document.createElement("div");
  hProgressWrap.className = "story-h-progress";
  hProgressWrap.setAttribute("aria-hidden", "true");
  var hProgressFill = document.createElement("div");
  hProgressFill.className = "story-h-progress-fill";
  hProgressWrap.appendChild(hProgressFill);
  storySection.appendChild(hProgressWrap);

  /* Track current step for arrow navigation */
  var currentNavStep = 0;

  /* Helper: scroll to a specific moment index */
  function scrollToMoment(index) {
    if (index < 0 || index >= totalMoments) return;
    var allST = ScrollTrigger.getAll();
    var st = null;
    for (var si = 0; si < allST.length; si++) {
      if (allST[si].trigger === storySection && allST[si].pin) {
        st = allST[si];
        break;
      }
    }
    if (!st) return;
    var targetProgress = (index + 0.5) / totalMoments;
    var targetScroll = st.start + (st.end - st.start) * targetProgress;
    window.scrollTo({ top: targetScroll, behavior: "smooth" });
  }

  /* Wire arrow clicks */
  arrowLeft.addEventListener("click", function () {
    scrollToMoment(currentNavStep - 1);
  });
  arrowRight.addEventListener("click", function () {
    scrollToMoment(currentNavStep + 1);
  });

  /* Wire dot clicks */
  dotRow.addEventListener("click", function (e) {
    var dot = e.target.closest(".story-nav-dot");
    if (dot) scrollToMoment(parseInt(dot.dataset.index, 10));
  });

  /* Keyboard: left/right arrows when section is in view */
  document.addEventListener("keydown", function (e) {
    if (e.key === "ArrowLeft" || e.key === "ArrowRight") {
      var rect = storySection.getBoundingClientRect();
      if (rect.top <= 0 && rect.bottom >= window.innerHeight * 0.5) {
        e.preventDefault();
        if (e.key === "ArrowLeft") scrollToMoment(currentNavStep - 1);
        else scrollToMoment(currentNavStep + 1);
      }
    }
  });

  /* ── Inject progress + counter + nav styles ── */
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
    "  cursor: pointer;",
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

    /* Bottom navigation */
    ".story-bottom-nav {",
    "  position: absolute; bottom: 48px; left: 50%; transform: translateX(-50%);",
    "  display: flex; align-items: center; gap: 16px; z-index: 10;",
    "}",
    ".story-arrow {",
    "  width: 36px; height: 36px; border-radius: 50%;",
    "  border: 1px solid rgba(255,255,255,0.15);",
    "  background: rgba(32, 32, 88, 0.5);",
    "  color: var(--text-secondary, #a1a1aa);",
    "  display: flex; align-items: center; justify-content: center;",
    "  cursor: pointer; transition: all 0.25s ease;",
    "  backdrop-filter: blur(8px);",
    "}",
    ".story-arrow:hover {",
    "  border-color: var(--accent, #5A54BD);",
    "  color: #fff;",
    "  background: rgba(90, 84, 189, 0.3);",
    "}",
    ".story-arrow:focus-visible {",
    "  outline: 2px solid var(--accent, #5A54BD);",
    "  outline-offset: 2px;",
    "}",
    ".story-dot-row {",
    "  display: flex; align-items: center; gap: 10px;",
    "}",
    ".story-nav-dot {",
    "  width: 10px; height: 10px; border-radius: 50%;",
    "  border: 1.5px solid rgba(255,255,255,0.2);",
    "  background: transparent; cursor: pointer;",
    "  transition: all 0.3s ease; padding: 0;",
    "}",
    ".story-nav-dot:hover {",
    "  border-color: var(--accent, #5A54BD);",
    "  background: rgba(90, 84, 189, 0.25);",
    "}",
    ".story-nav-dot.active {",
    "  background: var(--accent, #5A54BD);",
    "  border-color: var(--accent, #5A54BD);",
    "  transform: scale(1.3);",
    "  box-shadow: 0 0 10px rgba(90, 84, 189, 0.5);",
    "}",
    ".story-nav-dot:focus-visible {",
    "  outline: 2px solid var(--accent, #5A54BD);",
    "  outline-offset: 2px;",
    "}",

    /* Horizontal progress bar */
    ".story-h-progress {",
    "  position: absolute; bottom: 0; left: 0; right: 0;",
    "  height: 3px; background: rgba(255,255,255,0.06);",
    "  z-index: 10;",
    "}",
    ".story-h-progress-fill {",
    "  height: 100%; width: 0%;",
    "  background: linear-gradient(90deg, var(--accent, #5A54BD), var(--teal, #6BB3CD));",
    "  border-radius: 0 2px 2px 0;",
    "  transition: width 0.3s ease;",
    "}",

    "@media (max-width: 1024px) {",
    "  .story-progress, .story-step-counter { display: none; }",
    "  .story-bottom-nav { bottom: 24px; }",
    "  .story-h-progress { display: none; }",
    "}",
    "@media (max-width: 767px) {",
    "  .story-bottom-nav { display: none; }",
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
        /* Update vertical progress bar */
        var progress = self.progress;
        progressFill.style.height = progress * 100 + "%";

        /* Update horizontal progress bar */
        hProgressFill.style.width = progress * 100 + "%";

        /* Update step counter + dots */
        var currentStep = Math.min(
          Math.floor(progress * totalMoments),
          totalMoments - 1,
        );
        currentNavStep = currentStep;

        var currentEl = stepCounter.querySelector(".step-current");
        if (currentEl) {
          currentEl.textContent = String(currentStep + 1).padStart(2, "0");
        }

        /* Vertical dots */
        var dots = progressWrap.querySelectorAll(".story-progress-dot");
        dots.forEach(function (dot, idx) {
          if (idx <= currentStep) {
            dot.classList.add("active");
          } else {
            dot.classList.remove("active");
          }
        });

        /* Bottom nav dots */
        var navDots = dotRow.querySelectorAll(".story-nav-dot");
        navDots.forEach(function (nd, idx) {
          if (idx === currentStep) {
            nd.classList.add("active");
            nd.setAttribute("aria-selected", "true");
          } else {
            nd.classList.remove("active");
            nd.setAttribute("aria-selected", "false");
          }
        });

        /* Arrow states */
        arrowLeft.style.opacity = currentStep === 0 ? "0.3" : "1";
        arrowLeft.disabled = currentStep === 0;
        arrowRight.style.opacity =
          currentStep === totalMoments - 1 ? "0.3" : "1";
        arrowRight.disabled = currentStep === totalMoments - 1;
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

/* ── Section reveals ── */
/* S29: Removed GSAP gsap.from() section reveals. They set inline opacity:0
   that permanently overrides the CSS .reveal.visible system when GSAP loads
   asynchronously. The CSS IntersectionObserver in hub.html handles all
   scroll-triggered reveals via .reveal + .visible classes. */
function initSectionReveals() {
  /* No-op: CSS handles reveals now */
}

/* Initialize when DOM is ready */
document.addEventListener("DOMContentLoaded", function () {
  initStoryScroll();
  initSectionReveals();
});
