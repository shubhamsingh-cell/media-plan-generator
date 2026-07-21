/**
 * hub.js -- Nova AI Suite Hub Page Scripts
 * Extracted from hub.html for cacheability and performance.
 * All animations respect prefers-reduced-motion.
 */
(function () {
  "use strict";

  var prefersReducedMotion = window.matchMedia(
    "(prefers-reduced-motion: reduce)",
  ).matches;

  // ── Hero headline word-stagger (hero cinematic pass) ──
  // Single class toggle drives the whole per-word cascade via CSS
  // transition-delay (see .hw-word in hub.css). Runs as soon as this
  // script executes -- the DOM is already parsed by the time a bottom-
  // of-body <script src> like this one runs, so this is effectively
  // "on DOMContentLoaded" without the extra event round-trip. If this
  // script never runs at all, the head FOUC-guard force-reveals
  // .hw-word directly at 3s as a failsafe.
  var heroHeadline = document.querySelector(".hero-headline");
  if (heroHeadline) heroHeadline.classList.add("is-in");

  // ── Hamburger menu toggle ──
  var hamburger = document.querySelector(".nav-hamburger");
  if (hamburger) {
    hamburger.addEventListener("click", function () {
      document.querySelector(".nav-links").classList.toggle("nav-links--open");
    });
  }

  // ── Marquee pause button (Fix #3 + Fix #5: replaces inline onclick) ──
  var pauseBtn = document.querySelector(".marquee-pause");
  if (pauseBtn) {
    pauseBtn.addEventListener("click", function () {
      var bar = this.closest(".trust-bar");
      if (!bar) return;
      var isPaused = bar.classList.toggle("paused");
      this.textContent = isPaused ? "\u25B6" : "||";
      this.setAttribute(
        "aria-label",
        isPaused ? "Resume scrolling" : "Pause scrolling",
      );
    });
  }

  // ── Story scroll keyboard navigation (Fix #4) ──
  var storyInner = document.querySelector(".story-scroll-inner");
  if (storyInner) {
    var moments = document.querySelectorAll(".story-moment");
    var storyDots = document.querySelectorAll(".story-dot");
    var currentMoment = 0;
    var storyManuallyPaused = false;

    // Re-trigger the "Nova analyzes everything" intel-line draw each time
    // moment 3 (index 2) becomes active by removing + re-adding the class.
    function retriggerIntelDraw(idx) {
      var moment = moments[idx];
      if (!moment || !moment.classList.contains("moment-3")) return;
      var lines = moment.querySelectorAll(".intel-line");
      lines.forEach(function (line) {
        line.classList.remove("intel-line--draw");
      });
      // Force reflow so the animation restarts when the class is re-added.
      void moment.offsetWidth;
      lines.forEach(function (line) {
        line.classList.add("intel-line--draw");
      });
    }

    function showMoment(idx) {
      if (idx < 0 || idx >= moments.length) return;
      moments.forEach(function (m) {
        m.classList.remove("active");
      });
      moments[idx].classList.add("active");
      currentMoment = idx;
      // Sync pager dots (aria-current + active class for styling).
      storyDots.forEach(function (d, i) {
        var on = i === idx;
        d.classList.toggle("active", on);
        if (on) {
          d.setAttribute("aria-current", "true");
        } else {
          d.removeAttribute("aria-current");
        }
      });
      retriggerIntelDraw(idx);
    }

    storyInner.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight" || e.key === "ArrowDown") {
        e.preventDefault();
        showMoment(currentMoment + 1);
        resetAutoAdvance();
      } else if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
        e.preventDefault();
        showMoment(currentMoment - 1);
        resetAutoAdvance();
      }
    });

    // S48: Auto-advance story moments so users see all content.
    // A11y (CLAUDE.md §8): respect prefers-reduced-motion — show the first
    // moment statically and never cycle.
    var storyTimer = null;
    function startAutoAdvance() {
      if (prefersReducedMotion) {
        showMoment(0);
        return;
      }
      if (storyManuallyPaused) return;
      if (storyTimer) clearInterval(storyTimer);
      storyTimer = setInterval(function () {
        var next = (currentMoment + 1) % moments.length;
        showMoment(next);
      }, 5000);
    }
    function stopAutoAdvance() {
      if (storyTimer) {
        clearInterval(storyTimer);
        storyTimer = null;
      }
    }
    function resetAutoAdvance() {
      stopAutoAdvance();
      startAutoAdvance();
    }

    // Pager dots: jump to a moment and stop auto-advance (user took control).
    storyDots.forEach(function (dot) {
      dot.addEventListener("click", function () {
        var idx = parseInt(dot.getAttribute("data-moment"), 10);
        if (isNaN(idx)) return;
        storyManuallyPaused = true;
        stopAutoAdvance();
        showMoment(idx);
        updatePauseBtn();
      });
    });

    // Pause/resume toggle (mirrors the .marquee-pause logic).
    var storyPauseBtn = document.querySelector(".story-pause");
    function updatePauseBtn() {
      if (!storyPauseBtn) return;
      storyPauseBtn.textContent = storyManuallyPaused ? "▶" : "||";
      storyPauseBtn.setAttribute(
        "aria-label",
        storyManuallyPaused ? "Resume auto-advance" : "Pause auto-advance",
      );
    }
    if (storyPauseBtn) {
      storyPauseBtn.addEventListener("click", function () {
        storyManuallyPaused = !storyManuallyPaused;
        if (storyManuallyPaused) {
          stopAutoAdvance();
        } else {
          startAutoAdvance();
        }
        updatePauseBtn();
      });
    }

    // Start when section is visible
    var storyObs = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (e) {
          if (e.isIntersecting) {
            startAutoAdvance();
          } else {
            stopAutoAdvance();
          }
        });
      },
      { threshold: 0.3 },
    );
    storyObs.observe(storyInner.closest(".story-scroll") || storyInner);

    // Pause on hover (skip when the user has explicitly paused or reduced
    // motion is on — there is nothing to pause/resume).
    storyInner.addEventListener("mouseenter", function () {
      if (prefersReducedMotion || storyManuallyPaused) return;
      stopAutoAdvance();
    });
    storyInner.addEventListener("mouseleave", function () {
      if (prefersReducedMotion || storyManuallyPaused) return;
      startAutoAdvance();
    });

    // Ensure the initial moment's pager state + intel draw are in sync.
    showMoment(0);
  }

  // ── Single consolidated IntersectionObserver (Fix #13) ──
  var observerCallbacks = new Map();

  var unifiedObserver = new IntersectionObserver(
    function (entries) {
      entries.forEach(function (entry) {
        if (!entry.isIntersecting) return;
        var callbacks = observerCallbacks.get(entry.target);
        if (callbacks) {
          callbacks.forEach(function (cb) {
            cb(entry.target);
          });
          observerCallbacks.delete(entry.target);
          unifiedObserver.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.05, rootMargin: "0px 0px -40px 0px" },
  );

  function observeElement(el, callback, threshold) {
    // For most elements the unified observer works; for specific thresholds
    // we still use the unified one (the 0.05 threshold fires early enough).
    var existing = observerCallbacks.get(el);
    if (existing) {
      existing.push(callback);
    } else {
      observerCallbacks.set(el, [callback]);
      unifiedObserver.observe(el);
    }
  }

  // ── Reveal-on-scroll + reveal ──
  document
    .querySelectorAll(".reveal-on-scroll, .reveal")
    .forEach(function (el) {
      observeElement(el, function (target) {
        target.classList.add("visible");
        target.querySelectorAll(".word-reveal").forEach(function (w) {
          w.classList.add("visible");
        });
      });
    });
  // Tell the <head> FOUC-guard timer the reveal observers are wired, so it
  // doesn't force-reveal everything at 3s on a normal, non-stalled load.
  window.__novaRevealBooted = true;

  // ── Pause SMIL animations for reduced-motion ──
  if (prefersReducedMotion) {
    document.querySelectorAll(".product-demo-svg").forEach(function (svg) {
      if (svg.pauseAnimations) svg.pauseAnimations();
    });
  }

  // ── Animated number counters ──
  var counterEls = document.querySelectorAll("[data-counter]");

  function setCounterFinal(el) {
    var target = parseFloat(el.dataset.target);
    if (isNaN(target)) return;
    var suffix = el.dataset.suffix || "";
    var prefix = el.dataset.prefix || "";
    var decimals = parseInt(el.dataset.decimals, 10) || 0;
    el.textContent =
      decimals > 0
        ? prefix + target.toFixed(decimals) + suffix
        : prefix + target.toLocaleString() + suffix;
    el.dataset.animated = "1";
  }

  if (prefersReducedMotion) {
    counterEls.forEach(setCounterFinal);
  } else {
    // Higher-threshold observer for counters
    var counterObserver = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            var el = entry.target;
            if (el.dataset.animated === "1") return;
            var target = parseFloat(el.dataset.target);
            if (isNaN(target)) {
              counterObserver.unobserve(el);
              return;
            }
            el.dataset.animated = "1";
            var suffix = el.dataset.suffix || "";
            var prefix = el.dataset.prefix || "";
            var decimals = parseInt(el.dataset.decimals, 10) || 0;
            var startTime = null;
            var duration = 1200;
            function animateCount(timestamp) {
              if (!startTime) startTime = timestamp;
              var progress = Math.min((timestamp - startTime) / duration, 1);
              var eased = 1 - Math.pow(1 - progress, 3);
              var current = eased * target;
              el.textContent =
                decimals > 0
                  ? prefix + current.toFixed(decimals) + suffix
                  : prefix + Math.floor(current).toLocaleString() + suffix;
              if (progress < 1) {
                requestAnimationFrame(animateCount);
              } else {
                el.textContent =
                  decimals > 0
                    ? prefix + target.toFixed(decimals) + suffix
                    : prefix + target.toLocaleString() + suffix;
              }
            }
            requestAnimationFrame(animateCount);
            counterObserver.unobserve(el);
          }
        });
      },
      { threshold: 0.15 },
    );
    counterEls.forEach(function (el) {
      counterObserver.observe(el);
    });
    setTimeout(function () {
      counterEls.forEach(function (el) {
        if (el.dataset.animated !== "1") setCounterFinal(el);
      });
    }, 4000);
  }

  // Dashboard bar + plan-showcase animations removed: the Moment-4 dash card
  // and the standalone plan-showcase card were deleted in the de-repeat pass.
  // The live Prediction Engine (#plan-showcase) drives its own bars below.

  // ── Header shrink on scroll (combined with aurora parallax via rAF) ──
  var nav = document.querySelector(".nav");
  /* auroraLayers parallax removed -- caused scroll vibration */
  var scrollTicking = false;
  window.addEventListener(
    "scroll",
    function () {
      if (!scrollTicking) {
        requestAnimationFrame(function () {
          // Shrink via --nav-h (not an inline height): the nav's height, the
          // guest banner's top offset, and the mobile dropdown's anchor all
          // derive from this one token in hub.css, so writing the variable
          // keeps all three in lockstep.
          if (window.scrollY > 60) {
            nav.style.background = "rgba(0, 0, 0, 0.92)";
            document.documentElement.style.setProperty("--nav-h", "56px");
          } else {
            nav.style.background = "rgba(0, 0, 0, 0.8)";
            document.documentElement.style.setProperty("--nav-h", "64px");
          }
          /* Aurora parallax removed -- caused scroll vibration */
          scrollTicking = false;
        });
        scrollTicking = true;
      }
    },
    { passive: true },
  );

  // ── Button ripple effect ──
  document
    .querySelectorAll(".cta-btn, .btn-primary, .nav-cta")
    .forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        if (prefersReducedMotion) return;
        var ripple = document.createElement("span");
        ripple.className = "ripple";
        ripple.setAttribute("aria-hidden", "true");
        var rect = btn.getBoundingClientRect();
        ripple.style.left = e.clientX - rect.left + "px";
        ripple.style.top = e.clientY - rect.top + "px";
        btn.appendChild(ripple);
        setTimeout(function () {
          ripple.remove();
        }, 600);
      });
    });

  // ── Hero preview: animate bars + metrics on load ──
  if (!prefersReducedMotion) {
    setTimeout(function () {
      document.querySelectorAll(".hp-anim").forEach(function (el) {
        var delay = parseInt(el.dataset.delay) || 0;
        setTimeout(function () {
          el.classList.add("visible");
        }, delay);
      });
      document.querySelectorAll(".hp-anim-bar").forEach(function (el) {
        var delay = parseInt(el.dataset.delay) || 0;
        setTimeout(function () {
          el.classList.add("animated");
        }, delay);
      });
    }, 800);
  } else {
    // Show immediately without animation
    document.querySelectorAll(".hp-anim").forEach(function (el) {
      el.classList.add("visible");
    });
    document.querySelectorAll(".hp-anim-bar").forEach(function (el) {
      el.classList.add("animated");
    });
  }

  // ── Page enter transition ──
  document.body.classList.add("page-enter");

  // ── GSAP hero entrance (staggered reveal) ──
  if (typeof gsap !== "undefined" && !prefersReducedMotion) {
    document
      .querySelectorAll(".hero-anim, .hero-artifact")
      .forEach(function (el) {
        el.style.animation = "none";
        el.style.opacity = "1";
        el.style.filter = "none";
        el.style.transform = "none";
      });
    var heroAnims = document.querySelectorAll(".hero-anim");
    if (heroAnims.length) {
      gsap.from(heroAnims, {
        y: 30,
        opacity: 0,
        duration: 0.8,
        stagger: 0.15,
        ease: "power3.out",
        delay: 0.3,
      });
    }
  }

  // ── 3D Tilt + Magnetic Cursor on product cards ──
  var cards = document.querySelectorAll(".product-hero-card");
  cards.forEach(function (card) {
    card.addEventListener("mousemove", function (e) {
      if (prefersReducedMotion) return;
      var rect = card.getBoundingClientRect();
      var x = e.clientX - rect.left;
      var y = e.clientY - rect.top;
      var cx = rect.width / 2;
      var cy = rect.height / 2;
      var ry = ((x - cx) / cx) * 5;
      var rx = ((cy - y) / cy) * 5;
      var tx = ((x - cx) / cx) * 4;
      var ty = ((y - cy) / cy) * 4;
      card.style.setProperty("--card-rx", rx + "deg");
      card.style.setProperty("--card-ry", ry + "deg");
      card.style.setProperty("--card-tx", tx + "px");
      card.style.setProperty("--card-ty", ty + "px");
      card.style.setProperty("--mouse-x", x + "px");
      card.style.setProperty("--mouse-y", y + "px");
      // Apply will-change only during active interaction (Fix #8)
      card.style.willChange = "transform";
    });
    card.addEventListener("mouseleave", function () {
      card.style.setProperty("--card-rx", "0deg");
      card.style.setProperty("--card-ry", "0deg");
      card.style.setProperty("--card-tx", "0px");
      card.style.setProperty("--card-ty", "0px");
      card.style.willChange = "auto";
    });
  });

  // ── Animated product stat counters ──
  var statEls = document.querySelectorAll("[data-count]");
  var counted = false;
  function animateCounters() {
    if (counted || prefersReducedMotion) {
      statEls.forEach(function (el) {
        var target = parseInt(el.getAttribute("data-count"), 10);
        el.textContent = target.toLocaleString();
        el.closest(".products-stat").classList.add("counted");
      });
      counted = true;
      return;
    }
    counted = true;
    statEls.forEach(function (el, i) {
      var target = parseInt(el.getAttribute("data-count"), 10);
      var duration = 1800;
      var start = null;
      var parent = el.closest(".products-stat");
      setTimeout(function () {
        parent.classList.add("counted");
        parent.style.animation = "countUp 0.4s ease forwards";
      }, i * 120);
      function step(ts) {
        if (!start) start = ts;
        var progress = Math.min((ts - start) / duration, 1);
        var eased = 1 - Math.pow(1 - progress, 3);
        el.textContent = Math.floor(eased * target).toLocaleString();
        if (progress < 1) {
          requestAnimationFrame(step);
        } else {
          el.textContent = target.toLocaleString();
        }
      }
      setTimeout(function () {
        requestAnimationFrame(step);
      }, i * 120);
    });
  }

  var statsSection = document.querySelector(".products-stats");
  if (statsSection && "IntersectionObserver" in window) {
    var statObs = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting && !counted) {
            animateCounters();
            statObs.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.3 },
    );
    statObs.observe(statsSection);
  } else if (statsSection) {
    animateCounters();
  }
})();

/**
 * V2: Premium product section enhancements
 * Particle canvas, holographic overlays, spotlight, flow lines, typing effect
 */
(function () {
  "use strict";
  var reducedMotion = window.matchMedia(
    "(prefers-reduced-motion: reduce)",
  ).matches;
  if (reducedMotion) return;

  var section = document.querySelector(".products-section");
  if (!section) return;

  // ── 1. Particle Constellation Canvas (Fix #9: O(n) grid-based neighbor detection) ──
  var canvas = section.querySelector(".particle-canvas");
  if (canvas && canvas.getContext) {
    var ctx = canvas.getContext("2d");
    var particles = [];
    var mouseX = -9999,
      mouseY = -9999;
    var PARTICLE_COUNT = 40; // Reduced from 60 for perf (Fix #9)
    var CONNECT_DIST = 120;
    var GRID_SIZE = CONNECT_DIST; // Spatial grid cell size
    var COLORS = ["90,84,189", "107,179,205", "32,32,88"];
    var animId = null;
    var isVisible = false;

    function resizeCanvas() {
      var rect = section.getBoundingClientRect();
      canvas.width = rect.width;
      canvas.height = rect.height;
    }

    function initParticles() {
      particles = [];
      for (var i = 0; i < PARTICLE_COUNT; i++) {
        particles.push({
          x: Math.random() * canvas.width,
          y: Math.random() * canvas.height,
          vx: (Math.random() - 0.5) * 0.4,
          vy: (Math.random() - 0.5) * 0.4,
          r: Math.random() * 2 + 1,
          color: COLORS[Math.floor(Math.random() * COLORS.length)],
          alpha: Math.random() * 0.15 + 0.15,
        });
      }
    }

    function drawParticles() {
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      // Build spatial grid (Fix #9: O(n) instead of O(n^2))
      var cols = Math.ceil(canvas.width / GRID_SIZE) || 1;
      var rows = Math.ceil(canvas.height / GRID_SIZE) || 1;
      var grid = new Array(cols * rows);
      for (var g = 0; g < grid.length; g++) grid[g] = [];

      for (var i = 0; i < particles.length; i++) {
        var p = particles[i];
        // Mouse repulsion
        var dx = p.x - mouseX;
        var dy = p.y - mouseY;
        var dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 100 && dist > 0) {
          var force = ((100 - dist) / 100) * 0.8;
          p.x += (dx / dist) * force;
          p.y += (dy / dist) * force;
        }
        p.x += p.vx;
        p.y += p.vy;
        if (p.x < 0) p.x = canvas.width;
        if (p.x > canvas.width) p.x = 0;
        if (p.y < 0) p.y = canvas.height;
        if (p.y > canvas.height) p.y = 0;

        // Draw particle
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx.fillStyle = "rgba(" + p.color + "," + p.alpha + ")";
        ctx.fill();

        // Insert into grid
        var gx = Math.min(Math.floor(p.x / GRID_SIZE), cols - 1);
        var gy = Math.min(Math.floor(p.y / GRID_SIZE), rows - 1);
        if (gx >= 0 && gy >= 0) {
          grid[gy * cols + gx].push(p);
        }
      }

      // Connect nearby particles using spatial grid (Fix #9)
      for (var i = 0; i < particles.length; i++) {
        var p = particles[i];
        var gx = Math.min(Math.floor(p.x / GRID_SIZE), cols - 1);
        var gy = Math.min(Math.floor(p.y / GRID_SIZE), rows - 1);
        // Check this cell and adjacent cells
        for (
          var ny = Math.max(0, gy - 1);
          ny <= Math.min(rows - 1, gy + 1);
          ny++
        ) {
          for (
            var nx = Math.max(0, gx - 1);
            nx <= Math.min(cols - 1, gx + 1);
            nx++
          ) {
            var cell = grid[ny * cols + nx];
            for (var k = 0; k < cell.length; k++) {
              var p2 = cell[k];
              if (p2 === p) continue;
              var ddx = p.x - p2.x;
              var ddy = p.y - p2.y;
              var d = Math.sqrt(ddx * ddx + ddy * ddy);
              if (d < CONNECT_DIST) {
                var lineAlpha = (1 - d / CONNECT_DIST) * 0.12;
                ctx.beginPath();
                ctx.moveTo(p.x, p.y);
                ctx.lineTo(p2.x, p2.y);
                ctx.strokeStyle = "rgba(" + p.color + "," + lineAlpha + ")";
                ctx.lineWidth = 0.5;
                ctx.stroke();
              }
            }
          }
        }
      }
      if (isVisible) animId = requestAnimationFrame(drawParticles);
    }

    resizeCanvas();
    initParticles();

    var particleObserver = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            isVisible = true;
            if (!animId) animId = requestAnimationFrame(drawParticles);
          } else {
            isVisible = false;
            if (animId) {
              cancelAnimationFrame(animId);
              animId = null;
            }
          }
        });
      },
      { threshold: 0.05 },
    );
    particleObserver.observe(section);

    section.addEventListener("mousemove", function (e) {
      var rect = section.getBoundingClientRect();
      mouseX = e.clientX - rect.left;
      mouseY = e.clientY - rect.top;
    });
    section.addEventListener("mouseleave", function () {
      mouseX = -9999;
      mouseY = -9999;
    });

    var resizeTimer;
    window.addEventListener("resize", function () {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(function () {
        resizeCanvas();
        initParticles();
      }, 250);
    });
  }

  // ── 2. Holographic mouse tracking ──
  var holoCards = section.querySelectorAll(".holo-overlay");
  holoCards.forEach(function (holo) {
    var card = holo.parentElement;
    card.addEventListener("mousemove", function (e) {
      var rect = card.getBoundingClientRect();
      var x = ((e.clientX - rect.left) / rect.width) * 100;
      var y = ((e.clientY - rect.top) / rect.height) * 100;
      holo.style.backgroundPosition = x + "% " + y + "%";
    });
  });

  // ── 3. Spotlight cursor on section ──
  var spotlight = section.querySelector(".products-spotlight-overlay");
  if (spotlight) {
    section.addEventListener("mousemove", function (e) {
      var rect = section.getBoundingClientRect();
      spotlight.style.setProperty("--spot-x", e.clientX - rect.left + "px");
      spotlight.style.setProperty("--spot-y", e.clientY - rect.top + "px");
    });
  }

  // ── 4. Connected flow lines (SVG) ──
  var flowSvg = section.querySelector(".flow-lines-svg");
  if (flowSvg && window.innerWidth > 1024) {
    function updateFlowLines() {
      var grid = section.querySelector(".products-duo");
      if (!grid) return;
      flowSvg.innerHTML = "";
      var sRect = section.getBoundingClientRect();
      flowSvg.setAttribute(
        "viewBox",
        "0 0 " + sRect.width + " " + sRect.height,
      );
      var cardsByProduct = {};
      section.querySelectorAll(".product-hero-card").forEach(function (c) {
        var p = c.dataset.product;
        if (p) {
          var r = c.getBoundingClientRect();
          cardsByProduct[p] = {
            cx: r.left - sRect.left + r.width / 2,
            cy: r.top - sRect.top + r.height / 2,
            right: r.left - sRect.left + r.width,
            left: r.left - sRect.left,
            bottom: r.top - sRect.top + r.height,
            top: r.top - sRect.top,
          };
        }
      });
      var connections = [
        { from: "plan", to: "nova" },
        { from: "nova", to: "slotops" },
        { from: "plan", to: "cg" },
      ];
      connections.forEach(function (conn, idx) {
        var a = cardsByProduct[conn.from];
        var b = cardsByProduct[conn.to];
        if (!a || !b) return;
        var path = document.createElementNS(
          "http://www.w3.org/2000/svg",
          "path",
        );
        var startX = a.right;
        var startY = a.cy;
        var endX = b.left;
        var endY = b.cy;
        if (Math.abs(a.cy - b.cy) < 100) {
          var midX = (startX + endX) / 2;
          path.setAttribute(
            "d",
            "M" +
              startX +
              "," +
              startY +
              " C" +
              midX +
              "," +
              startY +
              " " +
              midX +
              "," +
              endY +
              " " +
              endX +
              "," +
              endY,
          );
        } else {
          startX = a.cx;
          startY = a.bottom;
          endX = b.cx;
          endY = b.top;
          var midY = (startY + endY) / 2;
          path.setAttribute(
            "d",
            "M" +
              startX +
              "," +
              startY +
              " C" +
              startX +
              "," +
              midY +
              " " +
              endX +
              "," +
              midY +
              " " +
              endX +
              "," +
              endY,
          );
        }
        path.classList.add("flow-line");
        if (idx % 2 === 1) path.classList.add("flow-line--reverse");
        flowSvg.appendChild(path);

        var dot = document.createElementNS(
          "http://www.w3.org/2000/svg",
          "circle",
        );
        dot.setAttribute("r", "2");
        dot.classList.add("flow-dot");
        flowSvg.appendChild(dot);
        var animMotion = document.createElementNS(
          "http://www.w3.org/2000/svg",
          "animateMotion",
        );
        animMotion.setAttribute("dur", 8 + idx * 2 + "s");
        animMotion.setAttribute("repeatCount", "indefinite");
        animMotion.setAttribute("path", path.getAttribute("d"));
        dot.appendChild(animMotion);
      });
    }
    setTimeout(updateFlowLines, 500);
    var flowResizeTimer;
    window.addEventListener("resize", function () {
      clearTimeout(flowResizeTimer);
      flowResizeTimer = setTimeout(updateFlowLines, 300);
    });
  }

  // ── 5. Typing effect on section title ──
  var titleEl = document.getElementById("products-typed-title");
  if (titleEl) {
    var fullText = titleEl.getAttribute("data-typed-text");
    var typed = false;
    var typingObserver = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting && !typed) {
            typed = true;
            typingObserver.unobserve(titleEl);
            titleEl.textContent = "";
            var cursor = document.createElement("span");
            cursor.className = "typed-cursor";
            cursor.textContent = "\u200B";
            titleEl.appendChild(cursor);
            var charIndex = 0;
            function typeNext() {
              if (charIndex < fullText.length) {
                titleEl.insertBefore(
                  document.createTextNode(fullText[charIndex]),
                  cursor,
                );
                charIndex++;
                setTimeout(typeNext, 35 + Math.random() * 25);
              } else {
                setTimeout(function () {
                  cursor.style.opacity = "0";
                  cursor.style.transition = "opacity 0.5s";
                  setTimeout(function () {
                    cursor.remove();
                  }, 500);
                }, 2000);
              }
            }
            typeNext();
          }
        });
      },
      { threshold: 0.5 },
    );
    typingObserver.observe(titleEl);
  }

  // ── 6. Hover preview tooltips ──
  var previewConfigs = {
    plan: { color: "#8680d6", bars: [70, 50, 35, 20], label: "Channel Mix" },
    nova: { color: "#6bb5ce", bars: [60, 80, 40, 55], label: "AI Chat" },
    slotops: { color: "#378fe9", bars: [90, 45, 65, 30], label: "Slot Score" },
    cg: { color: "#48c78e", bars: [50, 75, 60, 40], label: "Locations" },
    geoviz: { color: "#a78bfa", bars: [40, 60, 80, 50], label: "Globe" },
  };

  section
    .querySelectorAll(".product-hero-card[data-product]")
    .forEach(function (card) {
      var product = card.dataset.product;
      var cfg = previewConfigs[product];
      if (!cfg) return;

      var tip = document.createElement("div");
      tip.className = "card-preview-tooltip";
      tip.setAttribute("aria-hidden", "true");

      var header = document.createElement("div");
      header.className = "preview-header";
      header.style.background = cfg.color;
      tip.appendChild(header);

      var bar = document.createElement("div");
      bar.className = "preview-bar";
      bar.style.background = cfg.color;
      bar.style.width = "80%";
      tip.appendChild(bar);

      var chartRow = document.createElement("div");
      chartRow.className = "preview-chart-row";
      cfg.bars.forEach(function (h) {
        var b = document.createElement("div");
        b.className = "preview-chart-bar";
        b.style.height = h + "%";
        b.style.background = cfg.color;
        chartRow.appendChild(b);
      });
      tip.appendChild(chartRow);

      card.style.position = "relative";
      card.appendChild(tip);

      var hoverTimer = null;
      card.addEventListener("mouseenter", function () {
        hoverTimer = setTimeout(function () {
          tip.classList.add("visible");
        }, 1500);
      });
      card.addEventListener("mouseleave", function () {
        clearTimeout(hoverTimer);
        tip.classList.remove("visible");
      });
    });
})();

/**
 * S46: Premium GSAP ScrollTrigger animations
 * Uses gsap.utils.toArray for batched animations (Fix #12)
 */
(function () {
  "use strict";
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  if (typeof gsap === "undefined" || typeof ScrollTrigger === "undefined")
    return;
  gsap.registerPlugin(ScrollTrigger);

  // ── 1. Product card staggered entrance (Fix #12: batched with gsap.utils.toArray) ──
  var productCards = gsap.utils.toArray(".product-hero-card");
  if (productCards.length) {
    var tl = gsap.timeline({
      scrollTrigger: {
        trigger: ".products-section",
        start: "top 80%",
      },
    });
    tl.from(productCards, {
      y: 60,
      opacity: 0,
      scale: 0.95,
      rotateX: 5,
      duration: 0.8,
      stagger: 0.15,
      ease: "power3.out",
      clearProps: "all",
    });
  }

  // ── 2. Hero headline word-by-word reveal ──
  var heroTitle = document.querySelector(".hero-headline");
  if (heroTitle && !heroTitle.querySelector("*")) {
    var originalHTML = heroTitle.innerHTML;
    var parts = originalHTML.split(/(<br\s*\/?>)/i);
    var wordHTML = "";
    parts.forEach(function (part) {
      if (/^<br/i.test(part)) {
        wordHTML += part;
      } else {
        var words = part.trim().split(/\s+/);
        words.forEach(function (w) {
          if (w) {
            wordHTML +=
              '<span class="hero-word-gsap" style="display:inline-block;opacity:0;transform:translateY(20px)">' +
              w +
              " </span>";
          }
        });
      }
    });
    heroTitle.innerHTML = wordHTML;
    heroTitle.style.opacity = "1";
    gsap.to(".hero-word-gsap", {
      opacity: 1,
      y: 0,
      duration: 0.6,
      stagger: 0.08,
      ease: "power2.out",
      delay: 0.3,
    });
  }

  // ── 3. Animated footer stat counters on scroll ──
  gsap.utils.toArray(".footer-stat-value").forEach(function (el) {
    var raw = el.textContent.trim();
    var numMatch = raw.match(/[\d.]+/);
    if (!numMatch) return;
    var target = parseFloat(numMatch[0]);
    var suffix = raw.replace(/[\d.]+/, "");
    var isDecimal = raw.indexOf(".") !== -1;
    ScrollTrigger.create({
      trigger: el,
      start: "top 90%",
      once: true,
      onEnter: function () {
        var obj = { val: 0 };
        gsap.to(obj, {
          val: target,
          duration: 1.5,
          ease: "power2.out",
          onUpdate: function () {
            el.textContent =
              (isDecimal ? obj.val.toFixed(1) : Math.round(obj.val)) + suffix;
          },
        });
      },
    });
  });

  // ── 4. Magnetic hover effect on CTA buttons ──
  gsap.utils.toArray(".nav-cta, .btn-primary.btn-lg").forEach(function (btn) {
    btn.style.transition =
      "transform 0.3s cubic-bezier(0.25, 0.46, 0.45, 0.94)";
    btn.addEventListener("mousemove", function (e) {
      var rect = btn.getBoundingClientRect();
      var x = (e.clientX - rect.left - rect.width / 2) * 0.15;
      var y = (e.clientY - rect.top - rect.height / 2) * 0.15;
      btn.style.transform = "translate(" + x + "px, " + y + "px)";
    });
    btn.addEventListener("mouseleave", function () {
      btn.style.transform = "";
    });
  });

  // ── 5. Parallax depth on products orbs ──
  gsap.utils.toArray(".products-orb").forEach(function (orb, i) {
    gsap.to(orb, {
      scrollTrigger: {
        trigger: ".products-section",
        start: "top bottom",
        end: "bottom top",
        scrub: 1,
      },
      y: (i + 1) * -40,
      ease: "none",
    });
  });
})();

/**
 * Lenis + GSAP sync + product card mouse-tracking glow
 */
(function () {
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  if (
    typeof Lenis !== "undefined" &&
    typeof gsap !== "undefined" &&
    typeof ScrollTrigger !== "undefined"
  ) {
    gsap.registerPlugin(ScrollTrigger);

    var lenis = new Lenis({
      duration: 1.2,
      easing: function (t) {
        return Math.min(1, 1.001 - Math.pow(2, -10 * t));
      },
      smoothWheel: true,
      wheelMultiplier: 1,
      touchMultiplier: 2,
    });

    lenis.on("scroll", ScrollTrigger.update);
    gsap.ticker.add(function (time) {
      lenis.raf(time * 1000);
    });
    gsap.ticker.lagSmoothing(0);

    document.addEventListener("visibilitychange", function () {
      if (document.hidden) {
        lenis.stop();
      } else {
        lenis.start();
      }
    });

    // ── Word-by-word reveal for section titles ──
    document.querySelectorAll(".section-title").forEach(function (el) {
      var words = el.textContent.trim().split(/\s+/);
      el.innerHTML = words
        .map(function (w, i) {
          return (
            '<span class="word-reveal" style="transition-delay:' +
            i * 80 +
            'ms">' +
            w +
            "</span>"
          );
        })
        .join(" ");
    });

    // ── Product card mouse-tracking glow (additional layer) ──
    document.querySelectorAll(".product-hero-card").forEach(function (card) {
      card.addEventListener(
        "mousemove",
        function (e) {
          var rect = card.getBoundingClientRect();
          card.style.setProperty("--mouse-x", e.clientX - rect.left + "px");
          card.style.setProperty("--mouse-y", e.clientY - rect.top + "px");
        },
        { passive: true },
      );
    });
  }
})();

// ── Recent Activity Widget ──
(function initRecentActivity() {
  function _timeAgo(dateStr) {
    try {
      var d = new Date(dateStr);
      var now = new Date();
      var diff = Math.floor((now - d) / 1000);
      if (diff < 60) return "just now";
      if (diff < 3600) return Math.floor(diff / 60) + "m ago";
      if (diff < 86400) return Math.floor(diff / 3600) + "h ago";
      if (diff < 604800) return Math.floor(diff / 86400) + "d ago";
      return d.toLocaleDateString();
    } catch (e) {
      return "";
    }
  }

  function render() {
    var container = document.getElementById("recentActivityContent");
    if (!container) return;

    var plans = [];
    var chats = [];
    try {
      plans = JSON.parse(localStorage.getItem("nova_recent_plans") || "[]");
    } catch (e) {}
    try {
      chats = JSON.parse(localStorage.getItem("nova_recent_chats") || "[]");
    } catch (e) {}

    plans = plans.slice(0, 5);
    chats = chats.slice(0, 5);

    if (plans.length === 0 && chats.length === 0) {
      // On-brand empty state with real CTAs (styled via .activity-empty in CSS).
      container.innerHTML =
        '<div class="activity-empty">' +
        '<div class="activity-empty-glyph" aria-hidden="true">' +
        '<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 8v4l3 3"/><circle cx="12" cy="12" r="9"/></svg>' +
        "</div>" +
        '<div class="activity-empty-headline">Nothing here yet — let’s change that.</div>' +
        '<div class="activity-empty-actions">' +
        '<a href="/media-plan" class="btn-primary">Generate your first plan</a>' +
        '<a href="/nova" class="activity-empty-link">Chat with Nova</a>' +
        "</div>" +
        "</div>";
      return;
    }

    var html = '<div class="activity-list">';

    for (var i = 0; i < plans.length; i++) {
      var p = plans[i];
      html +=
        '<a href="' +
        (p.url || "/media-plan") +
        '" class="activity-row activity-row--plan">' +
        '<div class="activity-row-icon activity-row-icon--plan">&#128202;</div>' +
        '<div class="activity-row-body">' +
        '<div class="activity-row-title">' +
        (p.title || "Media Plan").replace(/</g, "&lt;") +
        "</div>" +
        '<div class="activity-row-meta">' +
        (p.industry || "").replace(/</g, "&lt;") +
        (p.budget ? " &middot; " + p.budget.replace(/</g, "&lt;") : "") +
        "</div>" +
        "</div>" +
        '<div class="activity-row-time">' +
        _timeAgo(p.timestamp) +
        "</div>" +
        '<div class="activity-row-open activity-row-open--plan">Open</div>' +
        "</a>";
    }

    for (var j = 0; j < chats.length; j++) {
      var c = chats[j];
      html +=
        '<a href="/nova" class="activity-row activity-row--chat">' +
        '<div class="activity-row-icon activity-row-icon--chat">&#128172;</div>' +
        '<div class="activity-row-body">' +
        '<div class="activity-row-title">' +
        (c.title || "Nova Chat").replace(/</g, "&lt;") +
        "</div>" +
        '<div class="activity-row-meta">Chatbot conversation</div>' +
        "</div>" +
        '<div class="activity-row-time">' +
        _timeAgo(c.timestamp) +
        "</div>" +
        '<div class="activity-row-open activity-row-open--chat">Open</div>' +
        "</a>";
    }

    html += "</div>";
    container.innerHTML = html;
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", render);
  } else {
    render();
  }
})();

// ── Nova AI Chat Widget (floating bottom-right) ──
window.addEventListener("load", function () {
  var s = document.createElement("script");
  s.src = "/static/nova-chat.js?v=3.5.4";
  s.async = true;
  s.onload = function () {
    if (typeof NovaChat !== "undefined") {
      NovaChat.init({ containerId: null });
    }
  };
  document.body.appendChild(s);
});

// ── GSAP + ScrollTrigger (multi-CDN fallback: unpkg -> jsdelivr -> cdnjs) ──
(function () {
  var cdns = [
    {
      gsap: "https://unpkg.com/gsap@3.12.5/dist/gsap.min.js",
      st: "https://unpkg.com/gsap@3.12.5/dist/ScrollTrigger.min.js",
    },
    {
      gsap: "https://cdn.jsdelivr.net/npm/gsap@3.12.5/dist/gsap.min.js",
      st: "https://cdn.jsdelivr.net/npm/gsap@3.12.5/dist/ScrollTrigger.min.js",
    },
    {
      gsap: "https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js",
      st: "https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/ScrollTrigger.min.js",
    },
  ];
  var modules = [
    "/static/js/animations.js?v=3.0.0",
    "/static/js/custom-cursor.js?v=2.0.0",
    "/static/js/hero-evolution.js?v=2.0.0",
    "/static/js/network-graph.js?v=2.0.0",
    "/static/js/role-cycling.js?v=2.0.0",
    "/static/js/motion-engine.js?v=2.0.0",
  ];

  function loadScript(url) {
    return new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = url;
      s.onload = resolve;
      s.onerror = reject;
      document.body.appendChild(s);
    });
  }

  function loadModules() {
    Promise.all(
      modules.map(function (src) {
        return loadScript(src);
      }),
    );
  }

  function tryLoadGSAP(idx) {
    if (idx >= cdns.length) {
      console.warn(
        "[Nova] All GSAP CDNs failed — loading modules without GSAP",
      );
      loadModules();
      return;
    }
    loadScript(cdns[idx].gsap)
      .then(function () {
        return loadScript(cdns[idx].st);
      })
      .then(function () {
        console.debug("[Nova] GSAP loaded from CDN #" + (idx + 1));
        loadModules();
      })
      .catch(function () {
        console.warn(
          "[Nova] GSAP CDN #" + (idx + 1) + " failed, trying next...",
        );
        tryLoadGSAP(idx + 1);
      });
  }

  tryLoadGSAP(0);
})();

// ── Nova Auth: Google Sign-In via Supabase (optional, non-blocking) ──
document.addEventListener("DOMContentLoaded", function () {
  if (typeof NovaAuth === "undefined") return;
  fetch("/api/config")
    .then(function (r) {
      return r.json();
    })
    .then(function (cfg) {
      if (cfg && cfg.auth_enabled) {
        NovaAuth.init({
          supabaseUrl: cfg.supabase_url || "",
          supabaseAnonKey: cfg.supabase_anon_key || "",
          allowedDomains: ["joveo.com"],
        });
      }
    })
    .catch(function () {
      // Auth init failure must never break the page
    });
});

// ── Page exit transition ──
document.addEventListener("DOMContentLoaded", function () {
  document.querySelectorAll('a[href^="/"]').forEach(function (link) {
    link.addEventListener("click", function (e) {
      if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
      e.preventDefault();
      document.body.style.opacity = "0";
      document.body.style.transform = "translateY(-10px)";
      document.body.style.transition = "opacity 0.2s ease, transform 0.2s ease";
      var href = link.href;
      setTimeout(function () {
        window.location.href = href;
      }, 200);
    });
  });
});

/* ── S91: Hero signature constellation ──
   A GPU-light canvas field of "supply sources" that drift, link to nearby
   neighbours, and trail thin lines toward the live plan card — visualising
   "Nova scanning 10,341 partners into one optimized plan". Paused when the
   hero is off-screen and fully disabled under prefers-reduced-motion. */
(function () {
  "use strict";
  var cv = document.getElementById("heroConstellation");
  var hero = document.getElementById("hero");
  if (!cv || !hero || !cv.getContext) return;
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  var ctx = cv.getContext("2d");
  var dpr = Math.min(window.devicePixelRatio || 1, 2);
  var W = 0,
    H = 0,
    nodes = [],
    focus = { x: 0, y: 0 },
    running = false,
    raf = 0;

  // Ambient depth (hero cinematic pass): the constellation drifts a few
  // pixels toward the cursor, lerped so it never snaps. Desktop-with-mouse
  // only; folded into the existing rAF loop so it automatically pauses
  // whenever `running` is false (hero off-screen), never fighting that gate.
  var supportsParallax = window.matchMedia("(pointer: fine)").matches;
  var parTarget = { x: 0, y: 0 };
  var parCurrent = { x: 0, y: 0 };
  var PARALLAX_MAX = 8;
  var PARALLAX_LERP = 0.06;
  if (supportsParallax) {
    hero.addEventListener("mousemove", function (e) {
      var r = hero.getBoundingClientRect();
      var nx = (e.clientX - r.left) / r.width - 0.5;
      var ny = (e.clientY - r.top) / r.height - 0.5;
      parTarget.x = nx * 2 * PARALLAX_MAX;
      parTarget.y = ny * 2 * PARALLAX_MAX;
    });
    hero.addEventListener("mouseleave", function () {
      parTarget.x = 0;
      parTarget.y = 0;
    });
  }

  function size() {
    var r = hero.getBoundingClientRect();
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    W = r.width;
    H = r.height;
    cv.width = Math.round(W * dpr);
    cv.height = Math.round(H * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    focus.x = W * 0.74;
    focus.y = H * 0.5;
  }
  function seed() {
    nodes = [];
    var n = Math.max(10, Math.min(26, Math.round(W / 52)));
    for (var i = 0; i < n; i++) {
      nodes.push({
        x: Math.random() * W * 0.62,
        y: 90 + Math.random() * Math.max(120, H - 150),
        vx: (Math.random() - 0.5) * 0.18,
        vy: (Math.random() - 0.5) * 0.18,
        r: Math.random() * 1.6 + 1,
      });
    }
  }
  function frame() {
    if (!running) return;
    ctx.clearRect(0, 0, W, H);
    for (var i = 0; i < nodes.length; i++) {
      var a = nodes[i];
      a.x += a.vx;
      a.y += a.vy;
      if (a.x < 0 || a.x > W * 0.66) a.vx *= -1;
      if (a.y < 70 || a.y > H - 36) a.vy *= -1;
      for (var j = i + 1; j < nodes.length; j++) {
        var b = nodes[j],
          dx = a.x - b.x,
          dy = a.y - b.y,
          d = Math.sqrt(dx * dx + dy * dy);
        if (d < 100) {
          ctx.strokeStyle = "rgba(139,133,230," + 0.13 * (1 - d / 100) + ")";
          ctx.lineWidth = 0.6;
          ctx.beginPath();
          ctx.moveTo(a.x, a.y);
          ctx.lineTo(b.x, b.y);
          ctx.stroke();
        }
      }
      var fdx = focus.x - a.x,
        fdy = focus.y - a.y,
        fd = Math.sqrt(fdx * fdx + fdy * fdy);
      if (fd < 300) {
        ctx.strokeStyle = "rgba(107,181,206," + 0.09 * (1 - fd / 300) + ")";
        ctx.lineWidth = 0.6;
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(focus.x, focus.y);
        ctx.stroke();
      }
      ctx.fillStyle = "rgba(170,164,255,0.65)";
      ctx.beginPath();
      ctx.arc(a.x, a.y, a.r, 0, 6.2832);
      ctx.fill();
    }
    if (supportsParallax) {
      parCurrent.x += (parTarget.x - parCurrent.x) * PARALLAX_LERP;
      parCurrent.y += (parTarget.y - parCurrent.y) * PARALLAX_LERP;
      cv.style.transform =
        "translate(" + parCurrent.x.toFixed(2) + "px, " + parCurrent.y.toFixed(2) + "px)";
    }
    raf = requestAnimationFrame(frame);
  }
  function start() {
    if (running) return;
    running = true;
    raf = requestAnimationFrame(frame);
  }
  function stop() {
    running = false;
    if (raf) cancelAnimationFrame(raf);
  }

  size();
  seed();
  var rt;
  window.addEventListener("resize", function () {
    clearTimeout(rt);
    rt = setTimeout(function () {
      size();
      seed();
    }, 150);
  });
  if ("IntersectionObserver" in window) {
    new IntersectionObserver(
      function (entries) {
        entries.forEach(function (en) {
          if (en.isIntersecting) start();
          else stop();
        });
      },
      { threshold: 0.01 }
    ).observe(hero);
  } else {
    start();
  }
})();

/* ==========================================================================
   PREDICTION ENGINE — the single interactive plan-card on the page.
   Picks a role + location + budget, predicts cost-per-hire live, and routes
   the spend across 5 channels. Numbers count up (cubic ease); reduced-motion
   jumps to final. rAF is cancelled per element so rapid changes don't fight.
   ========================================================================== */
(function () {
  "use strict";

  var card = document.getElementById("plan-showcase");
  if (!card || !card.classList.contains("pe-card")) return;

  var prefersReducedMotion = window.matchMedia(
    "(prefers-reduced-motion: reduce)",
  ).matches;

  // Per-role recommended channel split (Indeed, LinkedIn, Programmatic,
  // Niche boards, Social) so the mix visibly shifts by role rather than
  // staying static -- e.g. CDL Driver leans Indeed/Programmatic/niche with
  // minimal LinkedIn, Software Engineer leans LinkedIn. Illustrative but
  // realistic; each row sums to 1.0.
  var ROLES = [
    { k: "Registered Nurse", cpa: 18, ath: 0.017, days: 31, split: [0.34, 0.10, 0.20, 0.28, 0.08] },
    { k: "Sales Rep", cpa: 12, ath: 0.012, days: 24, split: [0.30, 0.24, 0.22, 0.12, 0.12] },
    { k: "CDL Driver", cpa: 9, ath: 0.022, days: 19, split: [0.40, 0.05, 0.30, 0.20, 0.05] },
    { k: "Software Engineer", cpa: 28, ath: 0.009, days: 42, split: [0.22, 0.34, 0.24, 0.10, 0.10] },
  ];
  var LOCS = [
    { k: "Houston, TX", m: 1.0 },
    { k: "Chicago, IL", m: 1.15 },
    { k: "Phoenix, AZ", m: 0.95 },
    { k: "Remote", m: 1.25 },
  ];
  var CH = [0.37, 0.24, 0.19, 0.12, 0.08];

  var roleIdx = 0;
  var locIdx = 0;
  var budget = 50000;

  // Element handles
  var roleChips = card.querySelectorAll("[data-role]");
  var locChips = card.querySelectorAll("[data-loc]");
  var range = card.querySelector("[data-pe-range]");
  var budgetEl = card.querySelector("[data-pe-budget]");
  var cphEl = card.querySelector("[data-pe-cph]");
  var hiresEl = card.querySelector("[data-pe-hires]");
  var appsEl = card.querySelector("[data-pe-apps]");
  var daysEl = card.querySelector("[data-pe-days]");
  var barFills = card.querySelectorAll(".pe-bar-fill");
  var barAmts = card.querySelectorAll("[data-pe-amt]");

  function fmtDollar(n) {
    return "$" + Math.round(n).toLocaleString("en-US");
  }

  // Per-element rAF token store so rapid input cancels in-flight animations.
  var rafTokens = {};
  // Resilience: rAF is paused when the tab is hidden/backgrounded, so guarantee
  // the final values land via a setTimeout snap (mirrors the genplan card).
  var peSnapTimer = null;
  function animateNumber(el, key, from, to, fmt) {
    if (!el) return;
    if (rafTokens[key]) {
      cancelAnimationFrame(rafTokens[key]);
      rafTokens[key] = null;
    }
    if (prefersReducedMotion) {
      el.textContent = fmt(to);
      return;
    }
    var dur = 800;
    var start = null;
    function step(ts) {
      if (start === null) start = ts;
      var p = Math.min((ts - start) / dur, 1);
      var eased = 1 - Math.pow(1 - p, 3); // cubic ease-out
      el.textContent = fmt(from + (to - from) * eased);
      if (p < 1) {
        rafTokens[key] = requestAnimationFrame(step);
      } else {
        rafTokens[key] = null;
      }
    }
    rafTokens[key] = requestAnimationFrame(step);
  }

  function readNum(el) {
    if (!el) return 0;
    var n = parseFloat(String(el.textContent).replace(/[^0-9.]/g, ""));
    return isNaN(n) ? 0 : n;
  }

  function compute() {
    var role = ROLES[roleIdx];
    var loc = LOCS[locIdx];
    var apps = budget / (role.cpa * loc.m);
    var hires = Math.max(1, Math.round(apps * role.ath));
    var cph = budget / hires;
    var days = Math.round(role.days * loc.m);

    animateNumber(cphEl, "cph", readNum(cphEl), cph, fmtDollar);
    animateNumber(hiresEl, "hires", readNum(hiresEl), hires, function (v) {
      return Math.round(v).toLocaleString("en-US");
    });
    animateNumber(appsEl, "apps", readNum(appsEl), apps, function (v) {
      return Math.round(v).toLocaleString("en-US");
    });
    animateNumber(daysEl, "days", readNum(daysEl), days, function (v) {
      return String(Math.round(v));
    });

    var split = role.split || CH;
    barFills.forEach(function (fill, i) {
      fill.style.width = (split[i] * 100).toFixed(1) + "%";
    });
    barAmts.forEach(function (amtEl, i) {
      animateNumber(amtEl, "amt" + i, readNum(amtEl), budget * split[i], fmtDollar);
    });

    // Snap to final values after the count-up window, so the prediction is
    // correct even if rAF never ran (hidden/backgrounded tab).
    if (peSnapTimer) clearTimeout(peSnapTimer);
    peSnapTimer = setTimeout(function () {
      if (cphEl) cphEl.textContent = fmtDollar(cph);
      if (hiresEl) hiresEl.textContent = Math.round(hires).toLocaleString("en-US");
      if (appsEl) appsEl.textContent = Math.round(apps).toLocaleString("en-US");
      if (daysEl) daysEl.textContent = String(days);
      barAmts.forEach(function (amtEl, i) {
        if (amtEl) amtEl.textContent = fmtDollar(budget * split[i]);
      });
    }, 900);
  }

  // Role chips
  roleChips.forEach(function (chip) {
    chip.addEventListener("click", function () {
      roleIdx = parseInt(chip.getAttribute("data-role"), 10) || 0;
      roleChips.forEach(function (c) {
        c.classList.toggle("is-on", c === chip);
      });
      compute();
    });
  });

  // Location chips
  locChips.forEach(function (chip) {
    chip.addEventListener("click", function () {
      locIdx = parseInt(chip.getAttribute("data-loc"), 10) || 0;
      locChips.forEach(function (c) {
        c.classList.toggle("is-on", c === chip);
      });
      compute();
    });
  });

  // Budget range
  if (range) {
    range.addEventListener("input", function () {
      budget = parseInt(range.value, 10) || 0;
      if (budgetEl) budgetEl.textContent = fmtDollar(budget);
      compute();
    });
  }

  // Initial paint
  compute();
})();

// ── Hero eyebrow: cycling vertical word (Scale.com-style rotator) ──
// The first word is already "is-active" in markup, so reduced-motion /
// no-JS visitors see a correct static state without any script running.
(function () {
  "use strict";

  var cycleEl = document.getElementById("eyebrowCycle");
  if (!cycleEl) return;

  var words = cycleEl.querySelectorAll(".eyebrow-cycle-word");
  if (words.length < 2) return;

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  var idx = 0;
  var timerId = null;

  function advance() {
    words[idx].classList.remove("is-active");
    idx = (idx + 1) % words.length;
    words[idx].classList.add("is-active");
  }

  function start() {
    if (timerId) return;
    timerId = setInterval(advance, 2200);
  }

  function stop() {
    if (timerId) {
      clearInterval(timerId);
      timerId = null;
    }
  }

  document.addEventListener("visibilitychange", function () {
    if (document.hidden) {
      stop();
    } else {
      start();
    }
  });

  start();
})();
