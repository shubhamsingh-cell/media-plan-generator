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
    var currentMoment = 0;

    function showMoment(idx) {
      if (idx < 0 || idx >= moments.length) return;
      moments.forEach(function (m) {
        m.classList.remove("active");
      });
      moments[idx].classList.add("active");
      currentMoment = idx;
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

    // S48: Auto-advance story moments every 3s so users see all content
    var storyTimer = null;
    function startAutoAdvance() {
      storyTimer = setInterval(function () {
        var next = (currentMoment + 1) % moments.length;
        showMoment(next);
      }, 3000);
    }
    function resetAutoAdvance() {
      if (storyTimer) clearInterval(storyTimer);
      startAutoAdvance();
    }

    // Start when section is visible
    var storyObs = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (e) {
          if (e.isIntersecting) {
            startAutoAdvance();
          } else {
            if (storyTimer) clearInterval(storyTimer);
          }
        });
      },
      { threshold: 0.3 },
    );
    storyObs.observe(storyInner.closest(".story-scroll") || storyInner);

    // Pause on hover
    storyInner.addEventListener("mouseenter", function () {
      if (storyTimer) clearInterval(storyTimer);
    });
    storyInner.addEventListener("mouseleave", function () {
      startAutoAdvance();
    });

    // Update dots if they exist
    var origShowMoment = showMoment;
    showMoment = function (idx) {
      if (idx < 0 || idx >= moments.length) return;
      origShowMoment(idx);
      var dots = document.querySelectorAll(".story-dot");
      dots.forEach(function (d, i) {
        d.classList.toggle("active", i === idx);
      });
    };
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

  // ── Dashboard bar animation (using unified observer) ──
  var dashWindow = document.querySelector(".dash-window");
  if (dashWindow) {
    var dashObs = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            var fills = entry.target.querySelectorAll(".dash-bar-fill");
            fills.forEach(function (fill, i) {
              setTimeout(function () {
                fill.classList.add("animated");
              }, i * 200);
            });
            var rows = entry.target.querySelectorAll(".dash-table-row");
            rows.forEach(function (row, i) {
              setTimeout(
                function () {
                  row.classList.add("visible");
                },
                800 + i * 300,
              );
            });
            dashObs.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.3 },
    );
    dashObs.observe(dashWindow);
  }

  // ── Plan showcase bar animation ──
  var planShowcase = document.getElementById("plan-showcase");
  if (planShowcase) {
    var showcaseObs = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            var fills = entry.target.querySelectorAll(
              ".plan-showcase-bar-fill",
            );
            fills.forEach(function (fill, i) {
              setTimeout(function () {
                fill.classList.add("animated");
              }, i * 150);
            });
            showcaseObs.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.3 },
    );
    showcaseObs.observe(planShowcase);
  }

  // ── Header shrink on scroll (combined with aurora parallax via rAF) ──
  var nav = document.querySelector(".nav");
  /* auroraLayers parallax removed -- caused scroll vibration */
  var scrollTicking = false;
  window.addEventListener(
    "scroll",
    function () {
      if (!scrollTicking) {
        requestAnimationFrame(function () {
          if (window.scrollY > 60) {
            nav.style.background = "rgba(0, 0, 0, 0.92)";
            nav.style.height = "56px";
          } else {
            nav.style.background = "rgba(0, 0, 0, 0.8)";
            nav.style.height = "64px";
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
    plan: { color: "#7b75d4", bars: [70, 50, 35, 20], label: "Channel Mix" },
    nova: { color: "#6bb3cd", bars: [60, 80, 40, 55], label: "AI Chat" },
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
  if (heroTitle) {
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
      container.innerHTML =
        '<div style="text-align: center; padding: 40px 20px; color: var(--text-muted); font-size: 14px; border: 1px dashed rgba(255,255,255,0.08); border-radius: 12px; background: rgba(17,17,17,0.4);">' +
        '<svg width="80" height="80" viewBox="0 0 24 24" fill="none" stroke="rgba(255,255,255,0.2)" stroke-width="1.5" style="margin:0 auto 16px" aria-hidden="true"><path d="M12 8v4l3 3"/><circle cx="12" cy="12" r="10"/></svg>' +
        '<div style="font-weight: 500; margin-bottom: 4px;">No recent activity</div>' +
        "<div>Generate a media plan or chat with Nova to see your activity here.</div>" +
        "</div>";
      return;
    }

    var html = '<div style="display: flex; flex-direction: column; gap: 8px;">';

    for (var i = 0; i < plans.length; i++) {
      var p = plans[i];
      html +=
        '<a href="' +
        (p.url || "/media-plan") +
        '" style="' +
        "display: flex; align-items: center; gap: 14px; padding: 14px 18px;" +
        "background: rgba(32, 32, 88, 0.12); border: 1px solid rgba(255,255,255,0.05);" +
        "border-radius: 10px; text-decoration: none; color: inherit;" +
        "transition: border-color 0.2s, background 0.2s;" +
        "\" onmouseenter=\"this.style.borderColor='rgba(90,84,189,0.25)';this.style.background='rgba(32,32,88,0.2)'\" onmouseleave=\"this.style.borderColor='rgba(255,255,255,0.05)';this.style.background='rgba(32,32,88,0.12)'\">" +
        '<div style="width: 32px; height: 32px; border-radius: 8px; background: rgba(90,84,189,0.15); display: flex; align-items: center; justify-content: center; flex-shrink: 0; font-size: 14px;">&#128202;</div>' +
        '<div style="flex: 1; min-width: 0;">' +
        '<div style="font-size: 13px; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">' +
        (p.title || "Media Plan").replace(/</g, "&lt;") +
        "</div>" +
        '<div style="font-size: 11px; color: var(--text-muted);">' +
        (p.industry || "").replace(/</g, "&lt;") +
        (p.budget ? " &middot; " + p.budget.replace(/</g, "&lt;") : "") +
        "</div>" +
        "</div>" +
        '<div style="font-size: 11px; color: var(--text-muted); flex-shrink: 0;">' +
        _timeAgo(p.timestamp) +
        "</div>" +
        '<div style="font-size: 11px; color: var(--accent-light); flex-shrink: 0;">Open</div>' +
        "</a>";
    }

    for (var j = 0; j < chats.length; j++) {
      var c = chats[j];
      html +=
        '<a href="/nova" style="' +
        "display: flex; align-items: center; gap: 14px; padding: 14px 18px;" +
        "background: rgba(107, 179, 205, 0.06); border: 1px solid rgba(255,255,255,0.05);" +
        "border-radius: 10px; text-decoration: none; color: inherit;" +
        "transition: border-color 0.2s, background 0.2s;" +
        "\" onmouseenter=\"this.style.borderColor='rgba(107,179,205,0.25)';this.style.background='rgba(107,179,205,0.1)'\" onmouseleave=\"this.style.borderColor='rgba(255,255,255,0.05)';this.style.background='rgba(107,179,205,0.06)'\">" +
        '<div style="width: 32px; height: 32px; border-radius: 8px; background: rgba(107,179,205,0.15); display: flex; align-items: center; justify-content: center; flex-shrink: 0; font-size: 14px;">&#128172;</div>' +
        '<div style="flex: 1; min-width: 0;">' +
        '<div style="font-size: 13px; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">' +
        (c.title || "Nova Chat").replace(/</g, "&lt;") +
        "</div>" +
        '<div style="font-size: 11px; color: var(--text-muted);">Chatbot conversation</div>' +
        "</div>" +
        '<div style="font-size: 11px; color: var(--text-muted); flex-shrink: 0;">' +
        _timeAgo(c.timestamp) +
        "</div>" +
        '<div style="font-size: 11px; color: var(--teal); flex-shrink: 0;">Open</div>' +
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
    "/static/js/story-scroll.js?v=3.1.0",
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
