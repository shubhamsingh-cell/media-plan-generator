/**
 * motion-engine.js — Corn Revolution-level motion system
 *
 * Adds life to every section:
 *   1. Parallax depth layers on scroll
 *   2. Mouse-following gradient spotlight on sections
 *   3. Scale-on-scroll for key elements
 *   4. Magnetic hover on CTAs
 *   5. Animated gradient mesh background
 *   6. Staggered text character reveals
 *   7. Continuous floating particles
 *
 * Zero dependencies. Pure CSS + JS. Respects prefers-reduced-motion.
 */
(function () {
  "use strict";

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  /* ═══════════════════════════════════════════
     1. ANIMATED GRADIENT MESH (hero background)
     ═══════════════════════════════════════════ */
  (function initGradientMesh() {
    // Disabled: gradient mesh orbs add noise behind the hero
    return;
    var hero = document.querySelector(".hero");
    if (!hero) return;

    var mesh = document.createElement("div");
    mesh.className = "gradient-mesh";
    mesh.setAttribute("aria-hidden", "true");
    mesh.innerHTML =
      '<div class="mesh-orb mesh-orb-1"></div>' +
      '<div class="mesh-orb mesh-orb-2"></div>' +
      '<div class="mesh-orb mesh-orb-3"></div>';
    hero.insertBefore(mesh, hero.firstChild);

    var style = document.createElement("style");
    style.textContent = [
      ".gradient-mesh {",
      "  position: absolute; inset: 0; overflow: hidden; z-index: 0;",
      "  pointer-events: none;",
      "}",
      ".mesh-orb {",
      "  position: absolute; border-radius: 50%; filter: blur(80px);",
      "  will-change: transform;",
      "}",
      ".mesh-orb-1 {",
      "  width: 600px; height: 600px; top: -20%; left: 30%;",
      "  background: radial-gradient(circle, rgba(90, 84, 189, 0.25) 0%, transparent 70%);",
      "  animation: mesh-drift-1 12s ease-in-out infinite;",
      "}",
      ".mesh-orb-2 {",
      "  width: 500px; height: 500px; top: 10%; right: 10%;",
      "  background: radial-gradient(circle, rgba(107, 179, 205, 0.18) 0%, transparent 70%);",
      "  animation: mesh-drift-2 15s ease-in-out infinite;",
      "}",
      ".mesh-orb-3 {",
      "  width: 400px; height: 400px; bottom: -10%; left: 10%;",
      "  background: radial-gradient(circle, rgba(90, 84, 189, 0.12) 0%, transparent 70%);",
      "  animation: mesh-drift-3 18s ease-in-out infinite;",
      "}",
      "/* Scroll-responsive parallax on mesh orbs */",
      ".gradient-mesh { transition: transform 0.5s ease-out; }",
      "@keyframes mesh-drift-1 {",
      "  0%, 100% { transform: translate(0, 0) scale(1); }",
      "  33% { transform: translate(60px, 40px) scale(1.1); }",
      "  66% { transform: translate(-40px, -20px) scale(0.95); }",
      "}",
      "@keyframes mesh-drift-2 {",
      "  0%, 100% { transform: translate(0, 0) scale(1); }",
      "  33% { transform: translate(-50px, 30px) scale(1.05); }",
      "  66% { transform: translate(30px, -50px) scale(1.1); }",
      "}",
      "@keyframes mesh-drift-3 {",
      "  0%, 100% { transform: translate(0, 0) scale(1); }",
      "  50% { transform: translate(40px, -30px) scale(1.15); }",
      "}",
    ].join("\n");
    document.head.appendChild(style);
  })();

  /* ═══════════════════════════════════════════
     2. MOUSE-FOLLOWING GRADIENT SPOTLIGHT
     ═══════════════════════════════════════════ */
  (function initMouseSpotlight() {
    var sections = document.querySelectorAll(
      ".products-section, .demo-section, .proof-section, .cta-section",
    );

    sections.forEach(function (section) {
      section.style.position = "relative";
      section.style.overflow = "hidden";

      var spot = document.createElement("div");
      spot.setAttribute("aria-hidden", "true");
      spot.style.cssText =
        "position:absolute;width:600px;height:600px;border-radius:50%;" +
        "background:radial-gradient(circle,rgba(90,84,189,0.06) 0%,transparent 70%);" +
        "pointer-events:none;z-index:0;transform:translate(-50%,-50%);" +
        "transition:left 0.3s ease-out,top 0.3s ease-out;opacity:0;" +
        "transition:left 0.3s ease-out,top 0.3s ease-out,opacity 0.3s ease;";
      section.appendChild(spot);

      section.addEventListener(
        "mousemove",
        function (e) {
          var rect = section.getBoundingClientRect();
          spot.style.left = e.clientX - rect.left + "px";
          spot.style.top = e.clientY - rect.top + "px";
          spot.style.opacity = "1";
        },
        { passive: true },
      );

      section.addEventListener(
        "mouseleave",
        function () {
          spot.style.opacity = "0";
        },
        { passive: true },
      );
    });
  })();

  /* ═══════════════════════════════════════════
     3. MAGNETIC HOVER ON CTA BUTTONS
     ═══════════════════════════════════════════ */
  (function initMagneticButtons() {
    var buttons = document.querySelectorAll(
      ".btn-primary, .btn-lg, .nav-cta, .demo-btn",
    );

    buttons.forEach(function (btn) {
      btn.style.transition =
        "transform 0.3s cubic-bezier(0.16, 1, 0.3, 1), box-shadow 0.3s ease";

      btn.addEventListener(
        "mousemove",
        function (e) {
          var rect = btn.getBoundingClientRect();
          var x = e.clientX - rect.left - rect.width / 2;
          var y = e.clientY - rect.top - rect.height / 2;
          btn.style.transform =
            "translate(" + x * 0.15 + "px, " + y * 0.15 + "px)";
        },
        { passive: true },
      );

      btn.addEventListener(
        "mouseleave",
        function () {
          btn.style.transform = "translate(0, 0)";
        },
        { passive: true },
      );
    });
  })();

  /* ═══════════════════════════════════════════
     4. PARALLAX DEPTH ON SCROLL
     ═══════════════════════════════════════════ */
  (function initParallax() {
    var parallaxEls = [];

    /* Hero glow */
    var heroGlow = document.querySelector(".hero::before");
    var heroBefore = document.querySelector(".hero");
    if (heroBefore) {
      parallaxEls.push({ el: heroBefore, speed: 0.3, prop: "before" });
    }

    /* Section labels float up faster */
    document.querySelectorAll(".section-label").forEach(function (el) {
      parallaxEls.push({ el: el, speed: -0.08 });
    });

    /* Product cards parallax */
    var planCard = document.querySelector(".product-plan");
    var novaCard = document.querySelector(".product-nova");
    if (planCard) parallaxEls.push({ el: planCard, speed: -0.04 });
    if (novaCard) parallaxEls.push({ el: novaCard, speed: 0.04 });

    if (!parallaxEls.length) return;

    var ticking = false;
    window.addEventListener(
      "scroll",
      function () {
        if (ticking) return;
        ticking = true;
        requestAnimationFrame(function () {
          var scrollY = window.scrollY;
          parallaxEls.forEach(function (item) {
            var rect = item.el.getBoundingClientRect();
            if (rect.bottom < -200 || rect.top > window.innerHeight + 200)
              return;
            var offset = scrollY * item.speed;
            item.el.style.transform = item.el.style.transform
              ? item.el.style.transform.replace(
                  /translateY\([^)]*\)/,
                  "translateY(" + offset + "px)",
                )
              : "translateY(" + offset + "px)";
          });
          ticking = false;
        });
      },
      { passive: true },
    );
  })();

  /* ═══════════════════════════════════════════
     5. FLOATING PARTICLES (subtle ambient dust)
     Barely-visible tiny dots that add depth without
     distracting from content. No glowing orbs.
     ═══════════════════════════════════════════ */
  (function initParticles() {
    var canvas = document.createElement("canvas");
    canvas.setAttribute("aria-hidden", "true");
    canvas.style.cssText =
      "position:fixed;inset:0;z-index:0;pointer-events:none;opacity:0.4;";
    document.body.appendChild(canvas);

    var ctx = canvas.getContext("2d");
    var particles = [];
    var PARTICLE_COUNT = 25;

    function resize() {
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
    }
    resize();
    window.addEventListener("resize", resize, { passive: true });

    for (var i = 0; i < PARTICLE_COUNT; i++) {
      particles.push({
        x: Math.random() * (canvas.width || 1400),
        y: Math.random() * (canvas.height || 900),
        r: Math.random() * 1.2 + 0.3,
        vx: (Math.random() - 0.5) * 0.15,
        vy: -Math.random() * 0.1 - 0.02,
        alpha: Math.random() * 0.15 + 0.03,
        pulse: Math.random() * Math.PI * 2,
        pulseSpeed: Math.random() * 0.01 + 0.003,
      });
    }

    var running = true;

    function animate() {
      if (!running) return;
      requestAnimationFrame(animate);

      ctx.clearRect(0, 0, canvas.width, canvas.height);

      for (var i = 0; i < particles.length; i++) {
        var p = particles[i];
        p.x += p.vx;
        p.y += p.vy;
        p.pulse += p.pulseSpeed;

        if (p.x < -10) p.x = canvas.width + 10;
        if (p.x > canvas.width + 10) p.x = -10;
        if (p.y < -10) p.y = canvas.height + 10;
        if (p.y > canvas.height + 10) p.y = -10;

        var alpha = p.alpha * (0.5 + 0.5 * Math.sin(p.pulse));

        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx.fillStyle = "rgba(255, 255, 255, " + alpha + ")";
        ctx.fill();
      }
    }
    animate();

    document.addEventListener("visibilitychange", function () {
      if (document.hidden) {
        running = false;
      } else {
        running = true;
        animate();
      }
    });
  })();

  /* ═══════════════════════════════════════════
     6. 3D TILT ON MOUSE MOVE (cards + dashboard)
     ═══════════════════════════════════════════ */
  (function initTilt() {
    var tiltEls = document.querySelectorAll(
      ".product-hero-card, .dash-window, .demo-card, .plan-showcase-frame",
    );

    tiltEls.forEach(function (el) {
      el.style.transformStyle = "preserve-3d";
      el.style.transition = "transform 0.4s cubic-bezier(0.16, 1, 0.3, 1)";

      el.addEventListener(
        "mousemove",
        function (e) {
          var rect = el.getBoundingClientRect();
          var x = (e.clientX - rect.left) / rect.width - 0.5;
          var y = (e.clientY - rect.top) / rect.height - 0.5;
          el.style.transform =
            "perspective(800px) rotateY(" +
            x * 8 +
            "deg) rotateX(" +
            -y * 8 +
            "deg) scale(1.02)";
        },
        { passive: true },
      );

      el.addEventListener(
        "mouseleave",
        function () {
          el.style.transform =
            "perspective(800px) rotateY(0deg) rotateX(0deg) scale(1)";
        },
        { passive: true },
      );
    });
  })();

  /* ═══════════════════════════════════════════
     7. SCROLL-TRIGGERED NUMBER COUNTERS
     ═══════════════════════════════════════════ */
  (function initCountUp() {
    var counters = document.querySelectorAll("[data-counter]");
    if (!counters.length) return;

    var observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          var el = entry.target;
          var target = parseFloat(el.dataset.target) || 0;
          var prefix = el.dataset.prefix || "";
          var suffix = el.dataset.suffix || "";
          var decimals = parseInt(el.dataset.decimals) || 0;
          var duration = 1500;
          var start = performance.now();

          function step(now) {
            var t = Math.min((now - start) / duration, 1);
            /* Ease out expo */
            var eased = t >= 1 ? 1 : 1 - Math.pow(2, -10 * t);
            var val = eased * target;
            el.textContent =
              prefix +
              (decimals > 0
                ? val.toFixed(decimals)
                : Math.round(val).toLocaleString()) +
              suffix;
            if (t < 1) requestAnimationFrame(step);
          }
          requestAnimationFrame(step);
          observer.unobserve(el);
        });
      },
      { threshold: 0.5 },
    );

    counters.forEach(function (el) {
      observer.observe(el);
    });
  })();

  /* ═══════════════════════════════════════════
     8. SCROLL PROGRESS BAR (top of page)
     ═══════════════════════════════════════════ */
  (function initScrollProgress() {
    var bar = document.createElement("div");
    bar.setAttribute("aria-hidden", "true");
    bar.style.cssText =
      "position:fixed;top:0;left:0;height:2px;z-index:9999;" +
      "background:linear-gradient(90deg,#5A54BD,#6BB3CD);" +
      "transform-origin:left;transform:scaleX(0);will-change:transform;" +
      "pointer-events:none;";
    document.body.appendChild(bar);

    window.addEventListener(
      "scroll",
      function () {
        var scrollTop = window.scrollY;
        var docHeight =
          document.documentElement.scrollHeight - window.innerHeight;
        var progress = docHeight > 0 ? scrollTop / docHeight : 0;
        bar.style.transform = "scaleX(" + progress + ")";
      },
      { passive: true },
    );
  })();
})();
