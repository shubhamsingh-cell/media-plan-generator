/**
 * Story Scroll — GSAP ScrollTrigger animation for "How Nova Works" section.
 * Pins the section and transitions between 5 moments as user scrolls.
 * Respects prefers-reduced-motion and falls back to vertical stack on mobile.
 */
function initStoryScroll() {
  "use strict";

  var storySection = document.querySelector(".story-scroll");
  if (!storySection) return;

  var moments = document.querySelectorAll(".story-moment");
  if (!moments.length) return;

  // ── Reduced motion: show all moments stacked vertically ──
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
    moments.forEach(function (m) {
      m.style.position = "relative";
      m.style.opacity = "1";
      m.style.pointerEvents = "auto";
      m.style.marginBottom = "120px";
    });
    // Make inner non-pinned
    var inner = storySection.querySelector(".story-scroll-inner");
    if (inner) {
      inner.style.height = "auto";
    }
    storySection.style.minHeight = "auto";
    return;
  }

  // ── Mobile: vertical stack with IntersectionObserver fade-in ──
  if (window.innerWidth < 768) {
    var inner = storySection.querySelector(".story-scroll-inner");
    if (inner) {
      inner.style.height = "auto";
    }
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

  // ── Desktop: GSAP ScrollTrigger pinned animation ──
  if (typeof gsap === "undefined" || typeof ScrollTrigger === "undefined") {
    // GSAP not loaded — fallback to showing all moments
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

  gsap.registerPlugin(ScrollTrigger);

  var momentArray = gsap.utils.toArray(".story-moment");

  // Pin the section
  var tl = gsap.timeline({
    scrollTrigger: {
      trigger: storySection,
      start: "top top",
      end: "+=" + momentArray.length * 100 + "%",
      pin: true,
      scrub: 0.8,
      anticipatePin: 1,
    },
  });

  // Transition between moments
  momentArray.forEach(function (moment, i) {
    if (i === 0) {
      // First moment starts visible
      tl.set(moment, { opacity: 1, pointerEvents: "auto" });
    }

    if (i < momentArray.length - 1) {
      // Fade out current
      tl.to(moment, {
        opacity: 0,
        y: -40,
        duration: 0.4,
        ease: "power2.in",
        pointerEvents: "none",
      });
      // Fade in next
      tl.fromTo(
        momentArray[i + 1],
        {
          opacity: 0,
          y: 50,
          pointerEvents: "none",
        },
        {
          opacity: 1,
          y: 0,
          duration: 0.4,
          ease: "power2.out",
          pointerEvents: "auto",
        },
        "-=0.15",
      );

      // Hold on each moment for a beat
      tl.to({}, { duration: 0.3 });
    }
  });

  // Moment 3 specific: animate SVG lines
  ScrollTrigger.create({
    trigger: storySection,
    start: "top+=" + 2 * 100 + "% top",
    end: "top+=" + 2.8 * 100 + "% top",
    onEnter: function () {
      gsap.to(".intel-line", {
        strokeDashoffset: 0,
        duration: 1.2,
        stagger: 0.15,
        ease: "power2.out",
      });
    },
  });

  // Moment 4 specific: animate dashboard build
  ScrollTrigger.create({
    trigger: storySection,
    start: "top+=" + 3 * 100 + "% top",
    onEnter: function () {
      // Grow bars
      gsap.to(".moment-4 .dash-bar-fill", {
        width: "var(--bar-width)",
        duration: 1,
        stagger: 0.1,
        ease: "power2.out",
      });
      // Slide in table rows
      gsap.to(".moment-4 .dash-table-row", {
        opacity: 1,
        y: 0,
        duration: 0.4,
        stagger: 0.08,
        ease: "power2.out",
      });
    },
  });
}

// Initialize when DOM is ready
document.addEventListener("DOMContentLoaded", initStoryScroll);
