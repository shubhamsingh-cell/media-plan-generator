/**
 * role-cycling.js — Smooth role/location/budget cycling across homepage
 *
 * Cycles through different recruitment scenarios every 3.5 seconds with
 * fade-slide transitions. Affects:
 *   - Moment 1 heading + chaos cards
 *   - Moment 2 form fields
 *   - Plan showcase (metrics + bars)
 *
 * No dependencies. Respects prefers-reduced-motion.
 */
(function () {
  "use strict";

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  var CYCLE_MS = 3500;
  var TRANSITION_MS = 500;

  var roles = [
    {
      role: "Registered Nurse",
      location: "Houston, TX",
      budget: "$50,000",
      budgetNum: 50000,
      cpa: "$24.30",
      apps: "847",
      confidence: "95%",
      channels: [
        { name: "Indeed", pct: 72, amount: "$18,000", color: "var(--accent)" },
        { name: "LinkedIn", pct: 55, amount: "$13,750", color: "#06b6d4" },
        {
          name: "ZipRecruiter",
          pct: 38,
          amount: "$9,500",
          color: "var(--green)",
        },
        {
          name: "Glassdoor",
          pct: 25,
          amount: "$6,250",
          color: "var(--amber, #f59e0b)",
        },
        { name: "Programmatic", pct: 10, amount: "$12,500", color: "#8b5cf6" },
      ],
    },
    {
      role: "Software Engineer",
      location: "San Francisco, CA",
      budget: "$80,000",
      budgetNum: 80000,
      cpa: "$48.50",
      apps: "624",
      confidence: "93%",
      channels: [
        { name: "LinkedIn", pct: 68, amount: "$27,200", color: "#06b6d4" },
        { name: "Indeed", pct: 42, amount: "$16,800", color: "var(--accent)" },
        { name: "GitHub Jobs", pct: 30, amount: "$12,000", color: "#22c55e" },
        { name: "Stack Overflow", pct: 22, amount: "$8,800", color: "#f59e0b" },
        { name: "Programmatic", pct: 19, amount: "$15,200", color: "#8b5cf6" },
      ],
    },
    {
      role: "Sales Associate",
      location: "Chicago, IL",
      budget: "$25,000",
      budgetNum: 25000,
      cpa: "$12.80",
      apps: "1,953",
      confidence: "97%",
      channels: [
        { name: "Indeed", pct: 80, amount: "$10,000", color: "var(--accent)" },
        {
          name: "ZipRecruiter",
          pct: 52,
          amount: "$6,500",
          color: "var(--green)",
        },
        { name: "Facebook", pct: 36, amount: "$4,500", color: "#3b82f6" },
        { name: "Glassdoor", pct: 20, amount: "$2,500", color: "#f59e0b" },
        { name: "Craigslist", pct: 12, amount: "$1,500", color: "#94a3b8" },
      ],
    },
    {
      role: "Truck Driver",
      location: "Dallas, TX",
      budget: "$35,000",
      budgetNum: 35000,
      cpa: "$18.40",
      apps: "1,902",
      confidence: "96%",
      channels: [
        { name: "Indeed", pct: 75, amount: "$13,125", color: "var(--accent)" },
        { name: "CDLjobs.com", pct: 48, amount: "$8,400", color: "#06b6d4" },
        { name: "Facebook", pct: 32, amount: "$5,600", color: "#3b82f6" },
        {
          name: "ZipRecruiter",
          pct: 24,
          amount: "$4,200",
          color: "var(--green)",
        },
        { name: "Programmatic", pct: 21, amount: "$3,675", color: "#8b5cf6" },
      ],
    },
    {
      role: "Data Scientist",
      location: "New York, NY",
      budget: "$60,000",
      budgetNum: 60000,
      cpa: "$52.10",
      apps: "415",
      confidence: "91%",
      channels: [
        { name: "LinkedIn", pct: 72, amount: "$21,600", color: "#06b6d4" },
        { name: "Indeed", pct: 40, amount: "$12,000", color: "var(--accent)" },
        { name: "Glassdoor", pct: 30, amount: "$9,000", color: "#f59e0b" },
        { name: "AngelList", pct: 22, amount: "$6,600", color: "#22c55e" },
        { name: "Programmatic", pct: 18, amount: "$10,800", color: "#8b5cf6" },
      ],
    },
    {
      role: "Warehouse Worker",
      location: "Phoenix, AZ",
      budget: "$20,000",
      budgetNum: 20000,
      cpa: "$8.90",
      apps: "2,247",
      confidence: "98%",
      channels: [
        { name: "Indeed", pct: 85, amount: "$8,500", color: "var(--accent)" },
        { name: "Facebook", pct: 50, amount: "$5,000", color: "#3b82f6" },
        { name: "Craigslist", pct: 28, amount: "$2,800", color: "#94a3b8" },
        {
          name: "ZipRecruiter",
          pct: 20,
          amount: "$2,000",
          color: "var(--green)",
        },
        { name: "Programmatic", pct: 17, amount: "$1,700", color: "#8b5cf6" },
      ],
    },
  ];

  var currentIdx = 0;
  var isTransitioning = false;

  /* ── Helper: animate text swap with fade ── */
  function fadeSwapText(el, newText) {
    if (!el) return;
    el.style.transition =
      "opacity " +
      TRANSITION_MS / 2 +
      "ms ease, transform " +
      TRANSITION_MS / 2 +
      "ms ease";
    el.style.opacity = "0";
    el.style.transform = "translateY(6px)";
    setTimeout(function () {
      el.textContent = newText;
      el.style.opacity = "1";
      el.style.transform = "translateY(0)";
    }, TRANSITION_MS / 2);
  }

  function fadeSwapHTML(el, newHTML) {
    if (!el) return;
    el.style.transition =
      "opacity " +
      TRANSITION_MS / 2 +
      "ms ease, transform " +
      TRANSITION_MS / 2 +
      "ms ease";
    el.style.opacity = "0";
    el.style.transform = "translateY(6px)";
    setTimeout(function () {
      el.innerHTML = newHTML;
      el.style.opacity = "1";
      el.style.transform = "translateY(0)";
    }, TRANSITION_MS / 2);
  }

  /* ── Moment 1: heading + chaos cards ── */
  function updateMoment1(role) {
    var h2 = document.querySelector(".moment-1 .moment-text h2");
    var p = document.querySelector(".moment-1 .moment-text p");

    var headings = {
      "Registered Nurse": "You need 50 nurses across Texas.",
      "Software Engineer": "You need 20 engineers in the Bay Area.",
      "Sales Associate": "You need 100 sales reps in Chicago.",
      "Truck Driver": "You need 75 drivers across Texas.",
      "Data Scientist": "You need 10 data scientists in NYC.",
      "Warehouse Worker": "You need 200 warehouse staff in Phoenix.",
    };

    var subtexts = {
      "Registered Nurse":
        "$50K budget. 12 possible platforms. No clear data on what works.",
      "Software Engineer":
        "$80K budget. 15+ job boards. Fierce competition for talent.",
      "Sales Associate":
        "$25K budget. High volume hiring. Which channels convert?",
      "Truck Driver":
        "$35K budget. Niche boards vs. general sites. What actually works?",
      "Data Scientist":
        "$60K budget. Specialized talent pool. Every dollar must count.",
      "Warehouse Worker":
        "$20K budget. Speed matters. 200 hires needed yesterday.",
    };

    fadeSwapText(h2, headings[role.role] || headings["Registered Nurse"]);
    fadeSwapText(p, subtexts[role.role] || subtexts["Registered Nurse"]);

    /* Update chaos cards */
    var cards = document.querySelectorAll(".moment-1 .chaos-card");
    var cardTexts = [
      role.channels[0] ? role.channels[0].name : "Indeed",
      role.channels[1] ? role.channels[1].name : "LinkedIn",
      role.budget,
      role.channels[2] ? role.channels[2].name : "ZipRecruiter",
      role.role.split(" ").length > 2
        ? role.role.split(" ").slice(0, 2).join(" ")
        : role.role,
      role.location.split(",")[0],
      role.channels[3] ? role.channels[3].name : "Glassdoor",
    ];
    cards.forEach(function (card, i) {
      if (cardTexts[i]) fadeSwapText(card, cardTexts[i]);
    });
  }

  /* ── Moment 2: form fields ── */
  function updateMoment2(role) {
    var values = document.querySelectorAll(".moment-2 .mini-value");
    if (values.length >= 3) {
      fadeSwapText(values[0], role.role);
      fadeSwapText(values[1], role.location);
      fadeSwapText(values[2], role.budget);
    }
  }

  /* ── Plan showcase ── */
  function updatePlanShowcase(role) {
    var title = document.querySelector(".plan-showcase-title");
    if (title) {
      fadeSwapText(title, role.role + " \u2014 " + role.location);
    }

    var metricValues = document.querySelectorAll(".plan-showcase-metric-value");
    if (metricValues.length >= 4) {
      fadeSwapText(metricValues[0], role.budget);
      fadeSwapText(metricValues[1], role.cpa);
      fadeSwapText(metricValues[2], role.apps);
      fadeSwapText(metricValues[3], role.confidence);
    }

    /* Update channel bars */
    var channelEls = document.querySelectorAll(".plan-showcase-channel");
    channelEls.forEach(function (chEl, i) {
      if (!role.channels[i]) return;
      var ch = role.channels[i];

      var nameEl = chEl.querySelector(".plan-showcase-channel-name");
      var barFill = chEl.querySelector(".plan-showcase-bar-fill");
      var amountEl = chEl.querySelector(".plan-showcase-channel-amount");

      if (nameEl) fadeSwapText(nameEl, ch.name);
      if (amountEl) fadeSwapText(amountEl, ch.amount);

      if (barFill) {
        barFill.style.transition =
          "width 1s cubic-bezier(0.16, 1, 0.3, 1), background 0.5s ease";
        barFill.style.width = ch.pct + "%";
        barFill.style.background = ch.color;
      }
    });
  }

  /* ── Cycle ── */
  function cycle() {
    if (isTransitioning) return;
    isTransitioning = true;

    currentIdx = (currentIdx + 1) % roles.length;
    var role = roles[currentIdx];

    updateMoment1(role);
    updateMoment2(role);
    updatePlanShowcase(role);

    setTimeout(function () {
      isTransitioning = false;
    }, TRANSITION_MS + 100);
  }

  /* Start cycling after a delay */
  var intervalId = null;

  function startCycling() {
    if (intervalId) return;
    intervalId = setInterval(cycle, CYCLE_MS);
  }

  function stopCycling() {
    if (intervalId) {
      clearInterval(intervalId);
      intervalId = null;
    }
  }

  /* Only cycle when page is visible */
  document.addEventListener("visibilitychange", function () {
    if (document.hidden) {
      stopCycling();
    } else {
      startCycling();
    }
  });

  /* Start after 4 seconds (let page load + user see first state) */
  setTimeout(startCycling, 4000);
})();
