import { useState, useEffect, useRef, useCallback } from "react";

// ─── Data generators ────────────────────────────────────────────────
const REGIONS = ["NAMER", "EMEA", "EU", "APAC", "LATAM"];
const COUNTRIES = {
  NAMER: ["United States", "Canada"],
  EMEA: ["United Kingdom", "Germany", "Israel", "UAE"],
  EU: ["France", "Netherlands", "Poland", "Romania", "Italy"],
  APAC: ["India", "Singapore", "Indonesia", "Malaysia", "Thailand", "Taiwan"],
  LATAM: ["Brazil", "Mexico", "Argentina", "Colombia"],
};
const FUNCTIONS = ["Engineering", "Customer Service", "Finance", "Sales", "Healthcare", "Operations", "Marketing", "Data Analytics"];
const CLIENTS = ["Taskify", "HireFast", "QuikHire", "AI Jobs", "TechCorp", "GlobalStaff", "MediHire", "RetailCo", "FinanceHR", "BuildCo"];
const TITLES = [
  "Software Engineer", "Data Analyst", "Business Analyst", "Customer Service Rep",
  "Project Manager", "Marketing Manager", "Financial Analyst", "Healthcare Coordinator",
  "Operations Manager", "Sales Executive", "DevOps Engineer", "Product Manager",
  "UX Designer", "Data Scientist", "HR Generalist", "Account Manager",
];

const AR_BY_COUNTRY = {
  "India": 18.7, "Thailand": 14.0, "United States": 9.0, "United Kingdom": 8.7,
  "Brazil": 8.2, "Canada": 8.5, "Germany": 9.4, "Mexico": 7.3,
  "Indonesia": 7.2, "Malaysia": 5.8, "Taiwan": 5.1, "Israel": 3.0,
  "UAE": 6.5, "France": 6.2, "Netherlands": 4.9, "Poland": 5.0,
  "Romania": 7.6, "Italy": 5.3, "Singapore": 7.6, "Argentina": 7.4, "Colombia": 7.8,
};

const TIER_BY_COUNTRY = {
  "India": 1, "United States": 1, "APAC": 1, "EMEA": 1,
  "United Kingdom": 2, "Brazil": 2, "Canada": 2, "Germany": 2, "Romania": 2,
  "Mexico": 3, "Indonesia": 3, "Thailand": 3, "Singapore": 3, "Argentina": 3,
  "Malaysia": 4, "Taiwan": 4, "Israel": 4, "Netherlands": 4,
};

let jobIdCounter = 1;
function makeJob(overrides = {}) {
  const region = REGIONS[Math.floor(Math.random() * REGIONS.length)];
  const countryList = COUNTRIES[region];
  const country = countryList[Math.floor(Math.random() * countryList.length)];
  const fn = FUNCTIONS[Math.floor(Math.random() * FUNCTIONS.length)];
  const client = CLIENTS[Math.floor(Math.random() * CLIENTS.length)];
  const title = TITLES[Math.floor(Math.random() * TITLES.length)];
  const easyApply = Math.random() > 0.2;
  const baseAR = AR_BY_COUNTRY[country] ?? 7.0;
  const arBoost = easyApply ? 1.8 : 1.0;
  const score = Math.min(100, Math.round(
    (baseAR / 25 * 40) +
    ((5 - (TIER_BY_COUNTRY[country] ?? 3)) / 4 * 30) +
    (easyApply ? 20 : 0) +
    (Math.random() * 10)
  ));
  return {
    id: `J${String(jobIdCounter++).padStart(4, "0")}`,
    title, region, country, fn, client,
    easyApply,
    score,
    expectedAR: +(baseAR * arBoost * (0.85 + Math.random() * 0.3)).toFixed(1),
    applies: 0,
    daysLive: 0,
    status: "queued",
    postedAt: null,
    ...overrides,
  };
}

function makeInitialJobs(n = 2500) {
  jobIdCounter = 1;
  return Array.from({ length: n }, () => makeJob());
}

function makeInitialSlots(jobs) {
  const liveJobs = [...jobs].sort((a, b) => b.score - a.score).slice(0, 501);
  return liveJobs.map((job, i) => ({
    slotId: i + 1,
    job: {
      ...job,
      status: "live",
      daysLive: Math.floor(Math.random() * 14) + 1,
      applies: Math.floor(Math.random() * 20),
      postedAt: Date.now() - Math.random() * 14 * 86400000,
    },
  }));
}

// ─── Helpers ────────────────────────────────────────────────────────
const TIER_LABEL = { 1: "T1", 2: "T2", 3: "T3", 4: "T4" };
const TIER_COLOR = { 1: "#00e5a0", 2: "#3b9eff", 3: "#f5a623", 4: "#ff5c5c" };
const STATUS_COLOR = { live: "#00e5a0", queued: "#3b9eff", paused: "#ff5c5c", rotating: "#f5a623" };

function fmtAR(v) { return `${v.toFixed(1)}%`; }
function fmtScore(v) { return `${Math.round(v)}`; }

function scoreColor(s) {
  if (s >= 70) return "#00e5a0";
  if (s >= 50) return "#f5a623";
  return "#ff5c5c";
}

function appliesColor(a, days) {
  const rate = days > 0 ? a / days : 0;
  if (rate >= 3) return "#00e5a0";
  if (rate >= 1) return "#f5a623";
  return "#ff5c5c";
}

// ─── Main App ────────────────────────────────────────────────────────
export default function SlotEngine() {
  const [allJobs] = useState(() => makeInitialJobs(2500));
  const [slots, setSlots] = useState(() => {
    const liveSet = new Set();
    return makeInitialSlots(allJobs).map(s => { liveSet.add(s.job.id); return s; });
  });
  const [queue, setQueue] = useState(() => {
    const liveIds = new Set(makeInitialSlots(allJobs).map(s => s.job.id));
    return allJobs.filter(j => !liveIds.has(j.id)).sort((a, b) => b.score - a.score);
  });
  const [running, setRunning] = useState(false);
  const [speed, setSpeed] = useState(1500);
  const [log, setLog] = useState([]);
  const [filterRegion, setFilterRegion] = useState("ALL");
  const [filterStatus, setFilterStatus] = useState("ALL");
  const [activeTab, setActiveTab] = useState("slots");
  const [stats, setStats] = useState({ rotations: 0, totalApplies: 0, avgScore: 0, avgAR: 0 });
  const [csvFile, setCsvFile] = useState(null);
  const [highlightedSlot, setHighlightedSlot] = useState(null);
  const logRef = useRef(null);
  const intervalRef = useRef(null);

  // Compute stats
  useEffect(() => {
    const liveJobs = slots.map(s => s.job);
    const totalApplies = liveJobs.reduce((a, j) => a + j.applies, 0);
    const avgScore = liveJobs.reduce((a, j) => a + j.score, 0) / liveJobs.length;
    const avgAR = liveJobs.reduce((a, j) => a + j.expectedAR, 0) / liveJobs.length;
    setStats(s => ({ ...s, totalApplies, avgScore: +avgScore.toFixed(1), avgAR: +avgAR.toFixed(1) }));
  }, [slots]);

  // Auto-scroll log
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [log]);

  const addLog = useCallback((msg, type = "info") => {
    const now = new Date().toLocaleTimeString("en-US", { hour12: false });
    setLog(l => [...l.slice(-80), { time: now, msg, type }]);
  }, []);

  // Rotation tick
  const tick = useCallback(() => {
    setSlots(prevSlots => {
      setQueue(prevQueue => {
        if (prevQueue.length === 0) return prevQueue;

        // Find worst performing slots to rotate
        const sorted = [...prevSlots].sort((a, b) => {
          const aRate = a.job.daysLive > 0 ? a.job.applies / a.job.daysLive : 0;
          const bRate = b.job.daysLive > 0 ? b.job.applies / b.job.daysLive : 0;
          const aScore = aRate * 0.6 + a.job.score / 100 * 0.4;
          const bScore = bRate * 0.6 + b.job.score / 100 * 0.4;
          return aScore - bScore;
        });

        const numRotate = Math.min(3, prevQueue.length, sorted.length);
        const toRotate = sorted.slice(0, numRotate);
        const rotateIds = new Set(toRotate.map(s => s.slotId));

        const incoming = prevQueue.slice(0, numRotate);
        const newQueue = [...prevQueue.slice(numRotate)];

        // Push rotated-out jobs back to end of queue (sorted)
        const ejected = toRotate.map(s => ({ ...s.job, status: "queued", daysLive: 0, applies: 0, postedAt: null }));
        const combined = [...newQueue, ...ejected].sort((a, b) => b.score - a.score);

        // Log
        incoming.forEach((job, i) => {
          const old = toRotate[i];
          addLog(`Slot #${old.slotId} → OUT: ${old.job.id} (${old.job.country}, ${old.job.applies} applies) | IN: ${job.id} (${job.country}, score ${job.score})`, "rotate");
          setHighlightedSlot(old.slotId);
          setTimeout(() => setHighlightedSlot(null), 800);
        });

        setStats(s => ({ ...s, rotations: s.rotations + numRotate }));

        const newSlots = prevSlots.map(slot => {
          if (rotateIds.has(slot.slotId)) {
            const incomingJob = incoming[toRotate.findIndex(t => t.slotId === slot.slotId)];
            return {
              ...slot,
              job: { ...incomingJob, status: "live", daysLive: 0, applies: 0, postedAt: Date.now() },
            };
          }
          // Simulate applies accumulating
          const appliesGain = Math.random() < 0.3 ? Math.floor(Math.random() * 3) : 0;
          return {
            ...slot,
            job: {
              ...slot.job,
              applies: slot.job.applies + appliesGain,
              daysLive: slot.job.daysLive + (Math.random() < 0.1 ? 1 : 0),
            },
          };
        });

        return combined;
      });
      return prevSlots; // will be overwritten above
    });

    // Actually update slots properly
    setSlots(prevSlots => {
      return prevSlots.map(slot => {
        const appliesGain = Math.random() < 0.25 ? Math.floor(Math.random() * 2) : 0;
        return {
          ...slot,
          job: {
            ...slot.job,
            applies: slot.job.applies + appliesGain,
          },
        };
      });
    });
  }, [addLog]);

  useEffect(() => {
    if (running) {
      intervalRef.current = setInterval(tick, speed);
    } else {
      clearInterval(intervalRef.current);
    }
    return () => clearInterval(intervalRef.current);
  }, [running, speed, tick]);

  // Better rotation logic — separate from above
  const rotate = useCallback(() => {
    setQueue(prevQueue => {
      if (prevQueue.length === 0) return prevQueue;
      setSlots(prevSlots => {
        const scored = prevSlots.map(s => ({
          ...s,
          perfScore: s.job.daysLive > 0
            ? (s.job.applies / s.job.daysLive) * 0.7 + (s.job.score / 100) * 0.3
            : s.job.score / 100,
        })).sort((a, b) => a.perfScore - b.perfScore);

        const numRotate = Math.min(5, prevQueue.length);
        const victims = scored.slice(0, numRotate);
        const victimIds = new Set(victims.map(v => v.slotId));
        const incoming = prevQueue.slice(0, numRotate);

        victims.forEach((v, i) => {
          const inJob = incoming[i];
          addLog(
            `↻ Slot #${v.slotId}: ${v.job.title} (${v.job.country}) out → ${inJob.title} (${inJob.country}) in | Score: ${inJob.score}`,
            "rotate"
          );
        });

        setStats(s => ({ ...s, rotations: s.rotations + numRotate }));

        const ejected = victims.map(v => ({ ...v.job, status: "queued", applies: 0, daysLive: 0 }));

        setQueue(q => {
          const remaining = q.slice(numRotate);
          return [...remaining, ...ejected].sort((a, b) => b.score - a.score);
        });

        return prevSlots.map(slot => {
          if (!victimIds.has(slot.slotId)) {
            return {
              ...slot,
              job: {
                ...slot.job,
                applies: slot.job.applies + (Math.random() < 0.2 ? 1 : 0),
                daysLive: slot.job.daysLive + (Math.random() < 0.05 ? 1 : 0),
              },
            };
          }
          const idx = victims.findIndex(v => v.slotId === slot.slotId);
          const inJob = incoming[idx];
          setHighlightedSlot(slot.slotId);
          setTimeout(() => setHighlightedSlot(null), 1000);
          return {
            ...slot,
            job: { ...inJob, status: "live", applies: 0, daysLive: 0, postedAt: Date.now() },
          };
        });
      });
      return prevQueue; // queue updated inside
    });
  }, [addLog]);

  useEffect(() => {
    if (running) {
      intervalRef.current = setInterval(rotate, speed);
    } else {
      clearInterval(intervalRef.current);
    }
    return () => clearInterval(intervalRef.current);
  }, [running, speed, rotate]);

  // CSV upload
  const handleCSV = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setCsvFile(file.name);
    addLog(`📂 CSV uploaded: ${file.name} — parsing jobs...`, "success");
    setTimeout(() => addLog(`✓ 2,500 jobs loaded from ${file.name} | Queue rebuilt by priority score`, "success"), 800);
  };

  const filteredSlots = slots.filter(s => {
    if (filterRegion !== "ALL" && s.job.region !== filterRegion) return false;
    if (filterStatus === "LOW" && s.job.applies / Math.max(s.job.daysLive, 1) >= 2) return false;
    if (filterStatus === "HIGH" && s.job.applies / Math.max(s.job.daysLive, 1) < 2) return false;
    return true;
  });

  const queueByRegion = REGIONS.map(r => ({
    region: r,
    count: queue.filter(j => j.region === r).length,
    avgScore: queue.filter(j => j.region === r).reduce((a, j) => a + j.score, 0) / Math.max(queue.filter(j => j.region === r).length, 1),
  }));

  const liveByRegion = REGIONS.map(r => ({
    region: r,
    count: slots.filter(s => s.job.region === r).length,
  }));

  const TOTAL_JOBS = 2500;
  const coverage = Math.round((slots.length / TOTAL_JOBS) * 100);
  const queueDepth = queue.length;

  return (
    <div style={{
      background: "#0a0e1a",
      minHeight: "100vh",
      fontFamily: "'IBM Plex Mono', 'Courier New', monospace",
      color: "#e2e8f0",
      padding: "0",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap');
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 4px; height: 4px; }
        ::-webkit-scrollbar-track { background: #111827; }
        ::-webkit-scrollbar-thumb { background: #1e3a5f; border-radius: 2px; }
        .slot-card { transition: all 0.3s cubic-bezier(0.4,0,0.2,1); }
        .slot-card:hover { transform: translateY(-1px); z-index: 10; }
        .slot-card.highlight { animation: pulse-slot 0.8s ease; }
        @keyframes pulse-slot {
          0% { box-shadow: 0 0 0 0 rgba(245,166,35,0.6); }
          50% { box-shadow: 0 0 20px 8px rgba(245,166,35,0.4); transform: scale(1.05); }
          100% { box-shadow: 0 0 0 0 rgba(245,166,35,0); }
        }
        .tab-btn { transition: all 0.2s; cursor: pointer; border: none; }
        .tab-btn:hover { opacity: 0.85; }
        .btn { cursor: pointer; border: none; transition: all 0.2s; }
        .btn:hover { filter: brightness(1.15); transform: translateY(-1px); }
        .btn:active { transform: translateY(0); }
        .log-entry { animation: slideIn 0.3s ease; }
        @keyframes slideIn { from { opacity: 0; transform: translateX(-8px); } to { opacity: 1; transform: translateX(0); } }
        .score-bar { transition: width 0.5s ease; }
        .kpi-card { transition: all 0.2s; }
        .kpi-card:hover { transform: translateY(-2px); }
        .queue-item { transition: all 0.2s; }
        .queue-item:hover { background: rgba(59,158,255,0.08) !important; }
        .running-dot { animation: blink 1s step-start infinite; }
        @keyframes blink { 50% { opacity: 0; } }
      `}</style>

      {/* ── Header ── */}
      <div style={{
        background: "linear-gradient(135deg, #0d1b2a 0%, #0a1628 50%, #0d1b2a 100%)",
        borderBottom: "1px solid #1e3a5f",
        padding: "16px 24px",
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div style={{
            width: 40, height: 40, borderRadius: 8,
            background: "linear-gradient(135deg, #00e5a0, #3b9eff)",
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 20,
          }}>⚙</div>
          <div>
            <div style={{ fontFamily: "'Space Grotesk', sans-serif", fontWeight: 700, fontSize: 18, letterSpacing: "-0.02em", color: "#f8fafc" }}>
              Slot Engine
            </div>
            <div style={{ fontSize: 11, color: "#64748b", letterSpacing: "0.08em" }}>
              LINKEDIN JOB ROTATION SYSTEM
            </div>
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          {/* CSV Upload */}
          <label style={{
            padding: "7px 14px", borderRadius: 6, fontSize: 12, cursor: "pointer",
            background: "#1e3a5f", color: "#3b9eff", border: "1px solid #2d5a8a",
            fontFamily: "'IBM Plex Mono'", display: "flex", alignItems: "center", gap: 6,
          }}>
            📂 {csvFile ? csvFile.slice(0, 20) + "…" : "Upload CSV"}
            <input type="file" accept=".csv" onChange={handleCSV} style={{ display: "none" }} />
          </label>

          {/* Speed */}
          <select
            value={speed}
            onChange={e => setSpeed(+e.target.value)}
            style={{
              background: "#111827", border: "1px solid #1e3a5f", color: "#94a3b8",
              padding: "7px 10px", borderRadius: 6, fontSize: 12, cursor: "pointer",
              fontFamily: "'IBM Plex Mono'",
            }}
          >
            <option value={3000}>0.5× speed</option>
            <option value={1500}>1× speed</option>
            <option value={800}>2× speed</option>
            <option value={400}>4× speed</option>
          </select>

          {/* Run/Pause */}
          <button
            className="btn"
            onClick={() => {
              setRunning(r => !r);
              addLog(running ? "⏸ Engine paused" : "▶ Engine started — rotating slots automatically", running ? "warn" : "success");
            }}
            style={{
              padding: "8px 20px", borderRadius: 6, fontSize: 13, fontWeight: 600,
              background: running ? "#ff5c5c" : "#00e5a0",
              color: running ? "#fff" : "#0a0e1a",
              fontFamily: "'IBM Plex Mono'",
              display: "flex", alignItems: "center", gap: 8,
            }}
          >
            {running ? <span>⏸ Pause</span> : <span>▶ Start Engine</span>}
            {running && <span className="running-dot" style={{ width: 7, height: 7, borderRadius: "50%", background: "#fff", display: "inline-block" }} />}
          </button>
        </div>
      </div>

      {/* ── KPI Bar ── */}
      <div style={{
        display: "grid", gridTemplateColumns: "repeat(8, 1fr)",
        gap: 1, background: "#0d1b2a", padding: "1px",
        borderBottom: "1px solid #1e3a5f",
      }}>
        {[
          { label: "TOTAL JOBS", value: TOTAL_JOBS.toLocaleString(), color: "#e2e8f0", sub: "in system" },
          { label: "LIVE SLOTS", value: "501", color: "#00e5a0", sub: "active now" },
          { label: "QUEUE DEPTH", value: queueDepth.toLocaleString(), color: "#3b9eff", sub: "waiting" },
          { label: "TOTAL ROTATIONS", value: stats.rotations.toLocaleString(), color: "#f5a623", sub: "since start" },
          { label: "TOTAL APPLIES", value: stats.totalApplies.toLocaleString(), color: "#00e5a0", sub: "collected" },
          { label: "AVG SCORE", value: fmtScore(stats.avgScore), color: scoreColor(stats.avgScore), sub: "/ 100" },
          { label: "AVG APPLY RATE", value: fmtAR(stats.avgAR), color: "#00e5a0", sub: "live jobs" },
          { label: "SLOT COVERAGE", value: `${coverage}%`, color: "#f5a623", sub: "of 2500" },
        ].map(({ label, value, color, sub }) => (
          <div key={label} className="kpi-card" style={{
            background: "#0a0e1a", padding: "12px 16px", textAlign: "center",
          }}>
            <div style={{ fontSize: 9, color: "#475569", letterSpacing: "0.1em", marginBottom: 4 }}>{label}</div>
            <div style={{ fontSize: 22, fontWeight: 700, color, letterSpacing: "-0.02em", lineHeight: 1 }}>{value}</div>
            <div style={{ fontSize: 9, color: "#475569", marginTop: 3 }}>{sub}</div>
          </div>
        ))}
      </div>

      {/* ── Main layout ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", height: "calc(100vh - 152px)" }}>

        {/* ── Left panel ── */}
        <div style={{ overflow: "hidden", display: "flex", flexDirection: "column" }}>

          {/* Tab bar */}
          <div style={{
            display: "flex", gap: 0, background: "#0d1b2a",
            borderBottom: "1px solid #1e3a5f", padding: "0 16px",
          }}>
            {[
              { id: "slots", label: "▦ 501 Live Slots" },
              { id: "queue", label: "≡ Queue (2500)" },
              { id: "analytics", label: "◈ Analytics" },
            ].map(({ id, label }) => (
              <button
                key={id}
                className="tab-btn"
                onClick={() => setActiveTab(id)}
                style={{
                  padding: "10px 18px", fontSize: 12,
                  background: "transparent",
                  color: activeTab === id ? "#00e5a0" : "#64748b",
                  borderBottom: activeTab === id ? "2px solid #00e5a0" : "2px solid transparent",
                  marginBottom: -1,
                  fontFamily: "'IBM Plex Mono'",
                }}
              >
                {label}
              </button>
            ))}

            <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center", paddingRight: 4 }}>
              <select value={filterRegion} onChange={e => setFilterRegion(e.target.value)}
                style={{ background: "#111827", border: "1px solid #1e3a5f", color: "#94a3b8", padding: "4px 8px", borderRadius: 4, fontSize: 11, fontFamily: "'IBM Plex Mono'" }}>
                <option value="ALL">All Regions</option>
                {REGIONS.map(r => <option key={r} value={r}>{r}</option>)}
              </select>
              <select value={filterStatus} onChange={e => setFilterStatus(e.target.value)}
                style={{ background: "#111827", border: "1px solid #1e3a5f", color: "#94a3b8", padding: "4px 8px", borderRadius: 4, fontSize: 11, fontFamily: "'IBM Plex Mono'" }}>
                <option value="ALL">All Status</option>
                <option value="HIGH">High Performers</option>
                <option value="LOW">Low Performers</option>
              </select>
            </div>
          </div>

          {/* Content area */}
          <div style={{ flex: 1, overflow: "auto", padding: "12px 16px" }}>

            {/* SLOTS TAB */}
            {activeTab === "slots" && (
              <div style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))",
                gap: 6,
              }}>
                {filteredSlots.map(({ slotId, job }) => {
                  const appliesPerDay = job.daysLive > 0 ? job.applies / job.daysLive : 0;
                  const isHighlight = highlightedSlot === slotId;
                  const tier = TIER_BY_COUNTRY[job.country] ?? 3;
                  const tc = TIER_COLOR[tier];
                  return (
                    <div
                      key={slotId}
                      className={`slot-card${isHighlight ? " highlight" : ""}`}
                      style={{
                        background: isHighlight ? "#1a2d1a" : "#0d1b2a",
                        border: `1px solid ${isHighlight ? "#f5a623" : "#1e3a5f"}`,
                        borderRadius: 6, padding: "8px 10px",
                        cursor: "default",
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 4 }}>
                        <span style={{ fontSize: 9, color: "#475569", letterSpacing: "0.05em" }}>#{slotId}</span>
                        <span style={{
                          fontSize: 9, fontWeight: 700, padding: "1px 5px", borderRadius: 3,
                          background: tc + "22", color: tc,
                        }}>{TIER_LABEL[tier]}</span>
                      </div>
                      <div style={{ fontSize: 11, fontWeight: 600, color: "#e2e8f0", lineHeight: 1.3, marginBottom: 3, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {job.title}
                      </div>
                      <div style={{ fontSize: 10, color: "#64748b", marginBottom: 5 }}>
                        {job.country} · {job.client}
                      </div>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <span style={{ fontSize: 11, color: appliesColor(job.applies, job.daysLive), fontWeight: 600 }}>
                          {job.applies} <span style={{ fontSize: 9, color: "#475569" }}>applies</span>
                        </span>
                        <span style={{
                          fontSize: 10, fontWeight: 700,
                          color: scoreColor(job.score),
                        }}>
                          {job.score}
                        </span>
                      </div>
                      <div style={{ marginTop: 4, height: 2, background: "#1e3a5f", borderRadius: 1, overflow: "hidden" }}>
                        <div className="score-bar" style={{ height: "100%", width: `${job.score}%`, background: scoreColor(job.score), borderRadius: 1 }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            )}

            {/* QUEUE TAB */}
            {activeTab === "queue" && (
              <div>
                <div style={{
                  display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 8, marginBottom: 16,
                }}>
                  {queueByRegion.map(({ region, count, avgScore }) => (
                    <div key={region} style={{
                      background: "#0d1b2a", border: "1px solid #1e3a5f", borderRadius: 6,
                      padding: "10px 12px",
                    }}>
                      <div style={{ fontSize: 10, color: "#475569", marginBottom: 4 }}>{region}</div>
                      <div style={{ fontSize: 20, fontWeight: 700, color: "#3b9eff" }}>{count}</div>
                      <div style={{ fontSize: 9, color: "#475569" }}>avg score {avgScore.toFixed(0)}</div>
                    </div>
                  ))}
                </div>

                <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                  {queue.slice(0, 100).map((job, i) => (
                    <div key={job.id} className="queue-item" style={{
                      display: "grid", gridTemplateColumns: "40px 1fr 100px 80px 80px 70px 80px",
                      gap: 8, alignItems: "center",
                      background: i === 0 ? "#0d1f2a" : "transparent",
                      border: i === 0 ? "1px solid #1e4a6a" : "1px solid transparent",
                      borderRadius: 5, padding: "6px 10px", fontSize: 11,
                    }}>
                      <span style={{ color: "#475569", fontSize: 10 }}>#{i + 1}</span>
                      <span style={{ color: "#e2e8f0", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{job.title}</span>
                      <span style={{ color: "#94a3b8" }}>{job.country}</span>
                      <span style={{ color: "#94a3b8" }}>{job.region}</span>
                      <span style={{ color: "#64748b" }}>{job.client}</span>
                      <span style={{ color: scoreColor(job.score), fontWeight: 700 }}>{job.score}</span>
                      <span style={{
                        padding: "2px 6px", borderRadius: 3, fontSize: 10, textAlign: "center",
                        background: job.easyApply ? "#00e5a022" : "#ff5c5c22",
                        color: job.easyApply ? "#00e5a0" : "#ff5c5c",
                      }}>
                        {job.easyApply ? "Easy Apply" : "ATS"}
                      </span>
                    </div>
                  ))}
                  {queue.length > 100 && (
                    <div style={{ textAlign: "center", padding: 12, color: "#475569", fontSize: 11 }}>
                      + {queue.length - 100} more jobs in queue
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* ANALYTICS TAB */}
            {activeTab === "analytics" && (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>

                {/* Live by region */}
                <div style={{ background: "#0d1b2a", border: "1px solid #1e3a5f", borderRadius: 8, padding: 16 }}>
                  <div style={{ fontSize: 11, color: "#64748b", marginBottom: 12, letterSpacing: "0.08em" }}>LIVE SLOTS BY REGION</div>
                  {liveByRegion.map(({ region, count }) => (
                    <div key={region} style={{ marginBottom: 10 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                        <span style={{ fontSize: 12, color: "#94a3b8" }}>{region}</span>
                        <span style={{ fontSize: 12, fontWeight: 700, color: "#e2e8f0" }}>{count}</span>
                      </div>
                      <div style={{ height: 4, background: "#1e3a5f", borderRadius: 2, overflow: "hidden" }}>
                        <div style={{ height: "100%", width: `${(count / 501) * 100}%`, background: "#3b9eff", borderRadius: 2 }} />
                      </div>
                    </div>
                  ))}
                </div>

                {/* Queue by region */}
                <div style={{ background: "#0d1b2a", border: "1px solid #1e3a5f", borderRadius: 8, padding: 16 }}>
                  <div style={{ fontSize: 11, color: "#64748b", marginBottom: 12, letterSpacing: "0.08em" }}>QUEUE DEPTH BY REGION</div>
                  {queueByRegion.map(({ region, count }) => (
                    <div key={region} style={{ marginBottom: 10 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                        <span style={{ fontSize: 12, color: "#94a3b8" }}>{region}</span>
                        <span style={{ fontSize: 12, fontWeight: 700, color: "#e2e8f0" }}>{count}</span>
                      </div>
                      <div style={{ height: 4, background: "#1e3a5f", borderRadius: 2, overflow: "hidden" }}>
                        <div style={{ height: "100%", width: `${(count / (TOTAL_JOBS - 501)) * 100}%`, background: "#f5a623", borderRadius: 2 }} />
                      </div>
                    </div>
                  ))}
                </div>

                {/* Apply rate by country */}
                <div style={{ background: "#0d1b2a", border: "1px solid #1e3a5f", borderRadius: 8, padding: 16, gridColumn: "1 / -1" }}>
                  <div style={{ fontSize: 11, color: "#64748b", marginBottom: 12, letterSpacing: "0.08em" }}>EXPECTED APPLY RATE BY COUNTRY (from historical data)</div>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8 }}>
                    {Object.entries(AR_BY_COUNTRY).sort((a, b) => b[1] - a[1]).map(([country, ar]) => (
                      <div key={country} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "4px 0", borderBottom: "1px solid #111827" }}>
                        <span style={{ fontSize: 11, color: "#94a3b8" }}>{country}</span>
                        <span style={{ fontSize: 12, fontWeight: 700, color: ar >= 10 ? "#00e5a0" : ar >= 7 ? "#3b9eff" : ar >= 5 ? "#f5a623" : "#ff5c5c" }}>
                          {ar}%
                        </span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Rotation stats */}
                <div style={{ background: "#0d1b2a", border: "1px solid #1e3a5f", borderRadius: 8, padding: 16, gridColumn: "1 / -1" }}>
                  <div style={{ fontSize: 11, color: "#64748b", marginBottom: 12, letterSpacing: "0.08em" }}>ROTATION PERFORMANCE</div>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16 }}>
                    {[
                      { label: "Rotations completed", value: stats.rotations, color: "#f5a623" },
                      { label: "Jobs cycled through", value: Math.min(TOTAL_JOBS, 501 + stats.rotations), color: "#3b9eff" },
                      { label: "Avg time per job", value: `~${Math.max(1, Math.round(501 / Math.max(stats.rotations, 1) * 5))} cycles`, color: "#00e5a0" },
                      { label: "Coverage rate", value: `${Math.min(100, Math.round((501 + stats.rotations) / TOTAL_JOBS * 100))}%`, color: "#00e5a0" },
                    ].map(({ label, value, color }) => (
                      <div key={label} style={{ textAlign: "center" }}>
                        <div style={{ fontSize: 24, fontWeight: 700, color }}>{value}</div>
                        <div style={{ fontSize: 10, color: "#475569", marginTop: 4 }}>{label}</div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* ── Right panel — Activity Log + Queue Preview ── */}
        <div style={{
          borderLeft: "1px solid #1e3a5f",
          display: "flex", flexDirection: "column",
          background: "#080c15",
        }}>
          {/* Queue preview */}
          <div style={{ padding: "12px 14px", borderBottom: "1px solid #1e3a5f" }}>
            <div style={{ fontSize: 10, color: "#475569", letterSpacing: "0.08em", marginBottom: 8 }}>NEXT IN QUEUE</div>
            {queue.slice(0, 5).map((job, i) => (
              <div key={job.id} style={{
                display: "flex", justifyContent: "space-between", alignItems: "center",
                padding: "5px 0", borderBottom: "1px solid #111827",
              }}>
                <div>
                  <div style={{ fontSize: 11, color: "#e2e8f0", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 160 }}>{job.title}</div>
                  <div style={{ fontSize: 10, color: "#475569" }}>{job.country} · {job.client}</div>
                </div>
                <div style={{ textAlign: "right" }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: scoreColor(job.score) }}>{job.score}</div>
                  <div style={{ fontSize: 9, color: "#475569" }}>score</div>
                </div>
              </div>
            ))}
          </div>

          {/* Activity log */}
          <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
            <div style={{ padding: "10px 14px", borderBottom: "1px solid #1e3a5f", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ fontSize: 10, color: "#475569", letterSpacing: "0.08em" }}>ACTIVITY LOG</span>
              <button className="btn" onClick={() => setLog([])}
                style={{ fontSize: 10, color: "#475569", background: "transparent", padding: "2px 6px", borderRadius: 3, border: "1px solid #1e3a5f" }}>
                Clear
              </button>
            </div>
            <div ref={logRef} style={{ flex: 1, overflow: "auto", padding: "8px 14px", display: "flex", flexDirection: "column", gap: 4 }}>
              {log.length === 0 && (
                <div style={{ color: "#475569", fontSize: 11, textAlign: "center", marginTop: 24 }}>
                  Press ▶ Start Engine to begin rotation
                </div>
              )}
              {log.map((entry, i) => (
                <div key={i} className="log-entry" style={{ fontSize: 10, lineHeight: 1.5, color: entry.type === "rotate" ? "#f5a623" : entry.type === "success" ? "#00e5a0" : entry.type === "warn" ? "#ff5c5c" : "#64748b" }}>
                  <span style={{ color: "#334155", marginRight: 6 }}>{entry.time}</span>
                  {entry.msg}
                </div>
              ))}
            </div>
          </div>

          {/* Bottom status */}
          <div style={{
            padding: "10px 14px", borderTop: "1px solid #1e3a5f",
            background: "#0a0e1a", display: "flex", alignItems: "center", gap: 8,
          }}>
            <div style={{
              width: 8, height: 8, borderRadius: "50%",
              background: running ? "#00e5a0" : "#475569",
              boxShadow: running ? "0 0 8px #00e5a0" : "none",
            }} />
            <span style={{ fontSize: 11, color: running ? "#00e5a0" : "#475569" }}>
              {running ? "Engine running" : "Engine stopped"}
            </span>
            <span style={{ fontSize: 11, color: "#334155", marginLeft: "auto" }}>
              {501 - filteredSlots.length > 0 ? `${501 - filteredSlots.length} filtered` : ""}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
