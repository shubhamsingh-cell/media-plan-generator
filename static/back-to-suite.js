/**
 * Global "Back to Suite" navigation button.
 * Auto-injects a fixed bottom-left button linking to /hub.
 * Skips the hub page itself.
 */
(function () {
  "use strict";
  var path = window.location.pathname;
  // Don't show on hub/home
  if (path === "/" || path === "/hub" || path === "/hub/") return;
  // Don't show if any link to /hub already exists in the page
  if (document.querySelector('a[href="/hub"]')) return;

  var btn = document.createElement("a");
  btn.href = "/hub";
  btn.id = "back-to-suite";
  btn.setAttribute("aria-label", "Back to Suite");
  btn.innerHTML =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" ' +
    'stroke-linecap="round" stroke-linejoin="round" width="14" height="14" ' +
    'style="flex-shrink:0;opacity:0.6">' +
    '<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/>' +
    '<rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>' +
    "<span>Back to Suite</span>";
  // On platform page, position below sidebar footer to avoid overlap (Issue 12)
  var isPlatform = path === "/platform" || path === "/platform/";
  var bottomPos = isPlatform ? "76px" : "20px";
  btn.style.cssText =
    "position:fixed;bottom:" +
    bottomPos +
    ";left:20px;z-index:9990;" +
    "display:flex;align-items:center;gap:8px;" +
    "padding:8px 16px;border-radius:10px;" +
    "background:rgba(20,20,40,0.85);backdrop-filter:blur(8px);" +
    "border:1px solid rgba(255,255,255,0.08);" +
    "color:rgba(255,255,255,0.6);text-decoration:none;" +
    "font-family:Inter,system-ui,sans-serif;font-size:13px;font-weight:500;" +
    "transition:all 0.2s ease;cursor:pointer;";
  btn.onmouseenter = function () {
    btn.style.color = "rgba(255,255,255,0.9)";
    btn.style.borderColor = "rgba(255,255,255,0.2)";
    btn.style.background = "rgba(30,30,60,0.95)";
  };
  btn.onmouseleave = function () {
    btn.style.color = "rgba(255,255,255,0.6)";
    btn.style.borderColor = "rgba(255,255,255,0.08)";
    btn.style.background = "rgba(20,20,40,0.85)";
  };
  document.body.appendChild(btn);
})();
