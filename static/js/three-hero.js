/**
 * three-hero.js v2.0 — Cinematic GLSL shader hero for Nova AI Suite
 *
 * Creates a full-screen organic flowing mesh background using vertex/fragment
 * shaders. Inspired by Awwwards-winning sites (cornrevolution.resn.global).
 *
 * Effect: A luminous neural-network-like mesh that breathes, flows, and
 * responds to mouse position with volumetric glow in brand colors.
 *
 * Requires: Three.js (r134 UMD) loaded before this script.
 */
(function () {
  "use strict";

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  if (typeof THREE === "undefined") {
    console.warn("[Nova] Three.js not loaded — skipping hero shader");
    return;
  }

  var canvas = document.getElementById("hero-canvas");
  if (!canvas) return;

  /* ── Scene setup ── */
  var renderer = new THREE.WebGLRenderer({
    canvas: canvas,
    alpha: true,
    antialias: true,
    powerPreference: "high-performance",
  });
  renderer.setSize(window.innerWidth, window.innerHeight);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

  var scene = new THREE.Scene();
  var camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 10);
  camera.position.z = 1;

  /* ── Full-screen quad with shader material ── */
  var vertexShader = [
    "varying vec2 vUv;",
    "void main() {",
    "  vUv = uv;",
    "  gl_Position = vec4(position, 1.0);",
    "}",
  ].join("\n");

  var fragmentShader = [
    "precision highp float;",
    "",
    "uniform float uTime;",
    "uniform vec2 uResolution;",
    "uniform vec2 uMouse;",
    "uniform float uScroll;",
    "",
    "varying vec2 vUv;",
    "",
    "/* Simplex-style noise */",
    "vec3 mod289(vec3 x) { return x - floor(x * (1.0/289.0)) * 289.0; }",
    "vec4 mod289(vec4 x) { return x - floor(x * (1.0/289.0)) * 289.0; }",
    "vec4 permute(vec4 x) { return mod289(((x*34.0)+1.0)*x); }",
    "vec4 taylorInvSqrt(vec4 r) { return 1.79284291400159 - 0.85373472095314 * r; }",
    "",
    "float snoise(vec3 v) {",
    "  const vec2 C = vec2(1.0/6.0, 1.0/3.0);",
    "  const vec4 D = vec4(0.0, 0.5, 1.0, 2.0);",
    "  vec3 i = floor(v + dot(v, C.yyy));",
    "  vec3 x0 = v - i + dot(i, C.xxx);",
    "  vec3 g = step(x0.yzx, x0.xyz);",
    "  vec3 l = 1.0 - g;",
    "  vec3 i1 = min(g.xyz, l.zxy);",
    "  vec3 i2 = max(g.xyz, l.zxy);",
    "  vec3 x1 = x0 - i1 + C.xxx;",
    "  vec3 x2 = x0 - i2 + C.yyy;",
    "  vec3 x3 = x0 - D.yyy;",
    "  i = mod289(i);",
    "  vec4 p = permute(permute(permute(",
    "    i.z + vec4(0.0, i1.z, i2.z, 1.0))",
    "    + i.y + vec4(0.0, i1.y, i2.y, 1.0))",
    "    + i.x + vec4(0.0, i1.x, i2.x, 1.0));",
    "  float n_ = 0.142857142857;",
    "  vec3 ns = n_ * D.wyz - D.xzx;",
    "  vec4 j = p - 49.0 * floor(p * ns.z * ns.z);",
    "  vec4 x_ = floor(j * ns.z);",
    "  vec4 y_ = floor(j - 7.0 * x_);",
    "  vec4 x = x_ * ns.x + ns.yyyy;",
    "  vec4 y = y_ * ns.x + ns.yyyy;",
    "  vec4 h = 1.0 - abs(x) - abs(y);",
    "  vec4 b0 = vec4(x.xy, y.xy);",
    "  vec4 b1 = vec4(x.zw, y.zw);",
    "  vec4 s0 = floor(b0)*2.0 + 1.0;",
    "  vec4 s1 = floor(b1)*2.0 + 1.0;",
    "  vec4 sh = -step(h, vec4(0.0));",
    "  vec4 a0 = b0.xzyw + s0.xzyw*sh.xxyy;",
    "  vec4 a1 = b1.xzyw + s1.xzyw*sh.zzww;",
    "  vec3 p0 = vec3(a0.xy,h.x);",
    "  vec3 p1 = vec3(a0.zw,h.y);",
    "  vec3 p2 = vec3(a1.xy,h.z);",
    "  vec3 p3 = vec3(a1.zw,h.w);",
    "  vec4 norm = taylorInvSqrt(vec4(dot(p0,p0),dot(p1,p1),dot(p2,p2),dot(p3,p3)));",
    "  p0 *= norm.x; p1 *= norm.y; p2 *= norm.z; p3 *= norm.w;",
    "  vec4 m = max(0.6 - vec4(dot(x0,x0),dot(x1,x1),dot(x2,x2),dot(x3,x3)), 0.0);",
    "  m = m * m;",
    "  return 42.0 * dot(m*m, vec4(dot(p0,x0),dot(p1,x1),dot(p2,x2),dot(p3,x3)));",
    "}",
    "",
    "void main() {",
    "  vec2 uv = vUv;",
    "  vec2 aspect = vec2(uResolution.x / uResolution.y, 1.0);",
    "  vec2 p = (uv - 0.5) * aspect;",
    "",
    "  /* Mouse influence */",
    "  vec2 mouseOffset = (uMouse - 0.5) * aspect;",
    "  float mouseDist = length(p - mouseOffset);",
    "  float mouseInfluence = smoothstep(0.8, 0.0, mouseDist) * 0.3;",
    "",
    "  /* Layered noise for organic flow */",
    "  float t = uTime * 0.15;",
    "  float n1 = snoise(vec3(p * 1.5 + t, t * 0.5)) * 0.5 + 0.5;",
    "  float n2 = snoise(vec3(p * 3.0 - t * 0.7, t * 0.3 + 10.0)) * 0.5 + 0.5;",
    "  float n3 = snoise(vec3(p * 6.0 + t * 0.4, t * 0.2 + 20.0)) * 0.5 + 0.5;",
    "",
    "  /* Combine noise layers */",
    "  float noise = n1 * 0.6 + n2 * 0.3 + n3 * 0.1;",
    "  noise += mouseInfluence;",
    "",
    "  /* Neural network lines effect */",
    "  float lines = abs(sin(noise * 12.0 + t * 2.0));",
    "  lines = pow(lines, 8.0);",
    "  float lineGlow = smoothstep(0.3, 1.0, lines) * 0.4;",
    "",
    "  /* Brand color palette */",
    "  vec3 violet = vec3(0.353, 0.329, 0.741);  /* #5A54BD */",
    "  vec3 teal = vec3(0.420, 0.702, 0.804);    /* #6BB3CD */",
    "  vec3 portGore = vec3(0.125, 0.125, 0.345); /* #202058 */",
    "  vec3 deep = vec3(0.02, 0.02, 0.06);",
    "",
    "  /* Color mixing based on noise */",
    "  vec3 col = mix(deep, portGore, noise * 0.6);",
    "  col = mix(col, violet, smoothstep(0.4, 0.8, noise) * 0.35);",
    "  col = mix(col, teal, smoothstep(0.6, 0.9, n2) * 0.2);",
    "",
    "  /* Add line glow */",
    "  col += violet * lineGlow * 0.5;",
    "  col += teal * lineGlow * 0.3 * n2;",
    "",
    "  /* Volumetric center glow */",
    "  float centerDist = length(p * vec2(0.8, 1.2));",
    "  float glow = exp(-centerDist * 1.8) * 0.25;",
    "  col += violet * glow;",
    "",
    "  /* Mouse glow */",
    "  float mGlow = exp(-mouseDist * 3.0) * 0.15;",
    "  col += teal * mGlow;",
    "",
    "  /* Vignette */",
    "  float vignette = 1.0 - smoothstep(0.3, 1.2, centerDist);",
    "  col *= vignette * 0.8 + 0.2;",
    "",
    "  /* Scroll fade to black */",
    "  float scrollFade = clamp(1.0 - uScroll * 1.2, 0.0, 1.0);",
    "  col *= scrollFade;",
    "",
    "  /* Subtle film grain */",
    "  float grain = fract(sin(dot(uv * uTime, vec2(12.9898, 78.233))) * 43758.5453);",
    "  col += (grain - 0.5) * 0.015;",
    "",
    "  gl_FragColor = vec4(col, 1.0);",
    "}",
  ].join("\n");

  var uniforms = {
    uTime: { value: 0 },
    uResolution: {
      value: new THREE.Vector2(window.innerWidth, window.innerHeight),
    },
    uMouse: { value: new THREE.Vector2(0.5, 0.5) },
    uScroll: { value: 0 },
  };

  var material = new THREE.ShaderMaterial({
    uniforms: uniforms,
    vertexShader: vertexShader,
    fragmentShader: fragmentShader,
    depthTest: false,
    depthWrite: false,
  });

  var geometry = new THREE.PlaneGeometry(2, 2);
  var mesh = new THREE.Mesh(geometry, material);
  scene.add(mesh);

  /* ── Mouse tracking ── */
  var mouseTarget = { x: 0.5, y: 0.5 };
  var mouseCurrent = { x: 0.5, y: 0.5 };

  document.addEventListener(
    "mousemove",
    function (e) {
      mouseTarget.x = e.clientX / window.innerWidth;
      mouseTarget.y = 1.0 - e.clientY / window.innerHeight;
    },
    { passive: true },
  );

  /* Touch */
  document.addEventListener(
    "touchmove",
    function (e) {
      if (e.touches.length > 0) {
        mouseTarget.x = e.touches[0].clientX / window.innerWidth;
        mouseTarget.y = 1.0 - e.touches[0].clientY / window.innerHeight;
      }
    },
    { passive: true },
  );

  /* ── Scroll tracking ── */
  var scrollY = 0;
  var heroSection = document.querySelector(".hero");
  var heroHeight = heroSection ? heroSection.offsetHeight : window.innerHeight;

  window.addEventListener(
    "scroll",
    function () {
      scrollY = window.scrollY / heroHeight;
    },
    { passive: true },
  );

  /* ── Animation loop ── */
  var clock = new THREE.Clock();

  function animate() {
    requestAnimationFrame(animate);

    /* Skip when scrolled past hero */
    if (scrollY > 1.5) return;

    var time = clock.getElapsedTime();

    /* Smooth mouse interpolation */
    mouseCurrent.x += (mouseTarget.x - mouseCurrent.x) * 0.05;
    mouseCurrent.y += (mouseTarget.y - mouseCurrent.y) * 0.05;

    uniforms.uTime.value = time;
    uniforms.uMouse.value.set(mouseCurrent.x, mouseCurrent.y);
    uniforms.uScroll.value = scrollY;

    renderer.render(scene, camera);
  }

  animate();

  /* ── Resize ── */
  var resizeTimeout;
  window.addEventListener("resize", function () {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(function () {
      renderer.setSize(window.innerWidth, window.innerHeight);
      renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
      uniforms.uResolution.value.set(window.innerWidth, window.innerHeight);
      heroHeight = heroSection ? heroSection.offsetHeight : window.innerHeight;
    }, 150);
  });

  /* ── Cleanup ── */
  window.addEventListener("beforeunload", function () {
    geometry.dispose();
    material.dispose();
    renderer.dispose();
  });
})();
