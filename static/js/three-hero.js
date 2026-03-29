/**
 * three-hero.js — Interactive 3D particle mesh for Nova AI Suite hero
 * Creates a flowing particle network that responds to mouse movement.
 * Inspired by cornrevolution.resn.global and Codrops particle tutorials.
 * Requires: Three.js r170+ loaded via CDN before this script.
 */
(function () {
  "use strict";

  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  if (typeof THREE === "undefined") {
    console.warn("[Nova] Three.js not loaded — skipping hero particles");
    return;
  }

  var canvas = document.getElementById("hero-canvas");
  if (!canvas) return;

  /* ── Constants ── */
  var PARTICLE_COUNT = 4000;
  var MOUSE_RADIUS = 2.5;
  var MOUSE_STRENGTH = 0.8;
  var WAVE_SPEED = 0.4;
  var WAVE_AMP = 0.3;
  var CONNECTION_DIST = 1.8;
  var MAX_CONNECTIONS = 800;

  /* ── Brand colors ── */
  var VIOLET = new THREE.Color(0x5a54bd);
  var TEAL = new THREE.Color(0x6bb3cd);
  var PORT_GORE = new THREE.Color(0x202058);

  /* ── Scene ── */
  var scene = new THREE.Scene();
  var camera = new THREE.PerspectiveCamera(
    60,
    window.innerWidth / window.innerHeight,
    0.1,
    100,
  );
  camera.position.z = 18;

  var renderer = new THREE.WebGLRenderer({
    canvas: canvas,
    alpha: true,
    antialias: false,
    powerPreference: "high-performance",
  });
  renderer.setSize(window.innerWidth, window.innerHeight);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

  /* ── Particles ── */
  var positions = new Float32Array(PARTICLE_COUNT * 3);
  var originalPositions = new Float32Array(PARTICLE_COUNT * 3);
  var colors = new Float32Array(PARTICLE_COUNT * 3);
  var sizes = new Float32Array(PARTICLE_COUNT);
  var velocities = new Float32Array(PARTICLE_COUNT * 3);

  var spread = 28;
  var depthSpread = 6;

  for (var i = 0; i < PARTICLE_COUNT; i++) {
    var i3 = i * 3;

    /* Distribute in a wide elliptical field */
    var angle = Math.random() * Math.PI * 2;
    var radius = Math.sqrt(Math.random()) * spread * 0.5;
    var x = Math.cos(angle) * radius * 1.4; /* wider horizontally */
    var y = Math.sin(angle) * radius * 0.7; /* shorter vertically */
    var z = (Math.random() - 0.5) * depthSpread;

    positions[i3] = x;
    positions[i3 + 1] = y;
    positions[i3 + 2] = z;
    originalPositions[i3] = x;
    originalPositions[i3 + 1] = y;
    originalPositions[i3 + 2] = z;

    velocities[i3] = 0;
    velocities[i3 + 1] = 0;
    velocities[i3 + 2] = 0;

    /* Color gradient: center = violet, edges = teal, with random variation */
    var distFromCenter = Math.sqrt(x * x + y * y) / (spread * 0.4);
    var t = Math.min(distFromCenter + (Math.random() - 0.5) * 0.3, 1);
    var color = VIOLET.clone().lerp(TEAL, t);
    colors[i3] = color.r;
    colors[i3 + 1] = color.g;
    colors[i3 + 2] = color.b;

    sizes[i] = Math.random() * 1.5 + 0.5;
  }

  var geometry = new THREE.BufferGeometry();
  geometry.setAttribute("position", new THREE.BufferAttribute(positions, 3));
  geometry.setAttribute("color", new THREE.BufferAttribute(colors, 3));
  geometry.setAttribute("aSize", new THREE.BufferAttribute(sizes, 1));

  /* ── Vertex Shader ── */
  var vertexShader = [
    "attribute float aSize;",
    "uniform float uTime;",
    "uniform float uPixelRatio;",
    "uniform float uScroll;",
    "varying vec3 vColor;",
    "varying float vAlpha;",
    "",
    "void main() {",
    "  vColor = color;",
    "  vec3 pos = position;",
    "",
    "  /* Gentle floating wave */",
    "  float wave = sin(pos.x * 0.3 + uTime * " +
      WAVE_SPEED.toFixed(1) +
      ") * " +
      WAVE_AMP.toFixed(1) +
      ";",
    "  wave += cos(pos.y * 0.4 + uTime * 0.3) * 0.15;",
    "  pos.z += wave;",
    "",
    "  /* Depth-based alpha */",
    "  float depth = (pos.z + 3.0) / 6.0;",
    "  vAlpha = clamp(depth * 0.6 + 0.4, 0.15, 0.9);",
    "",
    "  /* Scroll fade */",
    "  vAlpha *= clamp(1.0 - uScroll * 1.5, 0.0, 1.0);",
    "",
    "  vec4 mvPosition = modelViewMatrix * vec4(pos, 1.0);",
    "  gl_PointSize = aSize * uPixelRatio * (8.0 / -mvPosition.z);",
    "  gl_Position = projectionMatrix * mvPosition;",
    "}",
  ].join("\n");

  /* ── Fragment Shader ── */
  var fragmentShader = [
    "varying vec3 vColor;",
    "varying float vAlpha;",
    "",
    "void main() {",
    "  /* Soft circle with glow */",
    "  float dist = length(gl_PointCoord - vec2(0.5));",
    "  if (dist > 0.5) discard;",
    "",
    "  float glow = 1.0 - smoothstep(0.0, 0.5, dist);",
    "  glow = pow(glow, 1.5);",
    "",
    "  gl_FragColor = vec4(vColor, glow * vAlpha);",
    "}",
  ].join("\n");

  var pointsMaterial = new THREE.ShaderMaterial({
    uniforms: {
      uTime: { value: 0 },
      uPixelRatio: { value: renderer.getPixelRatio() },
      uScroll: { value: 0 },
    },
    vertexShader: vertexShader,
    fragmentShader: fragmentShader,
    transparent: true,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
    vertexColors: true,
  });

  var points = new THREE.Points(geometry, pointsMaterial);
  scene.add(points);

  /* ── Connection Lines ── */
  var linePositions = new Float32Array(MAX_CONNECTIONS * 6);
  var lineColors = new Float32Array(MAX_CONNECTIONS * 6);
  var lineGeometry = new THREE.BufferGeometry();
  lineGeometry.setAttribute(
    "position",
    new THREE.BufferAttribute(linePositions, 3),
  );
  lineGeometry.setAttribute("color", new THREE.BufferAttribute(lineColors, 3));
  lineGeometry.setDrawRange(0, 0);

  var lineMaterial = new THREE.LineBasicMaterial({
    vertexColors: true,
    transparent: true,
    opacity: 0.12,
    blending: THREE.AdditiveBlending,
    depthWrite: false,
  });

  var lines = new THREE.LineSegments(lineGeometry, lineMaterial);
  scene.add(lines);

  /* ── Mouse tracking ── */
  var mouse = { x: 0, y: 0, worldX: 0, worldY: 0 };
  var mouseTarget = { x: 0, y: 0 };
  var isMouseInHero = false;

  function onMouseMove(e) {
    var rect = canvas.getBoundingClientRect();
    mouseTarget.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
    mouseTarget.y = -(((e.clientY - rect.top) / rect.height) * 2 - 1);
    isMouseInHero = true;
  }

  function onMouseLeave() {
    isMouseInHero = false;
  }

  canvas.addEventListener("mousemove", onMouseMove, { passive: true });
  canvas.addEventListener("mouseleave", onMouseLeave, { passive: true });

  /* ── Touch support ── */
  canvas.addEventListener(
    "touchmove",
    function (e) {
      if (e.touches.length > 0) {
        var touch = e.touches[0];
        var rect = canvas.getBoundingClientRect();
        mouseTarget.x = ((touch.clientX - rect.left) / rect.width) * 2 - 1;
        mouseTarget.y = -(((touch.clientY - rect.top) / rect.height) * 2 - 1);
        isMouseInHero = true;
      }
    },
    { passive: true },
  );
  canvas.addEventListener("touchend", onMouseLeave, { passive: true });

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

  /* ── Update connections between nearby particles ── */
  function updateConnections() {
    var connectionCount = 0;
    var posAttr = geometry.attributes.position.array;
    var colAttr = geometry.attributes.color.array;
    var step = Math.max(1, Math.floor(PARTICLE_COUNT / 600));

    for (
      var i = 0;
      i < PARTICLE_COUNT && connectionCount < MAX_CONNECTIONS;
      i += step
    ) {
      var i3 = i * 3;
      var ax = posAttr[i3];
      var ay = posAttr[i3 + 1];
      var az = posAttr[i3 + 2];

      for (
        var j = i + step;
        j < PARTICLE_COUNT && connectionCount < MAX_CONNECTIONS;
        j += step
      ) {
        var j3 = j * 3;
        var dx = posAttr[j3] - ax;
        var dy = posAttr[j3 + 1] - ay;
        var dz = posAttr[j3 + 2] - az;
        var dist = dx * dx + dy * dy + dz * dz;

        if (dist < CONNECTION_DIST * CONNECTION_DIST) {
          var ci = connectionCount * 6;
          linePositions[ci] = ax;
          linePositions[ci + 1] = ay;
          linePositions[ci + 2] = az;
          linePositions[ci + 3] = posAttr[j3];
          linePositions[ci + 4] = posAttr[j3 + 1];
          linePositions[ci + 5] = posAttr[j3 + 2];

          /* Color: average of connected particles */
          var alpha = 1 - Math.sqrt(dist) / CONNECTION_DIST;
          lineColors[ci] = colAttr[i3] * alpha;
          lineColors[ci + 1] = colAttr[i3 + 1] * alpha;
          lineColors[ci + 2] = colAttr[i3 + 2] * alpha;
          lineColors[ci + 3] = colAttr[j3] * alpha;
          lineColors[ci + 4] = colAttr[j3 + 1] * alpha;
          lineColors[ci + 5] = colAttr[j3 + 2] * alpha;

          connectionCount++;
        }
      }
    }

    lineGeometry.setDrawRange(0, connectionCount * 2);
    lineGeometry.attributes.position.needsUpdate = true;
    lineGeometry.attributes.color.needsUpdate = true;
  }

  /* ── Animation loop ── */
  var clock = new THREE.Clock();
  var frameCount = 0;

  function animate() {
    requestAnimationFrame(animate);

    /* Skip rendering when scrolled past hero */
    if (scrollY > 1.2) return;

    var time = clock.getElapsedTime();
    frameCount++;

    /* Smooth mouse interpolation */
    mouse.x += (mouseTarget.x - mouse.x) * 0.08;
    mouse.y += (mouseTarget.y - mouse.y) * 0.08;

    /* Convert mouse to world coordinates */
    var vector = new THREE.Vector3(mouse.x, mouse.y, 0.5);
    vector.unproject(camera);
    var dir = vector.sub(camera.position).normalize();
    var distance = -camera.position.z / dir.z;
    var worldPos = camera.position.clone().add(dir.multiplyScalar(distance));
    mouse.worldX = worldPos.x;
    mouse.worldY = worldPos.y;

    /* Update particle positions */
    var posAttr = geometry.attributes.position;
    var posArray = posAttr.array;

    for (var i = 0; i < PARTICLE_COUNT; i++) {
      var i3 = i * 3;

      /* Mouse repulsion */
      if (isMouseInHero) {
        var dx = posArray[i3] - mouse.worldX;
        var dy = posArray[i3 + 1] - mouse.worldY;
        var dist = Math.sqrt(dx * dx + dy * dy);

        if (dist < MOUSE_RADIUS && dist > 0.01) {
          var force = (1 - dist / MOUSE_RADIUS) * MOUSE_STRENGTH;
          velocities[i3] += (dx / dist) * force;
          velocities[i3 + 1] += (dy / dist) * force;
        }
      }

      /* Spring back to original position */
      velocities[i3] += (originalPositions[i3] - posArray[i3]) * 0.02;
      velocities[i3 + 1] +=
        (originalPositions[i3 + 1] - posArray[i3 + 1]) * 0.02;
      velocities[i3 + 2] +=
        (originalPositions[i3 + 2] - posArray[i3 + 2]) * 0.01;

      /* Apply velocity with damping */
      velocities[i3] *= 0.92;
      velocities[i3 + 1] *= 0.92;
      velocities[i3 + 2] *= 0.95;

      posArray[i3] += velocities[i3];
      posArray[i3 + 1] += velocities[i3 + 1];
      posArray[i3 + 2] += velocities[i3 + 2];
    }

    posAttr.needsUpdate = true;

    /* Update connections every 3rd frame for performance */
    if (frameCount % 3 === 0) {
      updateConnections();
    }

    /* Update uniforms */
    pointsMaterial.uniforms.uTime.value = time;
    pointsMaterial.uniforms.uScroll.value = scrollY;
    lineMaterial.opacity = 0.12 * Math.max(0, 1 - scrollY * 1.5);

    /* Subtle camera sway */
    camera.position.x = Math.sin(time * 0.15) * 0.3;
    camera.position.y = Math.cos(time * 0.12) * 0.2;
    camera.lookAt(0, 0, 0);

    /* Gentle rotation of entire particle system */
    points.rotation.z = time * 0.02;
    lines.rotation.z = time * 0.02;

    renderer.render(scene, camera);
  }

  animate();

  /* ── Resize handler ── */
  var resizeTimeout;
  window.addEventListener("resize", function () {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(function () {
      camera.aspect = window.innerWidth / window.innerHeight;
      camera.updateProjectionMatrix();
      renderer.setSize(window.innerWidth, window.innerHeight);
      renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
      pointsMaterial.uniforms.uPixelRatio.value = renderer.getPixelRatio();
      heroHeight = heroSection ? heroSection.offsetHeight : window.innerHeight;
    }, 150);
  });

  /* ── Cleanup on page unload ── */
  window.addEventListener("beforeunload", function () {
    geometry.dispose();
    pointsMaterial.dispose();
    lineGeometry.dispose();
    lineMaterial.dispose();
    renderer.dispose();
  });

  /* ── Expose for debugging ── */
  window._novaHero = {
    scene: scene,
    camera: camera,
    renderer: renderer,
    particleCount: PARTICLE_COUNT,
  };
})();
