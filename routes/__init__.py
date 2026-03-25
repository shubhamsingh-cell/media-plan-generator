"""Route handlers for Nova AI Suite.

Each module exports a ``handle_*_routes(handler, path, parsed)`` function
that returns ``True`` when it matches and handles the route.  The *handler*
argument is the ``MediaPlanHandler`` instance so helpers like
``handler._send_json()`` are available.

Modules:
    admin       - Admin GET routes (/api/admin/*)
    campaign    - Campaign save/list and plan sharing routes
    chat        - Chat GET routes (/api/chat/*)
    competitive - Competitive intelligence POST routes
    copilot     - Copilot suggestion POST route
    diagram     - Excalidraw diagram generation POST route
    export      - Export/delivery POST routes
    health      - Health and monitoring GET routes
    pages       - HTML template page-serving GET routes
    pricing     - Pricing and VendorIQ routes
    tts         - ElevenLabs text-to-speech POST route
    utils       - Shared utilities (send_json_response, read_json_body, etc.)
"""
