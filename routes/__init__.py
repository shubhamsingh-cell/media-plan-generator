"""Route handlers for Nova AI Suite.

Each module exports a ``handle_*_routes(handler, path, parsed)`` function
that returns ``True`` when it matches and handles the route.  The *handler*
argument is the ``MediaPlanHandler`` instance so helpers like
``handler._send_json()`` are available.
"""
