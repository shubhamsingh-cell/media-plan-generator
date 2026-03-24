#!/usr/bin/env python3
"""Pre-deploy quality gate for Nova AI Suite.

Runs static checks before deployment:
1. No bare except clauses
2. No hardcoded API keys/secrets
3. All templates have matching routes
4. No syntax errors in Python files
5. Critical imports resolve
"""

import ast
import os
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent
_issues: list[dict] = []


def _add(severity: str, file: str, line: int, msg: str) -> None:
    """Record an issue."""
    _issues.append({"severity": severity, "file": file, "line": line, "msg": msg})


def check_bare_excepts() -> None:
    """Flag bare except: clauses."""
    for pyfile in ROOT.glob("*.py"):
        try:
            tree = ast.parse(pyfile.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler) and node.type is None:
                    _add("CRITICAL", pyfile.name, node.lineno, "Bare except: clause")
        except SyntaxError as e:
            _add("CRITICAL", pyfile.name, e.lineno or 0, f"Syntax error: {e.msg}")


def check_secrets() -> None:
    """Flag hardcoded secrets."""
    secret_patterns = [
        r'(?:api[_-]?key|secret|password|token)\s*=\s*["\'][a-zA-Z0-9_-]{20,}["\']',
        r"sk-[a-zA-Z0-9]{20,}",
        r"Bearer\s+[a-zA-Z0-9_-]{20,}",
    ]
    for pyfile in ROOT.glob("*.py"):
        content = pyfile.read_text()
        for i, line in enumerate(content.splitlines(), 1):
            if line.strip().startswith("#"):
                continue
            for pattern in secret_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    _add("CRITICAL", pyfile.name, i, "Possible hardcoded secret")


def check_syntax() -> None:
    """Verify all Python files parse."""
    for pyfile in ROOT.glob("*.py"):
        try:
            ast.parse(pyfile.read_text())
        except SyntaxError as e:
            _add("CRITICAL", pyfile.name, e.lineno or 0, f"Syntax error: {e.msg}")


def check_critical_imports() -> None:
    """Verify critical modules can be imported."""
    critical = ["json", "os", "logging", "threading", "urllib.request", "pathlib"]
    for mod in critical:
        try:
            __import__(mod)
        except ImportError:
            _add("CRITICAL", "stdlib", 0, f"Cannot import {mod}")


def check_template_routes() -> None:
    """Verify all templates in templates/ have routes in app.py."""
    templates_dir = ROOT / "templates"
    if not templates_dir.exists():
        return
    app_content = (ROOT / "app.py").read_text()
    for tmpl in templates_dir.glob("*.html"):
        name = tmpl.stem
        # Check if template name appears in app.py (in fragment map or serve_file calls)
        if name not in app_content and name.replace("-", "_") not in app_content:
            _add(
                "WARNING",
                f"templates/{tmpl.name}",
                0,
                f"Template may have no route in app.py",
            )


def run() -> bool:
    """Run all checks. Returns True if no CRITICAL issues."""
    print(f"\n{'='*60}")
    print(f"  PRE-DEPLOY QUALITY GATE")
    print(f"{'='*60}\n")

    check_syntax()
    check_bare_excepts()
    check_secrets()
    check_critical_imports()
    check_template_routes()

    criticals = [i for i in _issues if i["severity"] == "CRITICAL"]
    warnings = [i for i in _issues if i["severity"] == "WARNING"]

    for issue in _issues:
        icon = "BLOCK" if issue["severity"] == "CRITICAL" else "WARN"
        print(f"  [{icon}] {issue['file']}:{issue['line']} -- {issue['msg']}")

    if not _issues:
        print("  All checks passed.")

    print(f"\n{'─'*60}")
    print(f"  {len(criticals)} blockers, {len(warnings)} warnings")

    if criticals:
        print(f"\n  DEPLOY BLOCKED -- fix {len(criticals)} critical issues first")
    else:
        print(f"\n  DEPLOY OK -- no blocking issues")

    print(f"{'='*60}\n")
    return len(criticals) == 0


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
