"""The platform drawer's prior turns must reach the model.

The drawer posted ``conversation_history`` while the chat endpoints read
``history``, so its replies never saw earlier turns. The server now accepts
both keys (older cached drawer JS keeps working) and the drawer sends
``history``.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import app
import nova
from tests.test_nova_new_chat import _DRAWER_STUB, _drawer_js
from tests.wizard_js_harness import NODE, run_node

TURNS = [
    {"role": "user", "content": "We are hiring 30 registered nurses in Boston"},
    {"role": "assistant", "content": "Noted: nurses in Boston."},
]


class _CaptureIQ:
    def __init__(self):
        self.seen = None

    def chat(self, user_message, conversation_history=None, **kwargs):
        self.seen = conversation_history
        return {"response": "ok " * 30, "sources": [], "confidence": 0.9, "tools_used": []}


@pytest.mark.parametrize("key", ["history", "conversation_history"])
def test_prior_turns_reach_the_model_under_either_key(key):
    iq = _CaptureIQ()
    with patch.object(nova, "_get_iq", lambda: iq):
        nova.handle_chat_request({"message": "what budget do you suggest?", key: TURNS})
    assert iq.seen == TURNS


def test_history_key_wins_when_both_are_present():
    iq = _CaptureIQ()
    other = [{"role": "user", "content": "stale"}]
    with patch.object(nova, "_get_iq", lambda: iq):
        nova.handle_chat_request(
            {"message": "hello there friend", "history": TURNS, "conversation_history": other}
        )
    assert iq.seen == TURNS


def test_server_boundary_aliases_the_drawer_key_before_validation():
    d = {"message": "x", "conversation_history": TURNS}
    app._alias_history_key(d)
    assert d["history"] is TURNS
    d = {"history": TURNS, "conversation_history": [{"role": "user", "content": "z"}]}
    app._alias_history_key(d)
    assert d["history"] is TURNS
    d = {"conversation_history": "not a list"}
    app._alias_history_key(d)
    assert "history" not in d  # a bad type is left for the existing validators


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_drawer_posts_history_with_prior_turns():
    out = run_node(
        _DRAWER_STUB
        + _drawer_js()
        + r"""
store.nova_chat_v1 = JSON.stringify([
  {role: "user", text: "We are hiring 30 registered nurses in Boston"},
  {role: "nova", text: "Noted: nurses in Boston."}]);
NovaDrawer.open();
els["nova-input"] = Object.assign(new El(), {value: "what budget do you suggest?"});
NovaDrawer.send();
var b = fetches[0].body;
console.log(JSON.stringify({history: b.history, legacy: "conversation_history" in b}));
"""
    )
    assert out["history"][:2] == TURNS
    assert out["history"][-1]["content"] == "what budget do you suggest?"
    assert out["legacy"] is False
