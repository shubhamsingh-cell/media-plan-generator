"""
Nova Avatar Management

Handles avatar generation, storage, and selection for chat personas.
Supports multiple styles: ai-generated, gradients, emoji, initials.
"""

from __future__ import annotations

import logging
import random
import string
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Color palette for gradients
GRADIENT_COLORS = [
    ("#5A54BD", "#6BB3CD"),  # Purple to Teal
    ("#FF6B6B", "#FFA500"),  # Red to Orange
    ("#4ECDC4", "#44A08D"),  # Teal to Green
    ("#667EEA", "#764BA2"),  # Blue to Purple
    ("#F093FB", "#F5576C"),  # Pink to Red
    ("#4158D0", "#C850C0"),  # Blue to Purple
    ("#0093E9", "#80D0C7"),  # Blue to Teal
    ("#FA8072", "#FFB347"),  # Salmon to Mango
]

EMOJI_PERSONAS = [
    {"name": "Einstein", "emoji": "🧠", "color": "#FDB833"},
    {"name": "Rocket", "emoji": "🚀", "color": "#FF6B6B"},
    {"name": "Sparkle", "emoji": "✨", "color": "#FFD93D"},
    {"name": "Book", "emoji": "📚", "color": "#6C5CE7"},
    {"name": "Lightbulb", "emoji": "💡", "color": "#FDB833"},
    {"name": "Chart", "emoji": "📊", "color": "#00B894"},
    {"name": "Target", "emoji": "🎯", "color": "#FF7675"},
    {"name": "Star", "emoji": "⭐", "color": "#FFD93D"},
    {"name": "Shield", "emoji": "🛡️", "color": "#0984E3"},
    {"name": "Gear", "emoji": "⚙️", "color": "#636E72"},
    {"name": "Globe", "emoji": "🌍", "color": "#00B894"},
    {"name": "Crystal", "emoji": "🔮", "color": "#A29BFE"},
]

INITIALS_COLORS = [
    "#5A54BD",  # Purple
    "#6BB3CD",  # Teal
    "#FF6B6B",  # Red
    "#FFA500",  # Orange
    "#4ECDC4",  # Teal
    "#44A08D",  # Green
]


class AvatarGenerator:
    """Generate and manage avatars for chat personas."""

    @staticmethod
    def generate_gradient_avatar(
        persona_name: str,
        seed: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Generate a gradient avatar.

        Args:
            persona_name: Name of persona
            seed: Optional seed for deterministic generation

        Returns:
            Avatar dict with style, colors, SVG
        """
        if seed is not None:
            random.seed(seed)
        else:
            # Use persona name for deterministic colors
            seed = sum(ord(c) for c in persona_name) % len(GRADIENT_COLORS)
            random.seed(seed)

        color1, color2 = random.choice(GRADIENT_COLORS)

        # Generate simple SVG gradient avatar
        avatar_svg = f"""
        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
            <defs>
                <linearGradient id="grad" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" style="stop-color:{color1};stop-opacity:1" />
                    <stop offset="100%" style="stop-color:{color2};stop-opacity:1" />
                </linearGradient>
            </defs>
            <circle cx="50" cy="50" r="50" fill="url(#grad)"/>
            <text x="50" y="50" font-size="40" font-weight="bold" text-anchor="middle"
                  dominant-baseline="central" fill="white">
                {persona_name[0].upper()}
            </text>
        </svg>
        """

        return {
            "persona_name": persona_name,
            "style": "gradient",
            "colors": [color1, color2],
            "svg": avatar_svg.strip(),
            "initials": persona_name[0].upper(),
        }

    @staticmethod
    def generate_emoji_avatar(
        persona_name: str,
        emoji_idx: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Generate an emoji avatar.

        Args:
            persona_name: Name of persona
            emoji_idx: Optional emoji index

        Returns:
            Avatar dict with emoji and color
        """
        if emoji_idx is not None and 0 <= emoji_idx < len(EMOJI_PERSONAS):
            emoji_persona = EMOJI_PERSONAS[emoji_idx]
        else:
            # Deterministic selection based on name
            idx = sum(ord(c) for c in persona_name) % len(EMOJI_PERSONAS)
            emoji_persona = EMOJI_PERSONAS[idx]

        return {
            "persona_name": persona_name,
            "style": "emoji",
            "emoji": emoji_persona["emoji"],
            "emoji_name": emoji_persona["name"],
            "color": emoji_persona["color"],
        }

    @staticmethod
    def generate_initials_avatar(persona_name: str) -> Dict[str, Any]:
        """Generate an initials avatar.

        Args:
            persona_name: Name of persona

        Returns:
            Avatar dict with initials and color
        """
        # Extract initials
        parts = persona_name.split()
        if len(parts) >= 2:
            initials = (parts[0][0] + parts[-1][0]).upper()
        else:
            initials = persona_name[:2].upper()

        # Deterministic color selection
        color_idx = sum(ord(c) for c in persona_name) % len(INITIALS_COLORS)
        color = INITIALS_COLORS[color_idx]

        return {
            "persona_name": persona_name,
            "style": "initials",
            "initials": initials,
            "color": color,
        }

    @staticmethod
    def generate_default_avatars() -> List[Dict[str, Any]]:
        """Generate a set of default avatars for common personas.

        Returns:
            List of avatar dicts
        """
        personas = [
            "Nova Assistant",
            "Expert Advisor",
            "Research Analyst",
            "Data Scientist",
            "Strategy Guide",
        ]

        avatars = []
        for persona in personas:
            avatar = AvatarGenerator.generate_gradient_avatar(persona)
            avatars.append(avatar)

        return avatars


def get_default_avatar() -> Dict[str, Any]:
    """Get the default Nova avatar.

    Returns:
        Avatar dict
    """
    return {
        "persona_name": "Nova",
        "style": "gradient",
        "colors": ["#5A54BD", "#6BB3CD"],
        "initials": "N",
        "svg": """
        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
            <defs>
                <linearGradient id="nova-grad" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" style="stop-color:#5A54BD;stop-opacity:1" />
                    <stop offset="100%" style="stop-color:#6BB3CD;stop-opacity:1" />
                </linearGradient>
            </defs>
            <circle cx="50" cy="50" r="50" fill="url(#nova-grad)"/>
            <text x="50" y="50" font-size="40" font-weight="bold" text-anchor="middle"
                  dominant-baseline="central" fill="white">N</text>
        </svg>
        """.strip(),
    }


def avatar_to_html(avatar: Dict[str, Any]) -> str:
    """Convert avatar dict to inline HTML/CSS for display.

    Args:
        avatar: Avatar dict

    Returns:
        HTML string for avatar display
    """
    style = avatar.get("style", "gradient")

    if style == "gradient":
        colors = avatar.get("colors", ["#5A54BD", "#6BB3CD"])
        initials = avatar.get("initials", "N")
        color1, color2 = colors[0], colors[1]
        return (
            f'<div class="avatar avatar-gradient" '
            f'style="background: linear-gradient(135deg, {color1}, {color2});">'
            f"<span>{initials}</span></div>"
        )

    elif style == "emoji":
        emoji = avatar.get("emoji", "🚀")
        return f'<div class="avatar avatar-emoji">{emoji}</div>'

    elif style == "initials":
        initials = avatar.get("initials", "N")
        color = avatar.get("color", "#5A54BD")
        return (
            f'<div class="avatar avatar-initials" style="background-color: {color};">'
            f"<span>{initials}</span></div>"
        )

    else:
        return '<div class="avatar avatar-default">✨</div>'


def validate_avatar(avatar: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate avatar dict structure.

    Args:
        avatar: Avatar dict to validate

    Returns:
        (is_valid, error_message)
    """
    if not isinstance(avatar, dict):
        return False, "Avatar must be a dict"

    if not avatar.get("persona_name"):
        return False, "persona_name is required"

    style = avatar.get("style")
    if style not in ("gradient", "emoji", "initials", "ai-generated"):
        return False, f"Invalid style: {style}"

    if style == "gradient":
        colors = avatar.get("colors", [])
        if not isinstance(colors, list) or len(colors) != 2:
            return False, "gradient style requires 2 colors"

    elif style == "emoji":
        if not avatar.get("emoji"):
            return False, "emoji style requires emoji field"

    elif style == "initials":
        if not avatar.get("initials") or not avatar.get("color"):
            return False, "initials style requires initials and color"

    return True, ""


# Typing hint
def _validate_avatar_dict(d: Dict[str, Any]) -> Tuple[bool, str]:
    """Type-hinted version of validate_avatar."""
    return validate_avatar(d)
