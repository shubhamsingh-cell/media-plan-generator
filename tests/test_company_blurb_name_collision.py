"""A same-name company that lacks the client's qualifier is a different entity.

Verified 2026-10-16 (output sweep, bundle l_global_pharma) before the fix: for
client "Helix Biopharma Global" the cover blurb, the workbook's Company Profile
and the deck's competitive-landscape row all described Helix BioPharma Corp.,
a Toronto oncology company -- a name collision the gate missed because
``name_agrees`` ignores "Global" as noise, so "helix"+"biopharma" were both
found in the article's subject. The gate now also requires every qualifier
word of the client's name (Global / International / Group / Holdings) to
appear in the article's name, unless the client's website domain is the
article subject's own domain.

The Helix inputs below are the sweep brief's, copied by hand (the ledger is
not a test dependency).
"""

from __future__ import annotations

import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import company_blurb as cb  # noqa: E402

HELIX_CLIENT = "Helix Biopharma Global"
HELIX_ARTICLE = (
    "Helix BioPharma Corp. is a Canadian biopharmaceutical company, that was "
    "founded in 1995, with its headquarters in Toronto, Canada. Helix "
    "specializes in researching and developing novel drugs to tackle "
    "hard-to-treat cancers, including solid tumors and hematological "
    "malignancies."
)
HELIX_TITLE = "Helix BioPharma"
HELIX_ROLES = [
    "Clinical Research Associate",
    "Process Development Scientist",
    "Biostatistician",
    "Manufacturing Technician",
    "Medical Science Liaison",
    "QA Specialist",
]


def _helix_data(**extra):
    data = {
        "client_name": HELIX_CLIENT,
        "client_website": "https://www.helixbiopharma.example",
        "industry": "pharma_biotech",
        "industry_label": "Pharma & Biotech",
        "target_roles": HELIX_ROLES,
        "_enriched": {
            "company_info": {"description": HELIX_ARTICLE, "wiki_title": HELIX_TITLE}
        },
    }
    data.update(extra)
    return data


class TestHelixCollision:
    def test_validator_rejects_the_same_name_company(self):
        ok, why = cb.validate_company_description(
            HELIX_CLIENT,
            HELIX_ARTICLE,
            title=HELIX_TITLE,
            industry="Pharma & Biotech",
            roles=HELIX_ROLES,
        )
        assert ok is False
        assert "global" in why and "same-name" in why

    def test_cover_blurb_falls_back_to_none(self):
        assert cb.client_company_description(_helix_data()) == ""

    def test_lookup_form_without_an_industry_rejects_it_too(self):
        # api_enrichment's Wikipedia pre-filter passes no industry
        ok, _why = cb.validate_company_description(
            HELIX_CLIENT, HELIX_ARTICLE, title=HELIX_TITLE
        )
        assert ok is False

    def test_the_article_is_the_clients_own_without_the_qualifier(self):
        ok, _why = cb.validate_company_description(
            "Helix BioPharma",
            HELIX_ARTICLE,
            title=HELIX_TITLE,
            industry="Pharma & Biotech",
            roles=HELIX_ROLES,
        )
        assert ok is True

    def test_a_different_tld_is_not_the_same_site(self):
        data = _helix_data()
        data["_enriched"]["company_info"]["domain"] = "helixbiopharma.com"
        assert cb.client_company_description(data) == ""  # .example != .com

    def test_matching_website_domain_confirms_the_entity(self):
        data = _helix_data(client_website="https://www.helixbiopharma.com/careers")
        data["_enriched"]["company_info"]["domain"] = "helixbiopharma.com"
        assert cb.client_company_description(data) == HELIX_ARTICLE


class TestQualifierRule:
    @pytest.mark.parametrize(
        "client,subject",
        [
            ("Acme Global", "Acme Corp."),
            ("Acme International", "Acme Inc."),
            ("Acme Group", "Acme Foods Inc."),
            ("Acme Holdings", "Acme Corporation"),
            ("Acme Global Holdings", "Acme Global Inc."),  # one of two missing
        ],
    )
    def test_qualifier_only_the_client_carries_disagrees(self, client, subject):
        assert cb.name_qualifiers_agree(client, subject) is False

    @pytest.mark.parametrize(
        "client,subject",
        [
            ("Acme", "Acme Global Inc."),  # shorthand: article adds a word
            ("Marriott", "Marriott International, Inc."),
            ("Uber", "Uber Technologies, Inc."),
            ("Acme Global", "Acme Global Inc."),
            ("Acme Global", "Acme Inc. (Acme Global)"),
            ("Acme Inc.", "Acme Corp."),  # legal forms are not qualifiers
            ("Acme Company", "Acme"),
            ("", "Acme"),
        ],
    )
    def test_everything_else_agrees(self, client, subject):
        assert cb.name_qualifiers_agree(client, subject) is True

    def test_article_title_can_carry_the_qualifier(self):
        assert cb.name_qualifiers_agree("Acme Global", "Acme Inc.", "Acme Global")


class TestExistingBehaviourUnchanged:
    @pytest.mark.parametrize(
        "client,article,title,industry",
        [
            (
                "Uber",
                "Uber Technologies, Inc. is an American multinational "
                "transportation company that provides ride-hailing services.",
                "Uber",
                "rideshare",
            ),
            (
                "Marriott",
                "Marriott International, Inc. is an American multinational "
                "company that operates hotels.",
                "Marriott International",
                "hospitality_travel",
            ),
            (
                "Marriott International",
                "Marriott International, Inc. is an American multinational "
                "company that operates hotels.",
                "Marriott International",
                "hospitality_travel",
            ),
        ],
    )
    def test_shorthand_and_full_names_still_validate(
        self, client, article, title, industry
    ):
        ok, why = cb.validate_company_description(
            client, article, title=title, industry=industry
        )
        assert ok, why

    def test_verb_led_definition_applies_the_same_rule(self):
        article = (
            "Acme Foods owns and operates grocery stores across Ohio. The "
            "company was founded in 1978."
        )
        ok, _ = cb.validate_company_description("Acme Foods", article)
        assert ok is True
        ok, why = cb.validate_company_description("Acme Foods Global", article)
        assert ok is False and "qualifier" in why


class TestDomainsMatch:
    @pytest.mark.parametrize(
        "site,domain",
        [
            ("https://www.helixbiopharma.com", "helixbiopharma.com"),
            ("helixbiopharma.com", "https://helixbiopharma.com/about"),
            ("HTTP://WWW.Acme.COM:8080/x?y=1", "acme.com"),
            ("careers.acme.com", "acme.com"),
        ],
    )
    def test_same_site(self, site, domain):
        assert cb.domains_match(site, domain) is True

    @pytest.mark.parametrize(
        "site,domain",
        [
            ("https://www.helixbiopharma.example", "helixbiopharma.com"),
            ("acme.com", "acme.org"),
            ("acme.com", "notacme.com"),
            ("acme.com", ""),
            ("", "acme.com"),
            (None, None),
        ],
    )
    def test_not_the_same_site(self, site, domain):
        assert cb.domains_match(site, domain) is False
