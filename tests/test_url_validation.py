"""Tests for URL security validation in web_scraper_router.

Tests the _validate_url_security() function to ensure SSRF attacks are blocked.
"""

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from web_scraper_router import _validate_url_security


class TestURLSecurityValidation:
    """Tests for SSRF attack prevention in URL validation."""

    # =========================================================================
    # ALLOWED VALID URLs
    # =========================================================================

    def test_valid_https_public_urls(self) -> None:
        """Valid public HTTPS URLs should be allowed."""
        valid_urls = [
            "https://example.com",
            "https://www.google.com",
            "https://github.com/user/repo",
            "https://api.example.com/v1/data",
            "https://example.com/path/to/page?query=value",
            "https://example.com/path#fragment",
            "https://subdomain.example.com",
            "https://example.co.uk",
            "https://192.0.2.1",  # Public IP (documentation range)
            "https://203.0.113.42",  # Public IP (documentation range)
        ]

        for url in valid_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is True, f"Valid URL rejected: {url}, error: {error_msg}"
            assert error_msg == "", f"Valid URL has error message: {url}, {error_msg}"

    def test_valid_http_public_urls(self) -> None:
        """Valid public HTTP URLs should be allowed."""
        valid_urls = [
            "http://example.com",
            "http://www.example.com",
            "http://api.example.com/endpoint",
            "http://example.com:8080",
            "http://example.com:8080/path",
        ]

        for url in valid_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is True, f"Valid HTTP URL rejected: {url}"

    # =========================================================================
    # BLOCKED: DANGEROUS SCHEMES
    # =========================================================================

    def test_blocked_file_scheme(self) -> None:
        """File scheme (file://) should be blocked."""
        is_valid, error_msg = _validate_url_security("file:///etc/passwd")
        assert is_valid is False, "file:// scheme should be blocked"
        assert "scheme" in error_msg.lower()

    def test_blocked_gopher_scheme(self) -> None:
        """Gopher scheme should be blocked."""
        is_valid, error_msg = _validate_url_security("gopher://example.com")
        assert is_valid is False, "gopher:// scheme should be blocked"

    def test_blocked_ftp_scheme(self) -> None:
        """FTP scheme should be blocked."""
        is_valid, error_msg = _validate_url_security("ftp://example.com")
        assert is_valid is False, "ftp:// scheme should be blocked"

    def test_blocked_custom_scheme(self) -> None:
        """Custom schemes should be blocked."""
        is_valid, error_msg = _validate_url_security("custom://example.com")
        assert is_valid is False, "custom:// scheme should be blocked"

    # =========================================================================
    # BLOCKED: PRIVATE/INTERNAL IPs
    # =========================================================================

    def test_blocked_loopback_127(self) -> None:
        """127.* loopback range should be blocked."""
        blocked_ips = [
            "http://127.0.0.1",
            "https://127.0.0.1:8080",
            "http://127.0.0.2",
            "https://127.255.255.255",
        ]

        for url in blocked_ips:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"Loopback IP not blocked: {url}"
            # 127.0.0.1 is treated as reserved hostname; others as private IP
            assert (
                "private" in error_msg.lower()
                or "reserved" in error_msg.lower()
                or "ssrf" in error_msg.lower()
            )

    def test_blocked_private_192_168(self) -> None:
        """192.168.* private range should be blocked."""
        blocked_ips = [
            "http://192.168.1.1",
            "https://192.168.1.254",
            "http://192.168.0.1",
            "http://192.168.255.255",
        ]

        for url in blocked_ips:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"192.168 private IP not blocked: {url}"

    def test_blocked_private_10(self) -> None:
        """10.* private range should be blocked."""
        blocked_ips = [
            "http://10.0.0.1",
            "https://10.255.255.255",
            "http://10.1.2.3",
            "http://10.0.0.254",
        ]

        for url in blocked_ips:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"10.* private IP not blocked: {url}"

    def test_blocked_private_172_16_31(self) -> None:
        """172.16-31.* private range should be blocked."""
        blocked_ips = [
            "http://172.16.0.0",
            "https://172.16.0.1",
            "http://172.20.0.1",
            "http://172.31.255.255",
        ]

        for url in blocked_ips:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"172.16-31.* private IP not blocked: {url}"

    def test_blocked_link_local_169_254(self) -> None:
        """169.254.* link-local range should be blocked."""
        blocked_ips = [
            "http://169.254.0.1",
            "https://169.254.1.1",
            "http://169.254.169.253",
            "http://169.254.255.255",
        ]

        for url in blocked_ips:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"169.254.* link-local IP not blocked: {url}"

    # =========================================================================
    # BLOCKED: AWS METADATA ENDPOINT (CRITICAL SSRF VECTOR)
    # =========================================================================

    def test_blocked_aws_metadata_169_254_169_254(self) -> None:
        """AWS metadata endpoint 169.254.169.254 should be blocked."""
        blocked_urls = [
            "http://169.254.169.254/",
            "http://169.254.169.254/latest/meta-data/",
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            "https://169.254.169.254/latest/api/token",
        ]

        for url in blocked_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"AWS metadata endpoint not blocked: {url}"

    def test_blocked_aws_metadata_alternate_names(self) -> None:
        """AWS metadata via alternate names should be blocked."""
        blocked_urls = [
            "http://169.254.169.254",
        ]

        for url in blocked_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"AWS metadata endpoint not blocked: {url}"
            assert (
                "aws" in error_msg.lower()
                or "metadata" in error_msg.lower()
                or "169.254.169.254" in error_msg
            )

    # =========================================================================
    # BLOCKED: RESERVED HOSTNAMES
    # =========================================================================

    def test_blocked_localhost(self) -> None:
        """localhost hostname should be blocked."""
        blocked_urls = [
            "http://localhost",
            "https://localhost:3000",
            "http://localhost:8080",
            "http://LOCALHOST",  # Case insensitive
        ]

        for url in blocked_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"localhost not blocked: {url}"

    def test_blocked_0_0_0_0(self) -> None:
        """0.0.0.0 should be blocked."""
        is_valid, error_msg = _validate_url_security("http://0.0.0.0")
        assert is_valid is False, "0.0.0.0 should be blocked"

    def test_blocked_ipv6_loopback(self) -> None:
        """IPv6 loopback ::1 should be blocked."""
        is_valid, error_msg = _validate_url_security("http://[::1]")
        assert is_valid is False, "IPv6 loopback ::1 should be blocked"

    # =========================================================================
    # EDGE CASES
    # =========================================================================

    def test_invalid_url_format(self) -> None:
        """Malformed URLs should be rejected."""
        invalid_urls = [
            "not a url",
            "ht!tp://example.com",
            "",
            "   ",
        ]

        for url in invalid_urls:
            is_valid, error_msg = _validate_url_security(url)
            # Empty strings are handled by caller (scrape_url), but invalid formats fail here
            if url.strip():
                assert is_valid is False, f"Invalid URL should be rejected: {url}"

    def test_missing_hostname(self) -> None:
        """URL without hostname should be rejected."""
        is_valid, error_msg = _validate_url_security("http://")
        assert is_valid is False, "URL without hostname should be rejected"

    def test_url_with_port(self) -> None:
        """Valid URL with port should be allowed."""
        is_valid, error_msg = _validate_url_security("https://example.com:443")
        assert is_valid is True, "Valid URL with port should be allowed"

    def test_url_with_auth(self) -> None:
        """Valid URL with basic auth should be allowed (hostname still checked)."""
        is_valid, error_msg = _validate_url_security("https://user:pass@example.com")
        assert is_valid is True, "Valid URL with basic auth should be allowed"

    def test_case_insensitive_hostname(self) -> None:
        """Hostname validation should be case-insensitive."""
        is_valid, error_msg = _validate_url_security("http://LOCALHOST:3000")
        assert is_valid is False, "Localhost (uppercase) should be blocked"

    def test_case_insensitive_scheme(self) -> None:
        """Scheme validation should be case-insensitive."""
        valid_urls = [
            "HTTP://example.com",
            "HTTPS://example.com",
            "HtTpS://example.com",
        ]

        for url in valid_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is True, f"Mixed-case valid scheme rejected: {url}"

    def test_dangerous_scheme_case_insensitive(self) -> None:
        """Dangerous scheme blocking should be case-insensitive."""
        dangerous_urls = [
            "FILE://etc/passwd",
            "File://etc/passwd",
            "fILe://etc/passwd",
        ]

        for url in dangerous_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert (
                is_valid is False
            ), f"Dangerous scheme (mixed case) not blocked: {url}"

    # =========================================================================
    # REAL-WORLD SSRF ATTACK SCENARIOS
    # =========================================================================

    def test_ssrf_scenario_aws_credentials(self) -> None:
        """SSRF attack attempting to fetch AWS credentials should be blocked."""
        malicious_urls = [
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/my-role",
        ]

        for url in malicious_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"AWS credential theft attempt not blocked: {url}"

    def test_ssrf_scenario_internal_service(self) -> None:
        """SSRF attack targeting internal services should be blocked."""
        malicious_urls = [
            "http://10.0.0.1:5000/admin",
            "http://192.168.1.1:8080/api/users",
            "http://localhost:3000/secrets",
        ]

        for url in malicious_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"Internal service attack not blocked: {url}"

    def test_ssrf_scenario_localhost_port_scan(self) -> None:
        """SSRF attack attempting localhost port scan should be blocked."""
        malicious_urls = [
            "http://127.0.0.1:22",  # SSH
            "http://127.0.0.1:3306",  # MySQL
            "http://127.0.0.1:5432",  # PostgreSQL
            "http://localhost:6379",  # Redis
        ]

        for url in malicious_urls:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is False, f"Port scan attack not blocked: {url}"

    def test_valid_news_site_still_works(self) -> None:
        """Public news sites should still be accessible."""
        public_news_sites = [
            "https://news.ycombinator.com",
            "https://bbc.com",
            "https://reuters.com",
            "https://reuters.com/world/us",
        ]

        for url in public_news_sites:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is True, f"Valid news site blocked: {url}"

    def test_valid_job_boards_still_work(self) -> None:
        """Public job board sites should still be accessible."""
        public_job_sites = [
            "https://linkedin.com/jobs",
            "https://indeed.com",
            "https://glassdoor.com",
            "https://dice.com",
        ]

        for url in public_job_sites:
            is_valid, error_msg = _validate_url_security(url)
            assert is_valid is True, f"Valid job board blocked: {url}"
