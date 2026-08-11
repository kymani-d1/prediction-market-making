from __future__ import annotations

import base64
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa

from prediction_collector.kalshi.auth import KalshiAuthenticationError, KalshiSigner


@pytest.fixture
def ephemeral_rsa_key(workspace_tmp_path: Path) -> tuple[Path, rsa.RSAPrivateKey]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = workspace_tmp_path / "ephemeral-kalshi-test-key.pem"
    path.write_bytes(pem)
    return path, private_key


def verify(
    private_key: rsa.RSAPrivateKey,
    signature: str,
    message: bytes,
) -> None:
    private_key.public_key().verify(
        base64.b64decode(signature, validate=True),
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=hashes.SHA256().digest_size,
        ),
        hashes.SHA256(),
    )


def test_signer_uses_timestamp_uppercase_method_and_path_without_query(
    ephemeral_rsa_key: tuple[Path, rsa.RSAPrivateKey],
) -> None:
    path, private_key = ephemeral_rsa_key
    signer = KalshiSigner("ephemeral-key-id", path)

    signature = signer.sign(
        "1786456801230",
        "get",
        "wss://external-api-ws.kalshi.com/trade-api/ws/v2?ignored=yes",
    )

    verify(private_key, signature, b"1786456801230GET/trade-api/ws/v2")
    with pytest.raises(InvalidSignature):
        verify(
            private_key,
            signature,
            b"1786456801230GET/trade-api/ws/v2?ignored=yes",
        )


def test_headers_are_complete_and_signature_is_verifiable(
    ephemeral_rsa_key: tuple[Path, rsa.RSAPrivateKey],
) -> None:
    path, private_key = ephemeral_rsa_key
    signer = KalshiSigner("ephemeral-key-id", path)

    headers = signer.headers("GET", "/trade-api/ws/v2?query=ignored", timestamp_ms=123456)

    assert headers.keys() == {
        "KALSHI-ACCESS-KEY",
        "KALSHI-ACCESS-TIMESTAMP",
        "KALSHI-ACCESS-SIGNATURE",
    }
    assert headers["KALSHI-ACCESS-KEY"] == "ephemeral-key-id"
    assert headers["KALSHI-ACCESS-TIMESTAMP"] == "123456"
    verify(
        private_key,
        headers["KALSHI-ACCESS-SIGNATURE"],
        b"123456GET/trade-api/ws/v2",
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("trade-api/ws/v2", "/trade-api/ws/v2"),
        ("/trade-api/ws/v2?x=1", "/trade-api/ws/v2"),
        ("https://example.test/path/to/resource?x=1", "/path/to/resource"),
    ],
)
def test_signing_path_normalisation(value: str, expected: str) -> None:
    assert KalshiSigner.signing_path(value) == expected


def test_missing_invalid_and_non_rsa_keys_raise_safe_errors(
    workspace_tmp_path: Path,
) -> None:
    with pytest.raises(KalshiAuthenticationError, match="Cannot read"):
        KalshiSigner("id", workspace_tmp_path / "missing.pem").sign("1", "GET", "/path")

    invalid = workspace_tmp_path / "invalid.pem"
    invalid.write_text("not a private key", encoding="utf-8")
    with pytest.raises(KalshiAuthenticationError, match="not valid PEM"):
        KalshiSigner("id", invalid).sign("1", "GET", "/path")

    ec_key = ec.generate_private_key(ec.SECP256R1())
    ec_path = workspace_tmp_path / "ec.pem"
    ec_path.write_bytes(
        ec_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    with pytest.raises(KalshiAuthenticationError, match="must be an RSA key"):
        KalshiSigner("id", ec_path).sign("1", "GET", "/path")
