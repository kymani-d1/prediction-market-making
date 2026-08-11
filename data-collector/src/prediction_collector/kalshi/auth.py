from __future__ import annotations

import base64
import time
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


class KalshiAuthenticationError(RuntimeError):
    pass


class KalshiSigner:
    """RSA-PSS request signer for Kalshi's read-only WebSocket handshake."""

    def __init__(self, key_id: str, private_key_path: Path) -> None:
        self.key_id = key_id
        self.private_key_path = private_key_path
        self._private_key: rsa.RSAPrivateKey | None = None

    def _load_key(self) -> rsa.RSAPrivateKey:
        if self._private_key is not None:
            return self._private_key
        try:
            pem = self.private_key_path.read_bytes()
        except OSError as exc:
            raise KalshiAuthenticationError(
                f"Cannot read Kalshi private key file at {self.private_key_path}"
            ) from exc
        try:
            key = serialization.load_pem_private_key(pem, password=None)
        except (TypeError, ValueError) as exc:
            raise KalshiAuthenticationError("Kalshi private key file is not valid PEM") from exc
        if not isinstance(key, rsa.RSAPrivateKey):
            raise KalshiAuthenticationError("Kalshi private key must be an RSA key")
        self._private_key = key
        return key

    @staticmethod
    def signing_path(path_or_url: str) -> str:
        parsed = urlsplit(path_or_url)
        path = parsed.path if parsed.scheme or parsed.netloc else path_or_url.split("?", 1)[0]
        if not path.startswith("/"):
            path = "/" + path
        return path

    def sign(self, timestamp_ms: str, method: str, path_or_url: str) -> str:
        path = self.signing_path(path_or_url)
        message = f"{timestamp_ms}{method.upper()}{path}".encode("utf-8")
        signature = self._load_key().sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=hashes.SHA256().digest_size,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("ascii")

    def headers(
        self,
        method: str,
        path_or_url: str,
        *,
        timestamp_ms: int | None = None,
    ) -> dict[str, str]:
        timestamp = str(timestamp_ms if timestamp_ms is not None else int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": self.sign(timestamp, method, path_or_url),
        }

