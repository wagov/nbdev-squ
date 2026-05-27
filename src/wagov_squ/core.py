"""Core utilities for nbdev-squ package."""

__all__ = [
    "logger",
    "dirs",
    "cache",
    "retryer",
    "load_config",
    "login",
    "azcli",
    "datalake_path_safe",
    "datalake_path",
    "httpx",
    "chunks",
]

import logging
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from threading import RLock
from typing import Any

import httpx
import pandas
from benedict import benedict
from platformdirs import PlatformDirs
from tenacity import Retrying, stop_after_attempt, wait_random_exponential
from upath import UPath

logger = logging.getLogger(__name__)


def chunks(items, size):
    """Yield successive `size` chunks from `items`."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


dirs = PlatformDirs("nbdev-squ")
retryer = Retrying(wait=wait_random_exponential(), stop=stop_after_attempt(3), reraise=True)


class MemoryCache:
    """Small process-local cache with optional TTL, avoiding pickle-backed storage."""

    def __init__(self) -> None:
        self._items: dict[str, tuple[float | None, Any]] = {}
        self._lock = RLock()

    def set(self, key: str, value: Any, expire: float | None = None) -> None:
        expires_at = None if expire is None else time.monotonic() + expire
        with self._lock:
            self._items[key] = (expires_at, value)

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                return default
            expires_at, value = item
            if expires_at is not None and expires_at <= time.monotonic():
                self._items.pop(key, None)
                return default
            return value

    def delete(self, key: str) -> None:
        with self._lock:
            self._items.pop(key, None)

    def __getitem__(self, key: str) -> Any:
        if key not in self:
            raise KeyError(key)
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def __contains__(self, key: str) -> bool:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                return False
            expires_at, _ = item
            if expires_at is not None and expires_at <= time.monotonic():
                self._items.pop(key, None)
                return False
            return True


cache = MemoryCache()


def memoize_stampede(cache_obj: MemoryCache, expire: float) -> Callable:
    """Minimal memoization decorator compatible with the previous diskcache call sites."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = f"memo:{func.__module__}.{func.__qualname__}:{args!r}:{sorted(kwargs.items())!r}"
            if key in cache_obj:
                return cache_obj[key]
            value = func(*args, **kwargs)
            cache_obj.set(key, value, expire)
            return value

        return wrapper

    return decorator


def _az_cmd() -> list[str]:
    """Return the preferred Azure CLI command."""
    az = shutil.which("az")
    if az:
        return [az]
    return [sys.executable, "-m", "azure.cli"]


def _cli(cmd: list[str], capture_output=True):
    cmd = _az_cmd() + cmd
    if capture_output:  # Try lots, parse output as json
        try:
            result = retryer(
                subprocess.run,
                cmd + ["-o", "json"],
                capture_output=capture_output,
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError:
            # Clear login cache and show error
            cache.delete("logged_in")
            subprocess.run(cmd, check=True)
        try:
            result = benedict(result.stdout, format="json")
        except ValueError:  # handle if output not json
            logger.info("Falling back to plain cli output")
            result = result.stdout.strip().strip('"') or benedict()
        return result
    else:  # Run interactively, ignore success/fail
        subprocess.run(cmd)


def load_config(
    path=None,  # Path to read json config into cache from
):
    """Load configuration with Pydantic validation while maintaining backward compatibility."""
    import json

    from .settings import create_settings_from_dict, create_settings_from_env

    # If path provided, load from file (for testing/local dev)
    if path:
        data = json.loads(path.read_text())
        return create_settings_from_dict(data)

    try:
        # Configure Azure CLI for extension handling - only set if needed
        try:
            config = _cli(["config", "get"])
            # Parse config structure: {extension: [{name: "setting", value: "val"}]}
            ext_settings = {item["name"]: item["value"] for item in config.get("extension", [])}
            needs_update = (
                ext_settings.get("use_dynamic_install") != "yes_without_prompt"
                or ext_settings.get("dynamic_install_allow_preview") != "true"
            )
            if needs_update:
                _cli(["config", "set", "extension.use_dynamic_install=yes_without_prompt"])
                _cli(["config", "set", "extension.dynamic_install_allow_preview=true"])
        except subprocess.CalledProcessError:
            # Reset corrupted config - try CLI first, fallback to file removal
            try:
                subprocess.run(_az_cmd() + ["config", "delete", "--all"], check=True)
            except subprocess.CalledProcessError:
                # If CLI can't start due to corruption, remove config files directly
                azure_config = Path.home() / ".azure"
                if azure_config.exists():
                    shutil.rmtree(azure_config)
            _cli(["config", "set", "extension.use_dynamic_install=yes_without_prompt"])
            _cli(["config", "set", "extension.dynamic_install_allow_preview=true"])

        # Load from Key Vault
        vault_data = _cli(
            [
                "keyvault",
                "secret",
                "show",
                "--vault-name",
                cache["vault_name"],
                "--name",
                f"squconfig-{cache['tenant_id']}",
            ]
        ).value

        data = json.loads(vault_data)
        return create_settings_from_dict(data)

    except subprocess.CalledProcessError:
        cache.delete("logged_in")  # clear the logged in state
        # Fallback to environment variables
        return create_settings_from_env()


def login(
    refresh: bool = False,  # Force relogin
):
    if "/" in os.environ.get("SQU_CONFIG", ""):
        cache["vault_name"], cache["tenant_id"] = os.environ["SQU_CONFIG"].split("/")
    tenant = cache.get("tenant_id")
    try:
        _cli(["account", "show"])
        if tenant:
            tenant_visible = len(_cli(["account", "list"]).search(tenant)) > 0
            if not tenant_visible:
                from .exceptions import AuthenticationError

                raise AuthenticationError(
                    f"Tenant {tenant} not visible in authenticated account list"
                )
        cache.set("logged_in", True, 60 * 60 * 3)  # cache login state for 3 hrs
    except Exception:
        cache.delete("logged_in")
    while not cache.get("logged_in"):
        logger.info("Cache doesn't look logged in, attempting login")
        try:
            # See if we can login with a managed identity in under 5 secs and see the configured tenant
            subprocess.run(
                [
                    "timeout",
                    "5",
                    *_az_cmd(),
                    "login",
                    "--identity",
                    "-o",
                    "none",
                    "--allow-no-subscriptions",
                ],
                check=True,
            )
            if tenant:
                tenant_visible = len(_cli(["account", "list"]).search(tenant)) > 0
                if not tenant_visible:
                    from .exceptions import AuthenticationError

                    raise AuthenticationError(
                        f"Tenant {tenant} not accessible after authentication attempt"
                    )
        except Exception:
            # If managed identity unavailable, fall back on a manual login
            if tenant:
                tenant_scope = ["--tenant", tenant]
            else:
                tenant_scope = []
            _cli(
                [
                    "login",
                    *tenant_scope,
                    "--use-device-code",
                    "--allow-no-subscriptions",
                    "-o",
                    "none",
                ],
                capture_output=False,
            )
        # Finally, validate the login once more, and set the login state
        try:
            _cli(["account", "show"])
            cache.set("logged_in", True, 60 * 60 * 3)  # cache login state for 3 hrs
        except subprocess.CalledProcessError:
            cache.delete("logged_in")
    logger.info("Cache state is logged in")
    if cache.get("vault_name"):  # Always reload config on any login call
        logger.info("Loading config from keyvault")
        cache["config"] = load_config()  # Config lasts forever, don't expire


def azcli(basecmd: list[str]):
    if not cache.get("logged_in"):
        login()
    return _cli(basecmd)


@memoize_stampede(cache, expire=60 * 60 * 24)
def datalake_path_safe(expiry_days, permissions):
    if not cache.get("logged_in"):  # Have to login to grab keyvault config
        login()
    expiry = pandas.Timestamp("now") + pandas.Timedelta(days=expiry_days)
    account = cache["config"]["datalake_account"].split(".")[
        0
    ]  # Grab the account name, not the full FQDN
    container = cache["config"]["datalake_container"]
    sas = azcli(
        [
            "storage",
            "container",
            "generate-sas",
            "--auth-mode",
            "login",
            "--as-user",
            "--account-name",
            account,
            "--name",
            container,
            "--permissions",
            permissions,
            "--expiry",
            str(expiry.date()),
        ]
    )
    return (container, account, sas)


def datalake_path(
    expiry_days: int = 3,  # Number of days until the SAS token expires
    permissions: str = "racwdlt",  # Permissions to grant on the SAS token
):
    container, account, sas = datalake_path_safe(expiry_days, permissions)
    return UPath(f"az://{container}", account_name=account, sas_token=sas)
