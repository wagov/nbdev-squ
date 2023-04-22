# AUTOGENERATED! DO NOT EDIT! File to edit: ../nbs/00_core.ipynb.

# %% auto 0
__all__ = ['dirs', 'cache', 'retryer', 'load_config', 'login', 'azcli', 'datalake_path']

# %% ../nbs/00_core.ipynb 3
import json, subprocess, os, sys, pandas
from platformdirs import PlatformDirs
from diskcache import Cache, memoize_stampede
from tenacity import wait_random_exponential, stop_after_attempt, Retrying
from upath import UPath

# %% ../nbs/00_core.ipynb 5
dirs = PlatformDirs("nbdev-squ")
cache = Cache(dirs.user_cache_dir)
retryer = Retrying(wait=wait_random_exponential(), stop=stop_after_attempt(3))

# %% ../nbs/00_core.ipynb 6
def _cli(cmd: list[str], capture_output=True):
    cmd = [sys.executable, "-m", "azure.cli"] + cmd + ["-o", "json"]
    if capture_output: # Try lots, parse output as json
        result = retryer(subprocess.run, cmd, capture_output=capture_output, check=True)
        return json.loads(result.stdout.decode("utf8"))
    else: # Run interactively, ignore success/fail
        subprocess.run(cmd)

# %% ../nbs/00_core.ipynb 8
def load_config(path = None # Path to read json config into cache from
               ):
    if path:
        return json.loads(path.read_text())
    try:
        return json.loads(_cli(["keyvault", "secret", "show", 
                                "--vault-name", cache["vault_name"], 
                                "--name", f"squconfig-{cache['tenant_id']}"])["value"])
    except subprocess.CalledProcessError:
        return {}

def login(refresh: bool=False # Force relogin
         ):
    if "/" in os.environ.get("SQU_CONFIG", ""):
        cache["vault_name"], cache["tenant_id"] = os.environ["SQU_CONFIG"].split("/")
    if os.environ.get("IDENTITY_HEADER") and not cache.get("msi_failed"):
        if refresh: # Forced logout only makes sense for managed service identity (msi) signins
            _cli(["logout"])
        try:
            _cli(["login", "--identity"])
        except subprocess.CalledProcessError:
            cache["msi_failed"] = True
        else:
            cache.delete("msi_failed")
            cache["logged_in"] = True
            cache["login_time"] = pandas.Timestamp("now")
    if not os.environ.get("IDENTITY_HEADER") or cache.get("msi_failed"):
        while not cache.get("logged_in"):
            try:
                _cli(["account", "show"])
            except subprocess.CalledProcessError:
                tenant = cache.get("tenant_id", [])
                if tenant:
                    tenant = ["--tenant", tenant]
                _cli(["login", *tenant, "--use-device-code"], capture_output=False)
            else:
                cache["logged_in"] = True
                cache["login_time"] = pandas.Timestamp("now")
    if cache.get("vault_name"):
        for key, value in load_config().items():
            cache[key] = value

# %% ../nbs/00_core.ipynb 11
@memoize_stampede(cache, expire=300)
def azcli(basecmd: list[str]):
    if not cache.get("logged_in"):
        login()
    return _cli(basecmd)

# %% ../nbs/00_core.ipynb 13
@memoize_stampede(cache, expire=60 * 60 * 24)
def datalake_path(expiry_days: int=3, # Number of days until the SAS token expires
                  permissions: str="racwdlt" # Permissions to grant on the SAS token
                    ):
    expiry = pandas.Timestamp("now") + pandas.Timedelta(days=expiry_days)
    account = cache["datalake_account"].split(".")[0] # Grab the account name, not the full FQDN
    sas = azcli(["storage", "container", "generate-sas", "--auth-mode", "login", "--as-user", 
                 "--account-name", account, "--name", cache["datalake_container"], "--permissions", permissions, "--expiry", str(expiry.date())])
    return UPath(f"az://{cache['datalake_container']}", account_name=account, sas_token=sas)
