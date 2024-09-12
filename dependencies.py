import subprocess
import sys
import os
from typing import Callable, Any
import pandas as pd
import logging
import requests
from requests.auth import HTTPBasicAuth
import datetime as dt

# ===================
# pandarallel import
# ===================

def install_pandarallel():
    try:
        import pandarallel
    except ImportError:
        response = input("Pandarallel is not installed. Would you like to install it via pip? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            print("Installing pandarallel...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pandarallel"])
            print("Pandarallel has been installed.")
        else:
            print("Pandarallel was not installed.")


install_pandarallel()

try:
    from pandarallel import pandarallel
except ImportError:
    print("Pandarallel is not available.")

pandarallel.initialize(progress_bar=True, verbose=0)

