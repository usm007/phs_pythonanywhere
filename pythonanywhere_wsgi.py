"""WSGI entrypoint template for PythonAnywhere.

Usage on PythonAnywhere Web tab -> WSGI configuration file:
1) Adjust PROJECT_HOME and VENV_PATH for your username/path.
2) Paste this file content into the WSGI file (or import this file from there).
"""

import os
import sys

# Update these two paths for your PythonAnywhere account.
PROJECT_HOME = "/home/YOUR_USERNAME/phs_pythonanywhere"
VENV_PATH = "/home/YOUR_USERNAME/.virtualenvs/phs-venv"

if PROJECT_HOME not in sys.path:
    sys.path.insert(0, PROJECT_HOME)

# Optional: use virtualenv site-packages explicitly if needed.
python_version = f"python{sys.version_info.major}.{sys.version_info.minor}"
venv_site = os.path.join(VENV_PATH, "lib", python_version, "site-packages")
if os.path.isdir(venv_site) and venv_site not in sys.path:
    sys.path.insert(0, venv_site)

# Defaults suited for PythonAnywhere.
os.environ.setdefault("PHS_DB_MODE", "sqlite")
os.environ.setdefault("PHS_SECRET_KEY", "change-me-on-pythonanywhere")

from app import app as application
