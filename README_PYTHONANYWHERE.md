# PHS PythonAnywhere Copy

This folder is a PythonAnywhere-ready copy of the PHS site.

## What is preconfigured
- Uses SQLite by default (`PHS_DB_MODE=sqlite` in `app.py`).
- Includes app code, templates, static files, and `school_marks.db`.
- Includes `pythonanywhere_wsgi.py` template.

## Deploy steps on PythonAnywhere
1. Upload this folder to: `/home/<your-username>/phs_pythonanywhere`
2. Create a virtualenv (example):
   - `mkvirtualenv phs-venv --python=python3.10`
3. Install dependencies:
   - `pip install -r /home/<your-username>/phs_pythonanywhere/requirements.txt`
4. In the **Web** tab:
   - Set source code path to `/home/<your-username>/phs_pythonanywhere`
   - Open WSGI config file and replace with content from `pythonanywhere_wsgi.py`
   - Update `PROJECT_HOME` and `VENV_PATH` values in that file
5. In PythonAnywhere **Web** tab -> **Environment variables**, set:
   - `PHS_SECRET_KEY=<your-strong-random-secret>`
   - `PHS_ADMIN_PIN=2026`
6. Reload the web app from Web tab.

## Notes
- SQLite DB file is `school_marks.db` inside this folder.
- `PHS_SECRET_KEY` and `PHS_ADMIN_PIN` must be set in PythonAnywhere environment variables.
- Current required admin PIN for this deployment is `2026`.
- If you later want TiDB, set `PHS_DB_MODE=tidb` and configure TiDB env vars.

## Admin dashboard features
- Open the portal home and click the `Admin` button.
- Enter Master PIN to unlock `/admin/dashboard`.
- Global settings can update school/exam/session labels across pages and printouts.
- `Portal Lock` disables all edit/save/delete/import actions while keeping read-only views available.
- `Download Master Excel` exports settings, students, marks, and class summary to `.xlsx`.
- `Download Raw DB Backup` downloads `school_marks.db` when running in SQLite mode.
- `Soft Reset` removes all marks plus mark-related logs.
- `Factory Reset` removes students, marks, and related logs.
