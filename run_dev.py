"""
PHS Dev Server — live reload on file changes.
Run via run_dev.bat, not directly.
"""
import os
os.environ["FLASK_ENV"] = "development"
os.environ["FLASK_DEBUG"] = "1"

from app import app
from livereload import Server

server = Server(app.wsgi_app)

# Watch templates and static assets — reload browser on any change
server.watch("templates/")
server.watch("static/")

# Watch app.py — full server restart handled by livereload's subprocess mode
server.watch("app.py")

print("  PHS Dev Server running at http://127.0.0.1:5000")
print("  Watching: templates/, static/, app.py")
print("  Browser will auto-reload on file save.\n")

server.serve(host="127.0.0.1", port=5000, open_url_delay=1)
