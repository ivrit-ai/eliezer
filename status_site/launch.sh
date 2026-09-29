#!/bin/bash
set -e
python3 -c "import app; app.init_db()"
exec waitress-serve --host 0.0.0.0 --port "$PORT" app:app
