#!/bin/bash
set -e
pip install -r requirements.txt
python3 -c "import app; app.init_db()"
if [ "$RESET_DB" = "1" ]; then
  python3 -c "import app; app.reset_db()"
fi
