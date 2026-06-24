#!/bin/bash
exec waitress-serve --host 0.0.0.0 --port "$PORT" app:app
