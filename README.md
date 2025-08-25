# AI Accountant Detective

![App landing](./landing.jpeg)

AI Accountant Detective that does 100% materiality assesment and focus on the suspicious transactions.

**Quick Start**

- Prereqs: Python 3.10+ and `uv` installed.
- Create venv: `uv venv`
- Activate venv (macOS/Linux): `source .venv/bin/activate`
- Install deps: `uv pip install -r requirements.txt`

**Run the App**

- Start: `uv run main.py`
- Open: `http://127.0.0.1:8080` (home)

**Live Agent (OpenAI)**

- Set your key: `export OPENAI_API_KEY=sk-...`
- Enable live agent: `export LIVE_AGENT=true` (uses OpenAI for checks)
- No key? Leave live agent off (default) or run explicitly: `LIVE_AGENT=0 uv run main.py` to use the built-in dummy run.
