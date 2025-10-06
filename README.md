# DGA Quoting Tool (Streamlit)

## Quick start (PyCharm)
1) Open PyCharm → **Open** this folder (`dga-quote-tool`).  
2) Ensure the project interpreter is the virtualenv PyCharm created.  
3) Open `requirements.txt` → click **Install** (or use terminal: `pip install -r requirements.txt`).  
4) Copy `.env.example` to `.env` and fill in values.  
5) Run via Terminal: `streamlit run app.py`  
   - Or use the pre-made **Run Configuration**: Run ▶ Streamlit.

## One-liner (Terminal)
```
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt && streamlit run app.py
```

Products live in `products.csv`. Update freely.
