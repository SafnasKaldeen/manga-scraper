name: Hourly News Scraper (Anime & Manga)
on:
  schedule:
    - cron: '0 * * * *'   # every hour
  workflow_dispatch:        # manual trigger from GitHub UI
jobs:
  scrape-news:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Chrome
        uses: browser-actions/setup-chrome@v1
        with:
          chrome-version: stable

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install --no-cache-dir \
            selenium \
            requests \
            beautifulsoup4 \
            supabase

      - name: Debug - check for shadowing files
        run: |
          echo "=== Repo root files ==="
          ls -la
          echo ""
          echo "=== Any file/folder that could shadow packages ==="
          find . -maxdepth 2 \( \
            -name "selenium.py" -o -name "selenium" \
            -o -name "requests.py" -o -name "requests" \
            -o -name "bs4.py" -o -name "supabase.py" \
          \) ! -path "./.git/*" ! -path "./__pycache__/*" | sort
          echo ""
          echo "=== Where Python finds selenium ==="
          python -c "import importlib.util; s=importlib.util.find_spec('selenium'); print(s.origin if s else 'NOT FOUND')"
          echo ""
          echo "=== Selenium version ==="
          pip show selenium | grep Version

      - name: Verify imports
        run: |
          cd /tmp
          python -c "
          from selenium import webdriver
          from selenium.webdriver.by import By
          from selenium.webdriver.chrome.options import Options
          import requests
          from bs4 import BeautifulSoup
          from supabase import create_client
          print('✓ All imports OK')
          "

      - name: Run scraper
        env:
          SUPABASE_
