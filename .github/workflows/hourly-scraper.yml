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

      - name: Cache Python dependencies
        uses: actions/cache@v3
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-news-${{ hashFiles('**/requirements.txt') }}
          restore-keys: |
            ${{ runner.os }}-pip-news-

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install \
            selenium \
            requests \
            beautifulsoup4 \
            supabase

      - name: Run scraper
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
        run: |
          echo "▶ Starting anime & manga news scraper..."
          python scraper.py
          echo "✓ Scraper finished"

      - name: Upload logs on failure
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: scraper-logs-${{ github.run_number }}
          path: |
            *.log
            *.txt
          retention-days: 7
