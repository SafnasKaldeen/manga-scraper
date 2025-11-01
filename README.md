name: Scrape Manga Chapters

on:
repository_dispatch:
types: [scrape-manga]
workflow_dispatch:
inputs:
manga_slug:
description: 'Manga slug (e.g., one-piece)'
required: true
manga_url:
description: 'Manga URL'
required: true
manga_name:
description: 'Manga name (optional)'
required: false

jobs:
scrape:
runs-on: ubuntu-latest
timeout-minutes: 60

    steps:
      - name: Checkout repository
        uses: actions/checkout@v3
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
          cache: 'pip'

      - name: Install dependencies
        run: |
          pip install -r scraper/requirements.txt

      - name: Run scraper
        env:
          CLOUDINARY_CLOUD_NAME: ${{ secrets.CLOUDINARY_CLOUD_NAME }}
          CLOUDINARY_API_KEY: ${{ secrets.CLOUDINARY_API_KEY }}
          CLOUDINARY_API_SECRET: ${{ secrets.CLOUDINARY_API_SECRET }}
        run: |
          cd scraper
          python scrape_new_chapters.py \
            "${{ github.event.inputs.manga_slug || github.event.client_payload.manga_slug }}" \
            "${{ github.event.inputs.manga_url || github.event.client_payload.manga_url }}" \
            "${{ github.event.inputs.manga_name || github.event.client_payload.manga_name }}"

      - name: Commit metadata updates
        run: |
          git config user.name "Manga Scraper Bot"
          git config user.email "bot@github-actions"
          git add cloudinary_manga_metadata.csv
          git diff --staged --quiet || git commit -m "Update metadata for ${{ github.event.inputs.manga_slug || github.event.client_payload.manga_slug }}"
          git push
        continue-on-error: true
