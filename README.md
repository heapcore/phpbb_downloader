# phpbb_downloader

> **WARNING:** This repository may be unstable or non-functional. Use at your own risk.

Offline downloader for phpBB forums.

The tool crawls forum/topic pages, downloads local assets (CSS/JS/images/attachments), rewrites links, and saves a browsable local mirror.

## Requirements

- Python 3.9+
- `requests`
- `beautifulsoup4`

Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Primary command:

```bash
python main.py "https://forum.example.com/" \
  --output-dir mirror \
  --db-file database.txt \
  --log-file log.txt \
  --broken-file broken_links.txt
```

Notes:

- Download state is stored in:
  - `database.txt` for visited/downloaded URLs
  - `broken_links.txt` for failed URLs
  - `log.txt` for logs

## License

See `LICENSE`.
