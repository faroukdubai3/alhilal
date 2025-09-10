import argparse
import os
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as date_parser
from newspaper import Article
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import supabase
from postgrest.exceptions import APIError


def save_topic_headlines_to_json(newstopics_topicid: str, json_file_basename: str) -> str:
    """
    Fetch topic headlines from Google News and save the raw payload as JSON.
    Returns the path to the saved JSON file.
    """
    from pygooglenews import GoogleNews

    print(f"[1/5] Fetching Google News headlines for topic: {newstopics_topicid}")
    gn = GoogleNews(lang='ar', country='SA')
    news_results: Dict[str, Any] = gn.topic_headlines(newstopics_topicid)

    json_file_path = f'{json_file_basename}.json'
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(news_results, json_file, ensure_ascii=False, indent=4)
    print(f"[2/5] Saved raw headlines JSON to: {json_file_path}")
    return json_file_path


def extract_values_from_json_file(file_path: str) -> List[Dict[str, Optional[str]]]:
    print(f"[3/5] Extracting entries from: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    extracted_data: List[Dict[str, Optional[str]]] = []
    for entry in data.get('entries', []):
        link = entry.get('link')
        source_href = entry.get('source', {}).get('href') if isinstance(entry.get('source'), dict) else None
        source_title = entry.get('source', {}).get('title') if isinstance(entry.get('source'), dict) else None
        extracted_data.append({
            'link': link,
            'source_href': source_href,
            'source_title': source_title,
        })
    print(f"[3/5] Extracted {len(extracted_data)} entries from JSON")
    return extracted_data


def count_characters_and_check(string: Optional[str]) -> bool:
    try:
        if string is None:
            return False
        return len(string) > 500
    except Exception:
        return False


def ensure_nltk_punkt() -> bool:
    """
    Ensure NLTK 'punkt' tokenizer is available for newspaper3k's NLP summary.
    Returns True if available, False otherwise.
    """
    try:
        import nltk
        try:
            nltk.data.find('tokenizers/punkt')
            return True
        except LookupError:
            print("    - Downloading NLTK 'punkt' tokenizer...")
            nltk.download('punkt', quiet=True)
            nltk.data.find('tokenizers/punkt')
            return True
    except Exception as e:
        print(f"    - NLTK not available: {e}")
        return False


def get_final_url_with_selenium(url: str) -> Optional[str]:
    """
    Follow redirects including JavaScript redirects to get the final URL.

    Args:
        url (str): The Google News redirect URL

    Returns:
        Optional[str]: The final destination URL after all redirects
    """
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    try:
        print(f"    - Resolving final URL via headless Chrome")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        time.sleep(5)
        final_url = driver.current_url
        driver.quit()
        print(f"    - Resolved URL: {final_url}")
        return final_url
    except Exception as e:
        # Align with Optional[str] contract: return None on failure
        print(f"    - URL resolution failed: {e}")
        return None


def get_full_article(newsurl: str) -> Optional[Tuple[str, str, str, str, Any, str, str]]:
    """
    Resolve redirect, parse article with newspaper3k, and return details if the HTML length > 500 chars.
    Returns tuple: (title, article_html, text, top_image, publish_date, url, summary)
    """
    # Resolve via provided JS-aware resolver, fall back to original URL
    print(f"    - Fetching article for link: {newsurl}")
    final_url = get_final_url_with_selenium(newsurl) or newsurl
    try:
        article = Article(url=final_url, fetch_images=True, keep_article_html=True)
        article.download()
        article.parse()
        news_summary = None
        if ensure_nltk_punkt():
            try:
                article.nlp()
                news_summary = getattr(article, 'summary', None)
            except Exception as e:
                print(f"    - NLP summary generation failed: {e}")

        news_title = article.title
        news_articlehtml = article.article_html
        news_articletext = article.text
        news_topimg = article.top_image
        news_date = article.publish_date
        news_url = article.url

        if count_characters_and_check(news_articlehtml):
            print(f"    - Parsed article: '{news_title[:60]}...' (HTML length ok)")
            return (
                news_title,
                news_articlehtml,
                news_articletext,
                news_topimg,
                news_date,
                news_url,
                news_summary,
            )
        print("    - Skipping article: HTML too short")
        return None
    except Exception as e:
        print(f"    - Failed to parse article: {e}")
        return None


def convert_date_to_iso8601(given_date: Optional[str]) -> str:
    try:
        if given_date is None:
            return datetime.now().isoformat()
        return date_parser.parse(str(given_date)).isoformat()
    except Exception:
        return datetime.now().isoformat()


def create_supabase_client(project_url: str, api_key: str):
    print(f"[4/5] Connecting to Supabase: {project_url}")
    return supabase.create_client(project_url, api_key)


def upsert_article_record(client, data: dict) -> bool:
    try:
        client.table('news').upsert(data).execute()
        print("    - Upserted article into 'news' table")
        return True
    except APIError as e:
        # Handle duplicate key or other PostgREST API errors gracefully
        msg = getattr(e, 'message', str(e))
        try:
            payload = e.args[0] if e.args else {}
            code = payload.get('code') if isinstance(payload, dict) else None
        except Exception:
            code = None
        if code == '23505' or 'duplicate key value' in str(e):
            print("    - Duplicate detected (unique constraint). Skipping this article.")
            return False
        print(f"    - Upsert failed with API error: {msg}. Skipping this article.")
        return False
    except Exception as e:
        print(f"    - Upsert failed with unexpected error: {e}. Skipping this article.")
        return False


def fetch_and_upsert_by_topic(
    supabase_url: str,
    supabase_key: str,
    newstopics_topicid: str,
    newstopics_title: str,
    limit: int = 10,
) -> int:
    json_path = save_topic_headlines_to_json(newstopics_topicid, newstopics_title)
    entries = extract_values_from_json_file(json_path)
    client = create_supabase_client(supabase_url, supabase_key)

    processed = 0
    for idx, entry in enumerate(entries[:max(0, limit)], start=1):
        print(f"[5/5] Processing entry {idx}/{min(len(entries), max(0, limit))}")
        link = entry.get('link')
        source_href = entry.get('source_href')
        source_title = entry.get('source_title')
        if not link:
            print("    - Skipping entry: no link")
            continue

        details = get_full_article(link)
        if not details:
            print("    - Skipped: could not retrieve full article")
            continue

        (
            news_title,
            news_articlehtml,
            news_articletext,
            news_topimg,
            news_date,
            news_url,
            news_summary,
        ) = details

        normalized_date = convert_date_to_iso8601(news_date)

        record = {
            'news_title': news_title,
            'news_articlehtml': news_articlehtml,
            'news_articletext': news_articletext,
            'news_topimg': news_topimg,
            'news_date': normalized_date,
            'news_url': news_url,
            'news_summary': news_summary,
            'news_topicid': newstopics_topicid,
            'news_source_href': source_href,
            'news_source_title': source_title,
            'news_source_logo': f"https://logo.clearbit.com/{source_href}" if source_href else None,
        }

        if upsert_article_record(client, record):
            processed += 1
            print("    - Successfully upserted first article, stopping as requested.")
            break

    print(f"Completed. Total upserted articles: {processed}")
    return processed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Fetch Google News by topic ID and upsert articles into Supabase table news (standalone).'
    )
    parser.add_argument('--supabase-url', required=False, help='Supabase project URL (or SUPABASE_URL env)')
    parser.add_argument('--supabase-key', required=False, help='Supabase service or anon key (or SUPABASE_KEY env)')
    parser.add_argument('--topic-id', required=False, default='CAAqIggKIhxDQkFTRHdvSkwyMHZNRFF5Y214bUVnSmhjaWdBUAE', help='Google News topic ID (default provided)')
    parser.add_argument('--title', required=False, default='alhilal', help='Topic title; used for JSON filename (default: alhilal)')
    parser.add_argument('--limit', type=int, default=50, help='Max number of entries to process (default: 50)')
    return parser.parse_args()


def main() -> None:
    # Load environment variables from .env if available
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass

    args = parse_args()

    supabase_url = args.supabase_url or os.getenv('SUPABASE_URL')
    supabase_key = args.supabase_key or os.getenv('SUPABASE_KEY')
    topic_id = args.topic_id or 'CAAqIggKIhxDQkFTRHdvSkwyMHZNRFF5Y214bUVnSmhjaWdBUAE'
    # Hard-coded defaults inside the script per request
    title = args.title or 'alhilal'
    limit = args.limit if args.limit is not None else 50

    missing = [
        name for name, value in [
            ('SUPABASE_URL/--supabase-url', supabase_url),
            ('SUPABASE_KEY/--supabase-key', supabase_key),
            ('TOPIC_ID/--topic-id', topic_id),
        ] if not value
    ]
    if missing:
        raise SystemExit(
            'Missing required configuration: ' + ', '.join(missing) +
            '\nProvide via CLI flags or environment variables (.env supported).'
        )

    print("Starting fetch and upsert workflow...")
    count = fetch_and_upsert_by_topic(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        newstopics_topicid=topic_id,
        newstopics_title=title,
        limit=limit,
    )
    print("Done.")
    # Optionally, set exit code based on success for CI visibility
    if count == 0:
        raise SystemExit(1)


if __name__ == '__main__':
    main()