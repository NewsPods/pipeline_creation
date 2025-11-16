# # pipeline/db_pusher.py
# import pandas as pd
# from sqlalchemy import create_engine, text
# from sqlalchemy.pool import NullPool
# from pipeline.config import Config

# def push_articles_to_db(df: pd.DataFrame):
#     """
#     Pushes DataFrame of articles to CockroachDB with normalized schema.
#     df columns: title, description, news_source, created_at (datetime), audio_url
#     topic column expected to be a list (or str)
#     """
#     if df.empty:
#         return []

#     if not Config.COCKROACHDB_CONN_STRING:
#         raise RuntimeError("COCKROACHDB_CONN_STRING is not set.")

#     engine = create_engine(Config.COCKROACHDB_CONN_STRING, poolclass=NullPool, connect_args={"application_name": "news_processor"})
#     inserted_article_ids = []
#     with engine.begin() as connection:
#         # rename columns as in your snippet
#         articles_df = df.copy()
#         articles_df.rename(columns={'content': 'description', 'source': 'news_source', 'published_date': 'created_at'}, inplace=True)
#         # ensure created_at exists
#         if 'created_at' in articles_df.columns:
#             articles_df['created_at'] = pd.to_datetime(articles_df['created_at'], errors='coerce')
#         else:
#             articles_df['created_at'] = pd.Timestamp.now()

#         topics_data = articles_df.get('topic', pd.Series([[]]*len(articles_df))).tolist()
#         # ensure columns for insert (add audio_url)
#         if 'audio_url' not in articles_df.columns:
#             articles_df['audio_url'] = None
#         insert_cols = ['title', 'description', 'news_source', 'created_at', 'audio_url']
#         articles_df = articles_df[insert_cols]

#         insert_query = text("""
#             INSERT INTO public.articles (title, description, news_source, created_at, audio_url)
#             VALUES (:title, :description, :news_source, :created_at, :audio_url)
#             RETURNING article_id
#         """)
#         articles_records = articles_df.to_dict('records')

#         article_ids = []
#         for record in articles_records:
#             result = connection.execute(insert_query, record)
#             article_id = result.fetchone()[0]
#             article_ids.append(article_id)

#         # sections
#         sections_records = []
#         for idx, topics in enumerate(topics_data):
#             article_id = article_ids[idx]
#             if not isinstance(topics, list):
#                 topics = [topics] if pd.notna(topics) else []
#             for topic in topics:
#                 topic_str = str(topic).strip()
#                 if topic_str and topic_str.lower() != 'nan':
#                     sections_records.append({'article_id': article_id, 'news_section': topic_str})

#         if sections_records:
#             insert_sections_query = text("""
#                 INSERT INTO public.articles_sections (article_id, news_section)
#                 VALUES (:article_id, :news_section)
#                 ON CONFLICT (article_id, news_section) DO NOTHING
#             """)
#             for sr in sections_records:
#                 connection.execute(insert_sections_query, sr)

#         inserted_article_ids = article_ids

#     return inserted_article_ids

# pipeline/db_pusher.py
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import OperationalError
from pipeline.config import Config
import time

def _normalize_embedding(val):
    """
    Convert embedding to a Python list[float] suitable for a FLOAT8[] column.

    Handles:
    - None / NaN -> None
    - list/tuple -> list[float]
    - string like "[0.01, 0.02, ...]" or "0.01, 0.02, ..."
    """
    if val is None:
        return None

    # NaN from pandas
    if isinstance(val, float) and pd.isna(val):
        return None

    if isinstance(val, (list, tuple)):
        return [float(x) for x in val]

    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        # Strip [] if present
        if s[0] == "[" and s[-1] == "]":
            s = s[1:-1].strip()
        if not s:
            return None
        parts = [p.strip() for p in s.split(",") if p.strip()]
        try:
            return [float(p) for p in parts]
        except ValueError:
            # If something weird comes through, drop it instead of breaking the pipeline
            print(f"⚠️ Could not parse embedding string, storing NULL. Sample: {val[:80]!r}")
            return None

    # Any other type – just ignore and store NULL to avoid blowing up the insert
    return None


def push_articles_to_db(df: pd.DataFrame):
    """
    Pushes DataFrame of articles to CockroachDB with normalized schema.
    df columns: title, description, news_source, created_at (datetime), audio_key
    topic column expected to be a list (or str)
    """
    if df.empty:
        return []

    if not Config.COCKROACHDB_CONN_STRING:
        raise RuntimeError("COCKROACHDB_CONN_STRING is not set.")

    engine = create_engine(
        Config.COCKROACHDB_CONN_STRING,
        poolclass=NullPool,
        connect_args={"application_name": "news_processor"},
        pool_pre_ping=True
    )

    inserted_article_ids = []

    # Retry-safe DB session
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            with engine.begin() as connection:
                articles_df = df.copy()
                articles_df.rename(columns={
                    'content': 'description',
                    'source': 'news_source',
                    'published_date': 'created_at',
                    # if CSV/DF still has audio_url, rename it to audio_key
                    'audio_url': 'audio_key'
                }, inplace=True)

                # ensure created_at exists
                if 'created_at' in articles_df.columns:
                    articles_df['created_at'] = pd.to_datetime(articles_df['created_at'], errors='coerce')
                else:
                    articles_df['created_at'] = pd.Timestamp.now()

                topics_data = articles_df.get('topic', pd.Series([[]]*len(articles_df))).tolist()

                # ensure columns for insert (add audio_key)
                if 'audio_key' not in articles_df.columns:
                    articles_df['audio_key'] = None

                # ensure embedding exists, and normalize to list[float] for FLOAT8[]
                if 'embedding' not in articles_df.columns:
                    articles_df['embedding'] = None
                else:
                    articles_df['embedding'] = articles_df['embedding'].apply(_normalize_embedding)

                insert_cols = ['title', 'description', 'news_source', 'created_at', 'audio_key', 'embedding']
                articles_df = articles_df[insert_cols]

                insert_query = text("""
                                    INSERT INTO public.articles (title,
                                                                 description,
                                                                 news_source,
                                                                 created_at,
                                                                 audio_key,
                                                                 embedding)
                                    VALUES (:title,
                                            :description,
                                            :news_source,
                                            :created_at,
                                            :audio_key,
                                            :embedding) RETURNING article_id
                                    """)

                articles_records = articles_df.to_dict('records')

                article_ids = []
                for record in articles_records:
                    result = connection.execute(insert_query, record)
                    article_id = result.fetchone()[0]
                    article_ids.append(article_id)

                # sections
                sections_records = []
                for idx, topics in enumerate(topics_data):
                    article_id = article_ids[idx]
                    if not isinstance(topics, list):
                        topics = [topics] if pd.notna(topics) else []
                    for topic in topics:
                        topic_str = str(topic).strip()
                        if topic_str and topic_str.lower() != 'nan':
                            sections_records.append({'article_id': article_id, 'news_section': topic_str})

                if sections_records:
                    insert_sections_query = text("""
                        INSERT INTO public.articles_sections (article_id, news_section)
                        VALUES (:article_id, :news_section)
                        ON CONFLICT (article_id, news_section) DO NOTHING
                    """)
                    for sr in sections_records:
                        connection.execute(insert_sections_query, sr)

                inserted_article_ids = article_ids
            break  # ✅ success → exit retry loop

        except OperationalError as e:
            print(f"⚠️ Database connection lost (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                wait_time = 2 ** attempt
                print(f"🔁 Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                print("❌ Max retries reached. Database still unavailable.")
                raise e

    return inserted_article_ids

