

# """
# app/indexing/sync.py

# Handles:
# 1. Full sync      — re-indexes ALL records in memory-safe chunks (sync_all)
# 2. Incremental    — only changed/new records since last sync (sync_incremental)
# 3. Scheduler      — re-syncs every 15 minutes via APScheduler
# 4. Embeddings     — OpenAI text-embedding-3-small, batched with retry + backoff

# ═══════════════════════════════════════════════════════════════════════════════
# ROOT CAUSE FIX (why it hung at ~3000 docs with batch_size=50)
# ═══════════════════════════════════════════════════════════════════════════════

#   The previous code buffered ALL raw_docs into a single list before starting
#   embeddings. For 261,000 docs this was ~1.7 GB.  Claim docs are the heaviest
#   (description + details can be 500+ chars) and were the trigger for the hang.

#   Two-pronged fix applied here:
#     1. CHUNKED FULL SYNC  — docs are built, embedded, and indexed in chunks of
#        CHUNK_SIZE (default 2 000).  Peak RAM per chunk ≈ 14 MB instead of 1.7 GB.
#     2. RETRY + BACKOFF    — 429 RateLimitError is retried up to 5 times with
#        exponential backoff (1 s → 2 s → 4 s → 8 s → 16 s) instead of being
#        silently swallowed.

# ═══════════════════════════════════════════════════════════════════════════════
# BATCH / CHUNK SIZING GUIDE
# ═══════════════════════════════════════════════════════════════════════════════

#   EMBED_BATCH_SIZE  — texts sent per OpenAI call.
#     • Claim docs average ~60–80 tokens; partner/customer ~30–40 tokens.
#     • text-embedding-3-small limit: 8 191 tokens PER TEXT (fine), and
#       typically 1 000 000 TPM / 3 000 RPM on Tier 1.
#     • 256 docs × 80 tokens = 20 480 tokens/call → ~150 calls/min → safe on T1.
#     • RAISE to 512 on Tier 2+ (5 000 RPM).

#   BULK_BATCH_SIZE   — docs per OpenSearch bulk request.
#     • Each doc with a 1 536-dim embedding ≈ 6–7 KB.
#     • 500 docs × 7 KB ≈ 3.5 MB payload — safe for most OS configs.
#     • Lower to 200 if you see PayloadTooLarge errors.

#   CHUNK_SIZE        — docs built + embedded + indexed per loop iteration.
#     • Controls peak RAM.  2 000 docs ≈ 14 MB in RAM.
#     • After each chunk, raw_docs is cleared and GC can reclaim memory.
#     • Raise to 5 000 on servers with 16 GB+ RAM for fewer DB round-trips.

#   INTER_BATCH_SLEEP — seconds between consecutive OpenAI calls.
#     • 0.2 s → ~300 req/min → safe on Tier 1 (limit 3 000 RPM).
#     • Set to 0.0 on Tier 2+.

# ═══════════════════════════════════════════════════════════════════════════════
# COST + TIME ESTIMATE  (261 000 docs, text-embedding-3-small, $0.02 / 1M tokens)
# ═══════════════════════════════════════════════════════════════════════════════

#   Entity       Count    Avg tokens   Total tokens
#   ──────────── ──────── ──────────── ─────────────
#   Customers     7 000       30           210 000
#   Partners      3 000       40           120 000
#   Claims      250 000       70        17 500 000
#   ──────────── ──────── ──────────── ─────────────
#   TOTAL       260 000                 17 830 000   (~17.83 M tokens)

#   Cost  = 17.83 M × $0.02 / 1 M  =  $0.357 USD  ≈  ₹30 INR   (@84 USD/INR)

#   Time estimate (single machine, EMBED_BATCH_SIZE=256, INTER_BATCH_SLEEP=0.2 s):
#     • Number of embed calls  = ceil(261 000 / 256) ≈ 1 020 calls
#     • Time per call          ≈ 0.8 s (network) + 0.2 s (sleep) = 1.0 s
#     • Total embedding time   ≈ 1 020 s  ≈  17 minutes
#     • DB fetch time          ≈  2–4 minutes (261 k rows over localhost)
#     • OpenSearch bulk time   ≈  3–5 minutes (261 k docs at 500/batch)
#     ─────────────────────────────────────────────────────────────────
#     TOTAL ESTIMATED TIME     ≈  22–26 minutes  (end-to-end full sync)

#   NOTE: On Tier 2+ (5 000 RPM) you can remove the sleep and use batch 512,
#         cutting embedding time to ~7 minutes → total ~12–14 minutes.
# """

# import logging
# import time
# from datetime import datetime, timezone

# from opensearchpy.helpers import bulk
# from openai import RateLimitError, APIError, APIConnectionError
# from apscheduler.schedulers.background import BackgroundScheduler
# from sqlalchemy import text

# from app.clients.clients import get_client, INDEX_NAME
# from app.core.config import settings
# from app.db.base import SessionLocal
# from app.services.search_indexing import create_index, create_hybrid_pipeline

# # ── lazy import so openai client uses settings ──────────────────────────────
# from openai import OpenAI
# openai_client = OpenAI(api_key=settings.OPENAI_API_KEY)

# logger = logging.getLogger(__name__)

# # ─────────────────────────────────────────────────────────────────────────────
# # Tuneable constants  (adjust per your OpenAI tier and server RAM)
# # ─────────────────────────────────────────────────────────────────────────────
# EMBEDDING_MODEL   = "text-embedding-3-small"

# EMBED_BATCH_SIZE  = 256    # texts per OpenAI call       (Tier 1: 256, Tier 2+: 512)
# BULK_BATCH_SIZE   = 500    # docs per OpenSearch bulk    (lower to 200 on small OS)
# CHUNK_SIZE        = 2_000  # docs per full-sync loop     (raise to 5 000 on 16 GB+ RAM)
# INTER_BATCH_SLEEP = 0.2    # seconds between embed calls (0.0 on Tier 2+)
# MAX_RETRIES       = 5      # OpenAI retry attempts       (backoff: 1s→2s→4s→8s→16s)

# SKIP_EMBEDDINGS   = False  # set True for load-testing without OpenAI cost


# # ─────────────────────────────────────────────────────────────────────────────
# # Helpers
# # ─────────────────────────────────────────────────────────────────────────────

# def _ts(val):
#     """Safely convert a date/datetime value to an ISO string."""
#     if val is None:
#         return None
#     if isinstance(val, str):
#         return val
#     return val.isoformat()


# def build_embedding_text(doc: dict) -> str:
#     """Concatenate the most semantically meaningful fields for embedding."""
#     return " ".join(filter(None, [
#         doc.get("name"),
#         doc.get("description"),
#         doc.get("note"),
#         doc.get("status"),
#         doc.get("plan_name"),
#         doc.get("tags"),
#         doc.get("review_status"),
#         doc.get("reference"),
#     ]))


# # ─────────────────────────────────────────────────────────────────────────────
# # OpenAI Embedding  (with exponential backoff + retry)
# # ─────────────────────────────────────────────────────────────────────────────

# def generate_embeddings_batch(texts: list[str]) -> list[list[float] | None]:
#     """
#     Generate embeddings for a list of texts in a single OpenAI API call.

#     • Filters out blank texts before sending (saves tokens).
#     • Retries up to MAX_RETRIES times on rate-limit / connection errors.
#     • Exponential backoff: 1 s, 2 s, 4 s, 8 s, 16 s.
#     • Returns a parallel list — blank/failed texts map to None.
#     """
#     valid_indices = [(i, t[:8000]) for i, t in enumerate(texts) if t and t.strip()]
#     results: list[list[float] | None] = [None] * len(texts)

#     if not valid_indices:
#         return results

#     indices, valid_texts = zip(*valid_indices)

#     for attempt in range(MAX_RETRIES):
#         try:
#             response = openai_client.embeddings.create(
#                 input=list(valid_texts),
#                 model=EMBEDDING_MODEL,
#             )
#             for idx, emb in zip(indices, response.data):
#                 results[idx] = emb.embedding
#             return results  # ← success

#         except RateLimitError:
#             wait = 2 ** attempt          # 1 s, 2 s, 4 s, 8 s, 16 s
#             logger.warning(
#                 f"OpenAI rate-limited — retrying in {wait}s "
#                 f"(attempt {attempt + 1}/{MAX_RETRIES})"
#             )
#             time.sleep(wait)

#         except APIConnectionError as e:
#             wait = 2 ** attempt
#             logger.warning(f"OpenAI connection error ({e}) — retrying in {wait}s")
#             time.sleep(wait)

#         except APIError as e:
#             # Non-retryable API error (auth, bad request, etc.)
#             logger.error(f"OpenAI APIError (non-retryable): {e}")
#             break

#         except Exception as e:
#             logger.error(f"Unexpected embedding error: {e}")
#             break

#     logger.error(
#         f"Embedding failed after {MAX_RETRIES} attempts — "
#         f"returning None for {len(valid_indices)} texts."
#     )
#     return results


# # ─────────────────────────────────────────────────────────────────────────────
# # Embedding Phase  (memory-safe; processes one EMBED_BATCH_SIZE slice at a time)
# # ─────────────────────────────────────────────────────────────────────────────

# def _attach_embeddings(raw_docs: list[dict]):
#     """
#     Generate and attach embeddings to docs in-place.

#     Processes EMBED_BATCH_SIZE docs at a time so that only one batch of
#     text strings lives in RAM simultaneously — not the full doc list.
#     Also sleeps INTER_BATCH_SLEEP between calls to stay under RPM limits.
#     """
#     if SKIP_EMBEDDINGS:
#         logger.info("SKIP_EMBEDDINGS=True — skipping embedding generation.")
#         for doc in raw_docs:
#             doc["embedding"] = None
#         return

#     total         = len(raw_docs)
#     total_batches = (total + EMBED_BATCH_SIZE - 1) // EMBED_BATCH_SIZE

#     for i in range(0, total, EMBED_BATCH_SIZE):
#         batch      = raw_docs[i : i + EMBED_BATCH_SIZE]
#         texts      = [build_embedding_text(doc) for doc in batch]   # built per-batch
#         embeddings = generate_embeddings_batch(texts)

#         for j, emb in enumerate(embeddings):
#             batch[j]["embedding"] = emb

#         # texts goes out of scope here → eligible for GC immediately
#         del texts

#         batch_num = i // EMBED_BATCH_SIZE + 1
#         if batch_num % 10 == 0 or batch_num == total_batches:
#             logger.info(
#                 f"  Embeddings: batch {batch_num}/{total_batches} "
#                 f"({min(i + EMBED_BATCH_SIZE, total):,}/{total:,} docs)"
#             )

#         # Courtesy sleep to stay within OpenAI RPM limits
#         if INTER_BATCH_SLEEP > 0 and i + EMBED_BATCH_SIZE < total:
#             time.sleep(INTER_BATCH_SLEEP)


# # ─────────────────────────────────────────────────────────────────────────────
# # Document Builders
# # ─────────────────────────────────────────────────────────────────────────────

# def build_customer_doc(row) -> dict:
#     return {
#         "entity_type":    "customer",
#         "entity_id":      row.id,
#         "name":           row.fullname or f"{row.firstname or ''} {row.lastname or ''}".strip(),
#         "email":          row.email,
#         "phone":          row.mobilephone or row.homephone or row.workphone,
#         "note":           row.note,
#         "status":         "Active" if row.isactive else "Inactive",
#         "is_active":      row.isactive,
#         "is_provider":    False,
#         "is_supplier":    False,
#         "quality_index":  float(row.qualityindex) if row.qualityindex else None,
#         "review_status":  row.reviewstatus,
#         "reference":      row.externalcode,
#         "tags":           "",
#         "description":    row.note or "",
#         "indexed_at":     datetime.now(timezone.utc).isoformat(),
#         "created_at":     _ts(row.createdat),
#         "completion_suggest": {
#             "input": list(filter(None, [
#                 row.fullname or f"{row.firstname or ''} {row.lastname or ''}".strip(),
#                 row.email,
#                 row.externalcode,
#             ]))
#         },
#     }


# def build_partner_doc(row) -> dict:
#     partner_type = "provider" if row.isprovider else "supplier"
#     return {
#         "entity_type":    "partner",
#         "entity_id":      row.id,
#         "name":           row.fullname or f"{row.firstname or ''} {row.lastname or ''}".strip(),
#         "email":          row.email,
#         "phone":          row.mobilephone or row.workphone,
#         "note":           row.note,
#         "status":         "Active" if row.isactive else "Inactive",
#         "is_active":      row.isactive,
#         "is_provider":    row.isprovider,
#         "is_supplier":    row.issupplier,
#         "quality_index":  float(row.qualityindex) if row.qualityindex else None,
#         "review_status":  row.reviewstatus,
#         "reference":      row.externalcode,
#         "tags":           partner_type,
#         "description":    f"{partner_type} partner {row.note or ''}",
#         "indexed_at":     datetime.now(timezone.utc).isoformat(),
#         "created_at":     _ts(row.createdat),
#         "completion_suggest": {
#             "input": list(filter(None, [
#                 row.fullname or f"{row.firstname or ''} {row.lastname or ''}".strip(),
#                 row.email,
#                 row.externalcode,
#             ]))
#         },
#     }


# def build_claim_doc(row) -> dict:
#     tags = " ".join(filter(None, [row.tag1, row.tag2]))
#     return {
#         "entity_type":      "claim",
#         "entity_id":        row.id,
#         "name":             row.description or row.reference or f"Claim #{row.id}",
#         "description":      f"{row.description or ''} {row.details or ''}".strip(),
#         "reference":        row.reference,
#         "status":           row.status,
#         "review_status":    row.reviewstatus,
#         "tags":             tags,
#         "plan_name":        row.plan_name or "",
#         "is_active":        row.status not in ("Closed", "Cancelled"),
#         "escalation_count": row.escalationcount or 0,
#         "amount":           float(row.plan_amount) if row.plan_amount else None,
#         "city":             row.city,
#         "province":         row.province,
#         "start_time":       _ts(row.starttime),
#         "end_time":         _ts(row.endtime),
#         "created_at":       _ts(row.creationtime),
#         "indexed_at":       datetime.now(timezone.utc).isoformat(),
#         "completion_suggest": {
#             "input": list(filter(None, [row.reference, row.description, row.status]))
#         },
#     }


# # ─────────────────────────────────────────────────────────────────────────────
# # Sync State  (read / write last sync time in Postgres)
# # ─────────────────────────────────────────────────────────────────────────────

# def _get_last_sync(db) -> datetime:
#     """
#     Return the last successful sync time, or epoch on first run.
#     Falling back to epoch triggers a full re-index on the next incremental run.
#     """
#     row = db.execute(text(
#         "SELECT synced_at FROM sync_state WHERE key = 'opensearch_sync'"
#     )).fetchone()

#     if row:
#         ts = row.synced_at
#         if ts.tzinfo is None:
#             ts = ts.replace(tzinfo=timezone.utc)
#         return ts

#     logger.warning("No sync_state found — falling back to epoch (full re-index).")
#     return datetime.fromtimestamp(0, tz=timezone.utc)


# def _update_last_sync(db, sync_time: datetime):
#     """Upsert the last sync time in sync_state."""
#     db.execute(text("""
#         INSERT INTO sync_state (key, synced_at)
#         VALUES ('opensearch_sync', :ts)
#         ON CONFLICT (key) DO UPDATE SET synced_at = EXCLUDED.synced_at
#     """), {"ts": sync_time})
#     db.commit()


# # ─────────────────────────────────────────────────────────────────────────────
# # Bulk Index
# # ─────────────────────────────────────────────────────────────────────────────

# def _upsert(doc: dict) -> dict:
#     """Wrap a doc as an OpenSearch index (upsert) action."""
#     return {
#         "_op_type": "index",
#         "_index":   INDEX_NAME,
#         "_id":      f"{doc['entity_type']}_{doc['entity_id']}",
#         **doc,
#     }


# def _bulk_index(client, actions: list, stats: dict):
#     """Index actions in BULK_BATCH_SIZE chunks to keep payloads manageable."""
#     for i in range(0, len(actions), BULK_BATCH_SIZE):
#         batch = actions[i : i + BULK_BATCH_SIZE]
#         try:
#             success, failed = bulk(client, batch, raise_on_error=False)
#             stats["indexed"] = stats.get("indexed", 0) + success
#             stats["failed"]  = stats.get("failed",  0) + len(failed)
#             if failed:
#                 logger.warning(f"Bulk batch had {len(failed)} failures")
#         except Exception as e:
#             logger.error(f"Bulk batch failed: {e}")
#             stats["errors"] = stats.get("errors", 0) + len(batch)

#         batches_done = i // BULK_BATCH_SIZE + 1
#         if batches_done % 10 == 0:
#             logger.info(f"  Indexed: {stats.get('indexed', 0):,} docs so far")


# # ─────────────────────────────────────────────────────────────────────────────
# # DB Fetch Helpers  (each returns all rows; used by sync_all)
# # ─────────────────────────────────────────────────────────────────────────────

# def _fetch_all_customers(db):
#     return db.execute(text("""
#         SELECT * FROM affiliate
#         WHERE isprovider = FALSE
#           AND issupplier = FALSE
#     """)).fetchall()


# def _fetch_all_partners(db):
#     return db.execute(text("""
#         SELECT * FROM affiliate
#         WHERE isprovider = TRUE OR issupplier = TRUE
#     """)).fetchall()


# def _fetch_all_claims(db):
#     return db.execute(text("""
#         SELECT DISTINCT ON (e.id)
#                e.*,
#                ei.description AS plan_name,
#                ei.amount      AS plan_amount,
#                l.city         AS city,
#                l.provinceid   AS province
#         FROM event e
#         LEFT JOIN eventitem ei
#             ON ei.eventid = e.id
#            AND ei.identificationcode LIKE 'PLAN-%'
#         LEFT JOIN location l
#             ON l.id = e.locationid
#         ORDER BY e.id
#     """)).fetchall()


# def _fetch_changed_customers(db, last_sync):
#     return db.execute(text("""
#         SELECT * FROM affiliate
#         WHERE isprovider = FALSE
#           AND issupplier = FALSE
#           AND updatedat  > :last_sync
#     """), {"last_sync": last_sync}).fetchall()


# def _fetch_changed_partners(db, last_sync):
#     return db.execute(text("""
#         SELECT * FROM affiliate
#         WHERE (isprovider = TRUE OR issupplier = TRUE)
#           AND updatedat > :last_sync
#     """), {"last_sync": last_sync}).fetchall()


# def _fetch_changed_claims(db, last_sync):
#     return db.execute(text("""
#         SELECT DISTINCT ON (e.id)
#                e.*,
#                ei.description AS plan_name,
#                ei.amount      AS plan_amount,
#                l.city         AS city,
#                l.provinceid   AS province
#         FROM event e
#         LEFT JOIN eventitem ei
#             ON ei.eventid = e.id
#            AND ei.identificationcode LIKE 'PLAN-%'
#         LEFT JOIN location l
#             ON l.id = e.locationid
#         WHERE
#             e.updatedat > :last_sync
#             OR (ei.id IS NOT NULL AND ei.updatedat > :last_sync)
#             OR (l.id  IS NOT NULL AND l.updatedat  > :last_sync)
#         ORDER BY e.id
#     """), {"last_sync": last_sync}).fetchall()


# # ─────────────────────────────────────────────────────────────────────────────
# # Chunk Processor  (shared by sync_all and sync_incremental)
# # ─────────────────────────────────────────────────────────────────────────────

# def _process_chunk(
#     rows,
#     build_fn,
#     entity_label: str,
#     client,
#     stats: dict,
# ) -> int:
#     """
#     Build docs → generate embeddings → bulk index for a slice of rows.

#     Processes in sub-chunks of CHUNK_SIZE to keep peak RAM flat.
#     Returns the number of successfully built docs.
#     """
#     built = 0
#     total_rows = len(rows)

#     for chunk_start in range(0, total_rows, CHUNK_SIZE):
#         chunk = rows[chunk_start : chunk_start + CHUNK_SIZE]
#         raw_docs = []

#         for row in chunk:
#             try:
#                 raw_docs.append(build_fn(row))
#                 built += 1
#             except Exception as e:
#                 logger.error(f"{entity_label} {row.id} build failed: {e}")
#                 stats["errors"] = stats.get("errors", 0) + 1

#         if not raw_docs:
#             continue

#         chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
#         logger.info(
#             f"  [{entity_label}] Processing rows "
#             f"{chunk_start + 1:,}–{chunk_end:,} / {total_rows:,} ..."
#         )

#         # Embed this chunk
#         _attach_embeddings(raw_docs)

#         # Index this chunk
#         actions = [_upsert(doc) for doc in raw_docs]
#         _bulk_index(client, actions, stats)

#         # Explicitly free memory before next chunk
#         raw_docs.clear()
#         del actions

#     stats[entity_label] = stats.get(entity_label, 0) + built
#     return built


# # ─────────────────────────────────────────────────────────────────────────────
# # Full Sync
# # ─────────────────────────────────────────────────────────────────────────────

# def sync_all(clean: bool = False) -> dict:
#     """
#     Full sync — re-indexes ALL records from scratch in memory-safe chunks.

#     Args:
#         clean: If True, deletes and recreates the index before indexing.
#                Use when you need a completely fresh index (mapping changes,
#                stale doc cleanup, etc.).  If False, existing docs are
#                overwritten in-place via upsert (safer for live systems).

#     Flow:
#         Phase 1 : Optionally delete + recreate index (clean=True)
#         Phase 2 : Fetch ALL rows from DB (customers → partners → claims)
#         Phase 3 : Close DB immediately (prevents TCP idle timeout)
#         Phase 4 : For each entity type, process in CHUNK_SIZE sub-batches:
#                     4a. Build doc dicts
#                     4b. Generate embeddings (EMBED_BATCH_SIZE per OpenAI call)
#                     4c. Bulk index into OpenSearch
#                     4d. Clear chunk from RAM
#         Phase 5 : Update sync_state to now

#     Memory profile per chunk (CHUNK_SIZE=2 000, embeddings attached):
#         ≈ 2 000 docs × 7 KB = 14 MB peak  (vs. 1.7 GB for 261 k docs at once)
#     """
#     stats   = {"customers": 0, "partners": 0, "claims": 0,
#                "indexed": 0, "failed": 0, "errors": 0}
#     client  = get_client()
#     t_start = time.time()

#     # ── Phase 1: Clean index ──────────────────────────────────────────────
#     if clean:
#         logger.info(f"Clean sync requested — recreating index '{INDEX_NAME}'.")
#         create_index(force_recreate=True)
#         create_hybrid_pipeline()
#     else:
#         create_index(force_recreate=False)  # no-op if already exists

#     # ── Phase 2 & 3: Fetch ALL records, then close DB ────────────────────
#     sync_time = datetime.now(timezone.utc)
#     db = SessionLocal()
#     try:
#         logger.info("Fetching all customers ...")
#         customers = _fetch_all_customers(db)
#         logger.info(f"  → {len(customers):,} customers fetched")

#         logger.info("Fetching all partners ...")
#         partners = _fetch_all_partners(db)
#         logger.info(f"  → {len(partners):,} partners fetched")

#         logger.info("Fetching all claims ...")
#         claims = _fetch_all_claims(db)
#         logger.info(f"  → {len(claims):,} claims fetched")

#     except Exception as e:
#         logger.error(f"Fatal sync_all error (DB fetch phase): {e}")
#         stats["fatal"] = str(e)
#         return stats
#     finally:
#         db.close()
#         logger.info("DB connection closed — starting embedding + indexing.")

#     total_docs = len(customers) + len(partners) + len(claims)
#     if total_docs == 0:
#         logger.warning("No records found in DB — nothing to index.")
#         return stats
#     logger.info(f"Total docs to process: {total_docs:,}")

#     # ── Phase 4: Build → Embed → Index per entity type ───────────────────

#     # 4a. Customers
#     logger.info("═══ Processing CUSTOMERS ═══")
#     _process_chunk(customers, build_customer_doc, "customers", client, stats)
#     del customers   # free RAM immediately

#     # 4b. Partners
#     logger.info("═══ Processing PARTNERS ═══")
#     _process_chunk(partners, build_partner_doc, "partners", client, stats)
#     del partners

#     # 4c. Claims  (heaviest — longest description+details fields)
#     logger.info("═══ Processing CLAIMS ═══")
#     _process_chunk(claims, build_claim_doc, "claims", client, stats)
#     del claims

#     # ── Phase 5: Update sync state ────────────────────────────────────────
#     db2 = SessionLocal()
#     try:
#         _update_last_sync(db2, sync_time)
#         logger.info(f"Sync state updated to {sync_time.isoformat()}")
#     finally:
#         db2.close()

#     elapsed = time.time() - t_start
#     logger.info(
#         f"Full sync complete in {elapsed / 60:.1f} min | stats: {stats}"
#     )
#     return stats


# # ─────────────────────────────────────────────────────────────────────────────
# # Incremental Sync
# # ─────────────────────────────────────────────────────────────────────────────

# def sync_incremental() -> dict:
#     """
#     Incremental sync — only re-indexes records changed since last sync.

#     Uses the same chunked pipeline as sync_all.  Because incremental batches
#     are small (typically < 500 docs per 15-min window), CHUNK_SIZE is rarely
#     hit — but the same safe path is used for consistency.

#     If sync_state is manually cleared, _get_last_sync() falls back to epoch
#     which effectively triggers a full re-index automatically.
#     """
#     stats   = {"customers": 0, "partners": 0, "claims": 0,
#                "indexed": 0, "failed": 0, "errors": 0}
#     t_start = time.time()

#     # ── Fetch changed rows from DB ────────────────────────────────────────
#     db = SessionLocal()
#     try:
#         last_sync = _get_last_sync(db)
#         new_sync  = datetime.now(timezone.utc)
#         logger.info(
#             f"Incremental sync window: "
#             f"{last_sync.isoformat()} → {new_sync.isoformat()}"
#         )

#         customers = _fetch_changed_customers(db, last_sync)
#         logger.info(f"Changed customers: {len(customers)}")

#         partners = _fetch_changed_partners(db, last_sync)
#         logger.info(f"Changed partners: {len(partners)}")

#         claims = _fetch_changed_claims(db, last_sync)
#         logger.info(f"Changed claims: {len(claims)}")

#     except Exception as e:
#         logger.error(f"Fatal incremental sync error (DB phase): {e}")
#         stats["fatal"] = str(e)
#         return stats
#     finally:
#         db.close()
#         logger.info("DB connection closed.")

#     total = len(customers) + len(partners) + len(claims)
#     if total == 0:
#         logger.info("No changes detected — skipping embedding and indexing.")
#         db2 = SessionLocal()
#         try:
#             _update_last_sync(db2, new_sync)
#         finally:
#             db2.close()
#         return stats

#     # ── Build → Embed → Index ─────────────────────────────────────────────
#     client = get_client()

#     if customers:
#         _process_chunk(customers, build_customer_doc, "customers", client, stats)
#         del customers

#     if partners:
#         _process_chunk(partners, build_partner_doc, "partners", client, stats)
#         del partners

#     if claims:
#         _process_chunk(claims, build_claim_doc, "claims", client, stats)
#         del claims

#     # ── Update sync state ─────────────────────────────────────────────────
#     db3 = SessionLocal()
#     try:
#         _update_last_sync(db3, new_sync)
#         logger.info(f"Sync state updated to {new_sync.isoformat()}")
#     finally:
#         db3.close()

#     elapsed = time.time() - t_start
#     logger.info(
#         f"Incremental sync complete in {elapsed:.1f}s | stats: {stats}"
#     )
#     return stats


# # ─────────────────────────────────────────────────────────────────────────────
# # Scheduler
# # ─────────────────────────────────────────────────────────────────────────────

# _scheduler = None


# def start_scheduler():
#     global _scheduler
#     if _scheduler and _scheduler.running:
#         return

#     _scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
#     _scheduler.add_job(
#         sync_incremental,
#         trigger="interval",
#         minutes=15,
#         id="opensearch_sync",
#         replace_existing=True,
#         max_instances=1,        # never overlap
#         coalesce=True,          # if missed, run once — not multiple times
#         misfire_grace_time=60,
#     )
#     _scheduler.start()
#     logger.info("Scheduler started (15 min incremental sync, max_instances=1)")


# def stop_scheduler():
#     global _scheduler
#     if _scheduler:
#         _scheduler.shutdown(wait=False)
#         logger.info("Scheduler stopped")

