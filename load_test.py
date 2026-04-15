"""
load_test.py

Load test for the search API + incremental sync job simulation.

Tests:
1. Search endpoints — concurrent users hitting all 6 search modes
2. Sync simulation — inserts 200 new records + updates 200 existing records
   every 5 minutes into Postgres, mimicking real incremental sync load

Usage:
    pip install locust faker psycopg2-binary

    # Terminal 1 — start sync simulator (inserts/updates Postgres every 5 min)
    python load_test.py --mode sync --db-url postgresql://user:pass@host:5432/dbname

    # Terminal 2 — start Locust search load test
    locust -f load_test.py --host http://localhost:8000 --users 50 --spawn-rate 5

    # Or headless:
    locust -f load_test.py --host http://localhost:8000 --users 50 --spawn-rate 5 --run-time 30m --headless

Environment variables (alternative to CLI args for sync mode):
    DB_URL=postgresql://user:pass@host:5432/dbname
    SYNC_INTERVAL_SECONDS=300   (default: 300 = 5 minutes)
    SYNC_BATCH_SIZE=200         (default: 200)
"""

import os
import sys
import time
import random
import string
import argparse
import logging
import threading
from datetime import datetime, timezone

# ── Locust import (only needed for search load test) ──────────────────────────
try:
    from locust import HttpUser, task, between, events
    LOCUST_AVAILABLE = True
except ImportError:
    LOCUST_AVAILABLE = False

# ── Faker for realistic data ──────────────────────────────────────────────────
try:
    from faker import Faker
    fake = Faker("en_CA")
except ImportError:
    fake = None

# ── Postgres ──────────────────────────────────────────────────────────────────
try:
    import psycopg2
    import psycopg2.extras
    from psycopg2.extras import execute_values
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL_SECONDS", "600"))
SYNC_BATCH    = int(os.getenv("SYNC_BATCH_SIZE", "200"))
DB_URL        = os.getenv("DB_URL", "")

# ─────────────────────────────────────────────────────────────────────────────
# Fake data generators
# ─────────────────────────────────────────────────────────────────────────────

STATUSES      = ["Open", "In Review", "Closed", "Cancelled"]
REVIEW_STATES = ["Approved", "Pending", "Flagged", "Passed"]
PROVINCES     = ["ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB"]
PLAN_CODES    = ["PLAN-HOME-BASIC", "PLAN-HOME-PLUS", "PLAN-HOME-ELITE", "PLAN-APPLIANCE"]
CITIES        = ["Toronto", "Montreal", "Vancouver", "Calgary", "Ottawa",
                 "Edmonton", "Winnipeg", "Halifax", "Quebec City", "Mississauga"]
PLAN_LABELS   = ["Basic Home Plan", "Plus Home Plan", "Elite Home Plan", "Appliance Plan"]


def _rand_str(n=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def fake_affiliate_data():
    first = fake.first_name() if fake else _rand_str(6)
    last  = fake.last_name()  if fake else _rand_str(6)
    return {
        "firstname":    first,
        "lastname":     last,
        "fullname":     f"{first} {last}",
        "email":        f"{first.lower()}.{last.lower()}@{_rand_str(6).lower()}.com",
        "mobilephone":  f"+1{random.randint(2000000000, 9999999999)}",
        "isactive":     random.choice([True, True, True, False]),
        "isprovider":   False,
        "issupplier":   False,
        "qualityindex": round(random.uniform(50, 100), 2),
        "reviewstatus": random.choice(REVIEW_STATES),
        "externalcode": f"REF-{_rand_str(8)}",
        "note":         (fake.sentence() if fake else "Sample note")[:200],
    }


def fake_location_data():
    return {
        "address1":   (fake.street_address() if fake else _rand_str(10)),
        "city":       random.choice(CITIES),
        "provinceid": random.choice(PROVINCES),
        "postalcode": f"{_rand_str(3)} {_rand_str(3)}",
        "countryid":  "CA",
        "latitude":   round(random.uniform(43.0, 55.0), 6),
        "longitude":  round(random.uniform(-130.0, -74.0), 6),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Sync Simulator
# ─────────────────────────────────────────────────────────────────────────────

class SyncSimulator:
    def __init__(self, db_url: str, batch_size: int = 200, interval: int = 300):
        self.db_url     = db_url
        self.batch_size = batch_size
        self.interval   = interval
        self.conn       = None
        self.stats      = {
            "rounds":         0,
            "total_inserted": 0,
            "total_updated":  0,
            "errors":         0,
        }

    def connect(self):
        """Open a fresh connection with keepalive + statement timeout."""
        if self.conn and not self.conn.closed:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = psycopg2.connect(
            self.db_url,
            connect_timeout=10,
            options=(
                "-c statement_timeout=30000 "   # 30s max per statement
                "-c tcp_keepalives_idle=60 "    # keepalive after 60s idle
                "-c tcp_keepalives_interval=10 "
                "-c tcp_keepalives_count=5"
            ),
        )
        self.conn.autocommit = False
        logger.info("Sync simulator connected to Postgres.")

    def _ensure_connection(self):
        """Ping the connection and reconnect if stale."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            logger.warning("Connection stale — reconnecting...")
            self.connect()

    def _insert_batch(self, cur):
        n = self.batch_size

        # ── 1. Affiliates ─────────────────────────────────────────────────
        try:
            aff_rows = [fake_affiliate_data() for _ in range(n)]
            execute_values(cur, """
                INSERT INTO affiliate
                    (firstname, lastname, fullname, email, mobilephone,
                     isactive, isprovider, issupplier, qualityindex,
                     reviewstatus, externalcode, note)
                VALUES %s
            """, [(
                a["firstname"], a["lastname"], a["fullname"], a["email"],
                a["mobilephone"], a["isactive"], a["isprovider"], a["issupplier"],
                a["qualityindex"], a["reviewstatus"], a["externalcode"], a["note"],
            ) for a in aff_rows])
            logger.info(f"  ✓ Affiliates inserted: {len(aff_rows)}")
        except Exception as e:
            logger.error(f"  ✗ Affiliate insert failed: {e}")
            raise

        # ── 2. Locations ──────────────────────────────────────────────────
        try:
            loc_rows = [fake_location_data() for _ in range(n)]
            loc_ids = execute_values(cur, """
                INSERT INTO location
                    (address1, city, provinceid, postalcode, countryid, latitude, longitude)
                VALUES %s
                RETURNING id
            """, [(
                l["address1"], l["city"], l["provinceid"], l["postalcode"],
                l["countryid"], l["latitude"], l["longitude"],
            ) for l in loc_rows], fetch=True)
            loc_id_list = [r[0] for r in loc_ids]
            logger.info(f"  ✓ Locations inserted: {len(loc_id_list)}")
        except Exception as e:
            logger.error(f"  ✗ Location insert failed: {e}")
            raise

        # ── 3. Events ─────────────────────────────────────────────────────
        try:
            now = datetime.now(timezone.utc)
            evt_rows = [(
                1, 1, 1,
                random.choice(STATUSES),
                random.choice(STATUSES),
                "Open",
                now, now,
                (fake.sentence() if fake else f"Claim {_rand_str(6)}"),
                (fake.paragraph() if fake else "Details"),
                f"CLM-{now.year}-{_rand_str(6)}",
                random.choice(["water", "fire", "electrical", "structural", ""]),
                random.choice(["urgent", "routine", "escalated", ""]),
                random.choice(REVIEW_STATES),
                random.randint(0, 5),
                loc_id,
            ) for loc_id in loc_id_list]

            evt_ids = execute_values(cur, """
                INSERT INTO event
                    (eventtypeid, requesttypeid, subrequesttypeid, status,
                     progressstatus, previousstatus, starttime, creationtime,
                     description, details, reference, tag1, tag2,
                     reviewstatus, escalationcount, locationid)
                VALUES %s
                RETURNING id
            """, evt_rows, fetch=True)
            evt_id_list = [r[0] for r in evt_ids]
            logger.info(f"  ✓ Events inserted: {len(evt_id_list)}")
        except Exception as e:
            logger.error(f"  ✗ Event insert failed: {e}")
            raise

        # ── 4. Event items ────────────────────────────────────────────────
        try:
            execute_values(cur, """
                INSERT INTO eventitem
                    (eventid, identificationcode, description, amount, status, quantity)
                VALUES %s
            """, [(
                evt_id,
                random.choice(PLAN_CODES),
                random.choice(PLAN_LABELS),
                round(random.uniform(100, 5000), 2),
                random.choice(STATUSES),
                1,
            ) for evt_id in evt_id_list])
            logger.info(f"  ✓ Event items inserted: {len(evt_id_list)}")
        except Exception as e:
            logger.error(f"  ✗ Event item insert failed: {e}")
            raise

        return {"affiliates": len(aff_rows), "events": len(evt_id_list)}

    def _update_batch(self, cur):
        # ── Fetch random IDs ──────────────────────────────────────────────
        try:
            cur.execute("SELECT id FROM affiliate ORDER BY RANDOM() LIMIT %s", (self.batch_size,))
            aff_ids = [r[0] for r in cur.fetchall()]

            cur.execute("SELECT id FROM event ORDER BY RANDOM() LIMIT %s", (self.batch_size,))
            evt_ids = [r[0] for r in cur.fetchall()]
        except Exception as e:
            logger.error(f"  ✗ ID fetch failed: {e}")
            raise

        # ── Bulk update affiliates ────────────────────────────────────────
        try:
            execute_values(cur, """
                UPDATE affiliate SET
                    reviewstatus = v.reviewstatus,
                    qualityindex = v.qualityindex,
                    isactive     = v.isactive,
                    updatedat    = NOW()
                FROM (VALUES %s) AS v(reviewstatus, qualityindex, isactive, id)
                WHERE affiliate.id = v.id::int
            """, [(
                random.choice(REVIEW_STATES),
                round(random.uniform(50, 100), 2),
                random.choice([True, True, False]),
                aff_id,
            ) for aff_id in aff_ids])
            logger.info(f"  ✓ Affiliates updated: {len(aff_ids)}")
        except Exception as e:
            logger.error(f"  ✗ Affiliate update failed: {e}")
            raise

        # ── Bulk update events ────────────────────────────────────────────
        try:
            execute_values(cur, """
                UPDATE event SET
                    status          = v.status,
                    reviewstatus    = v.reviewstatus,
                    escalationcount = event.escalationcount + v.inc,
                    updatedat       = NOW()
                FROM (VALUES %s) AS v(status, reviewstatus, inc, id)
                WHERE event.id = v.id::int
            """, [(
                random.choice(STATUSES),
                random.choice(REVIEW_STATES),
                random.randint(0, 1),
                evt_id,
            ) for evt_id in evt_ids])
            logger.info(f"  ✓ Events updated: {len(evt_ids)}")
        except Exception as e:
            logger.error(f"  ✗ Event update failed: {e}")
            raise

        return {"affiliates": len(aff_ids), "events": len(evt_ids)}

    def run_round(self):
        self._ensure_connection()  # ← reconnect if stale after sleep
        try:
            with self.conn.cursor() as cur:
                logger.info(f"[Sync Round {self.stats['rounds'] + 1}] Inserting {self.batch_size} records...")
                inserted = self._insert_batch(cur)

                logger.info(f"[Sync Round {self.stats['rounds'] + 1}] Updating {self.batch_size} records...")
                updated = self._update_batch(cur)

                self.conn.commit()

                self.stats["rounds"]         += 1
                self.stats["total_inserted"] += inserted.get("events", 0)
                self.stats["total_updated"]  += updated.get("events", 0)

                logger.info(
                    f"Round {self.stats['rounds']} complete — "
                    f"inserted: {inserted}, updated: {updated} | "
                    f"cumulative: {self.stats}"
                )
        except Exception as e:
            logger.error(f"Round failed: {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            self.stats["errors"] += 1

    def start(self):
        if not PSYCOPG2_AVAILABLE:
            logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
            sys.exit(1)
        self.connect()
        logger.info(
            f"Sync simulator started — "
            f"{self.batch_size} inserts + {self.batch_size} updates every {self.interval}s"
        )
        while True:
            self.run_round()
            logger.info(f"Sleeping {self.interval}s until next round...")
            time.sleep(self.interval)


# ─────────────────────────────────────────────────────────────────────────────
# Locust Search Load Test
# ─────────────────────────────────────────────────────────────────────────────

SEARCH_QUERIES = [
    "water damage kitchen",
    "John Smith",
    "electrical fire",
    "roof leak urgent",
    "CLM-2024",
    "Toronto appliance claim",
    "escalated open claims",
    "Quebec City structural",
    "high value claims",
    "provider inactive",
    "approved pending review",
    "furnace broken",
]

ENTITY_TYPES = [
    None,
    ["customer"],
    ["partner"],
    ["claim"],
    ["customer", "partner"],
    ["claim", "customer"],
]

if LOCUST_AVAILABLE:

    class SearchUser(HttpUser):
        wait_time = between(0.5, 2.0)

        def _query(self):
            return random.choice(SEARCH_QUERIES)

        def _entity_types(self):
            return random.choice(ENTITY_TYPES)

        def _payload(self, extra=None):
            payload = {
                "query":        self._query(),
                "entity_types": self._entity_types(),
                "size":         random.choice([5, 10, 20]),
            }
            if extra:
                payload.update(extra)
            return payload

        @task(3)
        def hybrid_search(self):
            self.client.post("/api/search/hybrid", json=self._payload(), name="/api/search/hybrid")

        @task(1)
        def nlq_search(self):
            self.client.post(
                "/api/search/nlq",
                json={
                    "query": random.choice([
                        "show me all open claims in Toronto",
                        "find VIP customers with pending review",
                        "high value claims escalated this month",
                        "inactive providers in Quebec",
                        "claims closed in last 30 days",
                    ]),
                    "entity_types": self._entity_types(),
                    "size": 10,
                },
                name="/api/search/nlq",
            )

    @events.init_command_line_parser.add_listener
    def add_custom_args(parser):
        parser.add_argument("--run-sync", action="store_true",
                            help="Also run sync simulator in background thread")
        parser.add_argument("--db-url", default=os.getenv("DB_URL", ""),
                            help="Postgres DB URL for sync simulator")

    @events.test_start.add_listener
    def on_test_start(environment, **kwargs):
        run_sync = getattr(environment.parsed_options, "run_sync", False)
        db_url   = getattr(environment.parsed_options, "db_url", "")
        if run_sync:
            if not db_url:
                logger.warning("--run-sync set but --db-url not provided. Sync simulator disabled.")
                return
            sim = SyncSimulator(db_url=db_url, batch_size=SYNC_BATCH, interval=SYNC_INTERVAL)
            t   = threading.Thread(target=sim.start, daemon=True)
            t.start()
            logger.info("Sync simulator started in background thread.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI entrypoint
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync simulator / load test runner")
    parser.add_argument("--mode",     choices=["sync"], default="sync")
    parser.add_argument("--db-url",   default=DB_URL,   help="Postgres connection URL")
    parser.add_argument("--batch",    type=int, default=SYNC_BATCH,    help="Records per round")
    parser.add_argument("--interval", type=int, default=SYNC_INTERVAL, help="Seconds between rounds")
    args = parser.parse_args()

    if args.mode == "sync":
        if not args.db_url:
            print("ERROR: --db-url is required. Example:")
            print("  python load_test.py --mode sync --db-url postgresql://user:pass@host:5432/db")
            sys.exit(1)
        sim = SyncSimulator(db_url=args.db_url, batch_size=args.batch, interval=args.interval)
        sim.start()


        # https://ai.dev.horizon.ths.agency