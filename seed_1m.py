"""
seed_1m.py — Zucora Horizon Load Test Seeder
=============================================
Inserts 1M customers + 1M partners + 1M claims into the exact 4 tables
that sync.py reads and OpenSearch indexes:

    affiliate   → customers (isprovider=F, issupplier=F)
    affiliate   → partners  (isprovider/issupplier=T)
    event       → claims
    eventitem   → PLAN-* rows (joined by sync.py for plan_name / amount)
    location    → city / province (joined by sync.py)

All NL text fields (note, description, details, plan_name) are domain-specific
home-warranty content so BM25 + kNN embeddings have meaningful signal.

Usage:
    pip install psycopg2-binary tqdm

    python seed_1m.py --dsn "postgresql://user:pass@host:5432/db"

    # Custom counts
    python seed_1m.py --dsn "..." --customers 1000000 --partners 1000000 --claims 1000000

    # Smaller smoke test
    python seed_1m.py --dsn "..." --customers 1000 --partners 500 --claims 2000
"""

import argparse
import random
import string
import sys
from datetime import datetime, timedelta, timezone
from itertools import islice

import psycopg2
import psycopg2.extras
from tqdm import tqdm

random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# Domain data pools  (matches your UI screenshots exactly)
# ─────────────────────────────────────────────────────────────────────────────

FIRST_NAMES = [
    "Liam","Sophia","Noah","Emma","Oliver","Ava","William","Isabella","James","Mia",
    "Benjamin","Charlotte","Lucas","Amelia","Henry","Harper","Alexander","Evelyn",
    "Mason","Abigail","Ethan","Emily","Daniel","Elizabeth","Matthew","Sofia","Aiden",
    "Avery","Logan","Ella","Jackson","Scarlett","Sebastian","Grace","Jack","Chloe",
    "Owen","Victoria","Samuel","Riley","Ryan","Aria","Nathan","Lily","Caleb","Aurora",
    # French-Canadian
    "Luc","Marie","Jean","Nathalie","Pierre","Isabelle","François","Chantal","Michel",
    "Sylvie","Patrick","Véronique","Étienne","Mélanie","Benoît","Josée","Sébastien",
    "Geneviève","Alexandre","Stéphanie","Maxime","Valérie",
    # Multicultural (reflects Canadian demographics)
    "Fatima","Omar","Priya","Raj","Mei","Wei","Marcus","Amara","Kwame","Ranya",
    "Ibrahim","Aisha","Yuki","Kenji","Ravi","Deepa","Carlos","Maria","Andrei","Ioana",
]

LAST_NAMES = [
    "Tremblay","Martin","Roy","Gagnon","Côté","Bouchard","Gauthier","Morin","Lavoie",
    "Fortin","Okafor","Zhang","Nair","Hassan","Whitfield","Leclerc","Dubois","Asante",
    "Smith","Johnson","Brown","Wilson","Taylor","Anderson","Thomas","Jackson","White",
    "Harris","Clark","Lewis","Robinson","Walker","Hall","Allen","Young","King","Wright",
    "Scott","Torres","Nguyen","Patel","Kim","Singh","Chen","Kumar","Ahmed","Ali","Khan",
]

COMPANY_NAMES = [
    "Nordic HVAC Supply","TechParts Canada Inc","ProFix Services Ltd","Maple Repair Co",
    "GreenTech Solutions","CanadaFix Inc","Northern Supply Group","Elite Home Services",
    "Pacific Parts Depot","AllFixit Corp","HomeGuard Services","QuickRepair Canada",
    "Superior Appliance Parts","Atlantic Service Group","Ontario Tech Supply",
    "Western Home Solutions","Premier Parts Inc","Capital Service Partners",
    "National Repair Network","Trusted Trade Services","Summit HVAC Group",
    "Precision Appliance Co","Reliable Parts Canada","Metro Service Solutions",
]

# Customer notes — rich NL text for semantic search
CUSTOMER_NOTES = [
    "VIP customer – paid 3-year Complete Home Protection plan upfront.",
    "Fraud review triggered – duplicate claim filed within 30 days of enrollment.",
    "Corporate account – bulk plan covering Leon's furniture warranty program.",
    "Language preference: French – all communications must be in French.",
    "Multiple reopens – quality review pending, escalated to supervisor.",
    "Account deactivated – plan lapsed February 2025, renewal reminder sent.",
    "Escalation history – 3 reopens in 12 months, assigned to senior adjuster.",
    "New customer referred by existing policyholder, discount applied.",
    "Seasonal property – coverage suspended during winter months.",
    "Elderly customer – requires extra assistance, phone-only contact preferred.",
    "High-value account – Complete plan with HVAC and appliance rider.",
    "Customer requested Spanish-speaking adjuster for all appointments.",
    "Plan upgraded from Basic to Complete after first claim approval.",
    "Commercial account – rental property portfolio with 4 active plans.",
    "Claimant previously disputed settlement, agreed on revised terms.",
    "Auto-renewal active – credit card on file, no manual renewal needed.",
    "Customer flagged for excessive claims – under monitoring per policy section 12.",
    "Preferred contractor assigned: Kwame Asante (PRV-004).",
    "Plan covers two-unit property – both units on single policy agreement.",
    "Customer satisfaction score 4.8/5 – positive feedback submitted.",
    None, None,  # some customers have no note
]

# Claim descriptions — home warranty fault language
CLAIM_DESCRIPTIONS = [
    "The rear left leg joint snapped clean off — the wooden frame is structurally compromised.",
    "Large stain on cushion from orange juice spill, fabric is visibly discoloured.",
    "Stove element not working – strong electrical smell when burner switched on.",
    "Multiple outlets dead in bedroom – GFCI not tripping, possible wiring fault.",
    "Breaker panel tripping repeatedly – circuits 4 and 7 losing power intermittently.",
    "Dining table has a deep scratch across the surface from moving furniture.",
    "Sofa fabric stain – red wine spill, cleaning attempts unsuccessful.",
    "Freezer frost build-up excessive – unit not maintaining set temperature.",
    "Washing machine drum not spinning – belt may be broken, error code E3.",
    "AC unit not cooling – refrigerant leak suspected, warm air from vents.",
    "Water heater making loud popping noise and not reaching set temperature.",
    "Dishwasher leaving residue on dishes – spray arm clogged, door seal cracking.",
    "Furnace not igniting – pilot light goes out after 2 minutes, error code 31.",
    "Kitchen faucet dripping constantly – cartridge replacement required.",
    "Microwave turntable stopped rotating, sparking observed on interior wall.",
    "Garage door opener not responding to remote – motor running but chain slack.",
    "Dryer taking two cycles to dry a load – heating element failure suspected.",
    "Toilet running continuously – flapper valve worn, water wasting significantly.",
    "Oven not reaching temperature – bake element shows visible burn mark.",
    "Ceiling fan wobbling badly and making grinding noise at all speeds.",
    "Hot water pressure very low in master bathroom – possible pipe blockage.",
    "Refrigerator compressor running constantly – not cooling below 12°C.",
    "Heat pump making loud clicking sound on startup – capacitor issue likely.",
    "Sump pump not activating during heavy rain – float switch unresponsive.",
    "Range hood fan extremely noisy – motor bearing failure, grease filter clogged.",
    "Window AC unit leaking water inside – drainage pan cracked.",
    "Electrical panel upgrade required – 60-amp service insufficient for property.",
    "Patio furniture frame cracked at weld point – structural failure under normal use.",
    "Built-in oven control panel unresponsive – touchscreen dead after power surge.",
    "Central vacuum motor burned out – no suction, burning smell at wall outlets.",
]

CLAIM_DETAILS = [
    "Customer reports the issue started approximately 2 weeks ago. No prior repair attempts made. Unit is original to the home, estimated age 8 years.",
    "Damage occurred during normal use. Customer has photos available. Contractor has not been contacted yet.",
    "Issue is intermittent – happens mostly in the morning. Customer has tried resetting the circuit breaker without success.",
    "Unit was recently serviced by a third-party technician 3 months ago. Service records available on request.",
    "Customer is a tenant; landlord has been notified. Lease requires plan holder to manage warranty claims directly.",
    "Emergency situation – customer has young children and no heat. Priority dispatch requested.",
    "Customer attempted DIY repair which may have voided component warranty. Adjuster to assess impact.",
    "Item was replaced under a previous claim 18 months ago. Current failure is unrelated per customer.",
    "Damage is visible and measurable. Contractor dispatched for initial assessment, awaiting report.",
    "Customer requests specific contractor (previously used and approved). Assignment pending availability.",
    None,
]

# Plan names — exactly as shown in your Claims UI screenshot
PLANS = [
    ("COMP",   "Complete Home Protection – Appliances, Furniture, HVAC, Plumbing & Electrical",                          899.99),
    ("ELEC",   "Electrical Protection – Panels, Wiring, Outlets, Switches, Circuit Breakers, Exhaust Fans",              299.99),
    ("FRN",    "Furniture Protection – Sofas, Dining Sets, Beds, Mattresses, Office & Outdoor Furniture (stains, rips, structural)", 299.99),
    ("APP",    "Appliance Protection – Refrigerator, Washer, Dryer, Dishwasher, Oven, Microwave, Freezer, Stove",        449.99),
    ("HVAC",   "HVAC Protection – Furnace, AC Unit, Heat Pump, Ductwork, Thermostat",                                    499.99),
    ("PLMB",   "Plumbing Protection – Pipes, Drains, Water Heater, Sump Pump, Toilets, Fixtures",                        349.99),
    ("ROOF",   "Roofing & Structure Protection – Shingles, Flashing, Eavestroughs, Skylights",                           599.99),
    ("INT",    "Interior Protection – Flooring, Walls, Ceilings, Doors, Windows (accidental damage)",                    199.99),
]

CANADIAN_CITIES = [
    "Toronto","Vancouver","Montreal","Calgary","Ottawa","Edmonton","Mississauga",
    "Winnipeg","Quebec City","Hamilton","Brampton","Surrey","Kitchener","London",
    "Halifax","Victoria","Markham","St. John's","Kelowna","Saskatoon","Regina",
    "Barrie","Abbotsford","Sudbury","Kingston","Oakville","Richmond","Burlington",
    "Gatineau","Sherbrooke","Saguenay","Trois-Rivières","Thunder Bay","Guelph",
]

PROVINCES = ["ON","BC","QC","AB","MB","SK","NS","NB","NL","PE","NT","YT","NU"]

PROVINCE_MAP = {
    "Toronto":"ON","Mississauga":"ON","Brampton":"ON","Hamilton":"ON","Kitchener":"ON",
    "London":"ON","Barrie":"ON","Kingston":"ON","Oakville":"ON","Burlington":"ON",
    "Guelph":"ON","Thunder Bay":"ON","Markham":"ON",
    "Vancouver":"BC","Surrey":"BC","Kelowna":"BC","Abbotsford":"BC","Victoria":"BC","Richmond":"BC",
    "Montreal":"QC","Quebec City":"QC","Sherbrooke":"QC","Saguenay":"QC",
    "Trois-Rivières":"QC","Gatineau":"QC",
    "Calgary":"AB","Edmonton":"AB",
    "Winnipeg":"MB",
    "Saskatoon":"SK","Regina":"SK",
    "Halifax":"NS",
    "St. John's":"NL",
}

REVIEW_STATUSES = ["Approved", "Pending", "Flagged", "Rejected", None]
CLAIM_STATUSES  = ["Open", "Closed", "InProgress", "OnHold", "Cancelled"]
PROG_STATUSES   = ["New", "In Progress", "Dispatched", "Resolved", "Blocked"]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def rc(pool): return random.choice(pool)

def rand_date(years_back=4) -> datetime:
    base = datetime.now(timezone.utc) - timedelta(days=years_back * 365)
    return base + timedelta(seconds=random.randint(0, years_back * 365 * 86400))

def ext_code(prefix: str, n: int) -> str:
    return f"{prefix}-{n:07d}"

def batched(it, size):
    it = iter(it)
    while chunk := list(islice(it, size)):
        yield chunk


# ─────────────────────────────────────────────────────────────────────────────
# Row generators
# ─────────────────────────────────────────────────────────────────────────────

def gen_customers(n: int):
    emails = set()
    for i in range(1, n + 1):
        first = rc(FIRST_NAMES)
        last  = rc(LAST_NAMES)
        email = f"{first.lower().replace('é','e').replace('è','e').replace('ê','e').replace('ç','c')}.{last.lower().replace('é','e').replace('è','e').replace('ê','e').replace('ç','c')}.{i}@example.com"
        yield (
            first, last, f"{first} {last}", email,
            f"{random.randint(200,999)}{random.randint(1000000,9999999)}",   # mobilephone
            None,
            None,
            False, False,                                   # isprovider, issupplier
            random.random() > 0.12,                         # isactive
            round(random.uniform(40, 99), 1),               # qualityindex
            rc(REVIEW_STATUSES),
            ext_code("EXT", i),
            rc(CUSTOMER_NOTES),                             # note — rich NL
            None,                                           # coverageradius
            "CAD",
            rand_date(),
        )

def gen_partners(n: int, offset: int = 0):
    for i in range(1, n + 1):
        is_provider = random.random() > 0.35
        is_supplier = not is_provider or random.random() > 0.6
        if is_provider and not is_supplier:
            prefix, ptype = "PRV", "Provider"
        elif is_supplier and not is_provider:
            prefix, ptype = "SUP", "Supplier"
        else:
            prefix, ptype = "PRV", "Provider"

        use_company = random.random() > 0.45
        if use_company:
            name = rc(COMPANY_NAMES) + (f" {random.randint(2,9)}" if random.random() > 0.7 else "")
            first, last = name.split()[0], name.split()[-1]
            fullname = name
        else:
            first = rc(FIRST_NAMES)
            last  = rc(LAST_NAMES)
            fullname = f"{first} {last}"

        idx = offset + i
        yield (
            first, last, fullname,
            f"{first.lower()}.{last.lower()}.{idx}@partner.com",
            f"{random.randint(200,999)}{random.randint(1000000,9999999)}",
            None,
            f"{random.randint(200,999)}{random.randint(1000000,9999999)}" if random.random() > 0.5 else None,
            is_provider, is_supplier,
            random.random() > 0.1,
            round(random.uniform(60, 99), 1) if is_provider else None,
            rc(REVIEW_STATUSES),
            ext_code(prefix, idx),
            f"Certified {ptype.lower()} specializing in {rc(['HVAC','appliance repair','electrical','plumbing','furniture restoration','roofing'])}. {rc(['Coverage radius: city-wide.','Available 24/7 for emergency dispatch.','French and English service.','Licensed and insured in Ontario.','Serving GTA and surrounding regions.'])}",
            random.randint(20, 150) if is_provider else None,
            "CAD",
            rand_date(),
        )

def gen_locations(n: int):
    for _ in range(n):
        city = rc(CANADIAN_CITIES)
        prov = PROVINCE_MAP.get(city, rc(PROVINCES))
        postal = f"{rc('ABCEGHJKLMNPRSTVXY')}{random.randint(1,9)}{rc(string.ascii_uppercase)} {random.randint(1,9)}{rc(string.ascii_uppercase)}{random.randint(1,9)}"
        yield (
            f"{random.randint(1,9999)} {rc(['Main','King','Queen','Maple','Oak','Cedar','Elm','Wellington','Dundas','Bloor','Yonge','College','Spadina','Front'])} {rc(['St','Ave','Blvd','Dr','Rd','Cres','Lane','Way','Court'])}",
            None,
            city, postal, prov, "CA",
            round(random.uniform(43.0, 56.0), 6),
            round(random.uniform(-130.0, -52.0), 6),
            rand_date(),
        )

def gen_events(n: int, affiliate_ids: list, location_ids: list):
    year = datetime.now().year
    for i in range(1, n + 1):
        plan_prefix, _, _ = rc(PLANS)
        ref = f"{plan_prefix}-{year}-{i:06d}"
        status = rc(CLAIM_STATUSES)
        created = rand_date()
        start   = created + timedelta(hours=random.randint(0, 24))
        end     = start   + timedelta(hours=random.randint(2, 720))
        desc    = rc(CLAIM_DESCRIPTIONS)
        detail  = rc(CLAIM_DETAILS)
        yield (
            1, 1, 1,                                # eventtypeid, requesttypeid, subrequesttypeid
            status,
            rc(PROG_STATUSES),
            rc(CLAIM_STATUSES),
            start, end,
            start + timedelta(minutes=random.randint(0,120)),
            end   - timedelta(minutes=random.randint(0,120)),
            start - timedelta(minutes=random.randint(0,30)),
            None,
            created,
            rc(affiliate_ids),                      # organizeraffiliatedid
            rc(affiliate_ids),                      # associatedaffiliateid
            rc(location_ids),
            desc,                                   # description — NL text → embedding
            detail,                                 # details — NL text → embedding
            ref,
            rc(["urgent","vip","fraud","escalated","repeat","high-value","verified", None]),
            rc(["structural","electrical","plumbing","appliance","hvac","furniture", None]),
            rc(REVIEW_STATUSES),
            random.randint(0, 4),
            random.randint(0, 2),
        )

def gen_eventitems(event_ids: list):
    """One PLAN-* eventitem per event — this is what sync.py joins for plan_name + amount."""
    for eid in event_ids:
        prefix, plan_name, base_amount = rc(PLANS)
        code = f"PLAN-{prefix}-{eid}"
        amount = round(base_amount + random.uniform(-50, 200), 2)
        yield (
            eid,
            code, code,
            plan_name,                              # description → plan_name in OpenSearch
            plan_name,
            f"EI-{eid}",
            rand_date(), rand_date(), None,
            amount,
            round(amount * 0.1, 2),
            rc(["Active","Completed","Cancelled"]),
            1,
            rand_date(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# SQL
# ─────────────────────────────────────────────────────────────────────────────

AFFILIATE_SQL = """
INSERT INTO affiliate
  (firstname,lastname,fullname,email,
   mobilephone,homephone,workphone,
   isprovider,issupplier,isactive,
   qualityindex,reviewstatus,externalcode,note,
   coverageradius,currencyid,createdat)
VALUES %s RETURNING id
"""

LOCATION_SQL = """
INSERT INTO location
  (address1,address2,city,postalcode,
   provinceid,countryid,latitude,longitude,createdat)
VALUES %s RETURNING id
"""

EVENT_SQL = """
INSERT INTO event
  (eventtypeid,requesttypeid,subrequesttypeid,
   status,progressstatus,previousstatus,
   starttime,endtime,actualstarttime,actualendtime,
   notificationtime,cancellationtime,creationtime,
   organizeraffiliateid,associatedaffiliateid,locationid,
   description,details,reference,
   tag1,tag2,reviewstatus,
   escalationcount,customactionruncount)
VALUES %s RETURNING id
"""

EVENTITEM_SQL = """
INSERT INTO eventitem
  (eventid,identificationcode,originalidentificationcode,
   description,originaldescription,reference,
   consumptiontime,ordertime,shippeddate,
   amount,amount2,status,quantity,createdat)
VALUES %s
"""


# ─────────────────────────────────────────────────────────────────────────────
# Insert helpers
# ─────────────────────────────────────────────────────────────────────────────

def insert_ret(cur, sql, gen, total, batch, desc) -> list:
    ids = []
    with tqdm(total=total, desc=desc, unit="rows") as bar:
        for chunk in batched(gen, batch):
            psycopg2.extras.execute_values(cur, sql, chunk, page_size=batch)
            ids.extend(r[0] for r in cur.fetchall())
            bar.update(len(chunk))
    return ids

def insert_noret(cur, sql, gen, total, batch, desc):
    with tqdm(total=total, desc=desc, unit="rows") as bar:
        for chunk in batched(gen, batch):
            psycopg2.extras.execute_values(cur, sql, chunk, page_size=batch)
            bar.update(len(chunk))


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn",       required=True)
    ap.add_argument("--customers", type=int, default=1_000_000)
    ap.add_argument("--partners",  type=int, default=1_000_000)
    ap.add_argument("--claims",    type=int, default=1_000_000)
    ap.add_argument("--batch",     type=int, default=2_000)
    ap.add_argument("--dry-run",   action="store_true")
    args = ap.parse_args()

    print(f"""
╔══════════════════════════════════════════════════╗
║   Zucora Horizon — Load Test Seeder              ║
╠══════════════════════════════════════════════════╣
║  Customers  : {args.customers:>10,}                    ║
║  Partners   : {args.partners:>10,}                    ║
║  Claims     : {args.claims:>10,}                    ║
║  Batch size : {args.batch:>10,}                    ║
╠══════════════════════════════════════════════════╣
║  Tables written:                                 ║
║    affiliate  (customers + partners)             ║
║    event      (claims)                           ║
║    eventitem  (PLAN-* per claim)                 ║
║    location   (one per claim)                    ║
╚══════════════════════════════════════════════════╝
""")

    if args.dry_run:
        print("Dry-run — no writes."); sys.exit(0)

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 1. Customers
        print("\n[1/5] Customers → affiliate")
        cust_ids = insert_ret(cur, AFFILIATE_SQL,
                              gen_customers(args.customers),
                              args.customers, args.batch, "customers")
        conn.commit()
        print(f"      ✓ {len(cust_ids):,} inserted")

        # 2. Partners
        print("\n[2/5] Partners → affiliate")
        part_ids = insert_ret(cur, AFFILIATE_SQL,
                              gen_partners(args.partners, offset=args.customers),
                              args.partners, args.batch, "partners")
        conn.commit()
        print(f"      ✓ {len(part_ids):,} inserted")

        all_aff_ids = cust_ids + part_ids

        # 3. Locations (one per claim)
        print("\n[3/5] Locations → location")
        loc_ids = insert_ret(cur, LOCATION_SQL,
                             gen_locations(args.claims),
                             args.claims, args.batch, "locations")
        conn.commit()
        print(f"      ✓ {len(loc_ids):,} inserted")

        # 4. Events (claims)
        print("\n[4/5] Claims → event")
        evt_ids = insert_ret(cur, EVENT_SQL,
                             gen_events(args.claims, all_aff_ids, loc_ids),
                             args.claims, args.batch, "events")
        conn.commit()
        print(f"      ✓ {len(evt_ids):,} inserted")

        # 5. Eventitems — PLAN-* join used by sync.py
        print("\n[5/5] Plan items → eventitem")
        insert_noret(cur, EVENTITEM_SQL,
                     gen_eventitems(evt_ids),
                     args.claims, args.batch, "eventitems")
        conn.commit()
        print(f"      ✓ {args.claims:,} inserted")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Rolled back: {e}"); raise
    finally:
        cur.close(); conn.close()

    total = args.customers + args.partners + args.claims
    print(f"""
✅  Seeding complete — {total:,} total rows
    Run sync_all() to push to OpenSearch.
""")

if __name__ == "__main__":
    main()