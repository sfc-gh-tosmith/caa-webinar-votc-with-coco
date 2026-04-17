"""Generate realistic Voice-of-the-Customer CSV data for a CAA webinar demo.

Requires only the Python standard library.
Outputs: customers.csv, transactions.csv, call_transcripts.csv
"""
import csv, random, os
from datetime import datetime, timedelta

random.seed(42)

NUM_CUSTOMERS = 50
OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── helpers ─────────────────────────────────────────────────
def weighted_choice(options, weights):
    """random.choices backport that returns a single item."""
    return random.choices(options, weights=weights, k=1)[0]

def rand_date(start, max_days):
    return start + timedelta(days=random.randint(0, max_days))

# ── name pools ──────────────────────────────────────────────
first_names = [
    'James','Mary','Robert','Patricia','John','Jennifer','Michael','Linda',
    'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
    'Thomas','Sarah','Christopher','Karen','Charles','Lisa','Daniel','Nancy',
    'Matthew','Betty','Anthony','Margaret','Mark','Sandra','Donald','Ashley',
    'Steven','Dorothy','Andrew','Kimberly','Paul','Emily','Joshua','Donna',
    'Kenneth','Michelle','Kevin','Carol','Brian','Amanda','George','Melissa',
    'Timothy','Deborah',
]
last_names = [
    'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
    'Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson',
    'Thomas','Taylor','Moore','Jackson','Martin','Lee','Perez','Thompson',
    'White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson','Walker',
    'Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
    'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell',
    'Carter','Roberts',
]

age_groups  = ['18-24','25-34','35-44','45-54','55-64','65+']
age_weights = [0.10, 0.30, 0.27, 0.18, 0.10, 0.05]
regions     = ['Northeast','Southeast','Midwest','Southwest','West']
loyalty_tiers = ['bronze','silver','gold','platinum']

product_catalog = {
    'electronics':  ['Wireless Headphones','Bluetooth Speaker','Tablet Stand','USB-C Hub','Portable Charger'],
    'clothing':     ['Winter Jacket','Running Shoes','Cotton T-Shirt','Denim Jeans','Wool Sweater'],
    'home_goods':   ['Scented Candle Set','Throw Blanket','Kitchen Organizer','Ceramic Vase','Wall Clock'],
    'beauty':       ['Moisturizing Cream','Vitamin C Serum','Hair Styling Kit','Perfume Set','Sunscreen SPF 50'],
    'sports':       ['Yoga Mat','Resistance Bands','Water Bottle','Fitness Tracker Band','Jump Rope'],
    'books':        ['Bestseller Novel','Cookbook Collection','Self-Help Guide','History Anthology','Science Fiction Box Set'],
    'grocery':      ['Organic Coffee Beans','Protein Bar Variety Pack','Olive Oil Premium','Trail Mix Bundle','Green Tea Sampler'],
}
products_flat = [p for prods in product_catalog.values() for p in prods]
categories    = list(product_catalog.keys())

tier_spend = {'bronze': (15, 60), 'silver': (30, 120), 'gold': (50, 200), 'platinum': (80, 350)}

# ── satisfaction profiles (internal only) ───────────────────
SAT_POOL = ['high'] * 40 + ['medium'] * 40 + ['low'] * 20  # 40/40/20

# tier distribution conditioned on satisfaction
TIER_WEIGHTS = {
    'high':   [0.05, 0.20, 0.40, 0.35],
    'medium': [0.25, 0.40, 0.25, 0.10],
    'low':    [0.45, 0.35, 0.15, 0.05],
}

# ══════════════════════════════════════════════════════════════
#  CALL TRANSCRIPT TEMPLATES
# ══════════════════════════════════════════════════════════════

positive_templates = [
    # product praise
    "Agent: Thank you for calling, how can I help?\n"
    "Customer: Hi, I just wanted to say my {product} arrived and I absolutely love it! The quality exceeded my expectations.\n"
    "Agent: That's wonderful to hear! We really appreciate the feedback.\n"
    "Customer: Honestly, I've been shopping around for a while and this is the best I've found. I'll definitely be back for more.",

    "Agent: Good afternoon, thanks for reaching out. How can I assist you today?\n"
    "Customer: I received my {product} yesterday and the packaging was perfect — got here two days early!\n"
    "Agent: That's great to hear. I'll pass that along to our fulfillment team.\n"
    "Customer: Please do. Really impressed with the whole experience from ordering to delivery.",

    # easy service
    "Agent: Welcome to customer support. What can I do for you?\n"
    "Customer: I had a quick question about my recent {product} order. I wanted to see if I could add another item.\n"
    "Agent: Absolutely, I can help with that right away. What would you like to add?\n"
    "Customer: Perfect, that was so easy. I love how responsive you guys are. Thanks for the quick help!",

    "Agent: Hi there, thanks for calling. How can I assist you today?\n"
    "Customer: I wanted to check on the return policy for my {product}. Not that I want to return it — it's great — just curious.\n"
    "Agent: No problem at all! Our return window is 30 days with free shipping on returns.\n"
    "Customer: Awesome, that's really generous. Thanks so much!",

    # loyalty / repeat buyer
    "Agent: Thank you for calling. How may I help you today?\n"
    "Customer: Hi, I've been a customer for over a year now and just placed my latest order for a {product}. Wanted to check on my loyalty points.\n"
    "Agent: Of course! Let me pull that up. You've accumulated quite a few points.\n"
    "Customer: Great, I'm saving up for a big purchase. You guys make it easy to keep coming back.",

    "Agent: Hello, welcome back! I see you're one of our valued customers. What can I help with?\n"
    "Customer: Hey! Yeah, I order from you all the time. Just wanted to confirm my {product} shipped.\n"
    "Agent: It shipped this morning and should arrive by Thursday.\n"
    "Customer: Perfect as always. You guys never disappoint.",

    # recommendation / upsell accepted
    "Agent: Thanks for calling! Anything I can help with today?\n"
    "Customer: I just bought a {product} and I'm wondering if you have any accessories that go with it.\n"
    "Agent: Great timing — we actually have a bundle deal running right now. Would you like me to add it to your order?\n"
    "Customer: Yes please! I love that you proactively suggest things. Makes shopping so much easier.",
]

neutral_templates = [
    "Agent: Thank you for calling. How can I help?\n"
    "Customer: Hi, I'm looking at the {product} on your website and wanted to know if it comes in other colors.\n"
    "Agent: Great question! Let me check our inventory. We do have a few options available.\n"
    "Customer: Okay, thanks. I'll take a look and decide.",

    "Agent: Welcome to customer support. What can I do for you today?\n"
    "Customer: I placed an order for a {product} and I'm wondering when it will arrive. The tracking page isn't loading.\n"
    "Agent: Let me look that up. It looks like it's on schedule for delivery next Tuesday.\n"
    "Customer: Alright, thanks for checking. I appreciate it.",

    "Agent: Hi, how can I assist you?\n"
    "Customer: I want to exchange my {product} for a different size. What's the process?\n"
    "Agent: Sure thing. I can set up the exchange right now. You'll get a prepaid label by email.\n"
    "Customer: Okay, sounds straightforward. Thanks.",

    "Agent: Good morning, what can I help you with?\n"
    "Customer: I'm trying to use a promo code on my {product} order but it's not applying.\n"
    "Agent: Let me check — it looks like that code expired yesterday. I can offer you a 10% courtesy discount instead.\n"
    "Customer: That works. Go ahead and apply it. Thanks.",

    "Agent: Hi there, how can I help?\n"
    "Customer: I just need to update my shipping address before my {product} order goes out. Is it too late?\n"
    "Agent: Let me check... it hasn't shipped yet so I can update it now. What's the new address?\n"
    "Customer: Great, I'll give it to you now. Thanks for catching that in time.",
]

negative_templates = [
    # shipping delay
    "Agent: Thank you for calling, how can I help?\n"
    "Customer: I've been waiting over three weeks for my {product} and the tracking hasn't updated in 12 days. This is completely unacceptable.\n"
    "Agent: I'm so sorry about that. Let me look into the status right away.\n"
    "Customer: I needed this as a gift and now it's too late. Your shipping is terrible. I paid extra for faster delivery and got nothing. I'm seriously considering canceling my account.",

    # product defect
    "Agent: Welcome to support. What's going on?\n"
    "Customer: My {product} arrived completely damaged — the box was crushed and the item doesn't work at all. I've emailed support twice and nobody responded.\n"
    "Agent: I'm really sorry to hear that. Let me escalate this for you immediately.\n"
    "Customer: This is the second time I've had a quality issue. I've been a loyal customer but I'm losing trust fast. I want a full refund, not a replacement.",

    # billing / double charge
    "Agent: Hi, how can I assist?\n"
    "Customer: You charged me twice for my {product}! I see two identical charges on my credit card. I called about this three days ago and was told it'd be fixed in 24 hours. Nothing happened.\n"
    "Agent: I sincerely apologize for the billing error. Let me escalate this to our finance team.\n"
    "Customer: I shouldn't have to call multiple times for your mistake. This is really poor service and it's making me reconsider shopping here.",

    # wrong item
    "Agent: Thank you for calling. How can I help today?\n"
    "Customer: I received the completely wrong item. I ordered a {product} and got something else entirely. Your website return portal keeps giving me an error.\n"
    "Agent: That's definitely not the experience we want. Let me get this sorted out.\n"
    "Customer: I've wasted two hours trying to fix this myself. Between the wrong item and the broken website, I'm really frustrated. I want this resolved today or I'm disputing the charge.",

    # poor service history
    "Agent: Good afternoon, how may I help you?\n"
    "Customer: I'm calling about my {product} order AGAIN. I was promised a callback from a supervisor last week and never got one. This is my fourth time calling about the same issue.\n"
    "Agent: I'm very sorry for the lack of follow-up. Let me take ownership of this.\n"
    "Customer: At this point I just want my money back. I've been a customer for two years and I've never been treated this poorly. I'm done.",

    # return difficulty
    "Agent: Hi there, thanks for calling. What can I help with?\n"
    "Customer: I've been trying to return my {product} for two weeks. The return label you sent didn't work, and when I brought it to the carrier they said it was invalid.\n"
    "Agent: I apologize for that. Let me generate a new return label right away.\n"
    "Customer: This whole return process has been a nightmare. I just want my refund. If this isn't resolved this week I'm filing a complaint with my credit card company.",
]

# ══════════════════════════════════════════════════════════════
#  1. GENERATE CUSTOMERS
# ══════════════════════════════════════════════════════════════
customers = []
for cid in range(1, NUM_CUSTOMERS + 1):
    fname = random.choice(first_names)
    lname = random.choice(last_names)
    sat   = random.choice(SAT_POOL)
    tier  = weighted_choice(loyalty_tiers, TIER_WEIGHTS[sat])

    customers.append({
        'CUSTOMER_ID':   cid,
        'CUSTOMER_NAME': f'{fname} {lname}',
        'EMAIL':         f'{fname.lower()}.{lname.lower()}{cid}@example.com',
        'AGE_GROUP':     weighted_choice(age_groups, age_weights),
        'REGION':        random.choice(regions),
        'LOYALTY_TIER':  tier,
        'SIGNUP_DATE':   (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
        '_sat':          sat,   # internal — not written to CSV
    })

# ══════════════════════════════════════════════════════════════
#  2. GENERATE TRANSACTIONS
# ══════════════════════════════════════════════════════════════
transactions = []
txn_id = 1
for c in customers:
    sat = c['_sat']
    n = {'high': random.randint(18, 28),
         'medium': random.randint(12, 18),
         'low': random.randint(10, 13)}[sat]

    lo, hi = tier_spend[c['LOYALTY_TIER']]
    signup = datetime.strptime(c['SIGNUP_DATE'], '%Y-%m-%d')

    for _ in range(n):
        cat     = random.choice(categories)
        product = random.choice(product_catalog[cat])
        pdate   = rand_date(signup, 700)
        transactions.append({
            'TRANSACTION_ID':   txn_id,
            'CUSTOMER_ID':      c['CUSTOMER_ID'],
            'PURCHASE_DATE':    pdate.strftime('%Y-%m-%d'),
            'PRODUCT_CATEGORY': cat,
            'PRODUCT_NAME':     product,
            'AMOUNT':           round(random.uniform(lo, hi), 2),
            'QUANTITY':         random.randint(1, 4),
        })
        txn_id += 1

# ══════════════════════════════════════════════════════════════
#  3. GENERATE CALL TRANSCRIPTS
# ══════════════════════════════════════════════════════════════
calls = []
call_id = 1
for c in customers:
    sat    = c['_sat']
    signup = datetime.strptime(c['SIGNUP_DATE'], '%Y-%m-%d')

    n = {'high': random.randint(3, 5),
         'medium': random.randint(2, 4),
         'low': random.randint(2, 3)}[sat]

    for i in range(n):
        product  = random.choice(products_flat)
        is_last  = (i == n - 1)
        r = random.random()

        if sat == 'high':
            tpl = random.choice(positive_templates) if r < 0.85 else random.choice(neutral_templates)
        elif sat == 'medium':
            if r < 0.55:
                tpl = random.choice(positive_templates)
            elif r < 0.85:
                tpl = random.choice(neutral_templates)
            else:
                tpl = random.choice(negative_templates)
        else:  # low
            if is_last:
                tpl = random.choice(negative_templates)
            elif i == n - 2:
                tpl = random.choice(negative_templates) if r < 0.6 else random.choice(neutral_templates)
            else:
                if r < 0.3:
                    tpl = random.choice(positive_templates)
                elif r < 0.6:
                    tpl = random.choice(neutral_templates)
                else:
                    tpl = random.choice(negative_templates)

        cdate = signup + timedelta(days=30 + random.randint(0, 570) + i * 60)
        calls.append({
            'CALL_ID':      call_id,
            'CUSTOMER_ID':  c['CUSTOMER_ID'],
            'CALL_DATE':    cdate.strftime('%Y-%m-%d'),
            'TRANSCRIPT':   tpl.format(product=product),
        })
        call_id += 1

# ══════════════════════════════════════════════════════════════
#  WRITE CSVs
# ══════════════════════════════════════════════════════════════
def write_csv(filename, rows, fieldnames):
    path = os.path.join(OUT_DIR, filename)
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")

# strip internal field
customers_out = [{k: v for k, v in c.items() if not k.startswith('_')} for c in customers]

print("Writing CSVs...")
write_csv('customers.csv', customers_out,
          ['CUSTOMER_ID','CUSTOMER_NAME','EMAIL','AGE_GROUP','REGION','LOYALTY_TIER','SIGNUP_DATE'])
write_csv('transactions.csv', transactions,
          ['TRANSACTION_ID','CUSTOMER_ID','PURCHASE_DATE','PRODUCT_CATEGORY','PRODUCT_NAME','AMOUNT','QUANTITY'])
write_csv('call_transcripts.csv', calls,
          ['CALL_ID','CUSTOMER_ID','CALL_DATE','TRANSCRIPT'])

# ── validate ────────────────────────────────────────────────
print("\nValidating...")
txn_counts  = {}
call_counts = {}
for t in transactions:
    txn_counts[t['CUSTOMER_ID']] = txn_counts.get(t['CUSTOMER_ID'], 0) + 1
for cl in calls:
    call_counts[cl['CUSTOMER_ID']] = call_counts.get(cl['CUSTOMER_ID'], 0) + 1

ok = True
for cid in range(1, NUM_CUSTOMERS + 1):
    if txn_counts.get(cid, 0) < 10:
        print(f"  FAIL: customer {cid} has {txn_counts.get(cid,0)} transactions")
        ok = False
    if call_counts.get(cid, 0) < 2:
        print(f"  FAIL: customer {cid} has {call_counts.get(cid,0)} calls")
        ok = False

if ok:
    print("  All customers have >= 10 transactions and >= 2 calls.")
print("Done.")
