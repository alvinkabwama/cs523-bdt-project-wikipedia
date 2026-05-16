"""CS523 final project -- Wikipedia recent-changes Streamlit dashboard.

Reads HBase wikipedia_* tables via happybase Thrift (port 9090) and renders
metric cards, top-wikis bar charts, an edits-per-minute trend, and the
bonus Spark-SQL static-join breakdown by continent / region.

Run inside the lab container:
    streamlit run /opt/project/dashboard/app.py --server.address 0.0.0.0
Open http://localhost:8501.
"""

from collections import defaultdict
from datetime import datetime

import altair as alt
import happybase
import pandas as pd
import streamlit as st

HBASE_HOST = "localhost"
HBASE_THRIFT_PORT = 9090
REFRESH_SEC = 5

# ---------------------------------------------------------------------------
# Pretty-name lookups so the dashboard reads in plain English everywhere
# instead of exposing raw Wikimedia codes. Missing keys fall through to the
# raw code.
# ---------------------------------------------------------------------------
WIKI_DISPLAY_NAMES = {
    "enwiki": "English Wikipedia", "simplewiki": "Simple English Wikipedia",
    "dewiki": "German Wikipedia", "frwiki": "French Wikipedia",
    "eswiki": "Spanish Wikipedia", "ptwiki": "Portuguese Wikipedia",
    "itwiki": "Italian Wikipedia", "ruwiki": "Russian Wikipedia",
    "ukwiki": "Ukrainian Wikipedia", "plwiki": "Polish Wikipedia",
    "nlwiki": "Dutch Wikipedia", "svwiki": "Swedish Wikipedia",
    "nowiki": "Norwegian (Bokmal) Wikipedia",
    "nnwiki": "Norwegian (Nynorsk) Wikipedia",
    "dawiki": "Danish Wikipedia", "fiwiki": "Finnish Wikipedia",
    "iswiki": "Icelandic Wikipedia", "cswiki": "Czech Wikipedia",
    "skwiki": "Slovak Wikipedia", "hrwiki": "Croatian Wikipedia",
    "srwiki": "Serbian Wikipedia", "slwiki": "Slovenian Wikipedia",
    "bgwiki": "Bulgarian Wikipedia", "rowiki": "Romanian Wikipedia",
    "huwiki": "Hungarian Wikipedia", "elwiki": "Greek Wikipedia",
    "trwiki": "Turkish Wikipedia", "hewiki": "Hebrew Wikipedia",
    "arwiki": "Arabic Wikipedia", "fawiki": "Persian Wikipedia",
    "urwiki": "Urdu Wikipedia", "hiwiki": "Hindi Wikipedia",
    "bnwiki": "Bengali Wikipedia", "tawiki": "Tamil Wikipedia",
    "tewiki": "Telugu Wikipedia", "mlwiki": "Malayalam Wikipedia",
    "knwiki": "Kannada Wikipedia", "mrwiki": "Marathi Wikipedia",
    "guwiki": "Gujarati Wikipedia", "pawiki": "Punjabi Wikipedia",
    "thwiki": "Thai Wikipedia", "viwiki": "Vietnamese Wikipedia",
    "idwiki": "Indonesian Wikipedia", "mswiki": "Malay Wikipedia",
    "tlwiki": "Tagalog Wikipedia", "jawiki": "Japanese Wikipedia",
    "kowiki": "Korean Wikipedia", "zhwiki": "Chinese Wikipedia",
    "zh_yuewiki": "Cantonese Wikipedia",
    "zh_minnanwiki": "Min Nan Wikipedia",
    "mnwiki": "Mongolian Wikipedia", "kkwiki": "Kazakh Wikipedia",
    "azwiki": "Azerbaijani Wikipedia", "kawiki": "Georgian Wikipedia",
    "hywiki": "Armenian Wikipedia", "swwiki": "Swahili Wikipedia",
    "yowiki": "Yoruba Wikipedia", "amwiki": "Amharic Wikipedia",
    "afwiki": "Afrikaans Wikipedia", "zuwiki": "Zulu Wikipedia",
    "euwiki": "Basque Wikipedia", "cawiki": "Catalan Wikipedia",
    "glwiki": "Galician Wikipedia", "cywiki": "Welsh Wikipedia",
    "gawiki": "Irish Wikipedia", "lawiki": "Latin Wikipedia",
    "eowiki": "Esperanto Wikipedia",
    # Sister projects + cross-project wikis
    "commonswiki":    "Wikimedia Commons",
    "wikidatawiki":   "Wikidata",
    "metawiki":       "Wikimedia Meta",
    "mediawikiwiki":  "MediaWiki Documentation",
    "specieswiki":    "Wikispecies",
    "sourceswiki":    "Wikisource (multilingual)",
    "foundationwiki": "Wikimedia Foundation site",
    "incubatorwiki":  "Wikimedia Incubator",
    "outreachwiki":   "Wikimedia Outreach",
    "loginwiki":      "Wikimedia central login",
    # Per-language Wiktionaries (a few common ones)
    "enwiktionary": "English Wiktionary",
    "frwiktionary": "French Wiktionary",
    "dewiktionary": "German Wiktionary",
    "ruwiktionary": "Russian Wiktionary",
    "itwikiquote":  "Italian Wikiquote",
    "enwikinews":   "English Wikinews",
    "frwikinews":   "French Wikinews",
    "enwikiquote":  "English Wikiquote",
    "frwikiquote":  "French Wikiquote",
    "enwikisource": "English Wikisource",
    "frwikisource": "French Wikisource",
    "zhwikisource": "Chinese Wikisource",
}

NAMESPACE_NAMES = {
    0:  "Article",
    1:  "Article talk",
    2:  "User",
    3:  "User talk",
    4:  "Project",
    5:  "Project talk",
    6:  "File",
    7:  "File talk",
    8:  "MediaWiki",
    9:  "MediaWiki talk",
    10: "Template",
    11: "Template talk",
    12: "Help",
    13: "Help talk",
    14: "Category",
    15: "Category talk",
    100: "Portal",
    101: "Portal talk",
    118: "Draft",
    119: "Draft talk",
    710: "TimedText",
    711: "TimedText talk",
    828: "Module",
    829: "Module talk",
   -1: "Special (virtual)",
   -2: "Media (virtual)",
}

EDIT_TYPE_NAMES = {
    "edit":       "Edit (revision to existing page)",
    "new":        "New page creation",
    "log":        "Log entry (move, delete, protect, etc.)",
    "categorize": "Category membership change",
    "external":   "External data update (e.g. Wikidata)",
}

# Smart fallback so codes not in the curated WIKI_DISPLAY_NAMES still resolve
# to readable names. Wikimedia codes follow the pattern <lang_code><project>,
# e.g. "frwikibooks" -> French + Wikibooks -> "French Wikibooks".
LANGUAGE_CODE_TO_NAME = {
    "en": "English", "simple": "Simple English", "de": "German", "fr": "French",
    "es": "Spanish", "pt": "Portuguese", "it": "Italian", "ru": "Russian",
    "uk": "Ukrainian", "pl": "Polish", "nl": "Dutch", "sv": "Swedish",
    "no": "Norwegian (Bokmal)", "nn": "Norwegian (Nynorsk)", "da": "Danish",
    "fi": "Finnish", "is": "Icelandic", "cs": "Czech", "sk": "Slovak",
    "hr": "Croatian", "sr": "Serbian", "sl": "Slovenian", "bg": "Bulgarian",
    "ro": "Romanian", "hu": "Hungarian", "el": "Greek", "tr": "Turkish",
    "he": "Hebrew", "ar": "Arabic", "fa": "Persian", "ur": "Urdu",
    "hi": "Hindi", "bn": "Bengali", "ta": "Tamil", "te": "Telugu",
    "ml": "Malayalam", "kn": "Kannada", "mr": "Marathi", "gu": "Gujarati",
    "pa": "Punjabi", "ne": "Nepali", "si": "Sinhala", "my": "Burmese",
    "th": "Thai", "vi": "Vietnamese", "id": "Indonesian", "ms": "Malay",
    "tl": "Tagalog", "ja": "Japanese", "ko": "Korean", "zh": "Chinese",
    "zh_yue": "Cantonese", "zh_minnan": "Min Nan", "mn": "Mongolian",
    "kk": "Kazakh", "az": "Azerbaijani", "ka": "Georgian", "hy": "Armenian",
    "sw": "Swahili", "yo": "Yoruba", "ha": "Hausa", "am": "Amharic",
    "af": "Afrikaans", "zu": "Zulu", "xh": "Xhosa", "eu": "Basque",
    "ca": "Catalan", "gl": "Galician", "cy": "Welsh", "ga": "Irish",
    "sc": "Sardinian", "la": "Latin", "eo": "Esperanto", "io": "Ido",
    "vo": "Volapuk", "ie": "Interlingue", "be": "Belarusian",
    "mk": "Macedonian", "bs": "Bosnian", "li": "Limburgish",
    "lb": "Luxembourgish", "fy": "West Frisian", "ku": "Kurdish",
    "ckb": "Central Kurdish (Sorani)", "ps": "Pashto", "tg": "Tajik",
    "jv": "Javanese", "min": "Minangkabau", "ce": "Chechen",
    "an": "Aragonese", "bcl": "Central Bicolano", "gpe": "Ghanaian Pidgin",
    "kcg": "Tyap", "sh": "Serbo-Croatian", "br": "Breton",
    "oc": "Occitan", "co": "Corsican", "wa": "Walloon", "rm": "Romansh",
    "fo": "Faroese", "sq": "Albanian", "lt": "Lithuanian",
    "lv": "Latvian", "et": "Estonian", "uz": "Uzbek", "ky": "Kyrgyz",
    "tk": "Turkmen", "sd": "Sindhi", "or": "Odia", "as": "Assamese",
    "km": "Khmer", "lo": "Lao", "ig": "Igbo", "wuu": "Wu",
    "hak": "Hakka", "tt": "Tatar", "ba": "Bashkir", "cv": "Chuvash",
    "sah": "Yakut", "kw": "Cornish", "gd": "Scottish Gaelic",
    "gv": "Manx", "se": "Northern Sami", "smn": "Inari Sami",
    "ny": "Chichewa", "lg": "Ganda", "rw": "Kinyarwanda",
    "rn": "Kirundi", "ts": "Tsonga", "tn": "Tswana", "ss": "Swati",
    "ve": "Venda", "st": "Sotho", "nso": "Northern Sotho",
    # Popular bot-driven Wikipedias and minor language editions
    "ceb": "Cebuano", "war": "Waray", "nb": "Norwegian Bokmal",
    "nds": "Low German", "als": "Alemannic", "bar": "Bavarian",
    "frr": "North Frisian", "ksh": "Kolsch", "pms": "Piedmontese",
    "lmo": "Lombard", "vec": "Venetian", "nap": "Neapolitan",
    "scn": "Sicilian", "lij": "Ligurian", "pdc": "Pennsylvania German",
    "mt": "Maltese", "gan": "Gan Chinese", "bo": "Tibetan",
    "dz": "Dzongkha", "ti": "Tigrinya", "om": "Oromo",
    "so": "Somali", "sn": "Shona", "nv": "Navajo",
    "haw": "Hawaiian", "mi": "Maori", "sm": "Samoan",
    "to": "Tongan", "fj": "Fijian", "ty": "Tahitian",
    "qu": "Quechua", "gn": "Guarani", "ay": "Aymara",
    "ht": "Haitian Creole", "yi": "Yiddish", "lad": "Ladino",
    "iu": "Inuktitut", "kl": "Greenlandic", "ab": "Abkhazian",
    "os": "Ossetic", "tg": "Tajik", "fy": "West Frisian",
    "ang": "Old English", "non": "Old Norse", "got": "Gothic",
    "cu": "Old Church Slavonic", "pi": "Pali", "sa": "Sanskrit",
    "tlh": "Klingon", "jbo": "Lojban", "kr": "Kanuri",
    "ks": "Kashmiri", "ee": "Ewe", "tw": "Twi",
    "wo": "Wolof", "ff": "Fulah", "bm": "Bambara",
    "akan": "Akan", "ak": "Akan", "ny": "Chichewa",
    "tum": "Tumbuka", "lua": "Luba-Lulua", "kg": "Kongo",
    "ln": "Lingala", "umb": "Umbundu", "kab": "Kabyle",
    "shi": "Tachelhit", "ti": "Tigrinya", "ber": "Berber",
    "diq": "Zazaki", "lez": "Lezgian", "av": "Avaric",
    "bxr": "Buryat", "myv": "Erzya", "mdf": "Moksha",
    "krc": "Karachay-Balkar", "kv": "Komi", "udm": "Udmurt",
    "mhr": "Eastern Mari", "mrj": "Western Mari", "koi": "Komi-Permyak",
    "ady": "Adyghe", "kbd": "Kabardian",
}

PROJECT_SUFFIX_TO_NAME = {
    "wiktionary":  "Wiktionary",
    "wikiquote":   "Wikiquote",
    "wikibooks":   "Wikibooks",
    "wikisource":  "Wikisource",
    "wikinews":    "Wikinews",
    "wikiversity": "Wikiversity",
    "wikivoyage":  "Wikivoyage",
    "wiki":        "Wikipedia",
}


def pretty_wiki(code):
    """Resolve a Wikimedia wiki code to a human-readable name.

    Order:
      1. Exact match in the curated WIKI_DISPLAY_NAMES table.
      2. Smart parse: split the code into <lang><project> (longest project
         suffix wins so 'wiktionary' beats 'wiki') and look up both halves.
      3. Give up and return the raw code.
    """
    if not code:
        return ""
    if code in WIKI_DISPLAY_NAMES:
        return WIKI_DISPLAY_NAMES[code]
    for suffix in ("wiktionary", "wikiquote", "wikibooks", "wikisource",
                   "wikinews", "wikiversity", "wikivoyage", "wiki"):
        if code.endswith(suffix):
            lang_code = code[: -len(suffix)]
            if lang_code in LANGUAGE_CODE_TO_NAME:
                return f"{LANGUAGE_CODE_TO_NAME[lang_code]} {PROJECT_SUFFIX_TO_NAME[suffix]}"
    return code


def pretty_namespace(n):
    if n is None:
        return "(unknown)"
    return NAMESPACE_NAMES.get(int(n), f"Namespace {n}")


def pretty_edit_type(t):
    return EDIT_TYPE_NAMES.get(t, t) if t else "(unknown)"

st.set_page_config(page_title="Wikipedia Live", layout="wide")

# ---------------------------------------------------------------------------
# Color theme only (admin-portal style): dark navy sidebar, light-grey main,
# white metric cards with colored left-accent bars. No structural changes.
# ---------------------------------------------------------------------------
st.markdown(
    """
<style>
#MainMenu, [data-testid="stToolbar"], [data-testid="stStatusWidget"] {
    visibility: hidden;
}

/* Keep the sidebar collapse / expand toggle visible even while the rest of
   Streamlit's top chrome is hidden. Without this rule the user can collapse
   the sidebar but cannot bring it back. Different Streamlit versions use
   different testids, so target every known variant. */
[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"],
[data-testid="baseButton-headerNoPadding"],
button[kind="headerNoPadding"],
button[kind="header"] {
    visibility: visible !important;
    z-index: 999999 !important;
}
[data-testid="stSidebarCollapsedControl"] {
    position: fixed !important;
    top: 0.5rem !important;
    left: 0.5rem !important;
    background: #1d3a52 !important;
    color: white !important;
    border-radius: 6px !important;
    padding: 6px 10px !important;
    box-shadow: 0 2px 6px rgba(0,0,0,0.15) !important;
}
[data-testid="stSidebarCollapsedControl"] svg,
[data-testid="stSidebarCollapsedControl"] * {
    color: white !important;
    fill: white !important;
}

.stApp { background-color: #f3f4f6; }

[data-testid="stSidebar"] > div:first-child { background: #1d3a52; }
[data-testid="stSidebar"] *,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span { color: #e5e7eb !important; }
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] strong { color: #ffffff !important; }
[data-testid="stSidebar"] input[type="text"] {
    background: #2d4a63 !important;
    color: #ffffff !important;
    border: 1px solid #3d5a73 !important;
}

/* Selectbox in the sidebar (the dropdown's CLOSED state). BaseWeb renders
   the selected-value display on a white background; without this rule it
   inherits our global light-text colour and ends up grey-on-white. We
   force dark text here. The open menu (when the user clicks the
   dropdown) is rendered in a portal outside the sidebar and is unaffected. */
[data-testid="stSidebar"] [data-baseweb="select"] {
    background: #ffffff !important;
    border-radius: 6px !important;
}
[data-testid="stSidebar"] [data-baseweb="select"] *,
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="select"] > div > div,
[data-testid="stSidebar"] [data-baseweb="select"] span {
    color: #111827 !important;
    fill: #111827 !important;
}

[data-testid="stMetric"] {
    background: white;
    border-radius: 8px;
    padding: 16px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    border-left: 4px solid #3b82f6;
}
[data-testid="stMetricLabel"] {
    color: #6b7280 !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-size: 11px !important;
}
[data-testid="stMetricValue"] {
    color: #111827 !important;
    font-weight: 700 !important;
}
[data-testid="stColumn"]:nth-of-type(4n+1) [data-testid="stMetric"] { border-left-color: #3b82f6; }
[data-testid="stColumn"]:nth-of-type(4n+2) [data-testid="stMetric"] { border-left-color: #6b7280; }
[data-testid="stColumn"]:nth-of-type(4n+3) [data-testid="stMetric"] { border-left-color: #f59e0b; }
[data-testid="stColumn"]:nth-of-type(4n+4) [data-testid="stMetric"] { border-left-color: #14b8a6; }

[data-testid="stVerticalBlockBorderWrapper"] {
    background: white;
    border: none !important;
    border-radius: 8px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("CS523 -- Real-time Wikipedia Edit Stream Dashboard")
st.caption(
    "Wikimedia EventStreams (SSE)  ->  Kafka  ->  Spark Structured Streaming  ->  "
    "HBase  ->  happybase / Thrift  ->  Streamlit."
)

# ---------------------------------------------------------------------------
# Plain-language explanation for anyone who lands on the dashboard cold.
# ---------------------------------------------------------------------------
st.markdown(
    """
**What you are looking at.** Every time anyone, anywhere in the world,
clicks "publish" on a Wikipedia article, a Wikidata entry, a media upload
to Wikimedia Commons, or any other Wikimedia project, the Wikimedia
Foundation pushes an event describing that edit over a public real-time
feed. This dashboard subscribes to that feed, streams every edit through
Apache Kafka, processes it with Apache Spark Structured Streaming on a
Hadoop cluster, stores the results in Apache HBase, and renders the
charts below. The whole dashboard refreshes every five seconds; the
underlying Spark windows close every minute, which is why most charts
visibly tick once a minute.

**How to read this page.** The four metric cards at the top show what is
happening *right now* across the whole feed. The two bar charts directly
under them show the most active wikis in the last minute and the average
size change (in bytes) of edits on each wiki. The "Edit types observed"
and "Wikipedia namespaces observed" charts break down *what kind* of
editing is happening: ordinary article edits, bot category updates, page
moves, and so on. The trend chart shows the rate of edits per minute for
one wiki you select in the sidebar. The bonus section at the bottom
attaches a language, continent, and region to every wiki using a Spark
SQL join with a static lookup file on HDFS.
""",
    unsafe_allow_html=False,
)


def _decode(v):
    if v is None:
        return None
    try:
        return v.decode("utf-8")
    except Exception:
        return str(v)


def _float(v, default=None):
    s = _decode(v)
    try:
        return float(s) if s not in (None, "") else default
    except ValueError:
        return default


def _int(v, default=None):
    f = _float(v)
    return int(f) if f is not None else default


@st.cache_resource
def get_pool():
    return happybase.ConnectionPool(size=3, host=HBASE_HOST, port=HBASE_THRIFT_PORT)


def fetch_live():
    rows = []
    with get_pool().connection() as conn:
        tbl = conn.table("wikipedia_live")
        for key, data in tbl.scan(limit=2000):
            wiki = _decode(key)
            rows.append({
                "wiki":         wiki,
                "type":         _decode(data.get(b"info:type")),
                "title":        _decode(data.get(b"info:title")),
                "user":         _decode(data.get(b"info:user")),
                "namespace":    _int(data.get(b"info:namespace")),
                "server_name":  _decode(data.get(b"info:server_name")),
                "length_old":   _int(data.get(b"stats:length_old")),
                "length_new":   _int(data.get(b"stats:length_new")),
                "length_delta": _int(data.get(b"stats:length_delta")),
                "bot":          _decode(data.get(b"meta:bot")) == "true",
                "minor":        _decode(data.get(b"meta:minor")) == "true",
            })
    return pd.DataFrame(rows)


def fetch_latest_counts_per_wiki():
    """Return latest 1-minute count per wiki (newest window per wiki)."""
    latest_count = {}
    latest_delta = {}
    with get_pool().connection() as conn:
        tbl = conn.table("wikipedia_agg")
        for key, data in tbl.scan(limit=20000):
            row_key = _decode(key)
            if "|" not in row_key:
                continue
            wiki = row_key.split("|", 1)[0]
            if wiki not in latest_count:
                cnt = _float(data.get(b"m:count"))
                if cnt is not None:
                    latest_count[wiki] = int(cnt)
                delta = _float(data.get(b"m:avg_delta"))
                if delta is not None:
                    latest_delta[wiki] = delta
    rows = [
        {"wiki": w, "count": latest_count[w], "avg_delta": latest_delta.get(w, 0)}
        for w in latest_count
    ]
    return pd.DataFrame(rows).sort_values("count", ascending=False)


def fetch_wiki_history(wiki, max_points=12):
    rows = []
    prefix = f"{wiki}|".encode("utf-8")
    with get_pool().connection() as conn:
        tbl = conn.table("wikipedia_agg")
        for key, data in tbl.scan(row_prefix=prefix, limit=max_points):
            end = _float(data.get(b"m:window_end"))
            cnt = _float(data.get(b"m:count"))
            if end is None:
                continue
            rows.append({
                "window_end": datetime.fromtimestamp(end),
                "count":      cnt or 0,
            })
    rows.sort(key=lambda r: r["window_end"])
    return pd.DataFrame(rows)


def fetch_enriched_breakdown():
    by_continent = defaultdict(int)
    by_language = defaultdict(int)
    rows = []
    with get_pool().connection() as conn:
        try:
            tbl = conn.table("wikipedia_enriched")
            for key, data in tbl.scan(limit=2000):
                cont = _decode(data.get(b"ref:continent")) or "(unknown)"
                lang = _decode(data.get(b"ref:language")) or "(unknown)"
                by_continent[cont] += 1
                by_language[lang] += 1
                rows.append({
                    "wiki":             _decode(key),
                    "language":         lang,
                    "continent":        cont,
                    "region":           _decode(data.get(b"ref:region")) or "(unknown)",
                    "primary_country":  _decode(data.get(b"ref:primary_country")) or "(unknown)",
                    "type":             _decode(data.get(b"info:type")),
                    "title":            _decode(data.get(b"info:title")),
                    "user":             _decode(data.get(b"info:user")),
                    "bot":              _decode(data.get(b"info:bot")) == "true",
                    "length_delta":     _int(data.get(b"stats:length_delta")),
                })
        except Exception:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    cont_df = pd.DataFrame([{"continent": k, "count": v} for k, v in by_continent.items()])
    lang_df = pd.DataFrame([{"language": k, "count": v} for k, v in by_language.items()])
    if cont_df.empty:
        return cont_df, lang_df, pd.DataFrame()
    return (cont_df.sort_values("count", ascending=False),
            lang_df.sort_values("count", ascending=False),
            pd.DataFrame(rows))


def fetch_edit_type_breakdown():
    counts = defaultdict(int)
    with get_pool().connection() as conn:
        tbl = conn.table("wikipedia_live")
        for _, data in tbl.scan(limit=2000, columns=[b"info:type"]):
            t = _decode(data.get(b"info:type")) or "(unknown)"
            counts[t] += 1
    if not counts:
        return pd.DataFrame()
    return pd.DataFrame([
        {"edit_type": k, "Edit type": pretty_edit_type(k), "count": v}
        for k, v in counts.items()
    ]).sort_values("count", ascending=False)


def fetch_namespace_breakdown():
    counts = defaultdict(int)
    with get_pool().connection() as conn:
        tbl = conn.table("wikipedia_live")
        for _, data in tbl.scan(limit=2000, columns=[b"info:namespace"]):
            n = _int(data.get(b"info:namespace"))
            counts[n] += 1
    if not counts:
        return pd.DataFrame()
    return pd.DataFrame([
        {"namespace": k, "Namespace": pretty_namespace(k), "count": v}
        for k, v in counts.items()
    ]).sort_values("count", ascending=False)


def fetch_top_editor_per_language():
    """For each language, find the most-recently-edited title and its user."""
    seen = set()
    rows = []
    with get_pool().connection() as conn:
        try:
            tbl = conn.table("wikipedia_enriched")
            for key, data in tbl.scan(limit=2000):
                lang = _decode(data.get(b"ref:language")) or "(unknown)"
                if lang.startswith("(") or lang in seen:
                    continue
                seen.add(lang)
                rows.append({
                    "language":     lang,
                    "wiki":         _decode(key),
                    "title":        _decode(data.get(b"info:title")),
                    "user":         _decode(data.get(b"info:user")),
                    "type":         _decode(data.get(b"info:type")),
                    "bot":          _decode(data.get(b"info:bot")) == "true",
                    "length_delta": _int(data.get(b"stats:length_delta"), 0),
                })
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Fetch the live-aggregation data first -- the sidebar trend-wiki dropdown
# uses this list, so we need it before we render the sidebar controls.
# ---------------------------------------------------------------------------
counts_df = fetch_latest_counts_per_wiki()
live = fetch_live()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
sb = st.sidebar
sb.header("Controls")
auto_refresh = sb.toggle("Auto-refresh", value=True)
top_n = sb.slider("Top-N wikis", min_value=5, max_value=30, value=15, step=5)

# Trend-wiki dropdown: labels are human-readable names; the wiki code is
# resolved internally via a reverse lookup. Wikis are sorted by edit count
# descending so the most active ones surface first. Default = English
# Wikipedia (if active), otherwise the busiest wiki right now.
if not counts_df.empty:
    pretty_to_code = {pretty_wiki(w): w for w in counts_df["wiki"].tolist()}
    label_options = list(pretty_to_code.keys())
    default_idx = label_options.index("English Wikipedia") if "English Wikipedia" in label_options else 0
    selected_label = sb.selectbox(
        "Trend wiki",
        options=label_options,
        index=default_idx,
        help="Pick a wiki to plot its edits-per-minute trend below.",
    )
    chosen_wiki = pretty_to_code[selected_label]
else:
    chosen_wiki = "enwiki"
    sb.markdown("_Waiting for data..._")

last_updated = sb.empty()

# ---------------------------------------------------------------------------
# Stop early if we have no data yet (sidebar already rendered above)
# ---------------------------------------------------------------------------
if counts_df.empty:
    st.info("No data in wikipedia_agg yet -- waiting for Spark to write the first batch.")
    st.stop()

total_edits_per_min = int(counts_df["count"].sum())
active_wikis = len(counts_df)
top_wiki_row = counts_df.iloc[0]
bot_count = int(live["bot"].sum()) if not live.empty else 0
human_count = int((~live["bot"]).sum()) if not live.empty else 0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Edits in the last minute", f"{total_edits_per_min:,}",
          "across every Wikimedia wiki")
m2.metric("Active wikis right now", f"{active_wikis:,}",
          "distinct wikis with edits in last window")
m3.metric("Most active wiki", pretty_wiki(top_wiki_row["wiki"]),
          f"{int(top_wiki_row['count']):,} edits this minute")
m4.metric("Bots vs humans tracked",
          f"{bot_count} / {human_count}",
          f"bot share: {(100*bot_count/(bot_count+human_count)) if (bot_count+human_count) else 0:.1f}%")

# ---------------------------------------------------------------------------
# Charts: top wikis by edit count + average length delta
# ---------------------------------------------------------------------------
chart_col1, chart_col2 = st.columns(2)
with chart_col1:
    st.subheader("Top wikis by edits in the last minute")
    top_df = counts_df.head(top_n).copy()
    top_df["Wiki"] = top_df["wiki"].map(pretty_wiki)
    st.bar_chart(top_df.set_index("Wiki")["count"], height=320)
with chart_col2:
    st.subheader("Average length delta in the last minute")
    delta_df = counts_df.copy()
    delta_df = delta_df.reindex(delta_df["avg_delta"].abs().sort_values(ascending=False).index).head(top_n)
    delta_df["Wiki"] = delta_df["wiki"].map(pretty_wiki)
    st.bar_chart(delta_df.set_index("Wiki")["avg_delta"], height=320)
    st.caption("Positive values mean wikis are growing on average; negative means net deletions.")

# ---------------------------------------------------------------------------
# Edit type + namespace breakdowns (live, derived from wikipedia_live)
# ---------------------------------------------------------------------------
brkd1, brkd2 = st.columns(2)
with brkd1:
    st.subheader("Edit types observed")
    et_df = fetch_edit_type_breakdown()
    if et_df.empty:
        st.info("No edit-type data yet.")
    else:
        st.bar_chart(et_df.set_index("Edit type")["count"], height=320)
        st.caption(
            "`edit` = revision to an existing page, `new` = new page creation, "
            "`log` = move / delete / protect / user action, `categorize` = "
            "automatic category-membership change, `external` = remote data update."
        )
with brkd2:
    st.subheader("Wikipedia namespaces observed")
    ns_df = fetch_namespace_breakdown()
    if ns_df.empty:
        st.info("No namespace data yet.")
    else:
        st.bar_chart(ns_df.head(top_n).set_index("Namespace")["count"], height=320)
        st.caption(
            "Wikipedia separates pages by namespace: 0 is the main article "
            "space; 1 is talk pages; 6 is media files; 14 is categories; 100 "
            "is portals; and so on."
        )

# ---------------------------------------------------------------------------
# Trend chart for chosen wiki
# ---------------------------------------------------------------------------
st.subheader(f"Edits-per-minute trend -- {pretty_wiki(chosen_wiki) or chosen_wiki}")
hist = fetch_wiki_history(chosen_wiki)
if hist.empty:
    st.info(f"No windowed history for '{chosen_wiki}' yet.")
else:
    base = alt.Chart(hist).encode(
        x=alt.X("window_end:T", title="Window end (local time)"),
        y=alt.Y("count:Q", title="Edits in the 1-minute window",
                scale=alt.Scale(zero=False, nice=True)),
        tooltip=["window_end:T", "count:Q"],
    )
    chart = (
        base.mark_line(strokeWidth=2, color="#3b82f6")
        + base.mark_circle(size=90, color="#3b82f6", opacity=0.9)
    ).properties(height=300)
    st.altair_chart(chart, width="stretch")

# ---------------------------------------------------------------------------
# Bonus: enrichment breakdowns + top editor per language
# ---------------------------------------------------------------------------
cont_df, lang_df, enr_rows = fetch_enriched_breakdown()
if not cont_df.empty:
    st.subheader("Continent / language breakdown   (BONUS -- from Spark SQL static join)")

    a, b, c = st.columns([1.2, 1.2, 2])
    known_cont = cont_df[~cont_df["continent"].str.startswith("(") & (cont_df["continent"] != "(unknown)")]
    known_lang = lang_df[~lang_df["language"].str.startswith("(") & (lang_df["language"] != "(unknown)")]
    unknown_count = int(cont_df.loc[cont_df["continent"] == "(unknown)", "count"].sum()) if "(unknown)" in cont_df["continent"].values else 0

    with a:
        st.caption(f"By continent  (excluding {unknown_count} unmapped rows)")
        if not known_cont.empty:
            st.bar_chart(known_cont.set_index("continent")["count"])
    with b:
        st.caption("By language  (top 15)")
        if not known_lang.empty:
            st.bar_chart(known_lang.head(15).set_index("language")["count"])
    with c:
        st.caption(f"Sample enriched rows (showing {min(len(enr_rows), 50)} of {len(enr_rows)})")
        sample = enr_rows.head(50).copy()
        sample["Wiki"]      = sample["wiki"].map(pretty_wiki)
        sample["Edit type"] = sample["type"].map(pretty_edit_type)
        st.dataframe(
            sample[["Wiki", "language", "continent", "region",
                    "primary_country", "Edit type", "title", "user", "bot"]]
                .rename(columns={
                    "language":        "Language",
                    "continent":       "Continent",
                    "region":          "Region",
                    "primary_country": "Primary country",
                    "title":           "Page title",
                    "user":            "Editor",
                    "bot":             "Bot?",
                }),
            hide_index=True, width="stretch", height=350,
        )

    # ---------- Top editor per language (live metric) ----------
    movers = fetch_top_editor_per_language()
    if not movers.empty:
        st.subheader("Latest edit per language   (BONUS -- live metric from enriched join)")
        st.caption(
            f"For each language, the most recently observed page title and editor. "
            f"Values refresh every {REFRESH_SEC} seconds when the enrich job is running."
        )
        movers_pretty = movers.head(top_n).copy()
        movers_pretty["Wiki"]      = movers_pretty["wiki"].map(pretty_wiki)
        movers_pretty["Edit type"] = movers_pretty["type"].map(pretty_edit_type)
        st.dataframe(
            movers_pretty[["language", "Wiki", "title", "user", "Edit type", "bot", "length_delta"]]
                .rename(columns={
                    "language":     "Language",
                    "title":        "Page title",
                    "user":         "Editor",
                    "bot":          "Bot?",
                    "length_delta": "Length delta (bytes)",
                }),
            hide_index=True, width="stretch",
        )

# ---------- Sidebar last-refresh stamp ----------
last_updated.markdown(
    f'<div style="color:#94a3b8; font-size:12px;">Last refresh: '
    f'<strong style="color:white;">{datetime.now().strftime("%H:%M:%S")}</strong></div>',
    unsafe_allow_html=True,
)

# ---------- Auto-refresh ----------
if auto_refresh:
    import time as _t
    _t.sleep(REFRESH_SEC)
    st.rerun()
