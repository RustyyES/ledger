"""Render the Ledger book chapters into one self-contained HTML page."""
from __future__ import annotations
import html as _html
import pathlib
import re

import markdown

#: Both resolved relative to THIS FILE, so the script works from any working
#: directory and survives the repository being moved or cloned elsewhere.
BOOK = pathlib.Path(__file__).resolve().parent
OUT = BOOK / "ledger-book.html"

PARTS = [
    ("Front matter", [("00-preface.md", "Preface", "How to read this")]),
    ("Part I · Foundations", [
        ("01-what-we-are-building.md", "1", "What we're actually building"),
        ("02-what-a-pipeline-is.md", "2", "What a pipeline actually is"),
        ("03-why-build-the-app.md", "3", "Why we built the app first"),
    ]),
    ("Part II · The source system", [
        ("04-the-schema.md", "4", "A schema that's realistically awkward"),
        ("05-idempotency.md", "5", "Idempotency, or how a retry doubles your revenue"),
        ("06-the-load-generator.md", "6", "Generating data that has a shape"),
    ]),
    ("Part III · Getting data out", [
        ("07-cdc.md", "7", "Change data capture from first principles"),
        ("08-the-sink.md", "8", "The sink, and what “exactly once” really means"),
        ("09-schema-guard.md", "9", "Surviving a schema change"),
    ]),
    ("Part IV · Modelling", [
        ("10-staging.md", "10", "Staging, and why the layer contract matters"),
        ("11-star-schemas.md", "11", "Star schemas from scratch"),
        ("12-scd2.md", "12", "Slowly changing dimensions"),
        ("13-late-arriving-facts.md", "13", "The late-arriving fact"),
        ("14-three-metrics.md", "14", "Three metrics that need real SQL"),
        ("15-testing-data.md", "15", "Testing data (which is not testing code)"),
    ]),
    ("Part V · Running it", [
        ("16-orchestration.md", "16", "Orchestration"),
        ("17-serving.md", "17", "Serving"),
    ]),
    ("Part VI · Judgement", [
        ("18-ten-bugs.md", "18", "Ten bugs"),
        ("19-jenga.md", "19", "Jenga: what breaks if you pull this out"),
        ("20-tool-choices.md", "20", "Why this tool and not that one"),
        ("21-at-100x.md", "21", "At 100x"),
    ]),
]
STARRED = {"13-late-arriving-facts.md", "18-ten-bugs.md", "19-jenga.md"}


def slug(name: str) -> str:
    return "ch-" + name.replace(".md", "")


def convert(path: pathlib.Path) -> str:
    text = path.read_text()
    # Drop the H1 (the shell renders its own chapter header) and the trailing
    # "Next:" nav line (the shell provides prev/next controls instead).
    text = re.sub(r"^#\s+.*?\n", "", text, count=1)
    text = re.sub(r"\n---\n\s*\n(Next|\*\*\[← Back).*$", "", text, flags=re.S)
    text = re.sub(r"\nNext: \*\*\[.*$", "", text, flags=re.S)

    md = markdown.Markdown(extensions=["extra", "sane_lists", "admonition"])
    out = md.convert(text)

    return _rewrite_links(out)


def _rewrite_links(html_fragment: str) -> str:
    """Turn the markdown files' relative links into something a single page can use.

    Three cases, because the book has three kinds of link:

      ../../services/...  a path into the repository. There is no repository
                          next to the published page, so it becomes inline code
                          rather than a link that would 404.
      12-scd2.md          another chapter. Becomes an in-page anchor (#ch-12-scd2).
      README.md           the table of contents, which this page replaces with
                          its own sidebar. The link is dropped, the text kept.
    """

    def replace(match: re.Match) -> str:
        target, label = match.group(1), match.group(2)
        base = target.split("#")[0]

        if base.startswith("../../"):
            return f'<code class="path">{_html.escape(base[6:])}</code>'
        if base.endswith(".md") and base != "README.md":
            return f'<a href="#{slug(base)}">{label}</a>'
        return label

    # Group 1 is the href, group 2 is the link text -- that is the order they
    # appear in the tag, and getting it backwards is how this function briefly
    # grew a fake match object to swap them.
    return re.sub(r'<a href="([^"]+)">([^<]*)</a>', replace, html_fragment)


def main() -> None:
    chapters = []
    for part_name, entries in PARTS:
        for fname, num, title in entries:
            body = convert(BOOK / fname)
            chapters.append({
                "id": slug(fname), "part": part_name, "num": num,
                "title": title, "body": body, "starred": fname in STARRED,
            })

    # ---- sidebar ---------------------------------------------------------
    nav, seen = [], None
    for c in chapters:
        if c["part"] != seen:
            seen = c["part"]
            nav.append(f'<li class="nav-part">{_html.escape(seen)}</li>')
        star = '<span class="star" title="essential">◆</span>' if c["starred"] else ""
        label = "Pref." if c["num"] == "Preface" else c["num"]
        nav.append(
            f'<li><a class="nav-link" href="#{c["id"]}" data-target="{c["id"]}">'
            f'<span class="nav-num">{_html.escape(label)}</span>'
            f'<span class="nav-title">{_html.escape(c["title"])}{star}</span></a></li>'
        )

    # ---- chapter sections ------------------------------------------------
    secs = []
    for i, c in enumerate(chapters):
        prev_c = chapters[i - 1] if i else None
        next_c = chapters[i + 1] if i + 1 < len(chapters) else None
        pager = ['<nav class="pager">']
        if prev_c:
            pager.append(
                f'<a class="pager-link prev" href="#{prev_c["id"]}">'
                f'<span class="pager-dir">Previous</span>'
                f'<span class="pager-name">{_html.escape(prev_c["title"])}</span></a>')
        else:
            pager.append("<span></span>")
        if next_c:
            pager.append(
                f'<a class="pager-link next" href="#{next_c["id"]}">'
                f'<span class="pager-dir">Next</span>'
                f'<span class="pager-name">{_html.escape(next_c["title"])}</span></a>')
        else:
            pager.append("<span></span>")
        pager.append("</nav>")

        eyebrow = c["part"]
        num_label = "" if c["num"] == "Preface" else f'<span class="ch-num">{c["num"]}</span>'
        secs.append(f'''
<section class="chapter" id="{c["id"]}">
  <header class="ch-head">
    <p class="ch-part">{_html.escape(eyebrow)}</p>
    <h2 class="ch-title">{num_label}<span>{_html.escape(c["title"])}</span></h2>
  </header>
  <div class="prose">{c["body"]}</div>
  {"".join(pager)}
</section>''')

    html = TEMPLATE.replace("{{NAV}}", "\n".join(nav)).replace("{{CHAPTERS}}", "\n".join(secs))
    OUT.write_text(html)
    print(f"wrote {OUT}  ({len(html):,} bytes, {len(chapters)} chapters)")


TEMPLATE = r"""<title>The Ledger Book</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Spectral:ital,wght@0,300;0,400;0,600;0,700;1,400;1,600&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap">

<style>
/* ==========================================================================
   The Ledger Book
   --------------------------------------------------------------------------
   Palette drawn from double-entry bookkeeping: iron-gall ink oxidises to a
   blue-black, ledger paper was tinted cool green-grey to ease the eye, and
   corrections were entered in red. The book is about numbers that silently
   fail to balance, so red is reserved exclusively for silent-failure callouts
   -- never used decoratively.
   ========================================================================== */

:root {
  /* --- light: ledger paper ------------------------------------------- */
  --ink:          #16202b;   /* iron-gall blue-black                     */
  --ink-soft:     #46545f;
  --ink-faint:    #78858e;
  --paper:        #f7f7f3;   /* cool paper, not cream                    */
  --paper-raised: #ffffff;
  --paper-sunk:   #eeefe9;
  --rule:         #d8dbd2;   /* ledger rule                              */
  --rule-strong:  #b9bfb2;
  --accent:       #1d5c52;   /* ledger green -- structure, links         */
  --accent-soft:  #e3ece8;
  --correction:   #96271e;   /* red ink: corrections only                */
  --correction-bg:#f7e9e6;
  --code-bg:      #eef0ea;

  --f-body: "Spectral", "Iowan Old Style", Georgia, serif;
  --f-ui:   "IBM Plex Sans", ui-sans-serif, system-ui, sans-serif;
  --f-mono: "IBM Plex Mono", ui-monospace, "SF Mono", Menlo, monospace;

  --measure: 68ch;
  --rail: 310px;
}

@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --ink:          #dfe3e0;
    --ink-soft:     #a3aca8;
    --ink-faint:    #78827e;
    --paper:        #12181c;
    --paper-raised: #182026;
    --paper-sunk:   #0d1216;
    --rule:         #28323a;
    --rule-strong:  #3c4852;
    --accent:       #6fbfa8;
    --accent-soft:  #17302c;
    --correction:   #e07a6c;
    --correction-bg:#2b1a18;
    --code-bg:      #0e1519;
  }
}

:root[data-theme="dark"] {
  --ink:          #dfe3e0;
  --ink-soft:     #a3aca8;
  --ink-faint:    #78827e;
  --paper:        #12181c;
  --paper-raised: #182026;
  --paper-sunk:   #0d1216;
  --rule:         #28323a;
  --rule-strong:  #3c4852;
  --accent:       #6fbfa8;
  --accent-soft:  #17302c;
  --correction:   #e07a6c;
  --correction-bg:#2b1a18;
  --code-bg:      #0e1519;
}

* { box-sizing: border-box; }

html { scroll-behavior: smooth; scroll-padding-top: 2rem; }
@media (prefers-reduced-motion: reduce) {
  html { scroll-behavior: auto; }
  * { animation: none !important; transition: none !important; }
}

body {
  margin: 0;
  background: var(--paper);
  color: var(--ink);
  font-family: var(--f-body);
  font-size: 1.0625rem;
  line-height: 1.72;
  -webkit-font-smoothing: antialiased;
}

/* ---------- shell ---------------------------------------------------- */

.shell { display: grid; grid-template-columns: var(--rail) minmax(0, 1fr); }

/* ---------- rail ------------------------------------------------------ */

.rail {
  position: sticky; top: 0; height: 100vh;
  display: flex; flex-direction: column;
  background: var(--paper-sunk);
  border-right: 1px solid var(--rule);
  overflow: hidden;
}

.rail-head {
  padding: 1.6rem 1.5rem 1.1rem;
  border-bottom: 1px solid var(--rule);
}

.brand {
  font-family: var(--f-body);
  font-weight: 700; font-size: 1.28rem; letter-spacing: -0.015em;
  margin: 0; line-height: 1.15;
}
.brand em { font-style: italic; color: var(--accent); }

.brand-sub {
  font-family: var(--f-ui);
  font-size: 0.7rem; letter-spacing: 0.1em; text-transform: uppercase;
  color: var(--ink-faint); margin: 0.5rem 0 0;
}

.rail-scroll { flex: 1; overflow-y: auto; padding: 0.9rem 0 2.5rem; }

.nav-list { list-style: none; margin: 0; padding: 0; }

.nav-part {
  font-family: var(--f-ui);
  font-size: 0.66rem; font-weight: 600;
  letter-spacing: 0.13em; text-transform: uppercase;
  color: var(--ink-faint);
  padding: 1.5rem 1.5rem 0.45rem;
}
.nav-part:first-child { padding-top: 0.4rem; }

.nav-link {
  display: grid; grid-template-columns: 2.1rem 1fr; gap: 0.15rem;
  align-items: baseline;
  padding: 0.34rem 1.5rem 0.34rem 1.2rem;
  text-decoration: none; color: var(--ink-soft);
  border-left: 2px solid transparent;
  transition: color 120ms, background 120ms, border-color 120ms;
}
.nav-link:hover { color: var(--ink); background: var(--accent-soft); }
.nav-link:focus-visible { outline: 2px solid var(--accent); outline-offset: -2px; }
.nav-link.active {
  color: var(--ink); border-left-color: var(--accent);
  background: var(--accent-soft);
}
.nav-link.active .nav-num { color: var(--accent); }

.nav-num {
  font-family: var(--f-mono); font-size: 0.72rem;
  color: var(--ink-faint); font-variant-numeric: tabular-nums;
}
.nav-title { font-size: 0.845rem; line-height: 1.38; }

.star { color: var(--correction); font-size: 0.62em; vertical-align: 0.25em; margin-left: 0.35em; }

.rail-foot {
  border-top: 1px solid var(--rule);
  padding: 0.7rem 1.5rem;
  display: flex; align-items: center; justify-content: space-between;
  font-family: var(--f-ui); font-size: 0.72rem; color: var(--ink-faint);
}

.theme-btn {
  font-family: var(--f-ui); font-size: 0.72rem;
  background: none; border: 1px solid var(--rule-strong); color: var(--ink-soft);
  border-radius: 3px; padding: 0.22rem 0.55rem; cursor: pointer;
}
.theme-btn:hover { color: var(--ink); border-color: var(--accent); }
.theme-btn:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

/* ---------- reading column ------------------------------------------- */

.reader { min-width: 0; }

.masthead {
  padding: 5.5rem 3rem 3.2rem;
  border-bottom: 1px solid var(--rule);
  background:
    repeating-linear-gradient(
      to bottom,
      transparent, transparent 1.68rem,
      var(--rule) 1.68rem, var(--rule) calc(1.68rem + 1px)
    );
  background-clip: padding-box;
}
.masthead-inner { max-width: var(--measure); margin: 0 auto; background: var(--paper); }

.masthead h1 {
  font-size: clamp(2.4rem, 5vw, 3.6rem);
  line-height: 1.04; letter-spacing: -0.028em; font-weight: 700;
  margin: 0 0 1.1rem; text-wrap: balance;
}
.masthead h1 em { font-style: italic; color: var(--accent); }

.masthead .standfirst {
  font-size: 1.16rem; color: var(--ink-soft); margin: 0 0 2rem;
  max-width: 56ch;
}

.ledger-stat {
  display: grid; grid-template-columns: 1fr auto; gap: 0.1rem 1.5rem;
  max-width: 34rem;
  font-family: var(--f-ui); font-size: 0.9rem;
  border-top: 2px solid var(--ink);
  padding-top: 0.7rem;
}
.ledger-stat dt { padding: 0.34rem 0; border-bottom: 1px solid var(--rule); }
.ledger-stat dd {
  margin: 0; padding: 0.34rem 0; text-align: right;
  font-family: var(--f-mono); font-variant-numeric: tabular-nums;
  border-bottom: 1px solid var(--rule);
}
.ledger-stat dd.red { color: var(--correction); font-weight: 500; }

/* ---------- chapters -------------------------------------------------- */

.chapter { padding: 4.2rem 3rem 1rem; border-bottom: 1px solid var(--rule); }
.chapter:last-of-type { border-bottom: 0; }

.ch-head { max-width: var(--measure); margin: 0 auto 2.4rem; }

.ch-part {
  font-family: var(--f-ui); font-size: 0.68rem; font-weight: 600;
  letter-spacing: 0.13em; text-transform: uppercase;
  color: var(--accent); margin: 0 0 0.75rem;
}

.ch-title {
  font-size: clamp(1.75rem, 3.2vw, 2.4rem);
  line-height: 1.14; letter-spacing: -0.022em; font-weight: 700;
  margin: 0; text-wrap: balance;
  display: flex; gap: 0.85rem; align-items: baseline;
}
.ch-num {
  font-family: var(--f-mono); font-size: 0.55em; font-weight: 400;
  color: var(--ink-faint); font-variant-numeric: tabular-nums;
  flex: none; padding-top: 0.15em;
}

/* ---------- prose ----------------------------------------------------- */

.prose { max-width: var(--measure); margin: 0 auto; }
.prose > * { margin-inline: auto; }

.prose h1 { font-size: 1.75rem; }
.prose h1, .prose h2 {
  font-size: 1.62rem; line-height: 1.22; letter-spacing: -0.018em; font-weight: 700;
  margin: 3.2rem 0 1rem; text-wrap: balance;
  padding-bottom: 0.45rem; border-bottom: 1px solid var(--rule);
}
.prose h3 {
  font-size: 1.24rem; line-height: 1.3; font-weight: 600;
  margin: 2.4rem 0 0.7rem; text-wrap: balance;
}
.prose h4 {
  font-family: var(--f-ui); font-size: 0.95rem; font-weight: 600;
  margin: 1.9rem 0 0.5rem;
}

.prose p { margin: 0 0 1.15rem; }
.prose strong { font-weight: 600; }
.prose em { font-style: italic; }

.prose a {
  color: var(--accent); text-decoration: none;
  border-bottom: 1px solid color-mix(in srgb, var(--accent) 35%, transparent);
}
.prose a:hover { border-bottom-color: var(--accent); }
.prose a:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

.prose ul, .prose ol { margin: 0 0 1.15rem; padding-left: 1.4rem; }
.prose li { margin-bottom: 0.42rem; }
.prose li > ul, .prose li > ol { margin-top: 0.42rem; margin-bottom: 0.3rem; }

.prose hr {
  border: 0; height: 1px; background: var(--rule);
  margin: 2.8rem 0;
}

/* code */
.prose code {
  font-family: var(--f-mono); font-size: 0.855em;
  background: var(--code-bg); padding: 0.1em 0.34em; border-radius: 2px;
  border: 1px solid color-mix(in srgb, var(--rule) 60%, transparent);
}
.prose code.path { color: var(--ink-soft); font-size: 0.82em; }

.prose pre {
  background: var(--code-bg);
  border: 1px solid var(--rule);
  border-left: 3px solid var(--rule-strong);
  border-radius: 3px;
  padding: 1rem 1.15rem;
  overflow-x: auto;
  margin: 0 0 1.35rem;
  line-height: 1.6;
}
.prose pre code {
  background: none; border: 0; padding: 0; font-size: 0.845rem;
}

/* blockquote == the "silent failure" callout. Red is the correction entry
   in a ledger; it appears nowhere else in this design. */
.prose blockquote {
  margin: 1.6rem 0;
  padding: 0.9rem 1.3rem;
  border-left: 3px solid var(--correction);
  background: var(--correction-bg);
  border-radius: 0 3px 3px 0;
}
.prose blockquote p { margin-bottom: 0.7rem; }
.prose blockquote p:last-child { margin-bottom: 0; }
.prose blockquote strong { color: var(--correction); }
.prose blockquote code { background: color-mix(in srgb, var(--correction) 9%, transparent); }

/* tables */
.table-wrap { overflow-x: auto; margin: 0 0 1.5rem; }
.prose table {
  width: 100%; border-collapse: collapse;
  font-family: var(--f-ui); font-size: 0.875rem;
}
.prose thead th {
  text-align: left; font-weight: 600;
  border-bottom: 2px solid var(--ink);
  padding: 0.5rem 0.85rem 0.5rem 0;
  white-space: nowrap;
}
.prose tbody td {
  border-bottom: 1px solid var(--rule);
  padding: 0.55rem 0.85rem 0.55rem 0;
  vertical-align: top;
}
.prose tbody tr:last-child td { border-bottom: 1px solid var(--rule-strong); }
.prose td code, .prose th code { font-size: 0.86em; }

/* ---------- pager ------------------------------------------------------ */

.pager {
  max-width: var(--measure); margin: 3.4rem auto 1.5rem;
  display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;
  padding-top: 1.5rem; border-top: 1px solid var(--rule);
}
.pager-link {
  display: flex; flex-direction: column; gap: 0.2rem;
  text-decoration: none; color: var(--ink-soft);
  padding: 0.7rem 0.9rem; border: 1px solid var(--rule); border-radius: 3px;
  transition: border-color 120ms, color 120ms;
}
.pager-link:hover { border-color: var(--accent); color: var(--ink); }
.pager-link:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.pager-link.next { text-align: right; }
.pager-dir {
  font-family: var(--f-ui); font-size: 0.66rem; font-weight: 600;
  letter-spacing: 0.11em; text-transform: uppercase; color: var(--ink-faint);
}
.pager-name { font-size: 0.92rem; line-height: 1.35; }

/* ---------- progress --------------------------------------------------- */

.progress {
  position: fixed; top: 0; left: var(--rail); right: 0; height: 2px;
  background: transparent; z-index: 50; pointer-events: none;
}
.progress span {
  display: block; height: 100%; width: 0%;
  background: var(--accent);
}

/* ---------- mobile ----------------------------------------------------- */

.rail-toggle { display: none; }

@media (max-width: 940px) {
  .shell { grid-template-columns: 1fr; }
  .progress { left: 0; }

  .rail {
    position: fixed; inset: 0 auto 0 0; width: min(84vw, 320px);
    z-index: 100; transform: translateX(-100%);
    transition: transform 200ms ease;
    box-shadow: 0 0 0 100vmax color-mix(in srgb, var(--ink) 0%, transparent);
  }
  .rail.open { transform: translateX(0); }

  .rail-toggle {
    display: flex; align-items: center; gap: 0.45rem;
    position: fixed; top: 0.85rem; left: 0.85rem; z-index: 110;
    font-family: var(--f-ui); font-size: 0.78rem; font-weight: 500;
    background: var(--paper-raised); color: var(--ink);
    border: 1px solid var(--rule-strong); border-radius: 3px;
    padding: 0.45rem 0.7rem; cursor: pointer;
    box-shadow: 0 1px 3px color-mix(in srgb, var(--ink) 12%, transparent);
  }
  .rail-toggle:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

  .masthead { padding: 4.5rem 1.35rem 2.4rem; }
  .chapter  { padding: 3rem 1.35rem 1rem; }
  .pager    { grid-template-columns: 1fr; }
  body { font-size: 1.02rem; }
}

@media (max-width: 560px) {
  .ch-title { flex-direction: column; gap: 0.3rem; }
}
</style>

<button class="rail-toggle" id="railToggle" aria-expanded="false" aria-controls="rail">
  <span aria-hidden="true">☰</span> Contents
</button>

<div class="progress" aria-hidden="true"><span id="progressBar"></span></div>

<div class="shell">

  <aside class="rail" id="rail">
    <div class="rail-head">
      <p class="brand">The <em>Ledger</em> Book</p>
      <p class="brand-sub">21 chapters · ~2.5 hours</p>
    </div>
    <div class="rail-scroll">
      <ul class="nav-list">
        {{NAV}}
      </ul>
    </div>
    <div class="rail-foot">
      <span><span class="star">◆</span> essential</span>
      <button class="theme-btn" id="themeBtn" type="button">Theme</button>
    </div>
  </aside>

  <main class="reader">

    <header class="masthead">
      <div class="masthead-inner">
        <h1>Ten bugs.<br><em>Zero</em> exceptions.</h1>
        <p class="standfirst">
          A complete explanation of one data pipeline — every decision, every
          rejected alternative, and every failure that produced a wrong number
          instead of an error message.
        </p>
        <dl class="ledger-stat">
          <dt>Chapters</dt><dd>21</dd>
          <dt>Assumed knowledge</dt><dd>Python, SQL</dd>
          <dt>Lines of pipeline explained</dt><dd>16,621</dd>
          <dt>Bugs that raised an exception</dt><dd class="red">0</dd>
          <dt>Bugs that returned a wrong number</dt><dd class="red">10</dd>
        </dl>
      </div>
    </header>

    {{CHAPTERS}}

  </main>
</div>

<script>
(function () {
  "use strict";

  /* --- theme: cycles system -> light -> dark ------------------------- */
  var root = document.documentElement;
  var btn  = document.getElementById("themeBtn");
  var order = ["", "light", "dark"];
  var label = { "": "Theme", "light": "Light", "dark": "Dark" };

  function readStored() {
    try { return localStorage.getItem("ledger-book-theme") || ""; }
    catch (e) { return ""; }
  }
  function apply(v) {
    if (v) { root.setAttribute("data-theme", v); }
    else   { root.removeAttribute("data-theme"); }
    btn.textContent = label[v];
  }
  apply(readStored());

  btn.addEventListener("click", function () {
    var cur = root.getAttribute("data-theme") || "";
    var next = order[(order.indexOf(cur) + 1) % order.length];
    apply(next);
    try { localStorage.setItem("ledger-book-theme", next); } catch (e) {}
  });

  /* --- mobile rail ---------------------------------------------------- */
  var rail = document.getElementById("rail");
  var toggle = document.getElementById("railToggle");
  toggle.addEventListener("click", function () {
    var open = rail.classList.toggle("open");
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
  });
  rail.addEventListener("click", function (e) {
    if (e.target.closest(".nav-link") && window.innerWidth <= 940) {
      rail.classList.remove("open");
      toggle.setAttribute("aria-expanded", "false");
    }
  });

  /* --- active chapter + progress -------------------------------------- */
  var links = Array.prototype.slice.call(document.querySelectorAll(".nav-link"));
  var byId = {};
  links.forEach(function (a) { byId[a.dataset.target] = a; });
  var sections = Array.prototype.slice.call(document.querySelectorAll(".chapter"));
  var bar = document.getElementById("progressBar");
  var current = null;

  function setActive(id) {
    if (id === current) return;
    if (current && byId[current]) byId[current].classList.remove("active");
    current = id;
    if (byId[id]) {
      byId[id].classList.add("active");
      var a = byId[id];
      var scroller = a.closest(".rail-scroll");
      var top = a.offsetTop, h = a.offsetHeight;
      if (top < scroller.scrollTop || top + h > scroller.scrollTop + scroller.clientHeight) {
        scroller.scrollTop = top - scroller.clientHeight / 2 + h / 2;
      }
    }
  }

  if ("IntersectionObserver" in window) {
    var visible = new Set();
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (en) {
        if (en.isIntersecting) visible.add(en.target.id);
        else visible.delete(en.target.id);
      });
      for (var i = 0; i < sections.length; i++) {
        if (visible.has(sections[i].id)) { setActive(sections[i].id); break; }
      }
    }, { rootMargin: "-12% 0px -70% 0px" });
    sections.forEach(function (s) { io.observe(s); });
  }

  function onScroll() {
    var h = document.documentElement;
    var max = h.scrollHeight - h.clientHeight;
    bar.style.width = (max > 0 ? (h.scrollTop / max) * 100 : 0) + "%";
  }
  window.addEventListener("scroll", onScroll, { passive: true });
  onScroll();

  /* --- wrap tables so wide ones scroll inside themselves --------------- */
  Array.prototype.slice.call(document.querySelectorAll(".prose table")).forEach(function (t) {
    if (t.parentNode.classList.contains("table-wrap")) return;
    var w = document.createElement("div");
    w.className = "table-wrap";
    t.parentNode.insertBefore(w, t);
    w.appendChild(t);
  });
})();
</script>
"""

if __name__ == "__main__":
    main()
