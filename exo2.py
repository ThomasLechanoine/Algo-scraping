import os
import hashlib
import requests
from bs4 import BeautifulSoup
import networkx as nx
import json as _json

BASE_URL = "https://quotes.toscrape.com"
CACHE_DIR = "cache_html"

# Fonction utilitaire pour récupérer le contenu d'une URL
def fetch_with_cache(url):
    os.makedirs(CACHE_DIR, exist_ok=True)
    key = hashlib.sha1(url.encode("utf-8")).hexdigest()
    cache_path = os.path.join(CACHE_DIR, key + ".html")

    if os.path.exists(cache_path):
        with open(cache_path, "rb") as f:
            html = f.read()
    else:
        resp = requests.get(url)
        resp.raise_for_status()
        html = resp.content
        with open(cache_path, "wb") as f:
            f.write(html)

    return BeautifulSoup(html, "html.parser")

# Fonction pour obtenir les détails d'un auteur
def get_author_details(author_url):
    soup = fetch_with_cache(author_url)
    name_tag = soup.find("h3", class_="author-title")
    name = name_tag.text.strip() if name_tag else None

    bio_tag = soup.find("div", class_="author-description")
    bio = bio_tag.text.strip() if bio_tag else None

    born_date_tag = soup.find("span", class_="author-born-date")
    born_date = born_date_tag.text.strip() if born_date_tag else None

    born_loc_tag = soup.find("span", class_="author-born-location")
    born_location = born_loc_tag.text.strip() if born_loc_tag else None

    died_tag = soup.find("span", class_="author-died-date")
    death_date = died_tag.text.strip() if died_tag else None

    return {
        "name": name,
        "biography": bio,
        "born_date": born_date,
        "born_location": born_location,
        "death_date": death_date,
        "url": author_url
    }

# Fonction principale pour scraper le site et exporter les données
def scrape_and_export(base_url=BASE_URL, out_prefix="quotes_graph"):
    authors_cache = {}   
    quotes_data = []    

    page_url = base_url
    while page_url:
        print(f"Scraping page: {page_url}")
        soup = fetch_with_cache(page_url)

        quote_nodes = soup.select(".quote")
        for qnode in quote_nodes:
            text_tag = qnode.find("span", class_="text")
            quote_text = text_tag.text.strip() if text_tag else ""

            author_tag = qnode.find("small", class_="author")
            author_name = author_tag.text.strip() if author_tag else "Unknown"

            author_link_tag = qnode.select_one("a[href^='/author/']")
            author_url = BASE_URL + author_link_tag['href'] if author_link_tag else None

            tag_nodes = qnode.select(".tags a.tag")
            tags = [t.text.strip() for t in tag_nodes]

            quotes_data.append({
                "quote": quote_text,
                "author": author_name,
                "author_url": author_url,
                "tags": tags
            })

        next_btn = soup.find("li", class_="next")
        page_url = BASE_URL + next_btn.a['href'] if next_btn and next_btn.a else None

    for item in quotes_data:
        name = item["author"]
        url = item["author_url"]
        if name not in authors_cache:
            if url:
                authors_cache[name] = get_author_details(url)
            else:
                authors_cache[name] = {
                    "name": name,
                    "biography": None,
                    "born_date": None,
                    "born_location": None,
                    "death_date": None,
                    "url": None
                }

    G = nx.DiGraph()
    for a_name, a_info in authors_cache.items():
        G.add_node(f"author::{a_name}",
                type="author",
                label=a_name,
                biography=a_info.get("biography"),
                born_date=a_info.get("born_date"),
                born_location=a_info.get("born_location"),
                death_date=a_info.get("death_date"),
                url=a_info.get("url"))

    for i, item in enumerate(quotes_data):
        q_id = f"quote::{i}" 
        G.add_node(q_id, type="quote", text=item["quote"])
        a_node = f"author::{item['author']}"
        G.add_edge(q_id, a_node, relation="said_by")
        for tag in item["tags"]:
            tag_node = f"tag::{tag}"
            if not G.has_node(tag_node):
                G.add_node(tag_node, type="tag", label=tag)
            G.add_edge(a_node, tag_node, relation="has_tag")

    graphml_path = f"{out_prefix}.graphml"
    gexf_path = f"{out_prefix}.gexf"


    def _sanitize_value(v):
        if v is None:
            return ""
        if isinstance(v, (str, int, float, bool)):
            return v
        try:
            return _json.dumps(v)
        except Exception:
            return str(v)

    for n, attrs in list(G.nodes(data=True)):
        for k, val in list(attrs.items()):
            attrs[k] = _sanitize_value(val)

    for u, v, attrs in list(G.edges(data=True)):
        for k, val in list(attrs.items()):
            attrs[k] = _sanitize_value(val)

    nx.write_graphml(G, graphml_path)
    nx.write_gexf(G, gexf_path)
    print(f"Exported GraphML -> {graphml_path}")
    print(f"Exported GEXF   -> {gexf_path}")

    author_counts = {}
    for node, data in G.nodes(data=True):
        if data.get("type") == "author":
            in_edges = G.in_edges(node)
            count = sum(1 for (u, v) in in_edges if G.nodes[u].get("type") == "quote")
            author_counts[node.replace("author::", "")] = count

    top_authors = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)

    return {
        "graph": G,
        "graphml": graphml_path,
        "gexf": gexf_path,
        "top_authors": top_authors
    }


if __name__ == "__main__":
    result = scrape_and_export()
    print("\nTop auteurs par nombre de citations (author, count) :")
    for author, cnt in result["top_authors"][:10]:
        print(f"{author}: {cnt}")
    print("\nFichiers exportés:", result["graphml"], result["gexf"])
