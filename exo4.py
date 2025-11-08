import sys
import subprocess

def _ensure_packages(pkgs):
    missing = []
    for mod, pkg in pkgs:
        try:
            __import__(mod)
        except Exception:
            missing.append(pkg)
    if missing:
        print("Installing missing packages:", missing)
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])

_required = [
    ("requests", "requests"),
    ("bs4", "beautifulsoup4"),
    ("pandas", "pandas"),
    ("matplotlib", "matplotlib"),
    ("fpdf", "fpdf"),
    ("plotly", "plotly"),
    ("urllib", "urllib3"),
    ("numpy", "numpy"),
]
_ensure_packages(_required)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
import plotly.express as px
from urllib.parse import urljoin
import numpy as np

BASE_URL = "https://books.toscrape.com/"

# Fonction pour scraper toutes les pages et retourner un DataFrame avec les infos des livres
def scrape_books():
    books = []
    page_url = BASE_URL

    def rating_to_int(rating_str):
        ratings = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
        return ratings.get(rating_str, None)

    while page_url:
        print(f"Scraping {page_url}...")
        r = requests.get(page_url)
        soup = BeautifulSoup(r.text, "html.parser")

        for article in soup.select("article.product_pod"):
            title = article.h3.a["title"]
            rating = rating_to_int(article.p["class"][1])
            price_text = article.select_one(".price_color").text.replace("£", "")
            price = float(''.join(c for c in price_text if c.isdigit() or c == '.'))
            availability = "In stock" in article.select_one(".availability").text
            relative_link = article.h3.a["href"]
            link = urljoin(page_url, relative_link)
            category = soup.select_one(".breadcrumb li:nth-of-type(3)").text.strip() if soup.select_one(".breadcrumb li:nth-of-type(3)") else "Unknown"

            books.append({
                "title": title,
                "rating": rating,
                "price": price,
                "category": category,
                "in_stock": availability,
                "url": link
            })

        next_btn = soup.select_one("li.next a")
        if next_btn:
            page_url = urljoin(page_url, next_btn["href"])
        else:
            page_url = None

    df = pd.DataFrame(books)
    return df

# Fonction pour analyser les données des livres
def analyze_books(df):
    print("\nAnalyse en cours...")

    avg_price_by_rating = df.groupby("rating")["price"].mean().round(2)
    avg_price_by_category = df.groupby("category")["price"].mean().round(2)
    out_of_stock = df[~df["in_stock"]]

    correlation = df["rating"].corr(df["price"]).round(3)

    print("\n Prix moyen par note :")
    print(avg_price_by_rating)
    print("\n Prix moyen par catégorie :")
    print(avg_price_by_category.head())
    print(f"\n Livres en rupture de stock : {len(out_of_stock)}")
    print(f"\n Corrélation note/prix : {correlation}")

    return {
        "avg_price_by_rating": avg_price_by_rating,
        "avg_price_by_category": avg_price_by_category,
        "out_of_stock": out_of_stock,
        "correlation": correlation
    }

# Fonction pour générer le rapport
def generate_report(df, analysis):
    print("\nGénération du rapport...")

    plt.figure(figsize=(6, 4))
    df["rating"].value_counts().sort_index().plot(kind="bar", title="Distribution des notes")
    plt.xlabel("Note"); plt.ylabel("Nombre de livres")
    plt.tight_layout()
    plt.savefig("ratings_distribution.png")
    plt.close()

    plt.figure(figsize=(6, 4))
    analysis["avg_price_by_rating"].plot(kind="bar", color="orange", title="Prix moyen par note (£)")
    plt.tight_layout()
    plt.savefig("avg_price_by_rating.png")
    plt.close()

    plt.figure(figsize=(8, 5))
    analysis["avg_price_by_category"].sort_values(ascending=False)[:10].plot(kind="barh", title="Top 10 catégories par prix moyen")
    plt.tight_layout()
    plt.savefig("avg_price_by_category.png")
    plt.close()

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(200, 10, "Analyse de marché - Books to Scrape", ln=True, align="C")
    pdf.set_font("Arial", "", 12)
    pdf.cell(200, 10, f"Corrélation note/prix : {analysis['correlation']}", ln=True)
    pdf.cell(200, 10, f"Livres en rupture de stock : {len(analysis['out_of_stock'])}", ln=True)
    pdf.image("ratings_distribution.png", x=10, y=50, w=180)
    pdf.add_page()
    pdf.image("avg_price_by_rating.png", x=15, y=30, w=170)
    pdf.add_page()
    pdf.image("avg_price_by_category.png", x=10, y=30, w=180)
    pdf.output("books_analysis_report.pdf")
    print("Rapport PDF généré : books_analysis_report.pdf")

    fig = px.scatter(df, x="rating", y="price", color="category",
                    title="Corrélation note/prix (interactif)",
                    hover_data=["title"])
    fig.write_html("interactive_price_rating.html")
    print("Graphique interactif : interactive_price_rating.html")

# Fonction pour alerter sur les livres au-dessus d'un certain prix
def price_alert(df, threshold=50):
    expensive_books = df[df["price"] > threshold]
    if not expensive_books.empty:
        print(f"\n {len(expensive_books)} livres dépassent £{threshold} :")
        print(expensive_books[["title", "price", "category"]])
    else:
        print(f"\n Aucun livre au-dessus de £{threshold}.")


if __name__ == "__main__":
    df_books = scrape_books()
    analysis = analyze_books(df_books)
    generate_report(df_books, analysis)
    price_alert(df_books, threshold=45)
