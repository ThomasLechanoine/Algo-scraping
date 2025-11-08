import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import argparse
from urllib.parse import urljoin, urlparse

BASE_URL = "https://realpython.github.io/fake-jobs/"

# Fonction pour scraper les offres d'emploi du site
def scrape_jobs():
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    jobs = []
    job_cards = soup.select("div.card-content")

    for job in job_cards:
        title = job.find("h2", class_="title").text.strip()
        company = job.find("h3", class_="company").text.strip()
        location = job.find("p", class_="location").text.strip()
        date_posted = job.find("time")["datetime"]
        apply_link = job.find("a", text="Apply")["href"]
        full_url = urljoin(BASE_URL, apply_link)

        jobs.append({
            "title": title,
            "company": company,
            "location": location,
            "date_posted": date_posted,
            "apply_url": full_url
        })
    return jobs


# Fonction pour nettoyer et filtrer les offres d'emploi
def clean_and_filter(jobs, keyword="Python"):
    filtered = []
    seen = set()  
    for job in jobs:
        if keyword.lower() in job["title"].lower():
            parsed = urlparse(job["apply_url"])
            if not (parsed.scheme and parsed.netloc):
                continue  
            try:
                job["date_posted"] = datetime.strptime(job["date_posted"], "%Y-%m-%d").date()
            except Exception:
                job["date_posted"] = None

            job_id = (job["title"], job["company"], job["location"])
            if job_id in seen:
                continue
            seen.add(job_id)

            job["contract_type"] = "Full-time" if "Senior" in job["title"] else "Contract"
            filtered.append(job)

    return filtered

# Fonction pour afficher des statistiques sur les offres d'emploi
def job_statistics(df):
    print("\n Statistiques :")
    print("— Offres par ville —")
    print(df["location"].value_counts(), "\n")
    print("— Offres par type de contrat —")
    print(df["contract_type"].value_counts(), "\n")

# Fonction principale
def main():
    parser = argparse.ArgumentParser(description="Scrape les offres Fake Jobs (filtrage dynamique).")
    parser.add_argument("--keyword", type=str, default="Python", help="Mot-clé à rechercher (ex: Python)")
    parser.add_argument("--city", type=str, help="Filtrer par ville (optionnel)")
    parser.add_argument("--contract", type=str, help="Filtrer par type de contrat (optionnel)")
    args = parser.parse_args()

    print("Scraping des offres...")
    jobs = scrape_jobs()
    jobs = clean_and_filter(jobs, args.keyword)

    df = pd.DataFrame(jobs)

    if args.city:
        df = df[df["location"].str.contains(args.city, case=False)]
    if args.contract:
        df = df[df["contract_type"].str.contains(args.contract, case=False)]

    job_statistics(df)

    df.to_csv("fake_jobs_filtered.csv", index=False, encoding="utf-8")
    print(f"{len(df)} offres sauvegardées dans fake_jobs_filtered.csv")


if __name__ == "__main__":
    main()
