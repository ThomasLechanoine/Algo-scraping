import requests  
from bs4 import BeautifulSoup  
from urllib.parse import urljoin 
import sqlite3  
import json 
import os  

URL = "https://books.toscrape.com/"

# Fonction utilitaire pour récupérer le contenu d'une URL
def _get_soup(url):
    resp = requests.get(url)
    resp.raise_for_status()
    return BeautifulSoup(resp.content, 'html.parser')

# Fonction pour scraper une page de liste de livres
def scrape_list_page(list_url):
    soup = _get_soup(list_url)  
    items = [] 
    # Dictionnaire pour mapper le nom de la classe de notation (texte) à une valeur entière
    rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}

    # Sélectionne tous les éléments <article> ayant la classe 'product_pod'
    for article in soup.select('article.product_pod'):
        a = article.find('h3').find('a')
        title = a.get('title')  
        href = a.get('href') 
        # Construit une URL absolue pour la page de détail
        detail_url = urljoin(list_url, href)

        # Récupère le prix
        price_tag = article.select_one('.price_color')  
        price = price_tag.get_text(strip=True) if price_tag else ''

        # Récupère la disponibilité (stock)
        stock_tag = article.select_one('.instock.availability')  
        stock = stock_tag.get_text(strip=True) if stock_tag else ''  

        # Récupère la notation (star-rating)
        rating_tag = article.find('p', class_='star-rating')  
        rating = 0  
        if rating_tag:
            # Récupère la liste des classes (ex: ['star-rating', 'Three'])
            classes = rating_tag.get('class', [])
            for c in classes:
                if c != 'star-rating':
                    rating = rating_map.get(c, 0)  
                    break  

        # Ajoute les informations extraites à la liste 'items'
        items.append({
            'title': title,
            'price': price,
            'rating': rating,
            'stock': stock,
            'detail_url': detail_url
        })
    return items 

# Fonction pour scraper les détails d'un livre à partir de sa page de détail
def scrape_detail(detail_url):
    soup = _get_soup(detail_url) 
    desc = ''  
    
    # Trouve la <div> avec l'id 'product_description'
    desc_div = soup.find('div', id='product_description')
    if desc_div:
        # La description se trouve dans le <p> qui suit *immédiatement* cette div
        p = desc_div.find_next_sibling('p')
        if p:
            desc = p.get_text(strip=True)  

    cat = ''
    # Trouve le "fil d'Ariane" (breadcrumb) pour identifier la catégorie
    breadcrumb = soup.find('ul', class_='breadcrumb')
    if breadcrumb:
        # Récupère tous les éléments
        items = breadcrumb.find_all('li')
        # La catégorie est l'avant-dernier élément 
        if len(items) >= 3:  
            cat = items[-2].get_text(strip=True)
            
    # Retourne un dictionnaire avec les détails trouvés
    return {'description': desc, 'category': cat}

# Fonction pour créer la base de données et la table
def create_db(db_path='books.db'):
    conn = sqlite3.connect(db_path)  
    cur = conn.cursor()  
    # Exécute la commande SQL pour créer la table
    # 'IF NOT EXISTS' évite une erreur si la table existe déjà
    # 'detail_url' est 'UNIQUE' pour empêcher les doublons de livres
    cur.execute('''
    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY,
        title TEXT,
        price TEXT,
        rating INTEGER,
        stock TEXT,
        description TEXT,
        category TEXT,
        detail_url TEXT UNIQUE
    )
    ''')
    conn.commit()
    return conn  

# Fonction pour insérer ou mettre à jour un livre dans la BDD
def insert_book(conn, book):
    cur = conn.cursor()
    cur.execute('''
    INSERT OR REPLACE INTO books (title, price, rating, stock, description, category, detail_url)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        # Fournit les valeurs dans l'ordre des '?'
        book.get('title'),
        book.get('price'),
        book.get('rating'),
        book.get('stock'),
        book.get('description'),
        book.get('category'),
        book.get('detail_url')
    ))
    conn.commit()  

# Fonction principale pour scraper une page de liste et remplir la BDD
def populate_db_from_page(db_path, list_url):
    conn = create_db(db_path) 
    items = scrape_list_page(list_url)  
    
    # Pour chaque livre trouvé sur la page de liste
    for it in items:
        # scrape sa page de détail pour obtenir la description et la catégorie
        details = scrape_detail(it['detail_url'])
        # Fusionne les deux dictionnaires (infos de liste + infos de détail)
        book = {**it, **details}
        # Insère le livre complet dans la base de données
        insert_book(conn, book)
        
    conn.close()  # Ferme la connexion à la BDD une fois terminé

# Fonction pour interroger la base de données et récupérer des livres
def query_books(db_path, where_clause=None, params=(), limit=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = 'SELECT title, price, rating, stock, description, category, detail_url FROM books'
    
    # Ajoute dynamiquement la clause WHERE si elle est fournie
    if where_clause:
        sql += ' WHERE ' + where_clause  
        
    # Ajoute dynamiquement la clause LIMIT si elle est fournie
    if limit:
        sql += ' LIMIT %d' % int(limit)  
    
    # Exécute la requête SQL (en passant les paramètres 'params' pour la clause WHERE)
    cur.execute(sql, params)
    rows = cur.fetchall()  
    
    # Convertit la liste d'objets 'sqlite3.Row' en une liste de dictionnaires Python standards
    result = [dict(r) for r in rows]
    conn.close()  
    return result  

# Fonction pour exporter un fichier JSON à partir des résultats de la BDD
def export_books_json(db_path, out_path='books.json', where_clause=None, params=(), limit=None):
    # Réutilise la fonction query_books pour obtenir les données
    data = query_books(db_path, where_clause=where_clause, params=params, limit=limit)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    return data  

# Ce bloc n'est exécuté que si le script est lancé directement
if __name__ == '__main__':
    # Définit le chemin du fichier de BDD dans le même répertoire que ce script
    db_file = os.path.join(os.path.dirname(__file__), 'books.db')
    
    # Étape 1: Scrape la page d'accueil (URL) et peuple la base de données (db_file)
    populate_db_from_page(db_file, URL)
    
    # Étape 2: Interroge la BDD pour obtenir un échantillon de 10 livres
    sample = query_books(db_file, limit=10)
    print('Sample rows:', len(sample))  # Affiche le nombre de lignes récupérées
    
    # Étape 3: Exporte *tous* les livres de la BDD vers un fichier 'books.json'
    export_books_json(db_file, os.path.join(os.path.dirname(__file__), 'books.json'))