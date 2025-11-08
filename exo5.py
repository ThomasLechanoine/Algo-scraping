import requests
from bs4 import BeautifulSoup
import json
from collections import defaultdict
from urllib.parse import urljoin
import time

class BooksScraperAdvanced:
    def __init__(self, base_url="https://books.toscrape.com/"):
        self.base_url = base_url
        self.categories = {}
        self.books_by_category = defaultdict(list)

    # Fonction pour récupérer le contenu d'une URL
    def get_soup(self, url):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"Erreur lors de la récupération de {url}: {e}")
            return None

    # Fonction pour extraire les catégories
    def extract_categories(self):
        print("Extraction des catégories...")
        soup = self.get_soup(self.base_url)
        if not soup:
            return
        
        category_list = soup.find('ul', class_='nav-list').find('ul')
        
        for idx, cat_link in enumerate(category_list.find_all('a'), 1):
            cat_name = cat_link.text.strip()
            cat_url = urljoin(self.base_url, cat_link['href'])
            
            self.categories[cat_name] = {
                'id': idx,
                'name': cat_name,
                'url': cat_url,
                'books': [],
                'parent': None 
            }
        
        print(f"{len(self.categories)} catégories trouvées")
    
    # Fonction pour scraper les livres d'une catégorie
    def scrape_category(self, cat_name, cat_info):
        print(f"Scraping: {cat_name}...")
        
        current_url = cat_info['url']
        page_num = 1
        
        while current_url:
            soup = self.get_soup(current_url)
            if not soup:
                break
            
            books = soup.find_all('article', class_='product_pod')
            
            for book in books:
                book_data = self.extract_book_data(book)
                if book_data:
                    cat_info['books'].append(book_data)
            
            next_button = soup.find('li', class_='next')
            if next_button:
                next_link = next_button.find('a')['href']
                base_cat_url = '/'.join(current_url.split('/')[:-1])
                current_url = f"{base_cat_url}/{next_link}"
                page_num += 1
            else:
                current_url = None
        
        print(f"  → {len(cat_info['books'])} livres trouvés")

    # Fonction pour extraire les données d'un livre
    def extract_book_data(self, book_element):
        try:
            title = book_element.find('h3').find('a')['title']
            price_text = book_element.find('p', class_='price_color').text
            price = float(price_text.replace('£', '').strip())
            
            rating_class = book_element.find('p', class_='star-rating')['class'][1]
            rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
            rating = rating_map.get(rating_class, 0)
            
            availability = book_element.find('p', class_='instock availability')
            in_stock = 'In stock' in availability.text if availability else False
            
            return {
                'title': title,
                'price': price,
                'rating': rating,
                'in_stock': in_stock
            }
        except Exception as e:
            print(f"  ⚠️  Erreur extraction livre: {e}")
            return None

    # Fonction pour calculer les statistiques
    def calculate_statistics(self):
        print("\nCalcul des statistiques...")
        
        results = []
        
        for cat_name, cat_info in self.categories.items():
            books = cat_info['books']
            
            if not books:
                continue
            
            prices = [book['price'] for book in books]
            ratings = [book['rating'] for book in books if book['rating'] > 0]
            
            stats = {
                'id': cat_info['id'],
                'name': cat_name,
                'total_books': len(books),
                'price_min': min(prices),
                'price_max': max(prices),
                'price_avg': sum(prices) / len(prices),
                'rating_avg': sum(ratings) / len(ratings) if ratings else 0,
                'in_stock_count': sum(1 for b in books if b['in_stock']),
                'books': books
            }
            
            results.append(stats)
        
        return results

    # Fonction pour créer un classement des catégories
    def create_ranking(self, results):
        print("\nCréation du classement...")
        
        by_count = sorted(results, key=lambda x: x['total_books'], reverse=True)
        
        by_price = sorted(results, key=lambda x: x['price_avg'], reverse=True)
        
        by_rating = sorted(results, key=lambda x: x['rating_avg'], reverse=True)
        
        return {
            'by_book_count': [{'name': c['name'], 'count': c['total_books']} for c in by_count[:10]],
            'by_average_price': [{'name': c['name'], 'price': round(c['price_avg'], 2)} for c in by_price[:10]],
            'by_average_rating': [{'name': c['name'], 'rating': round(c['rating_avg'], 2)} for c in by_rating[:10]]
        }
    
    # Fonction pour détecter les catégories sous-représentées
    def detect_underrepresented(self, results, threshold=10):
        underrepresented = [
            {'name': cat['name'], 'total_books': cat['total_books']}
            for cat in results if cat['total_books'] < threshold
        ]
        return sorted(underrepresented, key=lambda x: x['total_books'])
    
    # Fonction pour rechercher des livres par mot-clé
    def search_books(self, results, query):
        query_lower = query.lower()
        found_books = []
        
        for cat in results:
            for book in cat['books']:
                if query_lower in book['title'].lower():
                    found_books.append({
                        'category': cat['name'],
                        'title': book['title'],
                        'price': book['price'],
                        'rating': book['rating']
                    })
        
        return found_books
    
    # Fonction pour exporter les données en JSON
    def export_to_json(self, results, rankings, underrepresented, filename='books_analysis.json'):
        output = {
            'metadata': {
                'total_categories': len(results),
                'total_books': sum(cat['total_books'] for cat in results),
                'scrape_date': time.strftime('%Y-%m-%d %H:%M:%S')
            },
            'categories': results,
            'rankings': rankings,
            'underrepresented_categories': underrepresented
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print(f"\nDonnées exportées dans '{filename}'")

    # Fonction principale pour exécuter le scraping avancé
    def run(self):
        print("Démarrage du scraping avancé...\n")

        self.extract_categories()
        
        for cat_name, cat_info in self.categories.items():
            self.scrape_category(cat_name, cat_info)
            time.sleep(0.5)  
        
        results = self.calculate_statistics()
        
        rankings = self.create_ranking(results)
        
        underrepresented = self.detect_underrepresented(results)
        
        self.display_summary(results, rankings, underrepresented)
        
        self.export_to_json(results, rankings, underrepresented)

        print("\nDémonstration recherche full-text (mot: 'harry'):")
        search_results = self.search_books(results, 'harry')
        for book in search_results[:5]:
            print(f"  - {book['title']} ({book['category']}) - £{book['price']}")
        
        return results
    
    # Fonction pour afficher un résumé des résultats
    def display_summary(self, results, rankings, underrepresented):
        print("\n" + "="*60)
        print("RÉSUMÉ DES RÉSULTATS")
        print("="*60)
        
        total_books = sum(cat['total_books'] for cat in results)
        avg_price_global = sum(cat['price_avg'] * cat['total_books'] for cat in results) / total_books
        
        print(f"\nStatistiques globales:")
        print(f"  • Catégories: {len(results)}")
        print(f"  • Livres totaux: {total_books}")
        print(f"  • Prix moyen global: £{avg_price_global:.2f}")
        
        print(f"\nTop 5 catégories (par nombre de livres):")
        for i, cat in enumerate(rankings['by_book_count'][:5], 1):
            print(f"  {i}. {cat['name']}: {cat['count']} livres")
        
        print(f"\nCatégories sous-représentées (<10 livres): {len(underrepresented)}")
        for cat in underrepresented[:5]:
            print(f"  • {cat['name']}: {cat['total_books']} livres")


if __name__ == "__main__":
    scraper = BooksScraperAdvanced()
    results = scraper.run()
    print("\n✅ Scraping terminé avec succès!")