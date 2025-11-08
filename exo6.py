import requests
from bs4 import BeautifulSoup
import time
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import sys

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ResilientScraper:
    def __init__(self, base_url: str, checkpoint_file: str = 'checkpoint.json'):
        self.base_url = base_url
        self.checkpoint_file = checkpoint_file
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'retries': 0,
            'blocked_detections': 0,
            'start_time': datetime.now().isoformat(),
            'books_scraped': 0
        }
        
        self.delay = 1.0
        self.min_delay = 0.5
        self.max_delay = 10.0
        self.timeout = 10
        self.max_retries = 5
        self.scraped_books = []
        
        self.load_checkpoint()
    
    # Fonction de gestion du checkpoint
    def load_checkpoint(self):
        checkpoint_path = Path(self.checkpoint_file)
        if checkpoint_path.exists():
            try:
                with open(checkpoint_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.scraped_books = data.get('books', [])
                    self.stats = data.get('stats', self.stats)
                    logger.info(f"Checkpoint charge: {len(self.scraped_books)} livres deja scraped")
            except Exception as e:
                logger.error(f"Erreur lors du chargement du checkpoint: {e}")
                
    # Sauvegarde du checkpoint
    def save_checkpoint(self):
        try:
            checkpoint_data = {
                'books': self.scraped_books,
                'stats': self.stats,
                'last_save': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
            logger.debug("Checkpoint sauvegarde")
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du checkpoint: {e}")

    # Ajustement du delai entre les requetes
    def adaptive_delay(self, success: bool):
        if success:
            self.delay = max(self.min_delay, self.delay * 0.9)
        else:
            self.delay = min(self.max_delay, self.delay * 1.5)
        logger.debug(f"Delai ajuste a {self.delay:.2f}s")
    
    # Detection de blocage
    def detect_blocking(self, response: requests.Response) -> bool:
        if response.status_code in [403, 429]:
            self.stats['blocked_detections'] += 1
            logger.warning(f"Blocage potentiel detecte: HTTP {response.status_code}")
            return True
        
        if 'captcha' in response.text.lower():
            self.stats['blocked_detections'] += 1
            logger.warning("Captcha detecte dans la reponse")
            return True
        
        return False
    
    # Requete avec gestion des erreurs et retries
    def make_request(self, url: str, retries: int = 0) -> Optional[requests.Response]:
        self.stats['total_requests'] += 1
        
        try:
            time.sleep(self.delay)
            
            timeout = self.timeout * (1 + retries * 0.5)
            response = self.session.get(url, timeout=timeout)
            
            if self.detect_blocking(response):
                if retries < self.max_retries:
                    wait_time = self.delay * (2 ** retries)
                    logger.info(f"Attente de {wait_time:.2f}s avant retry...")
                    time.sleep(wait_time)
                    self.stats['retries'] += 1
                    return self.make_request(url, retries + 1)
                else:
                    logger.error(f"Blocage confirme apres {self.max_retries} tentatives")
                    self.stats['failed_requests'] += 1
                    return None
            
            response.raise_for_status()
            self.stats['successful_requests'] += 1
            self.adaptive_delay(True)
            
            return response
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout sur {url} (tentative {retries + 1}/{self.max_retries})")
            if retries < self.max_retries:
                self.stats['retries'] += 1
                return self.make_request(url, retries + 1)
            else:
                self.stats['failed_requests'] += 1
                self.adaptive_delay(False)
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur requete sur {url}: {e}")
            if retries < self.max_retries:
                self.stats['retries'] += 1
                time.sleep(self.delay * (retries + 1))
                return self.make_request(url, retries + 1)
            else:
                self.stats['failed_requests'] += 1
                self.adaptive_delay(False)
                return None
    
    # Scraping des details d'un livre
    def scrape_book(self, book_url: str) -> Optional[Dict]:
        if any(book['url'] == book_url for book in self.scraped_books):
            logger.debug(f"Livre deja scrape: {book_url}")
            return None
        
        response = self.make_request(book_url)
        if not response:
            return None
        
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            title = soup.find('h1')
            price = soup.find('p', class_='price_color')
            availability = soup.find('p', class_='instock availability')
            rating = soup.find('p', class_='star-rating')
            
            book_data = {
                'url': book_url,
                'title': title.text.strip() if title else 'N/A',
                'price': price.text.strip() if price else 'N/A',
                'availability': availability.text.strip() if availability else 'N/A',
                'rating': rating['class'][1] if rating else 'N/A',
                'scraped_at': datetime.now().isoformat()
            }
            
            self.scraped_books.append(book_data)
            self.stats['books_scraped'] += 1
            logger.info(f"Livre scrape: {book_data['title']}")
            
            return book_data
            
        except Exception as e:
            logger.error(f"Erreur lors du parsing de {book_url}: {e}")
            return None
    
    # Scraping du catalogue complet
    def scrape_catalogue(self):
        logger.info("Debut du scraping du catalogue")
        page = 1
        
        try:
            while True:
                if page == 1:
                    url = f"{self.base_url}/catalogue/page-{page}.html"
                else:
                    url = f"{self.base_url}/catalogue/page-{page}.html"
                
                logger.info(f"Scraping de la page {page}")
                response = self.make_request(url)
                
                if not response:
                    logger.warning(f"Impossible de recuperer la page {page}")
                    break
                
                soup = BeautifulSoup(response.content, 'html.parser')
                books = soup.find_all('article', class_='product_pod')
                
                if not books:
                    logger.info("Aucun livre trouve, fin du catalogue")
                    break
                
                for book in books:
                    link = book.find('h3').find('a')['href']
                    book_url = f"{self.base_url}/catalogue/{link.replace('../../../', '')}"
                    self.scrape_book(book_url)
                    
                    if self.stats['books_scraped'] % 10 == 0:
                        self.save_checkpoint()
                        self.print_stats()
                
                page += 1
                
        except KeyboardInterrupt:
            logger.info("Interruption detectee, sauvegarde en cours...")
            self.save_checkpoint()
            self.print_stats()
            raise
        except Exception as e:
            logger.error(f"Erreur critique: {e}")
            self.save_checkpoint()
            raise
        finally:
            self.save_checkpoint()
            self.save_results()
    
    # Affichage des statistiques
    def print_stats(self):
        elapsed = (datetime.now() - datetime.fromisoformat(self.stats['start_time'])).total_seconds()
        success_rate = (self.stats['successful_requests'] / self.stats['total_requests'] * 100) if self.stats['total_requests'] > 0 else 0
        
        logger.info("=" * 60)
        logger.info("STATISTIQUES DE PERFORMANCE")
        logger.info("=" * 60)
        logger.info(f"Temps ecoule: {elapsed:.2f}s")
        logger.info(f"Livres scrapes: {self.stats['books_scraped']}")
        logger.info(f"Requetes totales: {self.stats['total_requests']}")
        logger.info(f"Requetes reussies: {self.stats['successful_requests']}")
        logger.info(f"Requetes echouees: {self.stats['failed_requests']}")
        logger.info(f"Retries effectues: {self.stats['retries']}")
        logger.info(f"Blocages detectes: {self.stats['blocked_detections']}")
        logger.info(f"Taux de succes: {success_rate:.2f}%")
        logger.info(f"Delai actuel: {self.delay:.2f}s")
        if self.stats['books_scraped'] > 0:
            logger.info(f"Vitesse moyenne: {self.stats['books_scraped'] / elapsed:.2f} livres/s")
        logger.info("=" * 60)
    
    # Sauvegarde des resultats finaux
    def save_results(self):
        output_file = f"books_scraped_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'books': self.scraped_books,
                    'stats': self.stats
                }, f, indent=2, ensure_ascii=False)
            logger.info(f"Resultats sauvegardes dans {output_file}")
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des resultats: {e}")


def main():
    scraper = ResilientScraper('http://books.toscrape.com')
    
    try:
        scraper.scrape_catalogue()
        scraper.print_stats()
        logger.info("Scraping termine avec succes")
    except KeyboardInterrupt:
        logger.info("Scraping interrompu par l'utilisateur")
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
    finally:
        scraper.print_stats()


if __name__ == "__main__":
    main()