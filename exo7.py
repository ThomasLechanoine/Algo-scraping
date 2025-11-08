import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin
from typing import Dict, List, Tuple
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Pipeline complet de scraping et nettoyage des données
class BooksScraperCleaner:
    
    def __init__(self, base_url: str = "https://books.toscrape.com/"):
        self.base_url = base_url
        self.raw_data = []
        self.cleaned_data = []
        self.quality_report = {}
    
    # Scraping avec gestion d'erreurs
    def scrape_books(self, max_pages: int = 3) -> List[Dict]:
        logger.info(f"Début du scraping de {max_pages} pages...")
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}catalogue/page-{page}.html"
            try:
                response = requests.get(url, timeout=10)
                response.encoding = 'utf-8'
                soup = BeautifulSoup(response.content, 'html.parser')
                
                books = soup.find_all('article', class_='product_pod')
                
                for book in books:
                    book_data = self._extract_book_data(book)
                    self.raw_data.append(book_data)
                    
                logger.info(f"Page {page}/{max_pages} scrapée: {len(books)} livres")
                
            except Exception as e:
                logger.error(f"Erreur page {page}: {e}")
                
        logger.info(f"Total scraped: {len(self.raw_data)} livres")
        return self.raw_data
    
    # Extraction des données brutes
    def _extract_book_data(self, book) -> Dict:
        try:
            # Titre
            title = book.find('h3').find('a')['title'] if book.find('h3') else None
            
            # Prix
            price = book.find('p', class_='price_color').text if book.find('p', class_='price_color') else None
            
            # Disponibilité
            availability = book.find('p', class_='instock availability').text.strip() if book.find('p', class_='instock availability') else None
            
            # Rating
            rating_tag = book.find('p', class_='star-rating')
            rating = rating_tag['class'][1] if rating_tag and len(rating_tag['class']) > 1 else None
            
            # URL
            link = book.find('h3').find('a')['href'] if book.find('h3') else None
            full_url = urljoin(self.base_url, link) if link else None
            
            return {
                'title': title,
                'price': price,
                'availability': availability,
                'rating': rating,
                'url': full_url
            }
        except Exception as e:
            logger.error(f"Erreur extraction: {e}")
            return {}
    
    # Nettoyage et validation des données
    def clean_data(self) -> pd.DataFrame:
        logger.info("Début du nettoyage des données...")
        
        df = pd.DataFrame(self.raw_data)
        initial_rows = len(df)
        
        # Nettoyage du titre
        df['title_cleaned'] = df['title'].apply(self._clean_title)
        
        # Nettoyage du prix
        df['price_numeric'] = df['price'].apply(self._clean_price)
        
        # Nettoyage de la disponibilité
        df['stock_quantity'] = df['availability'].apply(self._clean_availability)
        df['in_stock'] = df['stock_quantity'] > 0
        
        # Conversion du rating
        df['rating_numeric'] = df['rating'].apply(self._convert_rating)
        
        # Validation de l'URL
        df['url_valid'] = df['url'].apply(self._validate_url)
        
        # Détection d'anomalies
        df['is_anomaly'] = self._detect_anomalies(df)
        
        # Imputation des valeurs manquantes
        df = self._impute_missing_values(df)
        
        # Validation croisée
        df['data_coherent'] = self._cross_validate(df)
        
        self.cleaned_data = df
        
        # Génération du rapport
        self.quality_report = self._generate_quality_report(df, initial_rows)
        
        logger.info("Nettoyage terminé!")
        return df
    
    # Fonctions de nettoyage du titre, prix, disponibilité, rating, URL
    def _clean_title(self, title: str) -> str:
        if pd.isna(title):
            return ""
        
        # Suppression des espaces multiples et trim
        title = re.sub(r'\s+', ' ', str(title)).strip()
        
        # Correction d'encodage basique
        title = title.encode('utf-8', errors='ignore').decode('utf-8')
        
        # Capitalisation standardisée
        return title.title() if title else ""
    
    def _clean_price(self, price: str) -> float:
        if pd.isna(price):
            return None
        
        try:
            price_str = re.sub(r'[^\d.]', '', str(price))
            return float(price_str) if price_str else None
        except:
            return None
    
    def _clean_availability(self, availability: str) -> int:
        if pd.isna(availability):
            return 0
        
        # Recherche d'un nombre dans le texte
        match = re.search(r'(\d+)', str(availability))
        if match:
            return int(match.group(1))
        
        # Si "In stock" sans nombre, on considère qu'il y en a
        if 'in stock' in str(availability).lower():
            return 1
        
        return 0
    
    def _convert_rating(self, rating: str) -> int:
        """Convertit le rating texte en nombre"""
        rating_map = {
            'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5
        }
        return rating_map.get(rating, 0)
    
    def _validate_url(self, url: str) -> bool:
        if pd.isna(url):
            return False
        
        url_pattern = re.compile(
            r'^https?://'  
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  
            r'localhost|'  
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  
            r'(?::\d+)?'  
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        return bool(url_pattern.match(str(url)))
    
    # Détection d'anomalies
    def _detect_anomalies(self, df: pd.DataFrame) -> pd.Series:
        anomalies = pd.Series([False] * len(df), index=df.index)
        
        # Prix anormaux 
        if 'price_numeric' in df.columns:
            Q1 = df['price_numeric'].quantile(0.25)
            Q3 = df['price_numeric'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 3 * IQR
            upper_bound = Q3 + 3 * IQR
            
            price_anomalies = (df['price_numeric'] < lower_bound) | (df['price_numeric'] > upper_bound)
            anomalies |= price_anomalies
        
        # Titres vides ou trop courts
        title_anomalies = df['title_cleaned'].str.len() < 3
        anomalies |= title_anomalies
        
        # Rating invalide
        rating_anomalies = (df['rating_numeric'] < 1) | (df['rating_numeric'] > 5)
        anomalies |= rating_anomalies
        
        return anomalies
    
    # Impute valeurs manquantes
    def _impute_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        
        # Prix: utiliser la médiane
        if 'price_numeric' in df.columns:
            median_price = df['price_numeric'].median()
            df['price_numeric'].fillna(median_price, inplace=True)
            df['price_imputed'] = df['price'].isna()
        
        # Rating: utiliser le mode (rating le plus fréquent)
        if 'rating_numeric' in df.columns:
            mode_rating = df['rating_numeric'].mode()[0] if not df['rating_numeric'].mode().empty else 3
            df['rating_numeric'].fillna(mode_rating, inplace=True)
            df['rating_imputed'] = df['rating'].isna()
        
        # Stock: 0 par défaut
        if 'stock_quantity' in df.columns:
            df['stock_quantity'].fillna(0, inplace=True)
        
        return df
    
    # Validation croisée des données
    def _cross_validate(self, df: pd.DataFrame) -> pd.Series:
        coherent = pd.Series([True] * len(df), index=df.index)
        
        # Cohérence prix/rating: les livres chers devraient avoir de bons ratings
        if 'price_numeric' in df.columns and 'rating_numeric' in df.columns:
            expensive_books = df['price_numeric'] > df['price_numeric'].quantile(0.75)
            low_rating = df['rating_numeric'] <= 2
            coherent &= ~(expensive_books & low_rating)
        
        # Cohérence disponibilité/URL: si URL valide, devrait être en stock
        if 'url_valid' in df.columns and 'in_stock' in df.columns:
            coherent &= ~(df['url_valid'] & ~df['in_stock'])
        
        return coherent
    
    # Génération du rapport de qualité
    def _generate_quality_report(self, df: pd.DataFrame, initial_rows: int) -> Dict:
        
        report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'rows_initial': initial_rows,
            'rows_final': len(df),
            'rows_lost': initial_rows - len(df),
            'completeness': {},
            'validity': {},
            'anomalies': {},
            'imputation': {},
            'statistics': {}
        }
        
        # Complétude
        for col in df.columns:
            if col not in ['price_imputed', 'rating_imputed', 'is_anomaly', 'data_coherent', 'url_valid', 'in_stock']:
                non_null_pct = (df[col].notna().sum() / len(df)) * 100
                report['completeness'][col] = f"{non_null_pct:.2f}%"
        
        # Validité
        report['validity']['url_valid_count'] = int(df['url_valid'].sum())
        report['validity']['url_valid_pct'] = f"{(df['url_valid'].sum() / len(df)) * 100:.2f}%"
        
        # Anomalies
        report['anomalies']['count'] = int(df['is_anomaly'].sum())
        report['anomalies']['percentage'] = f"{(df['is_anomaly'].sum() / len(df)) * 100:.2f}%"
        
        # Imputation
        if 'price_imputed' in df.columns:
            report['imputation']['price_imputed_count'] = int(df['price_imputed'].sum())
        if 'rating_imputed' in df.columns:
            report['imputation']['rating_imputed_count'] = int(df['rating_imputed'].sum())
        
        # Statistiques
        if 'price_numeric' in df.columns:
            report['statistics']['price'] = {
                'mean': f"£{df['price_numeric'].mean():.2f}",
                'median': f"£{df['price_numeric'].median():.2f}",
                'std': f"£{df['price_numeric'].std():.2f}",
                'min': f"£{df['price_numeric'].min():.2f}",
                'max': f"£{df['price_numeric'].max():.2f}"
            }
        
        if 'rating_numeric' in df.columns:
            report['statistics']['rating'] = {
                'mean': f"{df['rating_numeric'].mean():.2f}",
                'distribution': df['rating_numeric'].value_counts().to_dict()
            }
        
        # Cohérence
        report['coherence'] = {
            'coherent_rows': int(df['data_coherent'].sum()),
            'coherence_rate': f"{(df['data_coherent'].sum() / len(df)) * 100:.2f}%"
        }
        
        return report
    
    # Affichage lisible du rapport 
    def print_quality_report(self):
        """Affiche le rapport de qualité de manière lisible"""
        print("\n" + "="*60)
        print("RAPPORT DE QUALITÉ DES DONNÉES")
        print("="*60)
        
        print(f"\nDate: {self.quality_report['timestamp']}")
        print(f"\nVOLUME")
        print(f"  • Lignes initiales: {self.quality_report['rows_initial']}")
        print(f"  • Lignes finales: {self.quality_report['rows_final']}")
        print(f"  • Lignes perdues: {self.quality_report['rows_lost']}")
        
        print(f"\nCOMPLÉTUDE")
        for field, pct in self.quality_report['completeness'].items():
            print(f"  • {field}: {pct}")
        
        print(f"\nVALIDITÉ")
        print(f"  • URLs valides: {self.quality_report['validity']['url_valid_count']} ({self.quality_report['validity']['url_valid_pct']})")
        
        print(f"\n ANOMALIES")
        print(f"  • Détectées: {self.quality_report['anomalies']['count']} ({self.quality_report['anomalies']['percentage']})")
        
        print(f"\nIMPUTATION")
        for field, count in self.quality_report['imputation'].items():
            print(f"  • {field}: {count}")
        
        print(f"\nSTATISTIQUES")
        if 'price' in self.quality_report['statistics']:
            print(f"  Prix:")
            for stat, val in self.quality_report['statistics']['price'].items():
                print(f"    - {stat}: {val}")
        
        if 'rating' in self.quality_report['statistics']:
            print(f"  Ratings:")
            print(f"    - Moyenne: {self.quality_report['statistics']['rating']['mean']}")
        
        print(f"\n✔️  COHÉRENCE")
        print(f"  • Lignes cohérentes: {self.quality_report['coherence']['coherent_rows']} ({self.quality_report['coherence']['coherence_rate']})")
        
        print("\n" + "="*60 + "\n")
    
    # Export des résultats nettoyés
    def export_results(self, filename: str = 'books_cleaned.csv'):
        if len(self.cleaned_data) > 0:
            self.cleaned_data.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Données exportées vers {filename}")
        else:
            logger.warning("Aucune donnée à exporter")



if __name__ == "__main__":
    # Création du pipeline
    pipeline = BooksScraperCleaner()
    
    # 1. Scraping
    pipeline.scrape_books(max_pages=3)
    
    # 2. Nettoyage et validation
    df_cleaned = pipeline.clean_data()
    
    # 3. Affichage du rapport
    pipeline.print_quality_report()
    
    # 4. Aperçu des données
    print("\nAPERÇU DES DONNÉES NETTOYÉES:")
    print(df_cleaned[['title_cleaned', 'price_numeric', 'rating_numeric', 'in_stock', 'is_anomaly']].head(10))
    
    # 5. Export
    pipeline.export_results('books_cleaned.csv')
    
    print("\nPipeline terminé avec succès!")