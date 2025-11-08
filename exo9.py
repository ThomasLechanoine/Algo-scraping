import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict
import json
import time
from datetime import datetime, timedelta
import os
from pathlib import Path

# Scraper avec gestion d'authentification et de session
class AuthenticatedScraper:    
    def __init__(self, base_url: str = "http://quotes.toscrape.com"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.is_authenticated = False
        self.session_expiry = None
        self.credentials_file = Path("credentials.json")
    
    # Méthode interne pour récupérer le token CSRF
    def _get_csrf_token(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Cherche le token CSRF dans les inputs cachés
            csrf_input = soup.find('input', {'name': 'csrf_token'})
            if csrf_input:
                return csrf_input.get('value')
            
            # Alternative : cherche dans les meta tags
            csrf_meta = soup.find('meta', {'name': 'csrf-token'})
            if csrf_meta:
                return csrf_meta.get('content')
                
            return None
        except Exception as e:
            print(f"Erreur lors de la récupération du CSRF token: {e}")
            return None
    
    # Méthodes pour gérer les credentials
    def save_credentials(self, username: str, password: str):
        credentials = {
            'username': username,
            'password': password 
        }
        
        with open(self.credentials_file, 'w') as f:
            json.dump(credentials, f)
        
        # Restreint les permissions du fichier (Unix)
        try:
            os.chmod(self.credentials_file, 0o600)
        except:
            pass
        
        print("Credentials sauvegardés (chiffrer en production)")
    
    # Méthode pour charger les credentials
    def load_credentials(self) -> Optional[Dict[str, str]]:
        if not self.credentials_file.exists():
            return None
        
        try:
            with open(self.credentials_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Erreur lors du chargement des credentials: {e}")
            return None
    
    # Méthode de login
    def login(self, username: str = None, password: str = None, 
            save: bool = False) -> bool:
        if not username or not password:
            creds = self.load_credentials()
            if creds:
                username = creds['username']
                password = creds['password']
                print("Credentials chargés depuis le fichier")
            else:
                print("Aucun credential fourni ou sauvegardé")
                return False
        
        login_url = f"{self.base_url}/login"
        
        try:
            # Étape 1 : Récupère le token CSRF
            csrf_token = self._get_csrf_token(login_url)
            
            # Étape 2 : Prépare les données de login
            login_data = {
                'username': username,
                'password': password
            }
            
            if csrf_token:
                login_data['csrf_token'] = csrf_token
                print(f"CSRF token récupéré: {csrf_token[:20]}...")
            
            # Étape 3 : Envoi de la requête de login
            response = self.session.post(login_url, data=login_data)
            
            # Étape 4 : Vérifie le succès de l'authentification
            if response.url != login_url and response.status_code == 200:
                self.is_authenticated = True
                self.session_expiry = datetime.now() + timedelta(hours=1)
                
                if save:
                    self.save_credentials(username, password)
                
                print("Authentification réussie!")
                print(f"Cookies actifs: {len(self.session.cookies)}")
                return True
            else:
                print("Échec de l'authentification")
                soup = BeautifulSoup(response.text, 'html.parser')
                error = soup.find('div', class_='error')
                if error:
                    print(f"   Erreur: {error.text.strip()}")
                return False
                
        except Exception as e:
            print(f"Erreur lors du login: {e}")
            return False
    
    # Méthode de logout
    def logout(self) -> bool:
        logout_url = f"{self.base_url}/logout"
        
        try:
            response = self.session.get(logout_url)
            self.is_authenticated = False
            self.session_expiry = None
            self.session.cookies.clear()
            
            print("Déconnexion réussie")
            return True
        except Exception as e:
            print(f"Erreur lors du logout: {e}")
            return False
    
    # Méthode pour vérifier la validité de la session
    def is_session_valid(self) -> bool:
        if not self.is_authenticated:
            return False
        
        if self.session_expiry and datetime.now() > self.session_expiry:
            print("Session expirée")
            return False
        
        return True
    
    # Méthode pour rafraîchir la session
    def refresh_session(self) -> bool:
        print("Rafraîchissement de la session...")
        
        creds = self.load_credentials()
        if not creds:
            print("Impossible de rafraîchir: pas de credentials sauvegardés")
            return False
        
        return self.login(creds['username'], creds['password'])
    
    # Méthode de scraping du contenu protégé
    def scrape_protected_content(self, url: str = None) -> Optional[list]:
        if not url:
            url = f"{self.base_url}/"
        
        # Vérifie et rafraîchit la session si nécessaire
        if not self.is_session_valid():
            print("Session invalide, tentative de refresh...")
            if not self.refresh_session():
                print("Impossible d'accéder au contenu protégé")
                return None
        
        try:
            response = self.session.get(url)
            
            # Vérifie si on a été redirigé vers le login
            if 'login' in response.url:
                print("Redirection vers login détectée")
                if self.refresh_session():
                    response = self.session.get(url)
                else:
                    return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extrait les citations
            quotes = []
            for quote_div in soup.find_all('div', class_='quote'):
                text = quote_div.find('span', class_='text')
                author = quote_div.find('small', class_='author')
                tags = [tag.text for tag in quote_div.find_all('a', class_='tag')]
                
                if text and author:
                    quotes.append({
                        'text': text.text.strip(),
                        'author': author.text.strip(),
                        'tags': tags
                    })
            
            print(f"{len(quotes)} citations extraites")
            return quotes
            
        except Exception as e:
            print(f"Erreur lors du scraping: {e}")
            return None
    
    # Méthodes pour exporter les cookies
    def export_cookies(self, filename: str = "cookies.json"):
        cookies_dict = {
            cookie.name: {
                'value': cookie.value,
                'domain': cookie.domain,
                'path': cookie.path,
                'expiry': cookie.expires
            }
            for cookie in self.session.cookies
        }
        
        with open(filename, 'w') as f:
            json.dump(cookies_dict, f, indent=2)
        
        print(f"Cookies exportés vers {filename}")
    
    # Méthode pour importer les cookies
    def import_cookies(self, filename: str = "cookies.json"):
        try:
            with open(filename, 'r') as f:
                cookies_dict = json.load(f)
            
            for name, cookie_data in cookies_dict.items():
                self.session.cookies.set(
                    name=name,
                    value=cookie_data['value'],
                    domain=cookie_data['domain'],
                    path=cookie_data['path']
                )
            
            self.is_authenticated = True
            print(f"Cookies importés depuis {filename}")
            return True
        except Exception as e:
            print(f"Erreur lors de l'import des cookies: {e}")
            return False

def main():
    
    print("=" * 60)
    print("SCRAPER AVEC AUTHENTIFICATION ET GESTION DE SESSION")
    print("=" * 60)
    
    scraper = AuthenticatedScraper()
    
    # Credentials par défaut du site de test
    USERNAME = "admin"
    PASSWORD = "admin"
    
    print("\nÉTAPE 1: Authentification")
    print("-" * 60)
    success = scraper.login(USERNAME, PASSWORD, save=True)
    
    if not success:
        print("Impossible de continuer sans authentification")
        return
    
    print("\nÉTAPE 2: Scraping du contenu protégé")
    print("-" * 60)
    quotes = scraper.scrape_protected_content()
    
    if quotes:
        print(f"\nExemple de citation:")
        first_quote = quotes[0]
        print(f"   Texte: {first_quote['text'][:50]}...")
        print(f"   Auteur: {first_quote['author']}")
        print(f"   Tags: {', '.join(first_quote['tags'])}")
    
    print("\nÉTAPE 3: Gestion des cookies")
    print("-" * 60)
    scraper.export_cookies("session_cookies.json")
    
    print("\nÉTAPE 4: Test de refresh de session")
    print("-" * 60)
    print(f"Session valide: {scraper.is_session_valid()}")
    
    # Simule une expiration
    scraper.session_expiry = datetime.now() - timedelta(seconds=1)
    print(f"Session valide après expiration simulée: {scraper.is_session_valid()}")
    
    # Scrape avec refresh automatique
    quotes = scraper.scrape_protected_content()
    
    print("\nÉTAPE 5: Déconnexion")
    print("-" * 60)
    scraper.logout()
    
    print("\n" + "=" * 60)
    print("DÉMONSTRATION TERMINÉE")
    print("=" * 60)

    print("\nPOINTS CLÉS DE SÉCURITÉ:")
    print("   • Credentials stockés localement ")
    print("   • Gestion automatique des tokens CSRF")
    print("   • Refresh automatique de session")
    print("   • Export/Import de cookies pour persistance")
    print("   • Headers User-Agent pour éviter les blocages")


if __name__ == "__main__":
    main()