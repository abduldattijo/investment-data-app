import requests
import time
import re
import random
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('vc_scraper')

class VCWebScraper:
    """
    Scraper for venture capital firm websites to extract relevant information
    about their investment thesis, portfolio, team, and contact details.
    """
    
    def __init__(self, max_pages=20, timeout=10, max_workers=5, delay=5):
        """
        Initialize the scraper with configurable parameters.
        
        Args:
            max_pages (int): Maximum number of pages to scrape per website
            timeout (int): Request timeout in seconds
            max_workers (int): Maximum number of concurrent workers for multi-threading
            delay (float): Delay between requests in seconds
        """
        self.max_pages = max_pages
        self.timeout = timeout
        self.max_workers = max_workers
        self.delay = delay
        
        # Setup session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Common user agents to rotate
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]
    
    def _get_random_user_agent(self):
        """Get a random user agent from the list."""
        return random.choice(self.user_agents)
    
    def _make_request(self, url):
        """
        Make an HTTP request with error handling and rate limiting.
        
        Args:
            url (str): URL to request
            
        Returns:
            requests.Response or None: Response object or None if request failed
        """
        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        headers = {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.google.com/'
        }
        
        try:
            # Add random delay for rate limiting
            time.sleep(self.delay * (0.5 + random.random()))
            
            response = self.session.get(
                url, 
                headers=headers, 
                timeout=self.timeout,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                return response
            else:
                logger.warning(f"Request failed with status code {response.status_code} for URL: {url}")
                return None
                
        except Exception as e:
            logger.error(f"Error requesting {url}: {str(e)}")
            return None
    
    def _extract_links(self, soup, base_url):
        """
        Extract internal links from the page.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            base_url (str): Base URL for resolving relative links
            
        Returns:
            list: List of internal links
        """
        internal_links = []
        domain = urlparse(base_url).netloc
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            full_url = urljoin(base_url, href)
            
            # Skip non-HTTP links, anchors, etc.
            if not full_url.startswith(('http://', 'https://')):
                continue
                
            # Only include internal links (same domain)
            if urlparse(full_url).netloc == domain:
                internal_links.append(full_url)
        
        return list(set(internal_links))  # Remove duplicates
    
    def _extract_portfolio(self, soup, base_url):
        """
        Extract portfolio company information.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            base_url (str): Base URL for context
            
        Returns:
            list: List of portfolio company data
        """
        portfolio = []
        
        # Look for portfolio sections by common keywords
        portfolio_sections = soup.find_all(['section', 'div'], class_=lambda c: c and any(
            keyword in c.lower() for keyword in ['portfolio', 'companies', 'investments']
        ))
        
        if not portfolio_sections:
            # Try to find by heading text
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4']):
                heading_text = heading.get_text().lower()
                if any(keyword in heading_text for keyword in ['portfolio', 'companies', 'investments']):
                    portfolio_sections.append(heading.find_parent(['section', 'div']))
        
        for section in portfolio_sections:
            # Look for company cards or list items
            company_elements = section.find_all(['div', 'li', 'a'], class_=lambda c: c and any(
                keyword in (c.lower() if c else '') for keyword in ['company', 'card', 'item', 'logo']
            ))
            
            if not company_elements:
                # Try to find all links within this section as fallback
                company_elements = section.find_all('a')
            
            for element in company_elements:
                company_name = None
                company_url = None
                company_description = None
                
                # Extract company name
                name_element = element.find(['h3', 'h4', 'h5', 'strong', 'b'])
                if name_element:
                    company_name = name_element.get_text().strip()
                elif element.name == 'a':
                    company_name = element.get_text().strip()
                
                # Extract URL if it's a link
                if element.name == 'a' and element.has_attr('href'):
                    company_url = urljoin(base_url, element['href'])
                else:
                    link = element.find('a', href=True)
                    if link:
                        company_url = urljoin(base_url, link['href'])
                
                # Try to extract description
                desc_element = element.find(['p', 'div'], class_=lambda c: c and any(
                    keyword in (c.lower() if c else '') for keyword in ['desc', 'summary', 'text']
                ))
                if desc_element:
                    company_description = desc_element.get_text().strip()
                
                # Only add if we have at least a name
                if company_name and len(company_name) > 1:
                    portfolio.append({
                        'name': company_name,
                        'url': company_url,
                        'description': company_description
                    })
        
        return portfolio
    
    def _extract_team(self, soup):
        """
        Extract team member information.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            
        Returns:
            list: List of team member data
        """
        team = []
        
        # Look for team sections by common keywords
        team_sections = soup.find_all(['section', 'div'], class_=lambda c: c and any(
            keyword in (c.lower() if c else '') for keyword in ['team', 'people', 'about-us', 'about']
        ))
        
        if not team_sections:
            # Try to find by heading text
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4']):
                heading_text = heading.get_text().lower()
                if any(keyword in heading_text for keyword in ['team', 'people', 'our partners', 'our investors']):
                    team_sections.append(heading.find_parent(['section', 'div']))
        
        for section in team_sections:
            # Look for team member cards or list items
            member_elements = section.find_all(['div', 'li'], class_=lambda c: c and any(
                keyword in (c.lower() if c else '') for keyword in ['member', 'person', 'card', 'profile']
            ))
            
            for element in member_elements:
                member_name = None
                member_title = None
                member_bio = None
                
                # Extract member name
                name_element = element.find(['h3', 'h4', 'h5', 'strong', 'b'])
                if name_element:
                    member_name = name_element.get_text().strip()
                
                # Extract title
                title_element = element.find(['p', 'div', 'span'], class_=lambda c: c and any(
                    keyword in (c.lower() if c else '') for keyword in ['title', 'role', 'position']
                ))
                if title_element:
                    member_title = title_element.get_text().strip()
                
                # Extract bio
                bio_element = element.find(['p', 'div'], class_=lambda c: c and any(
                    keyword in (c.lower() if c else '') for keyword in ['bio', 'description', 'about']
                ))
                if bio_element:
                    member_bio = bio_element.get_text().strip()
                
                # Only add if we have at least a name
                if member_name:
                    team.append({
                        'name': member_name,
                        'title': member_title,
                        'bio': member_bio
                    })
        
        return team
    
    def _extract_investment_thesis(self, soup):
        """
        Extract investment thesis or strategy information.
        
        Args:
            soup (BeautifulSoup): Parsed HTML
            
        Returns:
            str: Investment thesis text
        """
        # Look for sections with relevant keywords
        thesis_sections = soup.find_all(['section', 'div'], class_=lambda c: c and any(
            keyword in (c.lower() if c else '') for keyword in [
                'thesis', 'strategy', 'approach', 'philosophy', 'about', 'invest'
            ]
        ))
        
        if not thesis_sections:
            # Try to find by heading text
            for heading in soup.find_all(['h1', 'h2', 'h3']):
                heading_text = heading.get_text().lower()
                if any(keyword in heading_text for keyword in [
                    'thesis', 'strategy', 'approach', 'philosophy', 'how we invest', 'what we look for'
                ]):
                    parent = heading.find_parent(['section', 'div'])
                    if parent:
                        thesis_sections.append(parent)
        
        thesis_text = ""
        
        for section in thesis_sections:
            # Get all paragraphs in this section
            paragraphs = section.find_all('p')
            for p in paragraphs:
                text = p.get_text().strip()
                if len(text) > 50:  # Only substantial paragraphs
                    thesis_text += text + " "
        
        # If we couldn't find a specific thesis section, look for key sentences on the homepage
        if not thesis_text:
            relevant_sentences = []
            
            # Look for all paragraphs
            for p in soup.find_all('p'):
                text = p.get_text().strip()
                if len(text) > 20 and len(text) < 300:  # Reasonable sentence length
                    lower_text = text.lower()
                    if any(keyword in lower_text for keyword in [
                        'invest', 'focus', 'companies', 'founders', 'startups', 'portfolio',
                        'capital', 'fund', 'venture', 'strategy', 'partner'
                    ]):
                        relevant_sentences.append(text)
            
            thesis_text = " ".join(relevant_sentences[:3])  # Take top 3 most relevant sentences
        
        return thesis_text.strip()
    
    def _extract_sectors(self, text):
        """
        Extract industry sectors from text.
        
        Args:
            text (str): Text to analyze
            
        Returns:
            list: List of identified sectors
        """
        sectors = []
        
        # Common sectors in VC investments
        sector_keywords = {
            'Fintech': ['fintech', 'financial technology', 'financial services', 'banking', 'insurance', 'payments'],
            'Enterprise SaaS': ['enterprise', 'saas', 'software as a service', 'b2b software', 'cloud software'],
            'Health Tech': ['health', 'healthcare', 'medical', 'biotech', 'life sciences', 'digital health'],
            'AI/ML': ['ai', 'artificial intelligence', 'machine learning', 'deep learning', 'nlp', 'computer vision'],
            'Cybersecurity': ['security', 'cybersecurity', 'infosec', 'data protection', 'privacy'],
            'E-commerce': ['e-commerce', 'ecommerce', 'e commerce', 'retail', 'direct to consumer', 'd2c', 'dtc'],
            'Edtech': ['education', 'edtech', 'learning', 'teaching', 'training'],
            'Climate Tech': ['climate', 'cleantech', 'sustainability', 'green', 'renewable', 'carbon'],
            'Consumer Apps': ['consumer', 'apps', 'mobile apps', 'social media', 'social network'],
            'B2B Marketplace': ['b2b', 'marketplace', 'platform', 'exchange'],
            'Web3/Blockchain': ['web3', 'blockchain', 'crypto', 'bitcoin', 'ethereum', 'nft', 'defi', 'dao'],
            'Hardware': ['hardware', 'devices', 'iot', 'internet of things', 'sensors', 'electronics'],
            'Robotics': ['robotics', 'robots', 'automation', 'autonomous'],
            'AR/VR': ['augmented reality', 'virtual reality', 'ar', 'vr', 'mixed reality', 'metaverse'],
            'Space': ['space', 'aerospace', 'satellite', 'launch'],
            'AgTech': ['agriculture', 'agtech', 'farming', 'food production'],
            'Manufacturing': ['manufacturing', 'industry 4.0', 'industrial', 'factories'],
            'PropTech': ['real estate', 'proptech', 'construction', 'buildings'],
            'Mobility': ['mobility', 'transportation', 'automotive', 'electric vehicles', 'ev']
        }
        
        text_lower = text.lower()
        
        for sector, keywords in sector_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                sectors.append(sector)
        
        # If too many sectors found (>5), probably too generic text
        # so return the strongest matches only
        if len(sectors) > 5:
            sector_scores = {}
            for sector, keywords in sector_keywords.items():
                if sector in sectors:
                    # Count total occurrences of keywords
                    score = sum(text_lower.count(keyword) for keyword in keywords)
                    sector_scores[sector] = score
            
            # Take top 5 sectors by score
            sectors = [s for s, _ in sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)[:5]]
        
        return sectors
    
    def _extract_stages(self, text):
        """
        Extract investment stages from text.
        
        Args:
            text (str): Text to analyze
            
        Returns:
            list: List of identified investment stages
        """
        stages = []
        text_lower = text.lower()
        
        stage_patterns = {
            'Pre-seed': [r'pre.?seed', r'concept', r'idea stage', r'earliest'],
            'Seed': [r'seed', r'early stage', r'initial funding'],
            'Series A': [r'series a', r'series.a', r'early growth'],
            'Series B': [r'series b', r'series.b', r'growth stage'],
            'Series C+': [r'series c', r'series.c', r'series d', r'later stage', r'growth equity']
        }
        
        for stage, patterns in stage_patterns.items():
            if any(re.search(pattern, text_lower) for pattern in patterns):
                stages.append(stage)
        
        return stages
    
    def _extract_check_sizes(self, text):
        """
        Extract typical check sizes from text.
        
        Args:
            text (str): Text to analyze
            
        Returns:
            tuple: (minimum check size, maximum check size) in thousands of dollars
        """
        text_lower = text.lower()
        
        # Look for mentions of investment amounts
        amount_patterns = [
            # $X-Y million pattern
            r'\$?\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(?:m|million)',
            # $X million to $Y million pattern
            r'\$?\s*(\d+(?:\.\d+)?)\s*(?:m|million)\s*to\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:m|million)',
            # $X-Y thousand pattern
            r'\$?\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(?:k|thousand)',
            # $X thousand to $Y thousand pattern
            r'\$?\s*(\d+(?:\.\d+)?)\s*(?:k|thousand)\s*to\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:k|thousand)',
            # $X-Y pattern (without units, assume thousands)
            r'invest\s*\$?\s*(\d+(?:\.\d+)?)\s*-\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:in|per|each)',
            # Flat mentions of $X million
            r'initial\s*investments?\s*of\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:m|million)',
            r'typical\s*check\s*of\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:m|million)',
            # Flat mentions of $X thousand
            r'initial\s*investments?\s*of\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:k|thousand)',
            r'typical\s*check\s*of\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:k|thousand)'
        ]
        
        min_amount = None
        max_amount = None
        
        for pattern in amount_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                for match in matches:
                    if isinstance(match, tuple) and len(match) >= 2:
                        # Range pattern
                        try:
                            amount1 = float(match[0])
                            amount2 = float(match[1])
                            
                            # Convert to thousands
                            if 'm' in pattern or 'million' in pattern:
                                amount1 *= 1000  # Convert to thousands
                                amount2 *= 1000  # Convert to thousands
                            
                            if min_amount is None or amount1 < min_amount:
                                min_amount = amount1
                            
                            if max_amount is None or amount2 > max_amount:
                                max_amount = amount2
                        except ValueError:
                            continue
                    elif isinstance(match, str) or len(match) == 1:
                        # Single amount pattern
                        try:
                            if isinstance(match, tuple):
                                amount = float(match[0])
                            else:
                                amount = float(match)
                            
                            # Convert to thousands
                            if 'm' in pattern or 'million' in pattern:
                                amount *= 1000  # Convert to thousands
                            
                            if min_amount is None:
                                min_amount = amount
                            elif amount < min_amount:
                                min_amount = amount
                            
                            if max_amount is None:
                                max_amount = amount
                            elif amount > max_amount:
                                max_amount = amount
                        except ValueError:
                            continue
        
        # Default range if nothing found
        if min_amount is None:
            min_amount = 100  # Default $100k
        if max_amount is None:
            max_amount = 1000  # Default $1M
        
        # Round to nearest 50k
        min_amount = round(min_amount / 50) * 50
        max_amount = round(max_amount / 50) * 50
        
        return (min_amount, max_amount)
    
    def _extract_geo_focus(self, text):
        """
        Extract geographical focus from text.
        
        Args:
            text (str): Text to analyze
            
        Returns:
            str: Identified geographical focus
        """
        text_lower = text.lower()
        
        # Mapping of regions to their keywords
        geo_regions = {
            'Silicon Valley': ['silicon valley', 'bay area', 'san francisco', 'palo alto', 'menlo park'],
            'New York': ['new york', 'nyc', 'brooklyn', 'manhattan'],
            'Boston': ['boston', 'cambridge', 'massachusetts', 'new england'],
            'Midwest': ['midwest', 'chicago', 'detroit', 'minneapolis', 'ohio', 'michigan', 'illinois'],
            'Southeast': ['southeast', 'atlanta', 'miami', 'florida', 'carolina', 'tennessee', 'georgia'],
            'Texas': ['texas', 'austin', 'dallas', 'houston', 'san antonio'],
            'Europe': ['europe', 'european', 'uk', 'london', 'berlin', 'paris', 'amsterdam'],
            'Asia': ['asia', 'china', 'india', 'japan', 'singapore', 'hong kong'],
            'Global': ['global', 'worldwide', 'international', 'across the world'],
            'USA': ['usa', 'united states', 'america', 'american', 'nationwide', 'national', 'across the country']
        }
        
        for region, keywords in geo_regions.items():
            if any(keyword in text_lower for keyword in keywords):
                return region
        
        # Default to USA if nothing specific found
        return 'USA'
    
    def scrape_vc_website(self, url):
        """
        Scrape a VC website to extract relevant information.
        
        Args:
            url (str): Website URL
            
        Returns:
            dict: Extracted information
        """
        logger.info(f"Scraping VC website: {url}")
        
        # Initialize results dictionary
        results = {
            'about': '',
            'thesis': '',
            'portfolio': [],
            'team': [],
            'sectors': [],
            'stages': [],
            'check_range': None,
            'geo_focus': 'USA',  # Default
            'status': 'Active'  # Default
        }
        
        # Make initial request to homepage
        response = self._make_request(url)
        if not response:
            logger.warning(f"Could not access {url}")
            results['status'] = 'Disabled'  # Mark as disabled if site is inaccessible
            return results
        
        # Parse homepage
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract about text from meta description or main content
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            results['about'] = meta_desc.get('content').strip()
        
        # If no meta description, try to get first paragraph of main content
        if not results['about']:
            main_content = soup.find(['main', 'article', 'div', 'section'], class_=lambda c: c and any(
                keyword in (c.lower() if c else '') for keyword in ['content', 'main', 'about']
            ))
            
            if main_content:
                first_p = main_content.find('p')
                if first_p:
                    results['about'] = first_p.get_text().strip()
            
            # Fallback to first paragraph on page
            if not results['about']:
                first_p = soup.find('p')
                if first_p:
                    results['about'] = first_p.get_text().strip()
        
        # Extract links to other pages on the site
        internal_links = self._extract_links(soup, url)
        
        # Extract data from homepage
        results['portfolio'].extend(self._extract_portfolio(soup, url))
        results['team'].extend(self._extract_team(soup))
        thesis_text = self._extract_investment_thesis(soup)
        if thesis_text:
            results['thesis'] = thesis_text
        
        # Define important pages to check
        important_pages = {
            'about': ['about', 'about-us', 'who-we-are', 'team', 'our-team'],
            'portfolio': ['portfolio', 'companies', 'investments', 'our-portfolio'],
            'approach': ['approach', 'strategy', 'thesis', 'investment-strategy', 'how-we-invest']
        }
        
        # Find and visit important pages
        for page_type, keywords in important_pages.items():
            for link in internal_links:
                link_path = urlparse(link).path.lower()
                if any(keyword in link_path for keyword in keywords) and link != url:
                    resp = self._make_request(link)
                    if resp:
                        page_soup = BeautifulSoup(resp.text, 'html.parser')
                        
                        if page_type == 'about':
                            about_content = page_soup.find(['main', 'article', 'div', 'section'], class_=lambda c: c and any(
                                keyword in (c.lower() if c else '') for keyword in ['content', 'main', 'about']
                            ))
                            if about_content:
                                about_paragraphs = about_content.find_all('p')
                                about_text = ' '.join([p.get_text().strip() for p in about_paragraphs[:3]])
                                if about_text:
                                    results['about'] = about_text
                            
                            team_members = self._extract_team(page_soup)
                            if team_members:
                                results['team'] = team_members
                        
                        elif page_type == 'portfolio':
                            portfolio_companies = self._extract_portfolio(page_soup, link)
                            if portfolio_companies:
                                results['portfolio'] = portfolio_companies
                        
                        elif page_type == 'approach':
                            thesis_text = self._extract_investment_thesis(page_soup)
                            if thesis_text:
                                results['thesis'] = thesis_text
        
        # Combine all text for analysis
        all_text = results['about'] + ' ' + results['thesis']
        for member in results['team']:
            if member.get('bio'):
                all_text += ' ' + member['bio']
        
        # Extract sectors, stages, check sizes, and geo focus from combined text
        results['sectors'] = self._extract_sectors(all_text)
        results['stages'] = self._extract_stages(all_text)
        results['check_range'] = self._extract_check_sizes(all_text)
        results['geo_focus'] = self._extract_geo_focus(all_text)
        
        return results
    
    def scrape_multiple_vcs(self, vc_list):
        """
        Scrape multiple VC websites in parallel.
        
        Args:
            vc_list (list): List of dictionaries with VC information
                Each dict should have 'Name' and 'Website' keys
            
        Returns:
            list: Enhanced VC data with scraped information
        """
        enhanced_vcs = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Start scraping tasks
            future_to_vc = {
                executor.submit(self.scrape_vc_website, vc['Website']): vc
                for vc in vc_list if vc.get('Website')
            }
            
            # Process results as they complete
            for future in as_completed(future_to_vc):
                vc = future_to_vc[future]
                try:
                    scraped_data = future.result()
                    
                    # Create check range string
                    check_range_str = None
                    if scraped_data['check_range']:
                        min_check, max_check = scraped_data['check_range']
                        if min_check >= 1000:
                            check_range_str = f"${min_check/1000:.1f}M-{max_check/1000:.1f}M"
                        else:
                            check_range_str = f"${min_check}k-{max_check}k"
                    
                    # Detect sweet spot
                    check_sweet_spot = None
                    if scraped_data['check_range']:
                        min_check, max_check = scraped_data['check_range']
                        # Use the geometric mean as a simple heuristic for the sweet spot
                        sweet_spot = (min_check * max_check) ** 0.5
                        if sweet_spot >= 1000:
                            check_sweet_spot = f"${sweet_spot/1000:.1f}M"
                        else:
                            check_sweet_spot = f"${sweet_spot:.0f}k"
                    
                    # Create enhanced VC record
                    enhanced_vc = {
                        **vc,  # Include original data
                        'About': scraped_data['about'],
                        'Investment Thesis': scraped_data['thesis'],
                        'Sector Focus': scraped_data['sectors'],
                        'Preferred Deal Stage': scraped_data['stages'],
                        'Check Range': check_range_str,
                        'Check Sweet Spot': check_sweet_spot,
                        'Geo Focus': scraped_data['geo_focus'],
                        'Status': scraped_data['status'],
                        'Portfolio': scraped_data['portfolio'],
                        'Team': scraped_data['team'],
                        # Default Lead/Follow to "Both" if undetermined
                        'Lead/Follow': "Both"
                    }
                    
                    enhanced_vcs.append(enhanced_vc)
                    logger.info(f"Successfully scraped {vc['Name']}")
                    
                except Exception as e:
                    logger.error(f"Error processing {vc.get('Name', 'Unknown VC')}: {str(e)}")
                    # Add basic data with default values
                    enhanced_vcs.append({
                        **vc,
                        'About': '',
                        'Investment Thesis': '',
                        'Sector Focus': [],
                        'Preferred Deal Stage': [],
                        'Check Range': None,
                        'Check Sweet Spot': None,
                        'Geo Focus': 'USA',
                        'Status': 'Unknown',
                        'Portfolio': [],
                        'Team': [],
                        'Lead/Follow': "Unknown"
                    })
        
        return enhanced_vcs


# Example usage:
if __name__ == "__main__":
    # Sample VCs for testing
    test_vcs = [
        {"Name": "Acme Ventures", "Website": "acmeventures.com"},
        {"Name": "Beta Capital", "Website": "beta-capital.com"},
        {"Name": "Gamma Partners", "Website": "gammapartners.vc"}
    ]
    
    scraper = VCWebScraper(max_pages=5, max_workers=3)
    results = scraper.scrape_multiple_vcs(test_vcs)
    
    # Print results
    for vc in results:
        print(f"\n=== {vc['Name']} ===")
        print(f"About: {vc['About'][:100]}...")
        print(f"Sectors: {', '.join(vc['Sector Focus'])}")
        print(f"Stages: {', '.join(vc['Preferred Deal Stage'])}")
        print(f"Check Range: {vc['Check Range']}")
        print(f"Portfolio: {len(vc['Portfolio'])} companies")