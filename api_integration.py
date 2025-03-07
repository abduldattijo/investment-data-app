import requests
import pandas as pd
import json
import time
import os
import logging
from typing import List, Dict, Any, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('api_integration')

class ExternalDataProvider:
    """
    Abstract base class for external data providers.
    """
    def get_vc_info(self, name: str) -> Dict[str, Any]:
        """
        Get information about a VC firm.
        
        Args:
            name (str): Name of the VC firm
            
        Returns:
            Dict: Information about the VC firm
        """
        raise NotImplementedError("Subclasses must implement this method")
    
    def get_portfolio(self, vc_id: str) -> List[Dict[str, Any]]:
        """
        Get portfolio companies for a VC firm.
        
        Args:
            vc_id (str): ID of the VC firm
            
        Returns:
            List[Dict]: List of portfolio companies
        """
        raise NotImplementedError("Subclasses must implement this method")
    
    def get_deals(self, vc_id: str) -> List[Dict[str, Any]]:
        """
        Get investment deals for a VC firm.
        
        Args:
            vc_id (str): ID of the VC firm
            
        Returns:
            List[Dict]: List of investment deals
        """
        raise NotImplementedError("Subclasses must implement this method")


class MockCrunchbaseProvider(ExternalDataProvider):
    """
    Mock implementation of Crunchbase API.
    In a real implementation, this would use the actual Crunchbase API.
    """
    def __init__(self, api_key: Optional[str] = None, mock_data_path: Optional[str] = None):
        """
        Initialize the Crunchbase API client.
        
        Args:
            api_key (str): Crunchbase API key
            mock_data_path (str): Path to mock data files (for development)
        """
        self.api_key = api_key or os.getenv("CRUNCHBASE_API_KEY")
        self.base_url = "https://api.crunchbase.com/api/v4"
        self.mock_data_path = mock_data_path
        
        # Setup session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Make a request to the Crunchbase API.
        
        Args:
            endpoint (str): API endpoint
            params (Dict): Query parameters
            
        Returns:
            Dict: API response
        """
        # For mock implementation, generate realistic-looking data
        if self.mock_data_path:
            # Check if we have a mock file for this endpoint
            endpoint_name = endpoint.split("/")[-1]
            mock_file = os.path.join(self.mock_data_path, f"{endpoint_name}.json")
            
            if os.path.exists(mock_file):
                with open(mock_file, "r") as f:
                    return json.load(f)
        
        # Generate mock data
        time.sleep(0.2)  # Simulate API latency
        
        if "organizations" in endpoint:
            return self._generate_mock_organization()
        elif "portfolio" in endpoint:
            return self._generate_mock_portfolio()
        elif "deals" in endpoint:
            return self._generate_mock_deals()
        else:
            return {"data": {}, "count": 0}
    
    def _generate_mock_organization(self) -> Dict[str, Any]:
        """Generate mock organization data"""
        return {
            "data": {
                "uuid": "mock-uuid",
                "properties": {
                    "name": "Mock Ventures",
                    "short_description": "A leading venture capital firm investing in technology startups.",
                    "website": "mockventures.com",
                    "founded_on": "2010-01-01",
                    "location_identifiers": [{"value": "San Francisco, CA"}],
                    "categories": [
                        {"value": "Venture Capital"},
                        {"value": "Finance"}
                    ]
                }
            }
        }
    
    def _generate_mock_portfolio(self) -> Dict[str, Any]:
        """Generate mock portfolio data"""
        mock_companies = [
            {
                "uuid": f"company-{i}",
                "properties": {
                    "name": f"Portfolio Company {i}",
                    "short_description": f"Description for portfolio company {i}",
                    "website": f"company{i}.com",
                    "founded_on": "2018-01-01",
                    "categories": [{"value": "Software"}, {"value": "Artificial Intelligence"}]
                }
            } for i in range(1, 11)
        ]
        
        return {
            "data": {
                "cards": mock_companies
            },
            "count": len(mock_companies)
        }
    
    def _generate_mock_deals(self) -> Dict[str, Any]:
        """Generate mock deals data"""
        stages = ["Seed", "Series A", "Series B", "Series C"]
        amounts = [750000, 2500000, 8000000, 15000000]
        
        mock_deals = [
            {
                "uuid": f"deal-{i}",
                "properties": {
                    "name": f"Funding Round {i}",
                    "announced_on": f"2022-{i:02d}-01",
                    "investment_type": stages[i % len(stages)],
                    "money_raised": amounts[i % len(amounts)],
                    "lead_investor_identifiers": [{"value": "Lead VC"}] if i % 3 == 0 else [],
                    "investor_identifiers": [{"value": "Mock Ventures"}],
                    "organization_identifiers": [{"value": f"Portfolio Company {i}"}]
                }
            } for i in range(1, 16)
        ]
        
        return {
            "data": {
                "cards": mock_deals
            },
            "count": len(mock_deals)
        }
    
    def get_vc_info(self, name: str) -> Dict[str, Any]:
        """
        Get information about a VC firm.
        
        Args:
            name (str): Name of the VC firm
            
        Returns:
            Dict: Information about the VC firm
        """
        endpoint = f"/organizations/lookup"
        params = {
            "name": name,
            "field_ids": "name,short_description,website,founded_on,location_identifiers,categories"
        }
        
        response = self._make_request(endpoint, params)
        
        # Process response
        info = {}
        if "data" in response and "properties" in response["data"]:
            props = response["data"]["properties"]
            info = {
                "uuid": response["data"].get("uuid", ""),
                "name": props.get("name", ""),
                "description": props.get("short_description", ""),
                "website": props.get("website", ""),
                "founded": props.get("founded_on", ""),
                "location": props.get("location_identifiers", [{}])[0].get("value", "") if props.get("location_identifiers") else "",
                "categories": [c.get("value", "") for c in props.get("categories", [])]
            }
        
        return info
    
    def get_portfolio(self, vc_id: str) -> List[Dict[str, Any]]:
        """
        Get portfolio companies for a VC firm.
        
        Args:
            vc_id (str): ID of the VC firm
            
        Returns:
            List[Dict]: List of portfolio companies
        """
        endpoint = f"/organizations/{vc_id}/portfolio"
        params = {
            "field_ids": "name,short_description,website,founded_on,categories",
            "limit": 50
        }
        
        response = self._make_request(endpoint, params)
        
        # Process response
        portfolio = []
        if "data" in response and "cards" in response["data"]:
            for card in response["data"]["cards"]:
                if "properties" in card:
                    props = card["properties"]
                    company = {
                        "uuid": card.get("uuid", ""),
                        "name": props.get("name", ""),
                        "description": props.get("short_description", ""),
                        "website": props.get("website", ""),
                        "founded": props.get("founded_on", ""),
                        "categories": [c.get("value", "") for c in props.get("categories", [])]
                    }
                    portfolio.append(company)
        
        return portfolio
    
    def get_deals(self, vc_id: str) -> List[Dict[str, Any]]:
        """
        Get investment deals for a VC firm.
        
        Args:
            vc_id (str): ID of the VC firm
            
        Returns:
            List[Dict]: List of investment deals
        """
        endpoint = f"/organizations/{vc_id}/participated_funding_rounds"
        params = {
            "field_ids": "name,announced_on,investment_type,money_raised,lead_investor_identifiers,investor_identifiers,organization_identifiers",
            "limit": 50
        }
        
        response = self._make_request(endpoint, params)
        
        # Process response
        deals = []
        if "data" in response and "cards" in response["data"]:
            for card in response["data"]["cards"]:
                if "properties" in card:
                    props = card["properties"]
                    
                    # Determine if this VC was a lead investor
                    lead_investors = [li.get("value", "") for li in props.get("lead_investor_identifiers", [])]
                    is_lead = any(name in li for li in lead_investors)
                    
                    deal = {
                        "uuid": card.get("uuid", ""),
                        "name": props.get("name", ""),
                        "date": props.get("announced_on", ""),
                        "stage": props.get("investment_type", ""),
                        "amount": props.get("money_raised", 0),
                        "is_lead": is_lead,
                        "company": props.get("organization_identifiers", [{}])[0].get("value", "") if props.get("organization_identifiers") else ""
                    }
                    deals.append(deal)
        
        return deals


class MockPitchbookProvider(ExternalDataProvider):
    """
    Mock implementation of PitchBook API.
    In a real implementation, this would use the actual PitchBook API.
    """
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the PitchBook API client.
        
        Args:
            api_key (str): PitchBook API key
        """
        self.api_key = api_key or os.getenv("PITCHBOOK_API_KEY")
        self.base_url = "https://api.pitchbook.com/v1"
        
        # Setup session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Make a request to the PitchBook API.
        
        Args:
            endpoint (str): API endpoint
            params (Dict): Query parameters
            
        Returns:
            Dict: API response
        """
        # Generate mock data
        time.sleep(0.2)  # Simulate API latency
        
        if "investors" in endpoint:
            return self._generate_mock_investor()
        elif "portfolio" in endpoint:
            return self._generate_mock_portfolio()
        elif "deals" in endpoint:
            return self._generate_mock_deals()
        else:
            return {"data": [], "count": 0}
    
    def _generate_mock_investor(self) -> Dict[str, Any]:
        """Generate mock investor data"""
        return {
            "id": "mock-pb-id",
            "name": "PB Ventures",
            "description": "A venture capital firm with a focus on enterprise technology and fintech.",
            "website": "pbventures.com",
            "foundedDate": "2012",
            "headquarters": "New York, NY",
            "aum": 250000000,
            "sectors": ["Enterprise Software", "Financial Technology", "Data & Analytics"],
            "investmentStages": ["Early Stage", "Late Stage"],
            "status": "Active"
        }
    
    def _generate_mock_portfolio(self) -> List[Dict[str, Any]]:
        """Generate mock portfolio data"""
        return [
            {
                "id": f"comp-{i}",
                "name": f"PB Portfolio Co {i}",
                "description": f"Description for portfolio company {i}",
                "website": f"pbcompany{i}.com",
                "foundedDate": "2019",
                "headquarters": "San Francisco, CA",
                "sectors": ["Enterprise Software", "Artificial Intelligence"],
                "lastFundingStage": ["Series A", "Series B", "Seed"][i % 3],
                "totalFunding": [1000000, 5000000, 15000000][i % 3]
            } for i in range(1, 13)
        ]
    
    def _generate_mock_deals(self) -> List[Dict[str, Any]]:
        """Generate mock deals data"""
        stages = ["Seed", "Series A", "Series B", "Series C"]
        amounts = [800000, 3000000, 9000000, 18000000]
        
        return [
            {
                "id": f"deal-{i}",
                "dealName": f"Funding Round {i}",
                "dealDate": f"2023-{i % 12 + 1:02d}-15",
                "dealType": "Venture Capital",
                "dealStage": stages[i % len(stages)],
                "dealSize": amounts[i % len(amounts)],
                "isLead": bool(i % 3 == 0),
                "investedCompany": f"PB Portfolio Co {i % 12 + 1}",
                "investedCompanyId": f"comp-{i % 12 + 1}"
            } for i in range(1, 18)
        ]
    
    def get_vc_info(self, name: str) -> Dict[str, Any]:
        """
        Get information about a VC firm.
        
        Args:
            name (str): Name of the VC firm
            
        Returns:
            Dict: Information about the VC firm
        """
        endpoint = f"/investors/search"
        params = {
            "name": name,
            "limit": 1
        }
        
        response = self._make_request(endpoint, params)
        
        # Simplify to match expected format
        if isinstance(response, dict):
            info = {
                "uuid": response.get("id", ""),
                "name": response.get("name", ""),
                "description": response.get("description", ""),
                "website": response.get("website", ""),
                "founded": response.get("foundedDate", ""),
                "location": response.get("headquarters", ""),
                "categories": response.get("sectors", []),
                "investment_stages": response.get("investmentStages", []),
                "aum": response.get("aum", 0),
                "status": response.get("status", "")
            }
            return info
        
        return {}
    
    def get_portfolio(self, vc_id: str) -> List[Dict[str, Any]]:
        """
        Get portfolio companies for a VC firm.
        
        Args:
            vc_id (str): ID of the VC firm
            
        Returns:
            List[Dict]: List of portfolio companies
        """
        endpoint = f"/investors/{vc_id}/portfolio"
        params = {"limit": 50}
        
        response = self._make_request(endpoint, params)
        
        # Process response
        portfolio = []
        if isinstance(response, list):
            for company in response:
                portfolio_item = {
                    "uuid": company.get("id", ""),
                    "name": company.get("name", ""),
                    "description": company.get("description", ""),
                    "website": company.get("website", ""),
                    "founded": company.get("foundedDate", ""),
                    "categories": company.get("sectors", []),
                    "last_funding": company.get("lastFundingStage", ""),
                    "total_funding": company.get("totalFunding", 0)
                }
                portfolio.append(portfolio_item)
        
        return portfolio
    
    def get_deals(self, vc_id: str) -> List[Dict[str, Any]]:
        """
        Get investment deals for a VC firm.
        
        Args:
            vc_id (str): ID of the VC firm
            
        Returns:
            List[Dict]: List of investment deals
        """
        endpoint = f"/investors/{vc_id}/deals"
        params = {"limit": 50}
        
        response = self._make_request(endpoint, params)
        
        # Process response
        deals = []
        if isinstance(response, list):
            for deal in response:
                deal_item = {
                    "uuid": deal.get("id", ""),
                    "name": deal.get("dealName", ""),
                    "date": deal.get("dealDate", ""),
                    "stage": deal.get("dealStage", ""),
                    "amount": deal.get("dealSize", 0),
                    "is_lead": deal.get("isLead", False),
                    "company": deal.get("investedCompany", "")
                }
                deals.append(deal_item)
        
        return deals


class VCDataEnricher:
    """
    Enriches VC data using multiple external data sources.
    """
    def __init__(
        self, 
        crunchbase_api_key: Optional[str] = None,
        pitchbook_api_key: Optional[str] = None,
        use_mock: bool = True
    ):
        """
        Initialize the VC data enricher.
        
        Args:
            crunchbase_api_key (str): Crunchbase API key
            pitchbook_api_key (str): PitchBook API key
            use_mock (bool): Whether to use mock data providers
        """
        # Initialize data providers
        if use_mock:
            self.crunchbase = MockCrunchbaseProvider(crunchbase_api_key)
            self.pitchbook = MockPitchbookProvider(pitchbook_api_key)
        else:
            # In a real implementation, use the actual API clients
            self.crunchbase = MockCrunchbaseProvider(crunchbase_api_key)
            self.pitchbook = MockPitchbookProvider(pitchbook_api_key)
        
        # Cache for API responses to avoid duplicate requests
        self.cache = {
            "cb_info": {},
            "cb_portfolio": {},
            "cb_deals": {},
            "pb_info": {},
            "pb_portfolio": {},
            "pb_deals": {}
        }
    
    def _get_vc_info(self, name: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Get VC information from both data providers.
        
        Args:
            name (str): Name of the VC firm
            
        Returns:
            Tuple[Dict, Dict]: Crunchbase and PitchBook information
        """
        # Check cache first
        if name in self.cache["cb_info"]:
            cb_info = self.cache["cb_info"][name]
        else:
            cb_info = self.crunchbase.get_vc_info(name)
            self.cache["cb_info"][name] = cb_info
        
        if name in self.cache["pb_info"]:
            pb_info = self.cache["pb_info"][name]
        else:
            pb_info = self.pitchbook.get_vc_info(name)
            self.cache["pb_info"][name] = pb_info
        
        return cb_info, pb_info
    
    def _get_portfolio_and_deals(
        self, 
        cb_id: str, 
        pb_id: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Get portfolio and deals from both data providers.
        
        Args:
            cb_id (str): Crunchbase VC ID
            pb_id (str): PitchBook VC ID
            
        Returns:
            Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]: 
                Crunchbase portfolio, Crunchbase deals, PitchBook portfolio, PitchBook deals
        """
        # Check cache first
        if cb_id in self.cache["cb_portfolio"]:
            cb_portfolio = self.cache["cb_portfolio"][cb_id]
        else:
            cb_portfolio = self.crunchbase.get_portfolio(cb_id) if cb_id else []
            self.cache["cb_portfolio"][cb_id] = cb_portfolio
        
        if cb_id in self.cache["cb_deals"]:
            cb_deals = self.cache["cb_deals"][cb_id]
        else:
            cb_deals = self.crunchbase.get_deals(cb_id) if cb_id else []
            self.cache["cb_deals"][cb_id] = cb_deals
        
        if pb_id in self.cache["pb_portfolio"]:
            pb_portfolio = self.cache["pb_portfolio"][pb_id]
        else:
            pb_portfolio = self.pitchbook.get_portfolio(pb_id) if pb_id else []
            self.cache["pb_portfolio"][pb_id] = pb_portfolio
        
        if pb_id in self.cache["pb_deals"]:
            pb_deals = self.cache["pb_deals"][pb_id]
        else:
            pb_deals = self.pitchbook.get_deals(pb_id) if pb_id else []
            self.cache["pb_deals"][pb_id] = pb_deals
        
        return cb_portfolio, cb_deals, pb_portfolio, pb_deals
    
    def _merge_company_data(
        self, 
        cb_companies: List[Dict[str, Any]], 
        pb_companies: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge company data from multiple sources.
        
        Args:
            cb_companies (List[Dict]): Crunchbase companies
            pb_companies (List[Dict]): PitchBook companies
            
        Returns:
            List[Dict]: Merged company data
        """
        merged = {}
        
        # Process Crunchbase companies
        for company in cb_companies:
            name = company.get("name", "").lower()
            if name:
                merged[name] = company
        
        # Process PitchBook companies (add or update)
        for company in pb_companies:
            name = company.get("name", "").lower()
            if not name:
                continue
                
            if name in merged:
                # Update existing entry
                for key, value in company.items():
                    if value and (key not in merged[name] or not merged[name][key]):
                        merged[name][key] = value
            else:
                # Add new entry
                merged[name] = company
        
        return list(merged.values())
    
    def _merge_deal_data(
        self, 
        cb_deals: List[Dict[str, Any]], 
        pb_deals: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge deal data from multiple sources.
        
        Args:
            cb_deals (List[Dict]): Crunchbase deals
            pb_deals (List[Dict]): PitchBook deals
            
        Returns:
            List[Dict]: Merged deal data
        """
        merged = {}
        
        # Helper to create a unique key for a deal
        def deal_key(deal):
            company = deal.get("company", "").lower()
            date = deal.get("date", "")
            stage = deal.get("stage", "").lower()
            return f"{company}|{date}|{stage}"
        
        # Process Crunchbase deals
        for deal in cb_deals:
            key = deal_key(deal)
            if key:
                merged[key] = deal
        
        # Process PitchBook deals (add or update)
        for deal in pb_deals:
            key = deal_key(deal)
            if not key:
                continue
                
            if key in merged:
                # Update existing entry
                for k, v in deal.items():
                    if v and (k not in merged[key] or not merged[key][k]):
                        merged[key][k] = v
            else:
                # Add new entry
                merged[key] = deal
        
        return list(merged.values())
    
    def _derive_sector_focus(self, portfolio: List[Dict[str, Any]]) -> List[str]:
        """
        Derive sector focus from portfolio companies.
        
        Args:
            portfolio (List[Dict]): Portfolio companies
            
        Returns:
            List[str]: List of sector focuses
        """
        sector_counts = {}
        
        # Count sectors across all portfolio companies
        for company in portfolio:
            categories = company.get("categories", [])
            for category in categories:
                if isinstance(category, str) and category:
                    sector_counts[category] = sector_counts.get(category, 0) + 1
        
        # Get top sectors (at least 2 companies must have the sector)
        top_sectors = [
            sector for sector, count in sector_counts.items()
            if count >= 2
        ]
        
        # Sort by count (descending)
        top_sectors.sort(key=lambda s: sector_counts[s], reverse=True)
        
        # Limit to top 10 sectors
        return top_sectors[:10]
    
    def _derive_stage_preference(self, deals: List[Dict[str, Any]]) -> List[str]:
        """
        Derive investment stage preferences from deals.
        
        Args:
            deals (List[Dict]): Investment deals
            
        Returns:
            List[str]: List of preferred stages
        """
        stage_counts = {}
        
        # Count stages across all deals
        for deal in deals:
            stage = deal.get("stage", "")
            if stage:
                # Normalize stage names
                norm_stage = self._normalize_stage(stage)
                if norm_stage:
                    stage_counts[norm_stage] = stage_counts.get(norm_stage, 0) + 1
        
        # Get stages with at least 2 deals
        preferred_stages = [
            stage for stage, count in stage_counts.items()
            if count >= 2
        ]
        
        # Sort by count (descending)
        preferred_stages.sort(key=lambda s: stage_counts[s], reverse=True)
        
        return preferred_stages
    
    def _normalize_stage(self, stage: str) -> str:
        """
        Normalize investment stage names.
        
        Args:
            stage (str): Original stage name
            
        Returns:
            str: Normalized stage name
        """
        stage_lower = stage.lower()
        
        if "pre-seed" in stage_lower or "preseed" in stage_lower:
            return "Pre-seed"
        elif "seed" in stage_lower or "angel" in stage_lower:
            return "Seed"
        elif "series a" in stage_lower:
            return "Series A"
        elif "series b" in stage_lower:
            return "Series B"
        elif any(s in stage_lower for s in ["series c", "series d", "series e", "series f"]):
            return "Series C+"
        elif "late" in stage_lower and "stage" in stage_lower:
            return "Series C+"
        elif "early" in stage_lower and "stage" in stage_lower:
            return "Seed+"
        else:
            return ""
    
    def _calculate_check_range(self, deals: List[Dict[str, Any]]) -> Tuple[float, float]:
        """
        Calculate the range of check sizes from deals.
        
        Args:
            deals (List[Dict]): Investment deals
            
        Returns:
            Tuple[float, float]: Minimum and maximum check sizes (in thousands)
        """
        amounts = []
        
        # Collect amounts from all deals
        for deal in deals:
            amount = deal.get("amount", 0)
            if amount and amount > 0:
                amounts.append(amount)
        
        if not amounts:
            return (0, 0)
        
        # Calculate min and max (converted to thousands)
        min_amount = min(amounts) / 1000
        max_amount = max(amounts) / 1000
        
        # Round to nearest 50k
        min_amount = round(min_amount / 50) * 50
        max_amount = round(max_amount / 50) * 50
        
        return (min_amount, max_amount)
    
    def _format_check_range(self, min_amount: float, max_amount: float) -> str:
        """
        Format check range as a string.
        
        Args:
            min_amount (float): Minimum amount in thousands
            max_amount (float): Maximum amount in thousands
            
        Returns:
            str: Formatted check range
        """
        if min_amount == 0 and max_amount == 0:
            return "Unknown"
        
        # Format in K or M
        if min_amount >= 1000:
            min_str = f"${min_amount/1000:.1f}M"
        else:
            min_str = f"${min_amount:.0f}k"
        
        if max_amount >= 1000:
            max_str = f"${max_amount/1000:.1f}M"
        else:
            max_str = f"${max_amount:.0f}k"
        
        return f"{min_str}-{max_str}"
    
    def _calculate_sweet_spot(self, deals: List[Dict[str, Any]]) -> float:
        """
        Calculate the sweet spot check size from deals.
        
        Args:
            deals (List[Dict]): Investment deals
            
        Returns:
            float: Sweet spot check size (in thousands)
        """
        amounts = []
        
        # Collect amounts from all deals
        for deal in deals:
            amount = deal.get("amount", 0)
            if amount and amount > 0:
                amounts.append(amount)
        
        if not amounts:
            return 0
        
        # Use median as the sweet spot
        amounts.sort()
        if len(amounts) % 2 == 0:
            median = (amounts[len(amounts)//2 - 1] + amounts[len(amounts)//2]) / 2
        else:
            median = amounts[len(amounts)//2]
        
        # Convert to thousands
        sweet_spot = median / 1000
        
        # Round to nearest 50k
        sweet_spot = round(sweet_spot / 50) * 50
        
        return sweet_spot
    
    def _format_sweet_spot(self, sweet_spot: float) -> str:
        """
        Format sweet spot as a string.
        
        Args:
            sweet_spot (float): Sweet spot in thousands
            
        Returns:
            str: Formatted sweet spot
        """
        if sweet_spot == 0:
            return "Unknown"
        
        # Format in K or M
        if sweet_spot >= 1000:
            return f"${sweet_spot/1000:.1f}M"
        else:
            return f"${sweet_spot:.0f}k"
    
    def _determine_lead_follow(self, deals: List[Dict[str, Any]]) -> str:
        """
        Determine if a VC typically leads or follows.
        
        Args:
            deals (List[Dict]): Investment deals
            
        Returns:
            str: 'Lead', 'Follow', or 'Both'
        """
        lead_count = 0
        total_count = 0
        
        for deal in deals:
            is_lead = deal.get("is_lead", False)
            if is_lead:
                lead_count += 1
            total_count += 1
        
        if total_count == 0:
            return "Unknown"
        
        lead_ratio = lead_count / total_count
        
        if lead_ratio >= 0.7:
            return "Lead"
        elif lead_ratio <= 0.3:
            return "Follow"
        else:
            return "Both"
    
    def _determine_geo_focus(
        self, 
        cb_info: Dict[str, Any], 
        pb_info: Dict[str, Any],
        portfolio: List[Dict[str, Any]]
    ) -> str:
        """
        Determine geographical focus.
        
        Args:
            cb_info (Dict): Crunchbase info
            pb_info (Dict): PitchBook info
            portfolio (List[Dict]): Portfolio companies
            
        Returns:
            str: Geographical focus
        """
        # Check if location info is available in VC info
        cb_location = cb_info.get("location", "")
        pb_location = pb_info.get("location", "")
        
        location = cb_location or pb_location
        
        if location:
            # Map location to region
            location_lower = location.lower()
            
            if any(loc in location_lower for loc in [
                "san francisco", "palo alto", "menlo park", "mountain view", 
                "silicon valley", "bay area", "san jose", "oakland"
            ]):
                return "Silicon Valley"
            
            elif any(loc in location_lower for loc in [
                "new york", "nyc", "brooklyn", "manhattan"
            ]):
                return "New York"
            
            elif any(loc in location_lower for loc in [
                "boston", "cambridge, ma", "massachusetts"
            ]):
                return "Boston"
            
            elif any(loc in location_lower for loc in [
                "chicago", "detroit", "minneapolis", "ohio", "michigan", 
                "illinois", "wisconsin", "indiana"
            ]):
                return "Midwest"
            
            elif any(loc in location_lower for loc in [
                "atlanta", "miami", "florida", "carolina", "tennessee", 
                "georgia", "alabama", "louisiana"
            ]):
                return "Southeast"
            
            elif any(loc in location_lower for loc in [
                "texas", "austin", "dallas", "houston"
            ]):
                return "Texas"
            
            elif any(loc in location_lower for loc in [
                "seattle", "portland", "washington", "oregon"
            ]):
                return "Pacific Northwest"
            
            elif any(loc in location_lower for loc in [
                "london", "berlin", "paris", "amsterdam", "europe"
            ]):
                return "Europe"
            
            elif any(loc in location_lower for loc in [
                "china", "india", "japan", "singapore", "hong kong", "asia"
            ]):
                return "Asia"
            
            elif "ca" in location_lower or "california" in location_lower:
                return "California"
        
        # Default to USA
        return "USA"
    
    def _derive_investment_thesis(
        self, 
        cb_info: Dict[str, Any], 
        pb_info: Dict[str, Any],
        sectors: List[str],
        stages: List[str],
        portfolio: List[Dict[str, Any]]
    ) -> str:
        """
        Derive investment thesis from available information.
        
        Args:
            cb_info (Dict): Crunchbase info
            pb_info (Dict): PitchBook info
            sectors (List[str]): Sector focuses
            stages (List[str]): Stage preferences
            portfolio (List[Dict]): Portfolio companies
            
        Returns:
            str: Investment thesis
        """
        # Check if description is available
        cb_desc = cb_info.get("description", "")
        pb_desc = pb_info.get("description", "")
        
        description = cb_desc or pb_desc
        
        if description and len(description) > 20:
            # If we have a good description, use it as the thesis
            return f"Thesis: {description}"
        
        # Otherwise, derive a thesis from sectors, stages, and portfolio
        derived_thesis = "Pattern: "
        
        if sectors:
            derived_thesis += f"Invests in {', '.join(sectors[:3])}"
            if stages:
                derived_thesis += f" at {', '.join(stages)} stages"
            derived_thesis += "."
        elif stages:
            derived_thesis += f"Focuses on {', '.join(stages)} stage investments."
        else:
            derived_thesis += "General investment approach across various sectors and stages."
        
        # Add portfolio insight if available
        if portfolio and len(portfolio) >= 3:
            sample_companies = [c.get("name", "") for c in portfolio[:3] if c.get("name")]
            if sample_companies:
                derived_thesis += f" Portfolio includes {', '.join(sample_companies)}."
        
        return derived_thesis
    
    def enrich_vc_data(self, vc_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single VC record with additional information.
        
        Args:
            vc_data (Dict): Basic VC data with at least 'Name' field
            
        Returns:
            Dict: Enhanced VC data
        """
        logger.info(f"Enriching data for {vc_data.get('Name', 'Unknown VC')}")
        
        name = vc_data.get("Name", "")
        if not name:
            logger.warning("Cannot enrich VC data without a name")
            return vc_data
        
        try:
            # Step 1: Get VC information from data providers
            cb_info, pb_info = self._get_vc_info(name)
            
            # Get IDs for further API calls
            cb_id = cb_info.get("uuid", "")
            pb_id = pb_info.get("uuid", "")
            
            # Step 2: Get portfolio and deals
            cb_portfolio, cb_deals, pb_portfolio, pb_deals = self._get_portfolio_and_deals(cb_id, pb_id)
            
            # Step 3: Merge data from multiple sources
            merged_portfolio = self._merge_company_data(cb_portfolio, pb_portfolio)
            merged_deals = self._merge_deal_data(cb_deals, pb_deals)
            
            # Step 4: Analyze and derive additional fields
            sector_focus = self._derive_sector_focus(merged_portfolio)
            stage_preference = self._derive_stage_preference(merged_deals)
            min_check, max_check = self._calculate_check_range(merged_deals)
            check_range = self._format_check_range(min_check, max_check)
            sweet_spot = self._calculate_sweet_spot(merged_deals)
            sweet_spot_str = self._format_sweet_spot(sweet_spot)
            lead_follow = self._determine_lead_follow(merged_deals)
            geo_focus = self._determine_geo_focus(cb_info, pb_info, merged_portfolio)
            investment_thesis = self._derive_investment_thesis(
                cb_info, pb_info, sector_focus, stage_preference, merged_portfolio
            )
            
            # Determine if the VC is active
            status = "Active"
            if not merged_deals or not any(d.get("date", "").startswith("202") for d in merged_deals):
                if not merged_portfolio:
                    status = "Unknown"
            
            # Step 5: Create enhanced VC record
            enhanced_vc = {
                **vc_data,  # Include original data
                "About": cb_info.get("description", "") or pb_info.get("description", ""),
                "Investment Thesis": investment_thesis,
                "Sector Focus": sector_focus,
                "Preferred Deal Stage": stage_preference,
                "Check Range": check_range,
                "Check Sweet Spot": sweet_spot_str,
                "Geo Focus": geo_focus,
                "Status": status,
                "Lead/Follow": lead_follow,
                "Portfolio": merged_portfolio,
                "Deals": merged_deals
            }
            
            return enhanced_vc
            
        except Exception as e:
            logger.error(f"Error enriching data for {name}: {str(e)}")
            # Return original data with default values
            return {
                **vc_data,
                "About": "",
                "Investment Thesis": "",
                "Sector Focus": [],
                "Preferred Deal Stage": [],
                "Check Range": "Unknown",
                "Check Sweet Spot": "Unknown",
                "Geo Focus": "USA",
                "Status": "Unknown",
                "Lead/Follow": "Unknown",
                "Portfolio": [],
                "Deals": []
            }
    
    def enrich_multiple_vcs(self, vc_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich multiple VC records in parallel.
        
        Args:
            vc_data_list (List[Dict]): List of basic VC data
            
        Returns:
            List[Dict]: List of enhanced VC data
        """
        enhanced_vcs = []
        
        for i, vc in enumerate(vc_data_list):
            try:
                logger.info(f"Processing {i+1}/{len(vc_data_list)}: {vc.get('Name', 'Unknown')}")
                enhanced_vc = self.enrich_vc_data(vc)
                enhanced_vcs.append(enhanced_vc)
            except Exception as e:
                logger.error(f"Error enriching VC {vc.get('Name', 'Unknown')}: {str(e)}")
                # Add with default values
                enhanced_vcs.append({
                    **vc,
                    "About": "",
                    "Investment Thesis": "",
                    "Sector Focus": [],
                    "Preferred Deal Stage": [],
                    "Check Range": "Unknown",
                    "Check Sweet Spot": "Unknown",
                    "Geo Focus": "USA",
                    "Status": "Unknown",
                    "Lead/Follow": "Unknown",
                    "Portfolio": [],
                    "Deals": []
                })
        
        return enhanced_vcs


# Example usage:
if __name__ == "__main__":
    # Sample VCs
    sample_vcs = [
        {"Name": "Acme Ventures", "Website": "acmeventures.com"},
        {"Name": "Beta Capital", "Website": "betacapital.com"},
        {"Name": "Gamma Partners", "Website": "gammapartners.vc"}
    ]
    
    # Initialize the enricher
    enricher = VCDataEnricher(use_mock=True)
    
    # Enrich a single VC
    enhanced_vc = enricher.enrich_vc_data(sample_vcs[0])
    print(f"\n=== {enhanced_vc['Name']} ===")
    print(f"About: {enhanced_vc['About'][:100]}...")
    print(f"Thesis: {enhanced_vc['Investment Thesis']}")
    print(f"Sectors: {', '.join(enhanced_vc['Sector Focus'])}")
    print(f"Stages: {', '.join(enhanced_vc['Preferred Deal Stage'])}")
    print(f"Check Range: {enhanced_vc['Check Range']}")
    print(f"Sweet Spot: {enhanced_vc['Check Sweet Spot']}")
    print(f"Portfolio: {len(enhanced_vc['Portfolio'])} companies")
    
    # Enrich multiple VCs
    enhanced_vcs = enricher.enrich_multiple_vcs(sample_vcs)
    print(f"\nEnriched {len(enhanced_vcs)} VCs")
