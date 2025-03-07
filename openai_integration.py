import openai
from openai import OpenAI
import pandas as pd
import numpy as np
import json
import re
import tiktoken
import time
import logging
from typing import List, Dict, Any, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('vc_matcher')

class VCMatcher:
    """
    Uses OpenAI API to match startups with venture capital firms based on
    startup descriptions and VC profiles.
    """
    
    def __init__(self, api_key: str, model: str = "gpt-3.5-turbo"):
        """
        Initialize the VC Matcher.
        
        Args:
            api_key (str): OpenAI API key
            model (str): OpenAI model to use (default: gpt-3.5-turbo)
        """
        try:
            # Try the new OpenAI client version
            self.client = OpenAI(api_key=api_key)
        except TypeError:
            # Fall back to older style for compatibility
            import openai
            openai.api_key = api_key
            self.client = openai
        
        self.model = model
        try:
            self.encoding = tiktoken.encoding_for_model(model)
        except Exception:
            # Fallback if tiktoken fails
            self.encoding = None
    
    def _count_tokens(self, text: str) -> int:
        """
        Count the number of tokens in a string.
        
        Args:
            text (str): Text to count tokens for
            
        Returns:
            int: Token count
        """
        return len(self.encoding.encode(text))
    
    def _create_vc_context(self, vc_data: List[Dict[str, Any]], max_tokens: int = 3500) -> str:
        """
        Create a context string with VC data for the LLM prompt.
        Ensures the context doesn't exceed token limits.
        
        Args:
            vc_data (List[Dict]): List of VC data dictionaries
            max_tokens (int): Maximum tokens for context
            
        Returns:
            str: Formatted context string
        """
        context_parts = []
        current_tokens = 0
        
        # Sort VCs by completeness of data (more complete profiles first)
        def completeness_score(vc):
            score = 0
            score += 3 if vc.get('Investment Thesis') else 0
            score += 2 if vc.get('Sector Focus') and len(vc.get('Sector Focus', [])) > 0 else 0
            score += 2 if vc.get('Preferred Deal Stage') and len(vc.get('Preferred Deal Stage', [])) > 0 else 0
            score += 1 if vc.get('Check Range') else 0
            score += 1 if vc.get('About') else 0
            score += 1 if len(vc.get('Portfolio', [])) > 0 else 0
            return score
        
        sorted_vcs = sorted(vc_data, key=completeness_score, reverse=True)
        
        for vc in sorted_vcs:
            # Create a summary for this VC
            vc_summary = (
                f"Investor: {vc.get('Name', 'Unknown')}\n"
                f"Website: {vc.get('Website', '')}\n"
            )
            
            if vc.get('Sector Focus') and len(vc.get('Sector Focus', [])) > 0:
                vc_summary += f"Sectors: {', '.join(vc['Sector Focus'])}\n"
            
            if vc.get('Preferred Deal Stage') and len(vc.get('Preferred Deal Stage', [])) > 0:
                vc_summary += f"Stages: {', '.join(vc['Preferred Deal Stage'])}\n"
            
            if vc.get('Check Range'):
                vc_summary += f"Check Size: {vc['Check Range']}\n"
            
            if vc.get('Geo Focus'):
                vc_summary += f"Geography: {vc['Geo Focus']}\n"
            
            if vc.get('Lead/Follow'):
                vc_summary += f"Lead/Follow: {vc['Lead/Follow']}\n"
            
            if vc.get('Investment Thesis'):
                vc_summary += f"Thesis: {vc['Investment Thesis']}\n"
            
            if vc.get('About'):
                vc_summary += f"About: {vc['About'][:200]}...\n"
            
            if len(vc.get('Portfolio', [])) > 0:
                portfolio_sample = vc['Portfolio'][:3]  # Limit to 3 portfolio companies
                portfolio_str = ", ".join([p.get('name', 'Unknown') for p in portfolio_sample])
                vc_summary += f"Portfolio Examples: {portfolio_str}\n"
            
            vc_summary += f"Status: {vc.get('Status', 'Unknown')}\n\n"
            
            # Check if adding this VC would exceed the token limit
            tokens_to_add = self._count_tokens(vc_summary)
            if current_tokens + tokens_to_add > max_tokens:
                break
            
            context_parts.append(vc_summary)
            current_tokens += tokens_to_add
        
        return "".join(context_parts)
    
    def match_startup_to_vcs(
        self, 
        startup_description: str, 
        vc_data: List[Dict[str, Any]], 
        num_matches: int = 5,
        match_criteria: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Match a startup description to the most suitable VCs.
        
        Args:
            startup_description (str): Description of the startup
            vc_data (List[Dict]): List of VC data dictionaries
            num_matches (int): Number of matches to return
            match_criteria (Dict): Additional filtering criteria
                Can include: sector, stage, check_size, geography, lead_follow
            
        Returns:
            List[Dict]: List of matched VCs with match explanations
        """
        # Pre-filter VCs based on explicit criteria if provided
        filtered_vcs = vc_data
        
        if match_criteria:
            if match_criteria.get('sector'):
                filtered_vcs = [
                    vc for vc in filtered_vcs
                    if vc.get('Sector Focus') and any(
                        s.lower() == match_criteria['sector'].lower() 
                        for s in vc.get('Sector Focus', [])
                    )
                ]
            
            if match_criteria.get('stage'):
                filtered_vcs = [
                    vc for vc in filtered_vcs
                    if vc.get('Preferred Deal Stage') and any(
                        s.lower() == match_criteria['stage'].lower() 
                        for s in vc.get('Preferred Deal Stage', [])
                    )
                ]
            
            if match_criteria.get('geography'):
                filtered_vcs = [
                    vc for vc in filtered_vcs
                    if vc.get('Geo Focus') and match_criteria['geography'].lower() in vc['Geo Focus'].lower()
                ]
            
            if match_criteria.get('lead_follow'):
                filtered_vcs = [
                    vc for vc in filtered_vcs
                    if vc.get('Lead/Follow') and (
                        vc['Lead/Follow'].lower() == match_criteria['lead_follow'].lower() or
                        vc['Lead/Follow'] == 'Both'
                    )
                ]
        
        # Further filter to only active VCs
        filtered_vcs = [vc for vc in filtered_vcs if vc.get('Status') == 'Active']
        
        # Create context with filtered VCs (limited by token count)
        vc_context = self._create_vc_context(filtered_vcs)
        
        # If we have no VCs after filtering, return empty results
        if not filtered_vcs:
            return []
        
        # Create the prompt
        prompt = f"""
        You are a VC matching expert. Based on the startup description, find the {num_matches} most suitable 
        investors from the list below. Consider sector match, stage, check size, and thesis alignment.
        
        Startup description: {startup_description}
        
        Available investors:
        {vc_context}
        
        Return a JSON array of objects with:
        1. "name": The exact name of the VC
        2. "match_score": A number from 1-100 indicating how good a match this is
        3. "match_reason": A short explanation of why this VC is a good match
        4. "caution": Optional caution if there's any potential issue with the match
        
        The JSON should be formatted as:
        [
            {{"name": "VC Name", "match_score": 95, "match_reason": "Explanation...", "caution": "Optional caution"}}
        ]
        """
        
        try:
            # Get LLM response
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            
            result_text = response.choices[0].message.content
            
            # Extract JSON from the response
            json_match = re.search(r'\[.*\]', result_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                try:
                    matches = json.loads(json_str)
                    
                    # Get the full VC data for each match
                    result = []
                    for match in matches:
                        vc_name = match.get('name')
                        matching_vcs = [vc for vc in vc_data if vc.get('Name') == vc_name]
                        
                        if matching_vcs:
                            # Add the match info to the VC data
                            vc_data = matching_vcs[0].copy()
                            vc_data['match_score'] = match.get('match_score')
                            vc_data['match_reason'] = match.get('match_reason')
                            vc_data['caution'] = match.get('caution', '')
                            result.append(vc_data)
                    
                    return result
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON from OpenAI response: {str(e)}")
            
            # Fallback: simple matching if JSON parsing fails
            return self._fallback_matching(startup_description, filtered_vcs, num_matches)
            
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {str(e)}")
            # Fallback to simple keyword matching
            return self._fallback_matching(startup_description, filtered_vcs, num_matches)
    
    def _fallback_matching(
        self, 
        startup_description: str, 
        vc_data: List[Dict[str, Any]], 
        num_matches: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Simple keyword-based matching as a fallback when the API call fails.
        
        Args:
            startup_description (str): Description of the startup
            vc_data (List[Dict]): List of VC data dictionaries
            num_matches (int): Number of matches to return
            
        Returns:
            List[Dict]: List of matched VCs
        """
        description_lower = startup_description.lower()
        matches = []
        
        # Extract keywords from the description
        sector_keywords = [
            ('fintech', ['fintech', 'financial', 'banking', 'payment', 'insurance']),
            ('enterprise saas', ['enterprise', 'saas', 'software', 'b2b']),
            ('health tech', ['health', 'medical', 'biotech', 'healthcare']),
            ('ai/ml', ['ai', 'artificial intelligence', 'machine learning', 'ml']),
            ('ecommerce', ['ecommerce', 'e-commerce', 'retail', 'consumer']),
            ('edtech', ['education', 'learning', 'edtech']),
            ('climate tech', ['climate', 'clean', 'sustainability', 'green']),
        ]
        
        stage_keywords = [
            ('pre-seed', ['pre-seed', 'pre seed', 'idea', 'concept']),
            ('seed', ['seed', 'early', 'prototype']),
            ('series a', ['series a', 'growth', 'revenue']),
            ('series b', ['series b', 'scale', 'expansion']),
        ]
        
        # Find matching sectors and stages from description
        matched_sectors = []
        for sector, keywords in sector_keywords:
            if any(kw in description_lower for kw in keywords):
                matched_sectors.append(sector)
        
        matched_stages = []
        for stage, keywords in stage_keywords:
            if any(kw in description_lower for kw in keywords):
                matched_stages.append(stage)
        
        # Check for lead/follow preference
        lead_preference = None
        if any(kw in description_lower for kw in ['lead investor', 'lead round']):
            lead_preference = 'Lead'
        elif any(kw in description_lower for kw in ['follow-on', 'follow on']):
            lead_preference = 'Follow'
        
        # Score each VC
        for vc in vc_data:
            score = 0
            match_reasons = []
            
            # Match sectors
            if vc.get('Sector Focus') and matched_sectors:
                for sector in matched_sectors:
                    if any(vc_sector.lower() == sector.lower() for vc_sector in vc['Sector Focus']):
                        score += 30
                        match_reasons.append(f"Sector match: {sector}")
            
            # Match stages
            if vc.get('Preferred Deal Stage') and matched_stages:
                for stage in matched_stages:
                    if any(vc_stage.lower() == stage.lower() for vc_stage in vc['Preferred Deal Stage']):
                        score += 20
                        match_reasons.append(f"Stage match: {stage}")
            
            # Match lead/follow
            if lead_preference and vc.get('Lead/Follow'):
                if vc['Lead/Follow'] == lead_preference or vc['Lead/Follow'] == 'Both':
                    score += 10
                    match_reasons.append(f"Lead/Follow match: {lead_preference}")
            
            # Add score for thesis match (simple keyword matching)
            if vc.get('Investment Thesis') and any(
                keyword in vc['Investment Thesis'].lower() 
                for keyword in description_lower.split()
                if len(keyword) > 4  # Only consider meaningful keywords
            ):
                score += 15
                match_reasons.append("Thesis keywords match")
            
            # Add a small score for active status
            if vc.get('Status') == 'Active':
                score += 5
            
            if score > 0:
                matches.append({
                    **vc,
                    'match_score': min(score, 100),  # Cap at 100
                    'match_reason': "; ".join(match_reasons),
                    'caution': "This match was generated by a simplified algorithm due to API limitations."
                })
        
        # Sort by score and take top N
        matches.sort(key=lambda x: x['match_score'], reverse=True)
        return matches[:num_matches]
    
    def extract_startup_attributes(self, startup_description: str) -> Dict[str, Any]:
        """
        Extract structured attributes from a startup description using NLP.
        
        Args:
            startup_description (str): Detailed description of the startup
            
        Returns:
            Dict: Structured attributes (sector, stage, funding_needs, etc.)
        """
        # Create a prompt for attribute extraction
        prompt = f"""
        Extract the following attributes from this startup description:
        
        Description: {startup_description}
        
        Please output a JSON object with these fields:
        1. "sector": The primary industry sector (e.g., Fintech, Health Tech, AI/ML)
        2. "stage": The startup's current stage (e.g., Pre-seed, Seed, Series A)
        3. "funding_needs": How much funding they're seeking
        4. "location": Geographic location of the startup
        5. "lead_preference": Whether they need a lead investor or follow-on
        6. "use_of_funds": What they plan to use the funding for
        7. "unique_value": What makes this startup unique
        
        If any information is not present in the description, leave that field empty or null.
        Format the response as a valid JSON object.
        """
        
        try:
            # Get LLM response
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            
            result_text = response.choices[0].message.content
            
            # Extract JSON from the response
            json_match = re.search(r'{.*}', result_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                try:
                    attributes = json.loads(json_str)
                    return attributes
                except json.JSONDecodeError:
                    logger.error("Error parsing JSON from attribute extraction")
            
            # Return empty dict if parsing fails
            return {}
            
        except Exception as e:
            logger.error(f"Error in attribute extraction: {str(e)}")
            return {}
    
    def generate_custom_advice(
        self, 
        startup_description: str, 
        matched_vcs: List[Dict[str, Any]]
    ) -> str:
        """
        Generate custom advice for approaching the matched VCs.
        
        Args:
            startup_description (str): Description of the startup
            matched_vcs (List[Dict]): List of matched VCs
            
        Returns:
            str: Custom advice
        """
        if not matched_vcs:
            return "No investor matches were found. Consider broadening your search criteria."
        
        # Create a summary of the top matches
        match_summary = ""
        for i, vc in enumerate(matched_vcs[:3], 1):
            match_summary += f"{i}. {vc['Name']} ({vc.get('match_score', 0)}/100): {vc.get('match_reason', '')}\n"
        
        prompt = f"""
        As a VC fundraising expert, provide personalized advice for this startup on how to approach their best-matched investors.
        
        Startup: {startup_description}
        
        Top matched investors:
        {match_summary}
        
        Provide 3-5 bullet points of specific, actionable advice on:
        1. How to position their pitch to these specific investors
        2. What aspects of their business to emphasize based on the investors' focus
        3. Any potential concerns to address proactively
        4. Next steps for outreach
        
        Keep your advice specific to these investors and this startup. Be concise but insightful.
        """
        
        try:
            # Get LLM response
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7
            )
            
            advice = response.choices[0].message.content
            return advice
            
        except Exception as e:
            logger.error(f"Error generating custom advice: {str(e)}")
            return "Unable to generate custom advice due to API limitations."


# Example usage:
if __name__ == "__main__":
    api_key = "your_openai_api_key"
    matcher = VCMatcher(api_key)
    
    # Sample VC data
    sample_vcs = [
        {
            "Name": "Tech Ventures",
            "Website": "techventures.com",
            "Sector Focus": ["AI/ML", "Enterprise SaaS"],
            "Preferred Deal Stage": ["Seed", "Series A"],
            "Check Range": "$500k-1M",
            "Geo Focus": "USA",
            "Investment Thesis": "We back technical founders disrupting enterprise with AI.",
            "Status": "Active"
        },
        {
            "Name": "Health Capital",
            "Website": "healthcapital.com",
            "Sector Focus": ["Health Tech", "Biotech"],
            "Preferred Deal Stage": ["Series A", "Series B"],
            "Check Range": "$1M-5M",
            "Geo Focus": "Global",
            "Investment Thesis": "Investing in the future of healthcare and life sciences.",
            "Status": "Active"
        }
    ]
    
    # Sample startup description
    startup_description = """
    We're building an AI-powered diagnostic platform for telemedicine. 
    Our technology uses machine learning to analyze patient symptoms and medical history 
    to assist doctors in making faster, more accurate diagnoses. We're a team of 5 with 
    backgrounds in AI and healthcare, currently at seed stage looking for $750k to expand 
    our team and complete clinical validation.
    """
    
    # Find matches
    matches = matcher.match_startup_to_vcs(startup_description, sample_vcs)
    
    # Print results
    for match in matches:
        print(f"\n=== {match['Name']} ({match.get('match_score', 0)}/100) ===")
        print(f"Match Reason: {match.get('match_reason', '')}")
        if match.get('caution'):
            print(f"Caution: {match['caution']}")
        print(f"Sectors: {', '.join(match.get('Sector Focus', []))}")
        print(f"Stages: {', '.join(match.get('Preferred Deal Stage', []))}")
        print(f"Check Range: {match.get('Check Range', '')}")
    
    # Generate advice
    advice = matcher.generate_custom_advice(startup_description, matches)
    print("\n=== Fundraising Advice ===")
    print(advice)