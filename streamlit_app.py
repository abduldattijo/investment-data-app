import streamlit as st
import pandas as pd
import numpy as np
import os
import time
import json
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Import our custom modules
from web_scraper import VCWebScraper
from api_integration import VCDataEnricher
from openai_integration import VCMatcher

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('vc_tool')

# Set page configuration
st.set_page_config(
    page_title="VC Research & Matching Tool",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Application title and description
st.title("VC Research & Matching Tool")
st.markdown("### Find the perfect investors for your startup")

# Initialize session state variables
if 'vc_data' not in st.session_state:
    st.session_state.vc_data = None
if 'enhanced_data' not in st.session_state:
    st.session_state.enhanced_data = None
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'recommendations' not in st.session_state:
    st.session_state.recommendations = []
if 'api_key' not in st.session_state:
    st.session_state.api_key = os.getenv("OPENAI_API_KEY", "")
if 'processed_count' not in st.session_state:
    st.session_state.processed_count = 0
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'enrichment_method' not in st.session_state:
    st.session_state.enrichment_method = "web_scraping"
if 'startup_attributes' not in st.session_state:
    st.session_state.startup_attributes = {}

# Domain-specific data structures
SECTOR_OPTIONS = [
    'Fintech', 'Enterprise SaaS', 'Health Tech', 'AI/ML', 'Cybersecurity', 
    'E-commerce', 'Edtech', 'Climate Tech', 'Consumer Apps', 'B2B Marketplace',
    'Web3/Blockchain', 'Hardware', 'IoT', 'Robotics', 'Biotech', 'Clean Energy',
    'Retail Tech', 'Real Estate Tech', 'Gaming', 'Media', 'Transportation',
    'Logistics', 'Manufacturing', 'Agtech', 'Food Tech', 'Space', 'AR/VR',
    'Dev Tools', 'Mobile', 'Data Analytics', 'Advertising Tech', 'Marketplaces'
]

STAGE_OPTIONS = ['Pre-seed', 'Seed', 'Seed+', 'Series A', 'Series B', 'Series C+']

CHECK_RANGES = [
    '$0-100k', '$100-250k', '$250-500k', '$500k-1M', '$1-5M', '$5M+'
]

GEO_REGIONS = [
    'USA', 'Silicon Valley', 'New York', 'Boston', 'Midwest', 'Southeast', 
    'Texas', 'Pacific Northwest', 'California', 'Europe', 'Asia', 'Global'
]

# Sidebar for settings and data upload
with st.sidebar:
    st.header("Settings")
    
    # OpenAI API key input
    api_key = st.text_input("OpenAI API Key (for matching)",
                            type="password",
                            value=st.session_state.api_key)
    if api_key:
        st.session_state.api_key = api_key
    
    st.divider()
    st.header("Data Upload")
    uploaded_file = st.file_uploader("Upload VC CSV file", type=['csv'])
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            required_columns = ["Name", "Website"]
            
            # Check for required columns
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.error(f"Missing required columns: {', '.join(missing_columns)}")
            else:
                st.session_state.vc_data = df
                st.success(f"Successfully loaded {len(df)} VC records!")
        except Exception as e:
            st.error(f"Error loading CSV file: {e}")
    
    st.divider()
    
    # Data enrichment method
    st.header("Data Enrichment")
    enrichment_method = st.radio(
        "Choose data enrichment method:",
        ["Web Scraping Only", "Mock API Data", "Full API Integration (requires API keys)"],
        index=0
    )
    
    if enrichment_method == "Web Scraping Only":
        st.session_state.enrichment_method = "web_scraping"
    elif enrichment_method == "Mock API Data":
        st.session_state.enrichment_method = "mock_api"
    else:
        st.session_state.enrichment_method = "full_api"
        
        # Additional API key inputs when full API integration is selected
        cb_api_key = st.text_input("Crunchbase API Key", type="password")
        pb_api_key = st.text_input("PitchBook API Key", type="password")
        
        if cb_api_key:
            os.environ["CRUNCHBASE_API_KEY"] = cb_api_key
        if pb_api_key:
            os.environ["PITCHBOOK_API_KEY"] = pb_api_key
    
    # Process button
    if st.session_state.vc_data is not None:
        if st.button("Process VCs"):
            with st.spinner("Processing VC data..."):
                st.session_state.processing_complete = False
                
                # Convert dataframe to list of dicts
                vc_list = st.session_state.vc_data.to_dict('records')
                
                # Initialize progress bar
                progress_bar = st.progress(0)
                
                # Process VCs based on selected method
                if st.session_state.enrichment_method == "web_scraping":
                    # Use web scraping only
                    scraper = VCWebScraper(max_pages=10, max_workers=3)
                    
                    # Process in batches to show progress
                    enhanced_data = []
                    batch_size = 10
                    total_batches = (len(vc_list) + batch_size - 1) // batch_size
                    
                    for i in range(0, len(vc_list), batch_size):
                        batch = vc_list[i:i+batch_size]
                        batch_results = scraper.scrape_multiple_vcs(batch)
                        enhanced_data.extend(batch_results)
                        
                        # Update progress
                        progress = (i + len(batch)) / len(vc_list)
                        progress_bar.progress(progress)
                        
                        # Update processed count
                        st.session_state.processed_count = len(enhanced_data)
                        
                        # Sleep to allow UI to update
                        time.sleep(0.1)
                    
                else:
                    # Use API integration (mock or real)
                    use_mock = st.session_state.enrichment_method == "mock_api"
                    enricher = VCDataEnricher(use_mock=use_mock)
                    
                    # Process in batches to show progress
                    enhanced_data = []
                    batch_size = 5
                    total_batches = (len(vc_list) + batch_size - 1) // batch_size
                    
                    for i in range(0, len(vc_list), batch_size):
                        batch = vc_list[i:i+batch_size]
                        batch_results = enricher.enrich_multiple_vcs(batch)
                        enhanced_data.extend(batch_results)
                        
                        # Update progress
                        progress = (i + len(batch)) / len(vc_list)
                        progress_bar.progress(progress)
                        
                        # Update processed count
                        st.session_state.processed_count = len(enhanced_data)
                        
                        # Sleep to allow UI to update
                        time.sleep(0.1)
                
                # Store the enhanced data
                st.session_state.enhanced_data = enhanced_data
                st.session_state.processing_complete = True
                
                # Complete the progress bar
                progress_bar.progress(1.0)
                
                st.success(f"Enhanced data for {len(enhanced_data)} VCs!")

# Main tabs
tab1, tab2 = st.tabs(["VC Database", "Investor Matching"])

# VC Database Tab
with tab1:
    if st.session_state.enhanced_data:
        # Filters
        st.subheader("Filter VCs")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            selected_sector = st.selectbox("Sector", ["All Sectors"] + SECTOR_OPTIONS)
        
        with col2:
            selected_stage = st.selectbox("Stage", ["All Stages"] + STAGE_OPTIONS)
        
        with col3:
            selected_check = st.selectbox("Check Size", ["All Check Sizes"] + CHECK_RANGES)
        
        with col4:
            selected_geo = st.selectbox("Geography", ["All Regions"] + GEO_REGIONS)
        
        # Apply filters button
        if st.button("Apply Filters"):
            # Filter the data
            filtered_data = st.session_state.enhanced_data.copy()
            
            if selected_sector != "All Sectors":
                filtered_data = [
                    vc for vc in filtered_data
                    if selected_sector in vc.get("Sector Focus", [])
                ]
            
            if selected_stage != "All Stages":
                filtered_data = [
                    vc for vc in filtered_data
                    if selected_stage in vc.get("Preferred Deal Stage", [])
                ]
            
            if selected_check != "All Check Sizes":
                filtered_data = [
                    vc for vc in filtered_data
                    if vc.get("Check Range") == selected_check
                ]
            
            if selected_geo != "All Regions":
                filtered_data = [
                    vc for vc in filtered_data
                    if vc.get("Geo Focus") == selected_geo
                ]
            
            # Display results count
            st.markdown(f"### Found {len(filtered_data)} matching investors")
            
            # Display results
            for i, vc in enumerate(filtered_data[:10]):
                with st.expander(f"{vc['Name']} - {vc.get('Website', 'No website')}"):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        if vc.get("About"):
                            st.markdown(f"**About:** {vc['About']}")
                        
                        if vc.get("Investment Thesis"):
                            st.markdown(f"**Investment Thesis:** {vc['Investment Thesis']}")
                        
                        # Portfolio section
                        portfolio = vc.get("Portfolio", [])
                        if portfolio:
                            st.markdown("**Portfolio Companies:**")
                            for company in portfolio[:5]:  # Show top 5
                                company_name = company.get("name", "Unknown")
                                company_desc = company.get("description", "")
                                
                                if company_desc:
                                    st.markdown(f"- **{company_name}**: {company_desc}")
                                else:
                                    st.markdown(f"- **{company_name}**")
                    
                    with col2:
                        st.markdown("**Investment Details:**")
                        
                        if vc.get("Sector Focus"):
                            st.markdown(f"**Sectors:** {', '.join(vc['Sector Focus'])}")
                        
                        if vc.get("Preferred Deal Stage"):
                            st.markdown(f"**Stages:** {', '.join(vc['Preferred Deal Stage'])}")
                        
                        if vc.get("Check Range"):
                            st.markdown(f"**Check Range:** {vc['Check Range']}")
                        
                        if vc.get("Check Sweet Spot"):
                            st.markdown(f"**Sweet Spot:** {vc['Check Sweet Spot']}")
                        
                        if vc.get("Geo Focus"):
                            st.markdown(f"**Geography:** {vc['Geo Focus']}")
                        
                        if vc.get("Lead/Follow"):
                            st.markdown(f"**Lead/Follow:** {vc['Lead/Follow']}")
                        
                        if vc.get("Status"):
                            st.markdown(f"**Status:** {vc['Status']}")
            
            if len(filtered_data) > 10:
                st.info(f"Showing 10 of {len(filtered_data)} results. Use more specific filters to narrow down.")
        
    elif st.session_state.vc_data is not None and not st.session_state.processing_complete:
        st.info("Processing VC data... Please wait.")
        
        # Show progress counter
        if st.session_state.processed_count > 0:
            st.text(f"Processed {st.session_state.processed_count}/{len(st.session_state.vc_data)} VCs")
    else:
        st.info("Upload and process VC data in the sidebar to view the database.")

# Investor Matching Tab
with tab2:
    if st.session_state.enhanced_data:
        st.subheader("Describe Your Startup")
        
        user_input = st.text_area(
            "Tell us about your startup, including sector, stage, funding needs, and any specific requirements:",
            height=150,
            placeholder="Example: We're building an AI-powered fintech solution for freelancers to manage invoices and taxes. We're at the seed stage and looking for a lead investor with experience in fintech and creator economy. We need around $500k."
        )
        
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("Find Matches", disabled=not user_input):
                with st.spinner("Finding matching investors..."):
                    # Add user message to chat history
                    st.session_state.chat_history.append({
                        "role": "user", 
                        "content": user_input
                    })
                    
                    # Initialize matcher
                    if st.session_state.api_key:
                        matcher = VCMatcher(api_key=st.session_state.api_key)
                        
                        # Extract startup attributes
                        st.session_state.startup_attributes = matcher.extract_startup_attributes(user_input)
                        
                        # Find matching VCs
                        matches = matcher.match_startup_to_vcs(
                            user_input, 
                            st.session_state.enhanced_data,
                            num_matches=5
                        )
                        
                        # Generate custom advice
                        if matches:
                            advice = matcher.generate_custom_advice(user_input, matches)
                        else:
                            advice = "No suitable investors found based on your description."
                        
                        # Add responses to chat history
                        st.session_state.chat_history.append({
                            "role": "assistant", 
                            "content": f"Based on your description, I've found {len(matches)} potential investors that might be a good fit."
                        })
                        
                        if advice:
                            st.session_state.chat_history.append({
                                "role": "assistant", 
                                "content": f"**Fundraising Advice:**\n\n{advice}"
                            })
                        
                        # Store recommendations
                        st.session_state.recommendations = matches
                    else:
                        # Simple keyword matching if no API key
                        matches = []
                        description_lower = user_input.lower()
                        
                        # Extract keywords from the description
                        matched_sectors = []
                        for sector in SECTOR_OPTIONS:
                            if sector.lower() in description_lower:
                                matched_sectors.append(sector)
                        
                        matched_stages = []
                        if any(kw in description_lower for kw in ["pre-seed", "pre seed", "idea", "concept"]):
                            matched_stages.append("Pre-seed")
                        if any(kw in description_lower for kw in ["seed", "early", "prototype"]):
                            matched_stages.append("Seed")
                        if any(kw in description_lower for kw in ["series a", "growth", "revenue"]):
                            matched_stages.append("Series A")
                        if any(kw in description_lower for kw in ["series b", "scale", "expansion"]):
                            matched_stages.append("Series B")
                        
                        # Filter VCs based on matches
                        for vc in st.session_state.enhanced_data:
                            score = 0
                            match_reasons = []
                            
                            # Match sectors
                            for sector in matched_sectors:
                                if sector in vc.get("Sector Focus", []):
                                    score += 30
                                    match_reasons.append(f"Sector match: {sector}")
                            
                            # Match stages
                            for stage in matched_stages:
                                if stage in vc.get("Preferred Deal Stage", []):
                                    score += 20
                                    match_reasons.append(f"Stage match: {stage}")
                            
                            # Add a small score for active status
                            if vc.get("Status") == "Active":
                                score += 5
                            
                            if score > 0:
                                matches.append({
                                    **vc,
                                    "match_score": min(score, 100),  # Cap at 100
                                    "match_reason": "; ".join(match_reasons),
                                    "caution": "This match was generated by a simplified algorithm."
                                })
                        
                        # Sort by score and take top 5
                        matches.sort(key=lambda x: x.get("match_score", 0), reverse=True)
                        matches = matches[:5]
                        
                        # Add response to chat history
                        st.session_state.chat_history.append({
                            "role": "assistant", 
                            "content": f"Based on your description, I've found {len(matches)} potential investors that might be a good fit."
                        })
                        
                        # Store recommendations
                        st.session_state.recommendations = matches
        
        with col2:
            st.empty()
        
        # Display chat history
        st.subheader("Conversation")
        chat_container = st.container(height=250)
        with chat_container:
            for message in st.session_state.chat_history:
                if message["role"] == "user":
                    st.markdown(f"**You:** {message['content']}")
                else:
                    st.markdown(f"**Assistant:** {message['content']}")
        
        # Display extracted startup attributes
        if st.session_state.startup_attributes:
            with st.expander("Startup Profile (Extracted from Description)"):
                attrs = st.session_state.startup_attributes
                col1, col2 = st.columns(2)
                
                with col1:
                    if attrs.get("sector"):
                        st.markdown(f"**Sector:** {attrs['sector']}")
                    if attrs.get("stage"):
                        st.markdown(f"**Stage:** {attrs['stage']}")
                    if attrs.get("funding_needs"):
                        st.markdown(f"**Funding Needs:** {attrs['funding_needs']}")
                
                with col2:
                    if attrs.get("location"):
                        st.markdown(f"**Location:** {attrs['location']}")
                    if attrs.get("lead_preference"):
                        st.markdown(f"**Lead Preference:** {attrs['lead_preference']}")
                    if attrs.get("use_of_funds"):
                        st.markdown(f"**Use of Funds:** {attrs['use_of_funds']}")
                
                if attrs.get("unique_value"):
                    st.markdown(f"**Unique Value:** {attrs['unique_value']}")
        
        # Display recommendations
        if st.session_state.recommendations:
            st.subheader("Recommended Investors")
            
            for vc in st.session_state.recommendations:
                # Create a card-like expander
                match_score = vc.get("match_score", 0)
                match_reason = vc.get("match_reason", "")
                
                # Determine score color
                if match_score >= 80:
                    score_color = "green"
                elif match_score >= 60:
                    score_color = "orange"
                else:
                    score_color = "gray"
                
                header = f"{vc['Name']} - **Match Score: <span style='color:{score_color}'>{match_score}%</span>**"
                
                with st.expander(header):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        if match_reason:
                            st.markdown(f"**Match Reason:** {match_reason}")
                        
                        caution = vc.get("caution", "")
                        if caution:
                            st.warning(caution)
                        
                        if vc.get("Website"):
                            website = vc["Website"]
                            if not website.startswith(('http://', 'https://')):
                                website = 'https://' + website
                            st.markdown(f"**Website:** [{website}]({website})")
                        
                        if vc.get("About"):
                            st.markdown(f"**About:** {vc['About']}")
                        
                        if vc.get("Investment Thesis"):
                            st.markdown(f"**Investment Thesis:** {vc['Investment Thesis']}")
                            
                        # Portfolio section
                        portfolio = vc.get("Portfolio", [])
                        if portfolio:
                            st.markdown("**Selected Portfolio Companies:**")
                            for company in portfolio[:3]:  # Show only first 3
                                company_name = company.get("name", "Unknown")
                                company_desc = company.get("description", "")
                                
                                if company_desc:
                                    st.markdown(f"- **{company_name}**: {company_desc}")
                                else:
                                    st.markdown(f"- **{company_name}**")
                    
                    with col2:
                        st.markdown("**Investment Details:**")
                        
                        if vc.get("Sector Focus"):
                            st.markdown(f"**Sectors:** {', '.join(vc['Sector Focus'])}")
                        
                        if vc.get("Preferred Deal Stage"):
                            st.markdown(f"**Stages:** {', '.join(vc['Preferred Deal Stage'])}")
                        
                        if vc.get("Check Range"):
                            st.markdown(f"**Check Range:** {vc['Check Range']}")
                        
                        if vc.get("Check Sweet Spot"):
                            st.markdown(f"**Sweet Spot:** {vc['Check Sweet Spot']}")
                        
                        if vc.get("Geo Focus"):
                            st.markdown(f"**Geography:** {vc['Geo Focus']}")
                        
                        if vc.get("Lead/Follow"):
                            st.markdown(f"**Lead/Follow:** {vc['Lead/Follow']}")
                        
                        if vc.get("Status"):
                            st.markdown(f"**Status:** {vc['Status']}")
    
    else:
        st.info("First upload and process VC data in the sidebar to use the matching feature.")

# Add the setup instructions at the bottom
st.divider()
st.subheader("Setup Instructions")
st.markdown("""
1. **Upload Data**: Use the sidebar to upload your CSV file with VC information (columns should include: Name, Website)
2. **Choose Enrichment Method**: Select how you want to enrich the VC data (web scraping only, mock API data, or full API integration)
3. **Process VCs**: Click the 'Process VCs' button to enhance the data (this will scrape websites and gather additional information)
4. **Explore Database**: Use the filters in the 'VC Database' tab to browse the enhanced investor data
5. **Find Matches**: In the 'Investor Matching' tab, describe your startup to get personalized investor recommendations
6. **Optional**: Add your OpenAI API key in the sidebar to enable advanced matching and advice using AI
""")