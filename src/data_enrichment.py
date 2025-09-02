import pandas as pd
import requests
import time
import logging
from typing import Dict, Any
import json
from pathlib import Path

class LEIEnrichmentError(Exception):
    """Custom exception for LEI enrichment errors"""
    pass

class LEIDataEnricher:
    """
    Production-ready data enrichment service.
    
    Enriches transaction data by fetching legal entity information
    from the GLEIF API based on LEI codes.
    """
    
    def __init__(self, 
                 base_url: str = "https://api.gleif.org/api/v1/lei-records",
                 rate_limit_delay: float = 0.1,
                 max_retries: int = 3,
                 timeout: int = 30):
        """
        Initialize the LEI enricher.
        
        Args:
            base_url: GLEIF API base URL
            rate_limit_delay: Delay between API calls (seconds)
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.timeout = timeout
        self._lei_cache = {}
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _fetch_lei_data(self, lei_code: str) -> Dict[str, Any]:
        """
        Fetch LEI data from GLEIF API with retry logic.
        
        Args:
            lei_code: Legal Entity Identifier
            
        Returns:
            Dictionary containing LEI data
            
        Raises:
            LEIEnrichmentError: If API call fails after all retries
        """
        # Check cache first
        if lei_code in self._lei_cache:
            self.logger.debug(f"Using cached data for LEI: {lei_code}")
            return self._lei_cache[lei_code]
        
        url = f"{self.base_url}?filter[lei]={lei_code}"
        
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"Fetching LEI data for: {lei_code} (attempt {attempt + 1})")
                
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                data = response.json()
                
                # Extract relevant data
                if 'data' in data and len(data['data']) > 0:
                    lei_record = data['data'][0]
                    attributes = lei_record.get('attributes', {})
                    
                    # Extract legal name from entity.legalName.name
                    legal_name = ''
                    entity = attributes.get('entity', {})
                    if entity and 'legalName' in entity:
                        legal_name = entity['legalName'].get('name', '')
                    
                    # Extract BIC from attributes.bic array (take first one if available)
                    bic = ''
                    bic_list = attributes.get('bic', [])
                    if isinstance(bic_list, list) and len(bic_list) > 0:
                        bic = bic_list[0]
                    elif isinstance(bic_list, str):  # In case it's returned as string
                        bic = bic_list
                    
                    # Extract country from entity.legalAddress.country
                    country = ''
                    if entity and 'legalAddress' in entity:
                        country = entity['legalAddress'].get('country', '')
                    
                    result = {
                        'legalName': legal_name,
                        'bic': bic,
                        'country': country
                    }
                    
                    # Cache the result
                    self._lei_cache[lei_code] = result
                    
                    # Rate limiting
                    time.sleep(self.rate_limit_delay)
                    
                    return result
                else:
                    # No data found for this LEI
                    result = {'legalName': '', 'bic': '', 'country': ''}
                    self._lei_cache[lei_code] = result
                    return result
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed for LEI {lei_code} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise LEIEnrichmentError(f"Failed to fetch data for LEI {lei_code} after {self.max_retries} attempts: {e}")
            
            except (KeyError, json.JSONDecodeError) as e:
                self.logger.error(f"Error parsing response for LEI {lei_code}: {e}")
                result = {'legalName': '', 'bic': '', 'country': ''}
                self._lei_cache[lei_code] = result
                return result
    
    def enrich_dataset(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich the input dataset with LEI information.
        
        Args:
            input_data: DataFrame containing transaction data with 'lei' column
            
        Returns:
            Enriched DataFrame with additional 'legalName' and 'bic' columns
            
        Raises:
            LEIEnrichmentError: If required columns are missing
        """
        if 'lei' not in input_data.columns:
            raise LEIEnrichmentError("Input data must contain 'lei' column")
        
        self.logger.info(f"Starting enrichment for {len(input_data)} records")
        
        # Create a copy to avoid modifying the original
        enriched_data = input_data.copy()
        
        # Get unique LEI codes to minimize API calls
        unique_leis = input_data['lei'].unique()
        self.logger.info(f"Found {len(unique_leis)} unique LEI codes")
        
        # Fetch data for each unique LEI
        lei_info = {}
        for i, lei_code in enumerate(unique_leis, 1):
            self.logger.info(f"Processing LEI {i}/{len(unique_leis)}: {lei_code}")
            try:
                lei_info[lei_code] = self._fetch_lei_data(lei_code)
            except LEIEnrichmentError as e:
                self.logger.error(f"Failed to enrich LEI {lei_code}: {e}")
                lei_info[lei_code] = {'legalName': '', 'bic': '', 'country': ''}
        
        # Add enriched data to DataFrame
        enriched_data['legalName'] = enriched_data['lei'].map(lambda x: lei_info.get(x, {}).get('legalName', ''))
        enriched_data['bic'] = enriched_data['lei'].map(lambda x: lei_info.get(x, {}).get('bic', ''))
        enriched_data['country'] = enriched_data['lei'].map(lambda x: lei_info.get(x, {}).get('country', ''))
        
        # Calculate transaction_costs based on country and business logic
        self.logger.info("Calculating transaction costs based on country-specific logic")
        enriched_data['transaction_costs'] = enriched_data.apply(self._calculate_transaction_costs, axis=1)
        
        # Remove the temporary country column as it's not needed in final output
        enriched_data = enriched_data.drop('country', axis=1)
        
        self.logger.info("Enrichment completed successfully")
        return enriched_data
    
    def _calculate_transaction_costs(self, row) -> float:
        """
        Calculate transaction costs based on country-specific business logic.
        
        Args:
            row: DataFrame row containing transaction data
            
        Returns:
            Calculated transaction cost
        """
        try:
            country = row.get('country', '').upper()
            notional = float(row.get('notional', 0))
            rate = float(row.get('rate', 0))
            
            if country == 'GB':
                # For GB: transaction_costs = notional * rate - notional
                transaction_costs = notional * rate - notional
            elif country == 'NL':
                # For NL: transaction_costs = Abs(notional * (1/rate) - notional)
                if rate != 0:  # Avoid division by zero
                    transaction_costs = abs(notional * (1/rate) - notional)
                else:
                    self.logger.warning(f"Zero rate encountered for NL calculation, setting cost to 0")
                    transaction_costs = 0.0
            else:
                # For other countries or unknown, set to 0
                self.logger.info(f"No specific calculation rule for country '{country}', setting cost to 0")
                transaction_costs = 0.0
            
            return transaction_costs
            
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error calculating transaction costs: {e}")
            return 0.0
    
    def save_cache(self, cache_file: str = "lei_cache.json"):
        """Save the LEI cache to a file for future use."""
        try:
            with open(cache_file, 'w') as f:
                json.dump(self._lei_cache, f, indent=2)
            self.logger.info(f"Cache saved to {cache_file}")
        except Exception as e:
            self.logger.error(f"Failed to save cache: {e}")
    
    def load_cache(self, cache_file: str = "lei_cache.json"):
        """Load LEI cache from a file."""
        try:
            if Path(cache_file).exists():
                with open(cache_file, 'r') as f:
                    self._lei_cache = json.load(f)
                self.logger.info(f"Cache loaded from {cache_file}")
        except Exception as e:
            self.logger.error(f"Failed to load cache: {e}")


def main():
    """
    Main function to demonstrate the enrichment process.
    """

    try:
        # Initialize the enricher
        enricher = LEIDataEnricher()
        
        # Load existing cache if available
        enricher.load_cache()
        
        # Load CSV
        df = pd.read_csv("sample_input.csv")
        
        print("Original data shape:", df.shape)
        print("\nFirst few rows of original data:")
        print(df.head())
        
        # Enrich the data
        enriched_df = enricher.enrich_dataset(df)
        
        print("\nEnriched data shape:", enriched_df.shape)
        print("\nFirst few rows of enriched data:")
        print(enriched_df.head())
        
        # Save the enriched data
        output_file = 'output.csv'
        enriched_df.to_csv(output_file, index=False)
        print(f"\nEnriched data saved to: {output_file}")
        
        # Save cache for future runs
        enricher.save_cache()
        
        # Display summary
        print(f"\nEnrichment Summary:")
        print(f"- Total records processed: {len(enriched_df)}")
        print(f"- Unique LEIs processed: {len(df['lei'].unique())}")
        print(f"- Records with legal names: {len(enriched_df[enriched_df['legalName'] != ''])}")
        print(f"- Records with BIC codes: {len(enriched_df[enriched_df['bic'] != ''])}")
        print(f"- Records with transaction costs calculated: {len(enriched_df[enriched_df['transaction_costs'] != 0])}")
        
        # Show sample of calculated transaction costs
        print(f"\nSample Transaction Costs:")
        cost_sample = enriched_df[['lei', 'notional', 'rate', 'transaction_costs']].head()
        print(cost_sample.to_string(index=False))

    except Exception as e:
        logging.error(f"Error in enrichment process: {e}")


if __name__ == "__main__":
    main()