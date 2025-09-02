# Cardano Data Enrichment

## Overview

This Python application enriches transaction datasets with Legal Entity Identifier (LEI) information from the GLEIF API and calculates transaction costs based on country-specific business logic.

The tool is designed to supply enriched data that can be easily imported into Excel VBA scripts or Python applications used by advanced business teams for their own self-service calculations and simulations.

## Features

### **Data Enrichment**
- **LEI Data Fetching**: Automatically retrieves legal entity information from the GLEIF API
- **Legal Name Extraction**: Adds company legal names to transaction records  
- **BIC Code Integration**: Includes Bank Identifier Codes where available
- **Transaction Cost Calculation**: Applies country-specific business logic for cost calculations

### **Production-Ready**
- **Error Handling**: Comprehensive error handling with custom exceptions
- **Rate Limiting**: Respects API rate limits with configurable delays
- **Caching System**: Minimizes API calls through caching
- **Retry Logic**: Exponential backoff for failed API requests
- **Logging**: Detailed logging for monitoring and troubleshooting

### **Business Logic**
- **GB (Great Britain)**: `transaction_costs = notional × rate - notional`
- **NL (Netherlands)**: `transaction_costs = |notional × (1/rate) - notional|`
- **Other Countries**: `transaction_costs = 0`

## Installation

### Prerequisites
- Python 3.7 or higher
- Internet connection for GLEIF API access

### Setup
1. **Clone or download the project files**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Input File Preparation
I created a CSV file named `sample_input.csv` with the following structure:

```csv
transaction_uti,isin,notional,notional_currency,transaction_type,transaction_datetime,rate,lei
1030291281MARKITWIRE0000000000000112874138,EZ9724VTXK48,763000.0,GBP,Sell,2020-11-25T15:06:22Z,0.0070956000,XKZZ2JZF41MRHTR1V493
0000452AMARKITWIRE97461020,EZN7BQZMQBR8,5000000.0,GBP,Buy,2020-12-17T12:15:39Z,0.0062469000,213800MBWEIJDM5CU638
```

#### Required Columns:
- `lei`: Legal Entity Identifier (20-character alphanumeric code)
- `notional`: Transaction notional amount
- `rate`: Transaction rate
- Other columns: Will be preserved in the output

### Running the Application

1. **Place your input file** in the same directory as the script:
   ```
   sample_input.csv
   ```

2. **Execute the script**:
   ```bash
   python data_enrichment.py
   ```

3. **Monitor progress**: The application will display real-time progress and status updates

4. **Review output**: The enriched dataset will be saved as `output.csv`

### Output Structure

The output CSV file will contain all original columns plus three new enrichment fields:

| Original Columns | + | Enrichment Fields |
|------------------|---|-------------------|
| transaction_uti  |   | **legalName**     |
| isin            |   | **bic**           |
| notional        |   | **transaction_costs** |
| notional_currency|   |                   |
| transaction_type |   |                   |
| transaction_datetime |   |               |
| rate            |   |                   |
| lei             |   |                   |

## Configuration

### Default Settings
```python
DataEnricher(
    base_url="https://api.gleif.org/api/v1/lei-records",
    rate_limit_delay=0.1,     # 100ms delay between API calls
    max_retries=3,            # Maximum retry attempts
    timeout=30                # Request timeout in seconds
)
```

### Customization
You can modify these parameters when initializing the enricher:

```python
enricher = LEIDataEnricher(
    rate_limit_delay=0.2,     # Slower rate limiting
    max_retries=5,            # More retry attempts
    timeout=60                # Longer timeout
)
```

## API Integration

### GLEIF API
- **Endpoint**: `https://api.gleif.org/api/v1/lei-records`
- **Method**: GET requests with LEI filter
- **Rate Limiting**: Built-in delays to respect API limits
- **Response Caching**: Automatic caching to minimize requests

### Data Extraction Points
From the GLEIF API response:
- **Legal Name**: `data[0].attributes.entity.legalName.name`
- **BIC Code**: `data[0].attributes.bic[0]`
- **Country**: `data[0].attributes.entity.legalAddress.country`

## File Structure (after running the script)

```
src/
├── data_enrichment.py         # Main application script
├── sample_input.csv          # Input dataset (user-provided)
├── requirements.txt          # Python dependencies
├── README.md                # This documentation
├── output.csv               # Generated enriched dataset (auto-generated via script)
└── lei_cache.json           # API response cache (auto-generated)
```

## Error Handling

### Common Issues and Solutions

| Error Type | Description | Solution |
|------------|-------------|----------|
| **FileNotFoundError** | Input CSV not found | Ensure `sample_input.csv` exists in script directory |
| **LEIEnrichmentError** | API request failures | Check internet connection and LEI code validity |
| **Rate Limit Exceeded** | Too many API requests | Increase `rate_limit_delay` parameter |
| **Invalid LEI Format** | Malformed LEI codes | Verify LEI codes are 20-character alphanumeric |

### Logging
The application generates detailed logs including:
- API request attempts and responses
- Data processing progress
- Error details and stack traces
- Performance metrics

## Performance

### Optimization Features
- **Unique LEI Processing**: Only processes each unique LEI once
- **Response Caching**: Stores API responses for future runs
- **Batch Processing**: Efficiently handles large datasets
- **Memory Management**: Minimal memory footprint

### Typical Performance
- **Small datasets** (< 100 records): ~30-60 seconds
- **Medium datasets** (100-1000 records): ~2-5 minutes  
- **Large datasets** (1000+ records): Scales linearly with unique LEIs

## Integration with Business Tools

### Excel VBA Integration
The output CSV can be directly imported into Excel for VBA processing:

```vb
' VBA example for importing enriched data
Workbooks.Open("output.csv")
```

### Python Script Integration
Business teams can easily load the enriched data:

```python
import pandas as pd

# Load enriched dataset
df = pd.read_csv('output.csv')

# Access enriched fields
legal_names = df['legalName']
bic_codes = df['bic']
transaction_costs = df['transaction_costs']
```

## Troubleshooting

### Debug Mode
Enable detailed logging by modifying the log level:

```python
logging.basicConfig(level=logging.DEBUG)
```

### Cache Management
- **Clear cache**: Delete `lei_cache.json`
- **Inspect cache**: View cached LEI data in JSON format
- **Cache location**: Same directory as script

### API Issues
If experiencing API connectivity issues:
1. Verify internet connection
2. Check GLEIF API status
3. Validate LEI code formats
4. Review rate limiting settings

## Support

### Requirements
- Ensure all dependencies from `requirements.txt` are installed
- Verify Python version compatibility (3.7+)
- Confirm GLEIF API accessibility

### Best Practices
- **Backup input data** before processing
- **Run test batches** for large datasets
- **Monitor logs** for processing issues
- **Validate output** before using in business applications

---

## License

This tool is designed for internal business use and integrates with the public GLEIF API service.

## Version

**Current Version**: 1.0.0  
**Last Updated**: 2025  
**Python Compatibility**: 3.7+
