# Curve & Maverick LST pool analysis

## Overview

We analyse Curve & Maverick LST pool using on-chain data. Alchemy API is used as eth node provider for data collection.
Data is fetched using tokemak_quant_project/fetch_pool_data.py. The analysis for each pool is done on a separete notebook on the notebook folder. 

## Getting Started

### Pre-requisites
- poetry needs to be installed

### Configuration
Modify the parameters in the .env file as per your needs, specially for the Alchemy API key.
There is also a config.py file where all the constants are hardcoded. 

### Fetching the data
To fetche the data, run the following command from the project folder. Note that this will take several hours to finish as there are many queries to make to Alchemy API.
```
poetry run python tokemak_quant_project/fetch_pool_data.py
```

## Improvements and Next Steps

- Optimize data fetching from Alchemy for better scalability  
- Add data from Maverick pool (not just the token)
- Extend the analysis to inlcude an exhaustive analysis on historical total return for each pool (see Curve notebook for more details).
- Add pytests for the data fetching part
