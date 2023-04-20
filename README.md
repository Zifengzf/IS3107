# IS3107 Project Group 3

## Introduction

The Housing Development Board (HDB) is the sole provider of public housing in Singapore. These public houses offer affordable housing options for Singaporeans, with almost 80% of the population living in them. Given that housing is one of the largest purchases and investments that a household makes, the potential for price appreciation is a crucial factor to consider when planning a purchase. We believe that by aggregating and analysing data from various sources including housing data, transactional data, financial data, and economic indicators, we can derive meaningful insights and forecast housing prices. These insights will be presented via dashboards in Tableau.

## Data

The data folder contains the historical data we used for our analysis, which is uploaded to Google Cloud Storage and subsequently stored in BigQuery tables. Due to the constraints of file size, we present a subset of the resale transaction and combined dataset, along with the full datasets of BTO and Reddit data.

## DAGs

The DAGs folder contains the 3 airflow DAG files used in our data pipeline. The first and main workflow relates to resale and financial data. The second workflow updates the BTO data in a similar fashion while the final workflow recreates the Reddit comments table with the newest comments.

## Preprocessing

The preprocessing folder contains 2 Juypter notebooks, 1 for generating input features for resale price prediction and the other for preprocessing of the resale transactions dataset.
