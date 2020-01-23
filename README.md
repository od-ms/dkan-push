[![Gitpod Ready-to-Code](https://img.shields.io/badge/Gitpod-Ready--to--Code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/od-ms/dkan-push) 


# CSV2DKAN

Automatically imports CSV file content into DKAN instance.

Maintain all your datasets and resources in a CSV file, and then create or update the corresponding entries in your DKAN directory by running this script.

## Requirements

Python >3.5

## Usage

    python3 csv2dkan.py [datasetIds]

The optional parameter _datasetIds_ is not the dkan dataset ID, but your own dataset ID from the excel table.
E.g. to import only a few datasets, use your the identifiers from the first column of the excel:

    python3 csv2dkan.py a01,sw1,b02

## Setup

### Config file
Create a config file like this:

config.py

    user = "username"
    pass = "password"
    dkan_url = "https://dkan-portal.url"
    csv_url = "https://url-to-csv.host/file.csv"

Or copy and modify _config.dist.py_.
Then run

    python3 csv2dkan.py

### Set up default dataset data

In *dkanhandler.py* check and modify the first method where some default values for all Datasets are defined.
These default values can be set differenty, depending on the "group" value. Check the Array "groupData".

### Config options

If your DKAN Portal has htaccess protection, use this format for the _dkan_url_:

   dkan_url = "https://user:password@dkan-portal.url"
