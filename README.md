[![Gitpod Ready-to-Code](https://img.shields.io/badge/Gitpod-Ready--to--Code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/od-ms/dkan-push) 
# DKAN SCRIPTS

# 1. CREATE DKAN CONTENT STATISTIC

    python3 dkan-analyse.py

# 2. CSV2DKAN

Automatically imports CSV file content into DKAN instance.

Maintain all your datasets and resources in a CSV file, and then create or update the corresponding entries in your DKAN directory by running this script.

## Requirements

Python >3.5

## Install

1. Create config.py file (see section "Setup" below)
2. Install required packages:

    python3 -m venv venv # (optional: Create virtual environment)

    pip3 install -r requirements.txt

    (If you get an error with installing pydkan, use the install instructions from here: https://github.com/GetDKAN/pydkan)

## Usage

    source venv/bin/activate # (optional: Activate virtual environment)

    python3 csv2dkan.py [arguments]

### Possible arguments:
    --ids=x23,y48     Import only those. Comma separated list of dataset-identifiers.
    --skipCount=20    Skip first 20 datasets in csv file
    --force           Force update of resources (othwerise resources will only be updated if the resource title has changed)

### Usage examples:

    python3 csv2dkan.py
      (Download the csv file in config file and create or update all datasets)

    python3 csv2dkan.py --ids=h01 --force
      (Force the update of a single dataset with id "h01" and it's resources)

    python3 csv2dkan.py --ids=h01,b23,x09
      (Update or create only three specific datasets,
      and update resources only if the resource title has changed (=default behavior))

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

**Htaccess** - If your DKAN Portal has htaccess protection, use this format for the _dkan_url_:

    dkan_url = "https://user:password@dkan-portal.url"

**Proxy** - If you are behind a proxy, change the file *site-packages/dkan/client.py*
and add the following code in the method "requests" after the line "s = requests.Session()":

    s.proxies =  {
      'http': 'http://proxy.some:8080',
      'https': 'http://proxy.other:8080',
    }
