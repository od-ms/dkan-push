
# CSV2DKAN

Automatically imports CSV file content into DKAN instance.

Maintain all your datasets and resources in a CSV file, and then create or update the corresponding entries in your DKAN directory by running this script.

## Usage

Create a config file like this:

config.py

   user = "username"
   pass = "password"
   dkan_url = "https://dkan-portal.url"
   csv_url = "https://url-to-csv.host/file.csv"

Or copy and modify _config.dist.py_.
Then run

   python3 csv2dkan.py

### Config options

If your DKAN Portal has htaccess protection, use this format for the _dkan_url_:

   dkan_url = "https://user:password@dkan-portal.url"
