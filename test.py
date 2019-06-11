from dkan.client import DatasetAPI
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("uri")
parser.add_argument("user")
parser.add_argument("passw")
args = parser.parse_args()
print(args.uri)

api = DatasetAPI(args.uri, args.user, args.passw)
print (api.node('index').json())
