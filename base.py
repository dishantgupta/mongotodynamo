
import boto3
from pymongo import MongoClient



create_table_payload = {"KeySchema": [{"AttributeName": "a", "KeyType": "HASH"}], "TableName": "APP_test2", "AttributeDefinitions": [{"AttributeName": "a", "AttributeType": "N"}, {"AttributeName": "b", "AttributeType": "S"}], "ProvisionedThroughput": {"WriteCapacityUnits": 10, "ReadCapacityUnits": 10}}



d_client = boto3.client('dynamodb', endpoint_url="http://localhost:8000")
m_client = MongoClient(host="localhost")

prs = []



def put_item_attr(name, typ, value):
  if typ == "BOOL":
    if value == None:
      typ = "NULL"
      value = True
    else:
      if str(value).strip().lower() == "" or str(value).strip().lower()=="false":
        value = False
      else:
        value = True

  if typ == "M":
    if value == None:
      typ = "NULL"
      value = True
    else:
      if not isinstance(value, dict):
        value = {}

  if typ == "S" or typ == "N":
    if value == None or str(value).strip().lower() == "":
      typ = "NULL"
      value = True
    else:
      value = str(value)

  return {name:{typ:value}}


def get_put_request_payload(doc):
  attr_types = create_table_payload['AttributeDefinitions']
  put_req_item = {}
  for attr_type in attr_types:
    put_req_item.update(put_item_attr(attr_type['AttributeName'], attr_type['AttributeType'], (doc.get(attr_type['AttributeName']))))
  return {"PutRequest": {"Item": put_req_item}}



# get all documents from collection
db = m_client["noddy"]
collection = db["lala"]
_docs = collection.find({})[100:]
for doc in _docs:
  doc.pop("_id")
  prs.append(get_put_request_payload(doc))
  if len(prs)>=25:
    print prs
    d_client.batch_write_item(RequestItems={"APP_test": prs})
    prs = []


