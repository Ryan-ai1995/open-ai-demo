# Databricks notebook source
#SETUP
!pip install openai
import time
import os
import requests
import json
import pandas as pd 
import openai

# COMMAND ----------

#provided by user
openai.api_key = "d742160fa577468ea838ea6cfb6a91a7" #API key, can be KEY1 or KEY2 doesn't matter
openai.api_base =  "" # your endpoint should look like the following https://YOUR_RESOURCE_NAME.openai.azure.com/
deployment_name='gpt4' #This will correspond to the custom name you chose for your deployment when you deployed a model. 
#provided by azure
openai.api_type = 'azure'
openai.api_version = '2023-05-15' # this may change in the future

response = openai.ChatCompletion.create(
    engine="gpt4-32k", # The deployment name you chose when you deployed the ChatGPT or GPT-4 model.
    messages=[
        {"role": "system", "content": "Assistant is a large language model trained by OpenAI."},
        {"role": "user", "content": "Who were the founders of Microsoft?"}
    ]
)

print(response)

print(response['choices'][0]['message']['content'])

# COMMAND ----------

#provided by user
openai.api_key = "d742160fa577468ea838ea6cfb6a91a7" #API key, can be KEY1 or KEY2 doesn't matter
openai.api_base =  "" # your endpoint should look like the following https://YOUR_RESOURCE_NAME.openai.azure.com/
deployment_name='gpt4-32k' #This will correspond to the custom name you chose for your deployment when you deployed a model. 
#provided by azure
openai.api_type = 'azure'
openai.api_version = '2022-12-01' # this may change in the future
start_phrase = "Tell me about greece."
response = openai.Completion.create(
  engine=deployment_name,
  prompt= start_phrase,
  temperature=0,
  max_tokens=1000,
  top_p=1,
  frequency_penalty=0,
  presence_penalty=0,
 best_of=1,
  stop=None)
res_davinci = response['choices'][0]['text']
print(res_davinci)  

# COMMAND ----------

#Note: The openai-python library support for Azure OpenAI is in preview.
openai.api_type = "azure"
openai.api_base = "https://us-openai-test.openai.azure.com/"
openai.api_version = "2023-03-15-preview"
openai.api_key = "f9568d56fe574819a37db0602621c52a"

response = openai.ChatCompletion.create(
  engine="chatGPT",
  messages = [{"role":"system","content":"You are an expert in the English language You provide useful and accurate answers."},{"role":"user","content": """

You provide a well formatted JSON file of 10 synonyms for all elements in the following list of labels regarding the automotive industry:
[Contract work in progress,
Gross amounts due to customers,
Uncollected contract revenue,
Construction contract revenue,
Commissions,
Revenue - investment property rentals,
Deferred income - government grants,
Impairment loss on stock,
Reversal of impairment loss recognised on stock,
Reversal of impairment loss recognised on tangible fixed assets]"""}],
  temperature=0,
  max_tokens=3000,
  top_p=1,
  frequency_penalty=0,
  presence_penalty=0,
  stop=None)

res_chatgpt = response["choices"][0]["message"]["content"]
print(res_chatgpt)

# COMMAND ----------

#read in files to preprocess
import os
import pandas as pd
import json
MIN_FILE_COUNT = 0
MAX_FILE_COUNT = 10 #41 overall
c = 0
directory_path = "/dbfs/FileStore/shared_uploads/standard-mappings"
mappings = []

#get all files into a single list in a nice format
all_files = []
temp = []
for filename in os.listdir(directory_path):
    if c > MAX_FILE_COUNT or MIN_FILE_COUNT > c:
        c += 1
        continue
    if filename.endswith(".json"):
        with open(os.path.join(directory_path, filename)) as file:
            data = json.load(file)
    all_files += data["cdm_fields"]
    c += 1
    
#remove non-mapped instances (dummy)
for row in all_files:
    if row["map_type"] != "dummy":
        mappings.append(row)
    
#create a list of all CDM-ERP mappings - [{cdm_field1 : all_corresponging_erps1}, {...}]
#also get count of valuesa
dict_map = []
all_erps = set()
all_cdms = set()

for row in mappings:
    erp_fields = []
    for erps in row["erp_fields"]:
        erp_fields.append(erps["field_name"])
        all_erps.add(erps["field_name"])
    dict_map.append({row["cdm_field"] : erp_fields})
    all_cdms.add(row["cdm_field"])
    
print(f"Number of unique CDMs: {len(all_cdms)}")
print(f"Number of unique ERPs: {len(all_erps)}")
print(f"Number of mappings: {len(dict_map)}")
dict_map = {}
all_erps = set()
all_cdms = set()

for row in mappings:
    erp_fields = set()  # Use a set instead of a list to store unique ERP field names
    for erps in row["erp_fields"]:
        erp_fields.add(erps["field_name"])  # Add each ERP field name to the set
        all_erps.add(erps["field_name"])
    
    cdm_field = row["cdm_field"]
    if cdm_field not in dict_map:
        dict_map[cdm_field] = erp_fields
    else:
        dict_map[cdm_field].update(erp_fields)  # Use the 'update' method to add the unique ERP field names to the existing set
        
    all_cdms.add(cdm_field)

# Convert the dict_map dictionary to a list of dictionaries
dict_map_list = [{key: list(value)} for key, value in dict_map.items()]

# Get the count of values in each dictionary
count_map = [{key: len(value)} for key, value in dict_map.items()]

print(f"Number of unique CDMs: {len(all_cdms)}")
print(f"Number of unique ERPs: {len(all_erps)}")
print(f"Number of mappings: {len(dict_map)}")

erps = ", ".join(list(all_erps))
cdms = ", ".join(list(all_cdms))

# COMMAND ----------

dict_map_list

# COMMAND ----------

#provided by user
openai.api_key = "f9568d56fe574819a37db0602621c52a" #API key, can be KEY1 or KEY2 doesn't matter
openai.api_base =  "https://us-openai-test.openai.azure.com/" # your endpoint should look like the following https://YOUR_RESOURCE_NAME.openai.azure.com/
deployment_name='davinci' #This will correspond to the custom name you chose for your deployment when you deployed a model. 
#provided by azure
openai.api_type = 'azure'
openai.api_version = '2022-12-01' # this may change in the future


# Send a completion call to generate an answer
start_phrase = "Given two lists of text, A and B, map each element in list A to the element in list B that is most semantically similar in a financial setting. Every element in A must be matched exactly once to an element in B, but every element in B can be matched multiple times: List A: " + erps + " List B: " + cdms + " Only provide the mapping using the <-> seperator and a new line after a mapping with no other text, and do not modify the inputs for the output."
 
response = openai.Completion.create(
  engine=deployment_name,
  prompt=start_phrase,
  temperature=0,
  max_tokens=1000,
  top_p=1,
  frequency_penalty=0,
  presence_penalty=0,
 best_of=1,
  stop=None)
res_davinci = response['choices'][0]['text']
print(res_davinci)  

# COMMAND ----------

#create output list: [[ERP1, CDM1], [ERP2, CDM2] ...]
OUTPUT_NAME = res_davinci
SEP = " <-> "
out = []
res = OUTPUT_NAME.split("\n")
for i in res:
    out.append(i.split(SEP))
out = [x for x in out if x != [""]]

correct_count = 0
na_count = 0
corrects = []
incorrects = []

#skip N/A values as they crash the dictionary lookup
for i in out:
    if len(i) < 2 or not bool(dict_map.get(i[1])):
        print(i)
        na_count += 1
        continue
    if i[0] in dict_map.get(i[1]):
        correct_count += 1
        corrects.append([i])
    else:
        incorrects.append(i)
print(f"Accuracy: {(correct_count / (len(out)))*100:.2f}%")
print(f"N/A count: {na_count}")
print(f"Number of instances: {len(out)}")

# COMMAND ----------

corrects

# COMMAND ----------

incorrects

# COMMAND ----------


