from urllib.request import urlopen
import json


def retrieve_datacite_clients(base_url):
    datacite_clients = {}
    while base_url is not None:
        with urlopen(base_url) as response:
            print(f"requesting {base_url}")
            response_content = response.read()
            data = json.loads(response_content)
            if 'data' in data and len(data['data'])>0:
                for item in data['data']:
                    datacite_clients[item['id'].lower()]= item['attributes']['re3data'].lower().replace("https://doi.org/","")
                base_url = data['links']['next']
            else:
                base_url = None
    return datacite_clients


def retrieve_r3data(start_url):
    r3data_clients = {}
    page_number = 1
    base_url = start_url
    while base_url is not None:
        with urlopen(base_url) as response:
            print(f"requesting {base_url}")
            response_content = response.read()
            data = json.loads(response_content)
            if 'data' in data and len(data['data'])>0:
                for item in data['data']:
                    r3data_clients[item['id'].lower()]= dict(
                        openaire_id= "re3data_____::"+item['attributes']['re3dataId'].lower(),
                    official_name=item['attributes']['repositoryName']
                    )
                page_number +=1
                base_url = f"{start_url}&page[number]={page_number}"
            else:
                base_url = None
    return r3data_clients






base_url ="https://api.datacite.org/clients?query=re3data_id:*&page[size]=250"

dc = retrieve_datacite_clients(base_url)
r3 = retrieve_r3data("https://api.datacite.org/re3data?page[size]=250")

result = {}

for item in dc:
    res = dc[item].lower()
    if res not in r3:
        print(f"missing {res} for {item} in dictionary")
    else:
        result[item.upper()]= dict(openaire_id=r3[res]["openaire_id"],datacite_name=r3[res]["official_name"], official_name=r3[res]["official_name"] )


with open('hostedBy_map.json', 'w', encoding='utf8') as json_file:
    json.dump(result, json_file, ensure_ascii=False, indent=1)
