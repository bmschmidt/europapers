from rdflib import Graph, plugin
from rdflib.serializer import Serializer
import gzip
import zipfile
from pathlib import Path
import json
import sys

def parse_row(d):
    proxies = [f for f in json.loads(d) if 'http://www.openarchives.org/ore/terms/Proxy' in f['@type']]
    for proxy in proxies:
        if 'http://purl.org/dc/elements/1.1/language' in proxy:
            better_proxy = proxy
    out = {}
    for k, v in better_proxy.items():
        if "purl.org/dc" in k:
            try:
                out['dc:' + k.split("/")[-1]] = v[0]['@value']
            except KeyError:
                out['dc:' + k.split("/")[-1]] = v[0]['@id']
    return out

def write_chunk(f, start):
    dest = Path(f"{start}.json.gz")
    print(dest)
    if dest.exists():
        return
    with gzip.open(dest, "wt") as fout:
        for i in range(start*25_000, start*25_000 + 25_000):
            print(i, end = "\r")
            demo = f.open(f.filelist[i]).read().decode("utf-8")
            g = Graph().parse(data=demo, format="xml") #<-took a while to figure this line out!
            d = g.serialize(format='json-ld', indent=0)
            record = parse_row(d)
            record['@id'] = "9200300" + "/" + f.filelist[i].filename.replace(".xml", "")
            json.dump(record, fout)
            fout.write("\n")
            
if __name__ == "__main__":
    n = sys.argv[1]
    f = zipfile.ZipFile("9200300.zip")
    write_chunk(f, int(n))