#!/usr/bin/env python3
#
# WIP: xfer kernelCI test data from mongodb to BQ
# - assumes local copy of kci mongodb
#
import pymongo
import pprint
from bson.objectid import ObjectId
from google.cloud import bigquery

def main():
    mongo_client = pymongo.MongoClient()
    db = mongo_client['kernel-ci']

    bq_client = bigquery.Client()
    dataset_ref = bq_client.dataset('kernelci01')
    table_ref = dataset_ref.table("tests")

    test_group = db['test_group']
    test_case = db['test_case']

    tg_count = 0
    for tg in test_group.find():
        if tg['name'] == "lava":
            continue
        
        tg_count = tg_count + 1
        print(tg['name'])
    
        data = []
        for tc_id in tg['test_cases']:
            #print(tc_id)
            tc = test_case.find_one({'_id': ObjectId(tc_id)})
            #pprint.pprint(tc)
            tc_name = tg['name'] + '/' + tc['name']
            tc_status = tc['status']
            print("\t%s %s" %(tc_name, tc_status))

            data.append({"name": tc_name, "result": tc_status})

        job = bq_client.load_table_from_json(data, table_ref)
        job.result()

        # For now just send a few results
        if tg_count > 10:
            break
    
if __name__ == "__main__":
    main()
