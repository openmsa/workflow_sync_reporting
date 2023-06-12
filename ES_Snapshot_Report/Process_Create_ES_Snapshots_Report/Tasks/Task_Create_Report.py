from msa_sdk.variables import Variables
from msa_sdk.msa_api import MSA_API
from msa_sdk.device import Device
import datetime
import requests
import urllib3
import json
import math

if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    dev_var = Variables()
    dev_var.add('snapshot.0.snapshotName', var_type='String')
    dev_var.add('snapshot.0.repository', var_type='String')
    dev_var.add('snapshot.0.indices', var_type='String')
    dev_var.add('snapshot.0.shards', var_type='String')
    dev_var.add('snapshot.0.failedShards', var_type='String')
    dev_var.add('snapshot.0.dateCreated', var_type='String')
    dev_var.add('snapshot.0.duration', var_type='String')
    dev_var.add('snapshot.0.state', var_type='String')
    dev_var.add('snapshot.0.policyName', var_type='String')
    
    context = Variables.task_call(dev_var)
    
    context['snapshot'] = []
    
    # Get the snapshot (list) object from the context.
    snapshot_list_ctx = context.get('snapshot')

    kibana_ip = context["kibana_ip"]
    kibana_username = context["kibana_username"]
    kibana_password = context["kibana_password"]
    
    if kibana_ip is None:
        _url = 'http://msa-kibana:5601/kibana/api/snapshot_restore/snapshots'.format(kibana_ip)
    else:
        _url = 'http://{}/kibana/api/snapshot_restore/snapshots'.format(kibana_ip)

    _headers = {'Content-Type':'application/json', 'kbn-xsrf' : 'true'}
    
    try:
        response = requests.get(url=_url, headers=_headers, auth=(kibana_username,kibana_password), timeout=240, verify=False)
        output_code = response.status_code
        if int(output_code) < 400:
            response = response.json()
            snapshots = response["snapshots"]
            for snapshot in snapshots:
                #Initiate the snapshot object to be insert in snapshots list from the context.
                snapshot_display = dict()
                
                #Insert attributes values in the snapshot dict object.
                snapshot_display["snapshotName"] = snapshot["snapshot"]
                snapshot_display["repository"] = snapshot["repository"]
                snapshot_display["indices"] = len(snapshot["indices"])
                snapshot_display["shards"] = snapshot["shards"]["total"]
                snapshot_display["failedShards"] = str(snapshot["shards"]["failed"])
                datecreated = datetime.datetime.utcfromtimestamp((snapshot["startTimeInMillis"])/1000)
                snapshot_display["dateCreated"] = datecreated.strftime('%b %d, %Y %H:%M:%S') + " GMT"
                snapshot_display["duration"] = str(math.ceil((snapshot["endTimeInMillis"] - snapshot["startTimeInMillis"])/1000)) + "s"
                snapshot_display["state"] = snapshot["state"]
                snapshot_display["policyName"] = snapshot["policyName"]
                
                #Append the snapshot object to the list of snapshots.
                snapshot_list_ctx.append(snapshot_display)
            
            ret = MSA_API.process_content('ENDED', f' Snapshot report generation successful', context, True)
        else : 
            ret = MSA_API.process_content('FAIL', f' customer_id update in kibana index failed with message: {response.text}', context, True)
    except requests.exceptions.ConnectionError: 
        ret = MSA_API.process_content('FAIL', f' ConnectionError Please check kibana URL : {response.text}', context, True)
        pass
    
    print(ret)