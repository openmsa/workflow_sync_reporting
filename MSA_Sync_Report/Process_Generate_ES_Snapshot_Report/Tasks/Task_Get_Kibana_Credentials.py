from msa_sdk.variables import Variables
from msa_sdk.msa_api import MSA_API

if __name__ == "__main__":
    dev_var = Variables()
    #dev_var.add('kibana_ip', var_type='String')
    #dev_var.add('kibana_username', var_type='String')
    dev_var.add('kibana_password', var_type='String')

    context = Variables.task_call(dev_var)
    
    context["kibana_username"]="superuser"
    context["kibana_password"]="x^ZyuGM6~u=+fY2G"
    
    ret = MSA_API.process_content('ENDED', 'Instance created', context, True)
    print(ret)