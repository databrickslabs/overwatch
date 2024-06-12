This AiO will create Everything: 

The first file (main.tf) will deal with required azure resources: Resource Group + Two Storage accounts (one for the cluster logs, another for the overwatch database), configure diagnostic settings, create databricks workspace, secret scope, keyvault, put proper permission and roles, access policies in the keyvault, create the Event Hub Namespace, the event hub and it's authorization rules

The second file (main_databricks) will upload the overwatch notebook, create the secret scope, add a few entries in the keyvault, generate thew required PAT token, create a dummy job and create and schedule the overwatch job with the required libraries from maven put as a dependencies.

USAGE:
* Do a terraform init to download the azure and databricks providers
* Fill the Variables below in a new file called terraform.tfvars file with your own values
* run az login (to be able to create the secret scope, as service principal is not allowed)
* run terraform plan / terraform apply
* run the dummy job and then the overwatch job is scheduled to run every day (you can change the schedule in the variables.tf file, using the cron expression)


The terraform.tfvars file should contain the following vars defined:

tenant_id=
subscription_id= 
service_principal_id_mount=
user_id= 
overwatch_spn=
overwatch_spn_pass=