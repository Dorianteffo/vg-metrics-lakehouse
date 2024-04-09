tf-init: 
	terraform -chdir=./terraform init

tf-apply: 
	terraform -chdir=./terraform apply 

tf-plan: 
	terraform -chdir=./terraform plan 

tf-down: 
	terraform -chdir=./terraform destroy 

ec2-private-key: 
	terraform -chdir=./terraform output -raw private_key

airflow-ec2-dns: 
	terraform -chdir=./terraform output -raw airflow_ec2_public_dns