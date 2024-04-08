tf-init: 
	terraform -chdir=./terraform init

tf-apply: 
	terraform -chdir=./terraform apply 

tf-plan: 
	terraform -chdir=./terraform plan 

tf-down: 
	terraform -chdir=./terraform destroy 

