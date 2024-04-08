tf-init: 
	terraform -chdir=./terraform init

tf-apply: 
	terraform -chdir=./terraform apply 

tf-down: 
	terraform -chdir=./terraform destroy 

