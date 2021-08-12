# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Deploy and productionize a Python model
# MAGIC 
# MAGIC <img width=700px src="https://docs.microsoft.com/en-us/azure/machine-learning/media/how-to-use-mlflow/mlflow-diagram-track.png">
# MAGIC 
# MAGIC We will deploy the Python based model to [AzureML](https://www.mlflow.org/docs/latest/python_api/mlflow.azureml.html), and run real-time scoring against it.
# MAGIC 
# MAGIC Note that for a Spark model we would build an ACI image and deploy that.
# MAGIC 
# MAGIC *Another option is to deploy to AKS, steps are similar to ACI.*
# MAGIC 
# MAGIC Note: DO NOT `Run All`
# MAGIC * there is a step that requires manual authenticaiton
# MAGIC * there are some long steps - not really suitable for a live demo
# MAGIC 
# MAGIC ### How we got here
# MAGIC * [Exploratory data analysis]($./01_exploratory_data_analysis)
# MAGIC * [ML experimentation]($./02_machine_learning)
# MAGIC * [Deployment]($./03_deployment)
# MAGIC * [AML deployment]($./04_aml_deployment) <-- you are here

# COMMAND ----------

# DBTITLE 1,Install Azure ML library
# MAGIC %pip install azureml-mlflow

# COMMAND ----------

# DBTITLE 1,Input the ID of the model that you want to deploy
# experiment_id = "3549204499158513"
run_id = "707a806039504f96a8642c99479a69fe"
model_name = "boston-house-price-se3-model" # Replace this with the name of your registered model, if necessary.

# Coming soon: Refer to the model via the deployed stage or version

# COMMAND ----------

# DBTITLE 1,Set AzureML workspace (includes authentication step)
import mlflow
import mlflow.azureml
import azureml.mlflow
import azureml.core
from azureml.core import Workspace
# from azureml.mlflow import get_portal_url

subscription_id = '3f2e4d32-8e8d-46d6-82bc-5bb8d962328b'
resource_group = 'oneenv' 
workspace_name = 'oneenv-azureml'
ws = Workspace.get(name=workspace_name,
                   subscription_id=subscription_id,
                   resource_group=resource_group)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC At the [authentication link](https://microsoft.com/devicelogin) for the above command you will need to enter the code provided somewhere up here ^^^^^^^^^.
# MAGIC 
# MAGIC [Azure ML subscription](https://adb-2541733722036151.11.azuredatabricks.net/?o=2541733722036151#notebook/3752717244088570/command/1241894082272526) (RG: oneenv, Resource: oneenv-azureml)

# COMMAND ----------

# DBTITLE 1,Deploy Python model to AzureML endpoint
import mlflow.azureml

from azureml.core.webservice import AciWebservice, Webservice

aml_model_name = "boston-price-se5"
aml_service_name = "boston-price-se5"
# For now we refer to a model directly
model_uri = f"runs:/{run_id}/model"
# Coming soon: Refer to the model via the deployed stage or version
# model_uri = f"models:/{model_name}/production"
# model_uri = f"models:/{model_name}/1"

# Create a deployment config
aci_config = AciWebservice.deploy_configuration(cpu_cores=1, memory_gb=1)

# Register and deploy model to Azure Container Instance (ACI)
service, model = mlflow.azureml.deploy(model_uri=model_uri,
                                            workspace=ws,
                                            model_name=aml_model_name,
                                            service_name=aml_service_name,
                                            deployment_config=aci_config)


# COMMAND ----------

# MAGIC %md Wait for the endpoint to transition to `Healthy` status.
# MAGIC 
# MAGIC List of models
# MAGIC 
# MAGIC <img src="/files/scott.eade@databricks.com/demo/aml/aml1.png" width='70%' />
# MAGIC 
# MAGIC The model deployed by the notebook
# MAGIC 
# MAGIC <img src="/files/scott.eade@databricks.com/demo/aml/aml2.png" width='70%' />
# MAGIC 
# MAGIC The list of endpoints
# MAGIC 
# MAGIC <img src="/files/scott.eade@databricks.com/demo/aml/aml3.png" width='70%' />
# MAGIC 
# MAGIC Endpoint is `Healthy`
# MAGIC 
# MAGIC <img src="/files/scott.eade@databricks.com/demo/aml/aml4.png" width='70%' />

# COMMAND ----------

# DBTITLE 1,Get scoring dataset
score_dataset = spark.read.format('delta').load('/tmp/scott.eade@databricks.com/ml/boston_house_price').limit(5).drop('price').toPandas()
display(score_dataset)

# COMMAND ----------

# DBTITLE 1,Set query endpoint for scoring
import requests
import json

def query_endpoint_example(scoring_uri, inputs):
  headers = {
    "Content-Type": "application/json; format=pandas-split",
  }
  response = requests.post(scoring_uri, data=inputs, headers=headers)
  print(response.text)
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

# COMMAND ----------

print([x for x in Webservice.list(ws) if x.name == aml_service_name][0].scoring_uri)

# COMMAND ----------

inputs = score_dataset.to_json(orient='split')
scoring_uri = [x for x in Webservice.list(ws) if x.name == aml_service_name][0].scoring_uri

prediction = query_endpoint_example(scoring_uri=scoring_uri, inputs=inputs)

# COMMAND ----------

# MAGIC %md ## Cleaning up the deployment
# MAGIC 
# MAGIC When your model deployment is no longer needed, run `service.delete()` to delete it.

# COMMAND ----------

service.delete()
