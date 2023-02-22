from prefect.deployments import Deployment
from web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("zoom-github")

deployment = Deployment.build_from_flow(
     flow = etl_web_to_gcs,
     name = "github-example",
     storage = storage,
     entrypoint = "flows/etl_web_to_gcs.py:etl_web_to_gcs")

if __name__ == "__main__":
    deployment.apply()