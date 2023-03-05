from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterized_flow import etl_parent_flow

docker_block = DockerContainer.load("zoomcamp")

# create deplyment schema from github
docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='docker-flow2',
    infrastructure=docker_block
)

# apply deployment schema to prefect
if __name__ == '__main__':
    docker_dep.apply()