
"""An Elasticsearch module designed for development and CI purposes only.


"""
import dataclasses
import json
import contextlib
from typing import Annotated
from typing_extensions import Doc
import dagger
from dagger import dag, function, field, object_type




@object_type
class Elasticsearch:

    ctr: dagger.Container = dataclasses.field(init=False)
    version: dataclasses.InitVar[str] = "8.13.2"
    mode: dataclasses.InitVar[str] = "dev"
    java_opts: dataclasses.InitVar[str] = "-Xms4g -Xms4g"
    port: dataclasses.InitVar[int] = 9200


    curl: Annotated[dagger.Container, Doc("The container to run curl to call the Elasticsearch APIs")] = dag.container().from_("chainguard/curl")

    @property
    def host(self):
        return f"http://es:{self.port}"

    def __post_init__(self, version: str, mode: str, java_opts: str, port: int):
         # The container runs Elasticsearch as user elasticsearch using uid:gid 1000:0.
        # Bind mounted host directories and files must be accessible by this user, and the data and log directories must be writable by this user.
        self.ctr = (
            dag.container().from_(f"docker.elastic.co/elasticsearch/elasticsearch:{version}")
                    .with_mounted_cache(path="/usr/share/elasticsearch/data", cache=dag.cache_volume("es-data"), sharing=dagger.CacheSharingMode.SHARED, owner="1000:0")
                    .with_env_variable("discovery.type", "single-node")
                    .with_env_variable("xpack.security.enabled", str(mode != "dev").lower())
                    .with_env_variable("xpack.security.http.ssl.enabled", str(mode != "dev").lower())
                    .with_env_variable("xpack.license.self_generated.type", "trial")
                    .with_env_variable("ES_JAVA_OPTS", java_opts)
        )


    @contextlib.asynccontextmanager
    async def managed_service(self):
        """Start and stop a service."""

        svc = None
        try:
            svc = await self.service().start()
            yield self.curl.with_service_binding("es", svc)
        finally:
            if svc is not None:
                await svc.stop()



    @function
    def service(self) -> dagger.Service:
        """Create an Elasticsearch service in dev mode by default"""

        return self.ctr.with_exposed_port(self.port).as_service()


    @function
    async def get(self, path: str = "") -> str:
        """Sends a GET request to the ES service and returns the response."""

        async with self.managed_service() as es_cli:
           if (self.mode == "dev"):
                # see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#indices-update-settings
                no_replica_query = json.dumps({"index": {"number_of_replicas": "0"}})
                await es_cli.with_exec(["-s","-X", "PUT", f"{self.host}/_all/_settings?pretty","-H", "Content-Type: application/json", "-d", no_replica_query]).stdout()

           return await es_cli.with_exec(["-s", f"{self.host}/{path}"]).stdout()

    @function
    async def delete(self, index: str = "") -> str:
        """Delete an Elasticsearch index."""

        async with self.managed_service() as es_cli:
           return await es_cli.with_exec(["-s","-X", "DELETE", f"{self.host}/{index}"]).stdout()

    @function
    async def index_data(self, data: dagger.File, index: str = "my-index") -> str:
        """Index a document from a file into Elasticsearch."""

        async with self.managed_service() as es_cli:
           resp = await (
                    es_cli
                    .with_mounted_file("/data.json", data)
                    .with_workdir("/")
                    .with_exec(["-X", "POST",f"{self.host}/{index}/_doc?pretty", "-H", "Content-Type: application/x-ndjson", "--data-binary", "@data.json"])
                    .stdout()
                )
           if (self.mode == "dev"):
                # see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#indices-update-settings
                no_replica_query = json.dumps({"index": {"number_of_replicas": "0"}})
                await es_cli.with_exec(["-s","-X", "PUT", f"{self.host}/{index}/_settings?pretty","-H", "Content-Type: application/json", "-d", no_replica_query]).stdout()

           return resp

    @function
    async def index_bulk_data(self, data: dagger.File) -> str:
        """Index documents into Elasticsearch with the bulk API.

        The data file should be in a format that is compatible with the Elasticsearch bulk API.

        Doc: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk
        
        """

        async with self.managed_service() as es_cli:

           resp = await (
                es_cli
                .with_mounted_file("/data.json", data)
                .with_workdir("/")
                .with_exec(["-X", "POST",f"{self.host}/_bulk?pretty", "-H", "Content-Type: application/x-ndjson", "--data-binary", "@data.json"])
                .stdout()
            )
           if (self.mode == "dev"):
                # see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#indices-update-settings
                no_replica_query = json.dumps({"index": {"number_of_replicas": "0"}})
                await es_cli.with_exec(["-s","-X", "PUT", f"{self.host}/_all/_settings?pretty","-H", "Content-Type: application/json", "-d", no_replica_query]).stdout()

           return resp

    @function
    async def search(self, index: str = "", field: str = "" , query: str = "") -> str:
        """Returns search hits that match the query defined in the request."""

        async with self.managed_service() as es_cli:

           es_query_string = json.dumps({"query": {"match": {f"{field}": f"{query}"}}})
           return await (
                es_cli
                .with_exec(["-X", "POST",f"{self.host}/{index}/_search?pretty", "-H", "Content-Type: application/json", "-d", es_query_string])
                .stdout()
            )

