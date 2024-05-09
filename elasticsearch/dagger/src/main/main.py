
"""A dagger module to use Elasticsearch


"""
import json
import contextlib
import dagger
from dagger import dag, function, field, object_type


def init() -> dagger.Container:
    
    return (
        # The container runs Elasticsearch as user elasticsearch using uid:gid 1000:0. 
        # Bind mounted host directories and files must be accessible by this user, and the data and log directories must be writable by this user.
        dag.container().from_(f"docker.elastic.co/elasticsearch/elasticsearch:{Elasticsearch.version}")
                    .with_mounted_cache(path="/usr/share/elasticsearch/data", cache=dag.cache_volume("es-data"), sharing=dagger.CacheSharingMode.SHARED, owner="1000:0")
                    .with_env_variable("discovery.type", "single-node")
                    .with_env_variable("xpack.security.enabled", str(Elasticsearch.mode != "dev"))
                    .with_env_variable("xpack.security.http.ssl.enabled", str(Elasticsearch.mode != "dev"))
                    .with_env_variable("xpack.license.self_generated.type", "trial")
                    .with_env_variable("ES_JAVA_OPTS", str(Elasticsearch.java_opts))
    )

@contextlib.asynccontextmanager
async def managed_service(svc: dagger.Service):
    """Start and stop a service."""
    yield await svc.start()
    await svc.stop()

@object_type
class Elasticsearch:

    ctr: dagger.Container = field(default=init)
    version: str = field(default="8.13.2")
    mode: str = field(default="dev")
    java_opts: str = field(default="-Xms4g -Xms4g")

    curl: dagger.Container = dag.container().from_("chainguard/curl")

    @function
    async def service(self, port: int = 9200) -> dagger.Service:
        """Get an Elasticsearch service in dev mode by default (authentication and encryption disabled) and return the container object"""

        return self.ctr.with_exposed_port(port).as_service()


    @function
    async def get(self, path: str = "", port: int = 9200) -> str:
        """Sends a GET request to the ES service and returns the response."""

        es_service = await self.service(port)
        async with managed_service(es_service) as es_service:
           es_cli = self.curl.with_service_binding("es", es_service)
           if (self.mode == "dev"):
                # see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#indices-update-settings
                no_replica_query = json.dumps({"index": {"number_of_replicas": "0"}})
                await es_cli.with_exec(["-s","-X", "PUT", f"http://es:{port}/_all/_settings?pretty","-H", "Content-Type: application/json", "-d", no_replica_query]).stdout()

           return await es_cli.with_exec(["-s", f"http://es:{port}/{path}"]).stdout()

    @function
    async def delete(self, index: str = "", port: int = 9200) -> str:
        """Sends a DELETE request to the ES service and returns the response."""

        es_service = await self.service(port)
        async with managed_service(es_service) as es_service:
           es_cli = self.curl.with_service_binding("es", es_service)
           return await es_cli.with_exec(["-s","-X", "DELETE", f"http://es:{port}/{index}"]).stdout()

    @function
    async def index_data(self, data: dagger.File, index: str = "my-index", port: int = 9200) -> str:
        """Index documents from a file to Elasticsearch."""

        es_service = await self.service(port)
        async with managed_service(es_service) as es_service:
           es_cli = self.curl.with_service_binding("es", es_service)
           resp = await (
                    es_cli
                    .with_mounted_file("/data.json", data)
                    .with_workdir("/")
                    .with_exec(["-X", "POST",f"http://es:{port}/{index}/_doc?pretty", "-H", "Content-Type: application/x-ndjson", "--data-binary", "@data.json"])
                    .stdout()
                )
           if (self.mode == "dev"):
                # see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#indices-update-settings
                no_replica_query = json.dumps({"index": {"number_of_replicas": "0"}})
                await es_cli.with_exec(["-s","-X", "PUT", f"http://es:{port}/{index}/_settings?pretty","-H", "Content-Type: application/json", "-d", no_replica_query]).stdout()

           return resp

    @function
    async def index_bulk_data(self, data: dagger.File, port: int = 9200) -> str:
        """Index documents to Elasticsearch using the bulk API."""

        es_service = await self.service(port)
        async with managed_service(es_service) as es_service:
           es_cli = self.curl.with_service_binding("es", es_service)

           resp = await (
                es_cli
                .with_mounted_file("/data.json", data)
                .with_workdir("/")
                .with_exec(["-X", "POST",f"http://es:{port}/_bulk?pretty", "-H", "Content-Type: application/x-ndjson", "--data-binary", "@data.json"])
                .stdout()
            )
           if (self.mode == "dev"):
                # see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#indices-update-settings
                no_replica_query = json.dumps({"index": {"number_of_replicas": "0"}})
                await es_cli.with_exec(["-s","-X", "PUT", f"http://es:{port}/_all/_settings?pretty","-H", "Content-Type: application/json", "-d", no_replica_query]).stdout()

           return resp

    @function
    async def full_text_search(self, index: str = "", field: str = "" , query: str = "", port: int = 9200) -> str:
        """Sends a POST request to the ES service and returns the response."""

        es_service = await self.service(port)
        async with managed_service(es_service) as es_service:
           es_cli = self.curl.with_service_binding("es", es_service)

           es_query_string = json.dumps({"query": {"match": {f"{field}": f"{query}"}}})
           return await (
                es_cli
                .with_exec(["-X", "POST",f"http://es:{port}/{index}/_search?pretty", "-H", "Content-Type: application/json", "-d", es_query_string])
                .stdout()
            )

