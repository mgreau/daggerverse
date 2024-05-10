
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
    java_opts: dataclasses.InitVar[str] = "-Xms4g -Xms4g"

    port: int = field(default=9200)
    mode: str = field(default="dev")

    curl: dagger.Container = dag.container().from_("chainguard/curl")

    @property
    def host(self):
        return f"http://es:{self.port}"

    def __post_init__(self, version: str, java_opts: str):
         # The container runs Elasticsearch as user elasticsearch using uid:gid 1000:0.
        # Bind mounted host directories and files must be accessible by this user, and the data and log directories must be writable by this user.
        self.ctr = (
            dag.container().from_(f"docker.elastic.co/elasticsearch/elasticsearch:{version}")
                    .with_mounted_cache(path="/usr/share/elasticsearch/data", cache=dag.cache_volume("es-data"), owner="1000:0")
                    .with_env_variable("discovery.type", "single-node")
                    .with_env_variable("xpack.security.enabled", str(self.mode != "dev").lower())
                    .with_env_variable("xpack.security.http.ssl.enabled", str(self.mode != "dev").lower())
                    .with_env_variable("xpack.license.self_generated.type", "trial")
                    .with_env_variable("ES_JAVA_OPTS", java_opts)
        )

    def _curl(self, *args) -> dagger.Container:
        return (
            self.curl
            .with_service_binding("es", self.service())
            .with_exec([*args])
        )

    def format_data_for_bulk_indexing(self, documents: str, index: str) -> str:
        """Format data for bulk indexing."""
        operations = []
        for document in documents:
            operations.append(json.dumps({"index": {"_index": index}}))
            operations.append(json.dumps(document))

        return "\n".join([str(line) for line in operations]) + "\n"

    @function
    def service(self) -> dagger.Service:
        """Create an Elasticsearch service in dev mode by default"""
        return self.ctr.with_exposed_port(self.port).as_service()


    @function
    async def get(self, path: str = "") -> str:
        """Sends a GET request to the ES service and returns the response.

            dagger call --version 8.13.2 get --path="_cat/indices?v"
        """
        await self.set_replica("_all")
        return await self._curl("-s", f"{self.host}/{path}").stdout()

    @function
    async def delete(self, index: str = "") -> str:
        """Delete an Elasticsearch index.

         example: dagger call delete --index="movies"
        """
        return await self._curl("-s","-X", "DELETE", f"{self.host}/{index}").stdout()

    @function
    async def index_data(self,data: dagger.File, index: str = "my-index") -> str:
        """Index documents into Elasticsearch.

           example: dagger call index-data --index="movies" --data ./datasets/movies.json

           Automatically formats the JSON data into the ES bulk format.
           Doc: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#docs-bulk
        
        """
        # Make the JSON content compatible with the Elasticsearch bulk format
        operations = self.format_data_for_bulk_indexing(json.loads(await data.contents()), index)

        resp = await (
            self.curl
            .with_service_binding("es", self.service())
            .with_new_file("/data.json", contents=operations).with_workdir("/")
            .with_exec(["-X", "POST",f"{self.host}/_bulk?pretty", "-H", "Content-Type: application/json", "--data-binary", "@data.json"])
            .stdout()
        )
        await self.set_replica(index)
        return resp

    @function
    async def search(self, index: str = "", field: str = "" , query: str = "") -> str:
        """Returns search hits that match the query defined in the request.

           example: dagger call search --index="movies" --field="title" --query="Inception"
        """

        es_query_string = json.dumps({"query": {"match": {f"{field}": f"{query}"}}})
        return await (
                self._curl("-X", "POST",f"{self.host}/{index}/_search?pretty", "-H", "Content-Type: application/json", "-d", es_query_string)
                .stdout()
        )
    
    @function
    async def semantic_search_index_data(self, data: dagger.File, index: str = "my-index", rank_field: str = "content") -> str:
        """Index documents into Elasticsearch for semantic search.
        
        """
        # Make the JSON content compatible with the Elasticsearch bulk format
        operations = self.format_data_for_bulk_indexing(json.loads(await data.contents()), index)

        resp = await (
            self.curl
            .with_service_binding("es", self.service())
            .with_new_file("/data.json", contents=operations).with_workdir("/")
            .with_exec(["-X", "POST",f"{self.host}/_bulk?pretty", "-H", "Content-Type: application/json", "--data-binary", "@data.json"])
            .stdout()
        )
        await self.set_replica(index)

        return resp

    async def set_replica(self, index):
        if (self.mode == "dev"):
            # see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#indices-update-settings
            no_replica_query = json.dumps({"index": {"number_of_replicas": "0"}})
            await self._curl("-s","-X", "PUT", f"{self.host}/{index}/_settings?pretty","-H", "Content-Type: application/json", "-d", no_replica_query).stdout()




    @function
    async def semantic_search(self, index: str = "", field: str = "" , query: str = "") -> str:
        """Elastic Learned Sparse EncodeR - or ELSER - is an NLP model trained by Elastic that enables you to perform semantic search by using sparse vector representation. 
        Instead of literal matching on search terms, semantic search retrieves results based on the intent and the contextual meaning of a search query.

        Documentation: https://www.elastic.co/guide/en/elasticsearch/reference/8.13/semantic-search-elser.html
        """

        return await (
            self._curl("-X", "POST",f"{self.host}/{index}/_search?pretty", "-H", "Content-Type: application/json", "-d", self.q_text_expansion(field, query))
            .stdout()
        )

    def q_vector_field(self, field: str = "" , query: str = ""):
        """Helper function to create the index mapping for the vector field.
        see https://www.elastic.co/guide/en/elasticsearch/reference/8.13/semantic-search-elser.html#elser-mappings
        """
        return json.dumps({"mappings": {
                                        "properties": {
                                                f"{field}_embedding": {
                                                    "type": "sparse_vector"
                                                },
                                                f"{field}": {
                                                    "type": "text"
                                                }
                                        }}}
                        )


    def q_text_expansion(self, field: str = "" , query: str = ""):
        """Helper function to format the query for semantic search.
        see https://www.elastic.co/guide/en/elasticsearch/reference/8.13/semantic-search-elser.html#text-expansion-query
        """
        return json.dumps({"query": {
                                        "text_expansion": {
                                                f"{field}": {
                                                    "model_id": ".elser_model_2",
                                                    "model_text": f"{query}"
                                                }
                                        }}}
                        )
