
"""An Elasticsearch module designed for development and CI purposes only.


"""
import asyncio
import dataclasses
import json
import contextlib
import logging
from typing import Annotated
from typing_extensions import Doc
import dagger
from dagger import dag, function, field, object_type


@object_type
class Elasticsearch:

    ctr: dagger.Container = dataclasses.field(init=False)
    version: dataclasses.InitVar[str] = "8.13.4"
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
                    .with_mounted_cache(path="/usr/share/elasticsearch/config/models", cache=dag.cache_volume("es-models"), owner="1000:0")
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

        # create the index mapping for the sparse_vector field
        r1 = await self._curl("-X", "PUT",f"{self.host}/{index}?pretty", "-H", "Content-Type: application/json", "-d", self.q_vector_field(rank_field)).stdout()
        await self.set_replica(index)

        # Create an ingest pipeline with an inference processor
        r2 = await self._curl("-X", "PUT",f"{self.host}/_ingest/pipeline/elser-pipeline?pretty", "-H", "Content-Type: application/json", "-d", self.q_ingest_pipeline(rank_field)).stdout()

        # load the data into a temp index
        index_tmp = "index_tmp"
        r3 = await self.index_data(data, index_tmp)
        await self.set_replica(index_tmp)

        # reindex the data to the final index through the inference pipeline
        r4 = await self._curl("-X", "POST",f"{self.host}/_reindex?wait_for_completion=true&pretty", "-H", "Content-Type: application/json", "-d", self.q_reindex_data(index, index_tmp, "elser-pipeline")).stdout()

        return r1 + r2 + r3 + r4

    async def set_replica(self, index):
        if (self.mode == "dev"):
            # see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html#indices-update-settings
            no_replica_query = json.dumps({"index": {"number_of_replicas": "0"}})
            await self._curl("-s","-X", "PUT", f"{self.host}/{index}/_settings?pretty","-H", "Content-Type: application/json", "-d", no_replica_query).stdout()




    @function
    async def semantic_search(self, index: str = "", field: str = "content" , query: str = "") -> str:
        """Elastic Learned Sparse EncodeR - or ELSER - is an NLP model trained by Elastic that enables you to perform semantic search by using sparse vector representation. 
        Instead of literal matching on search terms, semantic search retrieves results based on the intent and the contextual meaning of a search query.

        Documentation: https://www.elastic.co/guide/en/elasticsearch/reference/8.13/semantic-search-elser.html
        """

        return await (
            self._curl("-X", "POST",f"{self.host}/{index}/_search?pretty", "-H", "Content-Type: application/json", "-d", self.q_text_expansion(field, query))
            .stdout()
        )

    def q_vector_field(self, field: str = ""):
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

    def q_ingest_pipeline(self, field: str = ""):
        """Helper function to create an ingest pipeline with an inference processor to use ELSER to infer against the data that is being ingested in the pipeline.
        see Create an ingest pipeline with an inference processor to use ELSER to infer against the data that is being ingested in the pipeline.
        """
        return json.dumps({"processors": [
                                {"inference": {
                                    "model_id": ".elser_model_2",
                                    "input_output": [
                                         {
                                            "input_field": f"{field}",
                                            "output_field": f"{field}_embedding"
                                         }
                                    ]
                                }}]
                            }
                        )

    def q_reindex_data(self, dest_index: str = "", source_index: str = "" ,pipeline: str = ""):
        """Helper function to create the tokens from the text by reindexing the data throught the inference pipeline that uses ELSER as the inference model.
        see https://www.elastic.co/guide/en/elasticsearch/reference/8.13/semantic-search-elser.html#reindexing-data-elser
        """
        return json.dumps({
                            "source": {
                                "index": f"{source_index}",
                                "size": 50
                            },
                            "dest": {
                                "index": f"{dest_index}",
                                "pipeline": f"{pipeline}"
                            }
                        }
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

    @function
    async def deploy_elser(self, id: str = "for_search") -> str:
        """Sends a POST request to the ES service and returns the response.

        This function creates an ELSER model and deploys it using the start trained model deployment API.
        see https://www.elastic.co/search-labs/tutorials/search-tutorial/semantic-search/elser-model
        """
        await self.install_elser()

        # define the model location
        model_location = json.dumps({"persistent": {"xpack.ml.model_repository": "/usr/share/elasticsearch/config/models"}})
        await (
            self._curl("-X", "PUT",f"{self.host}/_cluster/settings?pretty", "-H", "Content-Type: application/json", "-d", model_location)
            .stdout()
        )

        # Is it better to use the Inference API https://www.elastic.co/guide/en/elasticsearch/reference/current/put-inference-api.html#inference-example-elser ?

        # Create the ELSER model configuration
        es_create_elser_model = json.dumps({"input": {"field_names": ["text_field"]}})
        model_creation_output = await (
            self._curl("-X", "PUT",f"{self.host}/_ml/trained_models/.elser_model_2", "-H", "Content-Type: application/json", "-d", es_create_elser_model).stdout()
        )

        # Deploy the model by using the start trained model deployment API
        model_deployment_output = await (
            self._curl("-X", "POST",f"{self.host}/_ml/trained_models/.elser_model_2/deployment/_start?wait_for=started&timeout=200m&deployment_id={id}").stdout()
        )

        return model_creation_output + model_deployment_output

    @function
    async def install_elser(self) -> str:
        models_files_urls = ["https://ml-models.elastic.co/elser_model_2.metadata.json",
                        "https://ml-models.elastic.co/elser_model_2.pt",
                        "https://ml-models.elastic.co/elser_model_2.vocab.json"]
        out = ""
        for model_file_url in models_files_urls:
            out += await self.curl.with_user("1000:0").with_workdir("/tmp").with_exec(["-X", "GET",model_file_url, "-O", "-m", "120"]).with_mounted_cache(path="/usr/share/elasticsearch/config/models", cache=dag.cache_volume("es-models"), owner="1000:0").stdout()

        return out
