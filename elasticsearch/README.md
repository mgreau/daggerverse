# Elasticsearch Dagger

An Elasticsearch dagger module.

**Pre-requisites**

- Install dagger `brew install dagger` (or check the official doc)
- Have a container runtime running locally, like Docker Desktop

## Examples

### full text search

Index a list of movies into a `movies` index
```sh
dagger call index-data --index="movies" --data ./datasets/movies.json
```

Search
```sh
dagger call search --index="movies" --field="title" --query="Inception"

{
  "took" : 48,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 1,
      "relation" : "eq"
    },
    "max_score" : 2.7946267,
    "hits" : [
      {
        "_index" : "books",
        "_id" : "AD_uWo8BzYVJ-bdFNzhj",
        "_score" : 2.7946267,
        "_source" : {
          "title" : "Inception",
          "runtime" : "148",
          "plot" : "A thief who steals corporate secrets through the use of dream-sharing technology is given the inverse task of planting an idea into thed of a C.E.O.",
          "keyScene" : "Leonardo DiCaprio explains the concept of inception to Ellen Page by using a child's spinning top.",
          "genre" : "Action, Adventure, Sci-Fi, Thriller",
          "released" : "2010"
        }
      }
    ]
  }
}
```


**List of useful commands**


```sh
dagger call get --path="_cluster/health/movies?level=shards" | jq .
```
```sh
dagger call delete --index="movies"
```
