# Agent guide

Personal Elasticsearch experiment from October 2016. A text menu stores a `JustABean` (a map of fields) through `ElasticDAO`. Keep that shape. Do not turn it into a web app, Spring Boot service, or a different search engine.

## Layout

- `src/main/java/beans/JustABean.java` — the document. `metaData` is the Elasticsearch `_source`.
- `src/main/java/dao/ElasticDAO.java` — create, get, search, delete. Prints results; search does not return them.
- `src/main/java/main/Main.java` — the menu. Index name `documentrepository`, type name `document`, ids start at 1.
- `src/test/java` — unit tests, plus `ElasticDAOIntegrationTest`, which skips when no cluster is listening.
- `docker-compose.yml` — single-node Elasticsearch 9.5.5 with security disabled for local use.
- `pom.xml` — Java 21, pinned plugin and dependency versions, Spotless check on `verify`.

Maven coordinates stay `ElasticSearch:ElasticSearch`. Packages stay `beans`, `dao`, and `main`.

## Commands

```bash
./mvnw -B verify          # unit tests, integration tests, format check
./mvnw spotless:apply     # rewrite Java to the formatter
docker compose up         # local cluster at http://localhost:9200
./mvnw -q exec:java       # menu, against ELASTICSEARCH_URL or localhost:9200
```

`ElasticDAOIntegrationTest` runs only when `GET $ELASTICSEARCH_URL` (default `http://localhost:9200`) returns 200. `./mvnw test` is green without a cluster; that test is skipped.

## Conventions

- Java 21. Google Java Format via Spotless. Do not hand-format against it.
- Four DAO methods keep their original arguments, including `typeName`.
- Documents are the bean map, not a wrapped POJO. Search fields are the keys the user typed.
- Match text is a single token (`Scanner.next()`). Do not switch the menu to `nextLine()` unless you also fix the leftover-newline cases and update the tests.
- Credentials come from the environment, never from source: `ELASTICSEARCH_URL`, `ELASTICSEARCH_API_KEY`, or `ELASTICSEARCH_USERNAME` plus `ELASTICSEARCH_PASSWORD`. An API key wins over basic auth.
- No license file. Do not add one.

## Gotchas

- Elasticsearch 2.4's embedded `NodeBuilder` and mapping types are gone. `typeName` is checked and printed on delete, and it is not sent as a type. Do not bring the embedded node back.
- The old Windows path `C:\ElasticSearch\elasticsearch-2.4.1\...` was an install directory, not a URL. The constructor argument is now the cluster URL.
- Create and delete use `refresh=true`. A following get or search in the menu would otherwise miss the write for about a second.
- Search still asks for up to 10,000 hits and prints each `_source` map.
- AND is menu answer `1`. Any other answer is OR. An empty bool matches everything; the menu always sends at least one clause.
- A missing get returns an empty bean. A missing delete still prints the index, type, id, and the version Elasticsearch returns (version 1 when the id was never indexed). If the cluster responds 404, the menu prints `Version: (not found)` instead of aborting.
- Choice `6` sets the next id. Any other non-menu integer still exits. A non-numeric choice stays in the menu.
- `Inserted? true` means the index result was `Created`. Saving the same id again prints `false`.
- The local Compose file turns security off. That is only for a laptop. A real cluster needs the env vars above, and HTTPS additionally needs a CA or fingerprint on the client, which this project does not configure.
- Integration coverage needs a real 9.x node. Mocking the HTTP client is more brittle than skipping when port 9200 is closed.
