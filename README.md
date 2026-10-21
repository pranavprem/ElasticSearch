# ElasticSearch

A small Java text menu that stores a bean in Elasticsearch and gets, searches, and deletes it again.

The menu talks to one data-access class. That class indexes the bean's fields, loads them back by id, runs an AND or OR match query, and deletes by id. There is no web UI. The whole program is the menu in the terminal.

## Original context

Pranav Prem (`pranavprem`, commits as `vanarp96`) wrote this on 2 October 2016 as a personal experiment. The repository description and the first README both say "My Experiments with ElasticSearch". The objective in that README was "to build a working DAO class that will perform basic operations on a simple bean." The only code commit, later the same evening, is "Working concept for Elastic Search with a TUI to use it."

Nothing in the history, the README, or the code names a course, a company, or an internship. It was last touched that day.

The 2016 project was an Eclipse Maven module pinned to Elasticsearch 2.4.1. It started an embedded node from a hardcoded Windows install path, `C:\ElasticSearch\elasticsearch-2.4.1\elasticsearch-2.4.1`, and stored documents under an index plus a mapping type. That client and those mapping types are gone, so this tree now talks to a normal Elasticsearch cluster over HTTP. The menu, the bean, and the four DAO operations are the same idea.

## How it works

`JustABean` is a map. The keys and values you type are the Elasticsearch document. They are not wrapped in another object.

`ElasticDAO` is the DAO:

| Call | What it does |
| --- | --- |
| `createDocument` | Indexes the map at the id you pass. Prints `Inserted? true` the first time that id is saved, and `false` if that id already existed. |
| `getDocument` | Loads that id and returns a bean. A missing id returns an empty bean. |
| `searchDocument` | Match query across the fields you pass. `true` means every field must match (AND). Anything else means any field may match (OR). Prints up to 10,000 hits. |
| `deleteDocument` | Deletes that id and prints the index, type name, id, and version. |

The menu in `Main` always uses index `documentrepository` and type name `document`. Ids start at 1 and increase after each create. Elasticsearch 8 removed mapping types, so the type name is no longer sent to the cluster. It is still part of the DAO calls, and delete still prints it, because that was the shape of the original class.

Create and delete refresh the index before they return, so the next get or search in the menu can see the write.

```
you type fields
      |
      v
  JustABean  --map-->  ElasticDAO  --HTTP-->  Elasticsearch index
      ^                     |
      |                     v
      +-------- menu prints the bean, the hits, or the delete line
```

## How to run it today

You need Java 21 and a local Elasticsearch 9.5 node. Docker is the straightforward way to get the node. The Compose file turns security off so the client can connect without a password. Use that only on your own machine.

```bash
docker compose up
```

Wait until `curl http://localhost:9200` returns cluster JSON, then:

```bash
./mvnw -q exec:java
```

The Maven wrapper downloads Maven on first use. A URL argument overrides the default:

```bash
./mvnw -q exec:java -Dexec.args="http://127.0.0.1:9200"
```

Environment variables, when the cluster is not the local open one:

| Variable | Role |
| --- | --- |
| `ELASTICSEARCH_URL` | Cluster URL. Defaults to `http://localhost:9200`. |
| `ELASTICSEARCH_API_KEY` | API key. Used instead of basic auth when it is set. |
| `ELASTICSEARCH_USERNAME` | Basic-auth user. |
| `ELASTICSEARCH_PASSWORD` | Basic-auth password. Required when the username is set. |

No credentials are stored in the source. HTTPS with a cluster CA is not wired up; the local Compose cluster speaks plain HTTP.

### Menu

Field names and values are one token each. `hello world` is two tokens, so use a single word, or the next prompt will consume the extra word.

1. **Create Doc.** Shows the id, then asks for `key` and `value` until you type `EXIT`. `EXIT` is not stored.
2. **Get Doc.** Prints the fields for the id you type.
3. **Search Doc.** One or more field/value pairs. Answer `n` to stop, then `1` for AND or anything else for OR.
4. **Remove Doc.** Deletes the id you type.
5. **Exit.** Any other integer exits too.
6. **Set next id.** The following create uses this id.

A word that is not a number stays in the menu. A failed request prints `Request failed: ...` and returns to the menu.

### Example session

This is a terminal menu, so there is no graphical screen to capture. The transcript below is real output from Elasticsearch 9.5.5 on this project. The program does not echo keystrokes. The tokens typed, in order, were: `1`, `city`, `Paris`, `EXIT`, `2`, `1`, `3`, `city`, `Paris`, `n`, `1`, `4`, `1`, `5`.

```
Using Elasticsearch at http://localhost:9200
what do you want to do?
1. Create Doc
2. Get Doc
3. Search Doc
4. Remove Doc
5. Exit
6. Set next id
Choice:
id  is: 1
Enter the specs of the new doc:
Enter key of data to store for doc (enter 'EXIT' if completed defining doc):
Enter the value for the key - city:
Enter key of data to store for doc (enter 'EXIT' if completed defining doc):
Inserted? true
what do you want to do?
1. Create Doc
2. Get Doc
3. Search Doc
4. Remove Doc
5. Exit
6. Set next id
Choice:
Enter the Id of the doc you need:
city : Paris

what do you want to do?
1. Create Doc
2. Get Doc
3. Search Doc
4. Remove Doc
5. Exit
6. Set next id
Choice:
Enter the field you wish to search for
Enter the value in the field you wish to search for
more?
And(1)/Or(2)?
Searching for Paris in city
------------------------------
{city=Paris}
what do you want to do?
1. Create Doc
2. Get Doc
3. Search Doc
4. Remove Doc
5. Exit
6. Set next id
Choice:
Enter the id of the doc you wish to delete:
Information on deleted Document
Index: documentrepository
Type: document
Id: 1
Version: 2
what do you want to do?
1. Create Doc
2. Get Doc
3. Search Doc
4. Remove Doc
5. Exit
6. Set next id
Choice:
```

### Tests

```bash
./mvnw -B verify
```

That compiles, runs the unit tests, runs the Elasticsearch integration test when a cluster is up, and checks formatting. Without a cluster the integration test is skipped and the rest still pass. `./mvnw spotless:apply` rewrites Java sources to the formatter.

## Tech stack

- Java 21
- Maven, with the wrapper in `mvnw` and plugins pinned in `pom.xml`
- Elasticsearch Java API Client 9.5.5 (HTTP, not the removed embedded node)
- Elasticsearch 9.5.5, via Docker Compose or the CI service container
- JUnit 6.1.3
- Spotless 3.10.4 with Google Java Format

The 2016 code used Java 8 and Elasticsearch 2.4.1.

## Project structure

```
.
├── .github/workflows/ci.yml    # Java 21, Elasticsearch service, ./mvnw verify
├── src/main/java/beans/        # JustABean
├── src/main/java/dao/          # ElasticDAO
├── src/main/java/main/         # text menu
├── src/test/java/              # unit tests and one live-cluster test
├── docker-compose.yml          # local single-node cluster, security off
├── pom.xml
├── AGENTS.md                   # guide for coding agents
└── CLAUDE.md                   # points at AGENTS.md
```

The original sources lived in an Eclipse project folder named `ElasticSearch/`, with `.project`, `.classpath`, and `.settings`. Those IDE files are not part of the build. Maven is the build.
