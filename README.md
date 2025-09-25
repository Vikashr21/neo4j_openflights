# Neo4j OpenFlights Data Loader

This repository contains a Python script and accompanying instructions for
loading the [OpenFlights](https://openflights.org/data.html) dataset into a
[Neo4j](https://neo4j.com/) graph database.  The dataset consists of three
comma‑separated files: `airports.dat`, `airlines.dat` and `routes.dat`.  Each
file describes a different entity in the flight network:

* `airports.dat` – basic information about each airport (identifier, name,
  city, country, IATA and ICAO codes, latitude, longitude and more).
* `airlines.dat` – basic information about each airline (identifier, name,
  IATA/ICAO codes, callsign, country and active flag).
* `routes.dat` – details of individual flight routes between two airports
  including airline, equipment and number of stops.

The provided `load_openflights.py` script reads these files and uses the
official Neo4j Python driver to create nodes and relationships in your Neo4j
database.  Airports and airlines become nodes; flights become directed
relationships between airports.  The script is written to run on systems
without GPUs and has been tested on a typical Windows environment using the
Community edition of IntelliJ as an IDE.

## Getting started

1. **Install Python dependencies**

   Ensure you have Python 3.7+ installed.  Install the required packages
   (Neo4j driver and pandas) with pip.  You can do this from your system
   terminal or from IntelliJ’s terminal:

   ```bash
   pip install -r requirements.txt
   ```

   If you already installed `neo4j`, `pandas`, `numpy`, `requests` and
   `tqdm` via pip (as you mentioned), you should be ready to go.

2. **Prepare your Neo4j database**

   Make sure you have a Neo4j server running (either locally or hosted via
   [AuraDB](https://neo4j.com/cloud/aura/) or another provider).  You must
   have the ability to connect via the Bolt protocol.  Note the URI, user and
   password for your database.

3. **Place the data files**

   Copy `airports.dat`, `airlines.dat` and `routes.dat` into this
   repository’s root directory (`neo4j_openflights`).  The script assumes
   these files are present alongside itself.  The `.dat` files you uploaded
   are already present in the sample environment; adjust the paths in the
   script if you store them elsewhere.

4. **Configure connection details**

   The script reads your Neo4j connection settings from environment
   variables.  Before running, set the following variables in your terminal
   or via IntelliJ’s run configuration:

   * `NEO4J_URI` – Bolt URI for your database (e.g. `bolt://localhost:7687`
     for a local instance or `neo4j+s://<hostname>.databases.neo4j.io` for
     AuraDB).
   * `NEO4J_USER` – your database username (often `neo4j`).
   * `NEO4J_PASSWORD` – your database password.

   On Windows PowerShell you might run:

   ```powershell
   $env:NEO4J_URI="bolt://localhost:7687"
   $env:NEO4J_USER="neo4j"
   $env:NEO4J_PASSWORD="your-password"
   ```

   On Linux/macOS bash you would use:

   ```bash
   export NEO4J_URI="bolt://localhost:7687"
   export NEO4J_USER="neo4j"
   export NEO4J_PASSWORD="your-password"
   ```

5. **Run the loader**

   Execute the script to import the data.  It will create constraints to
   ensure uniqueness of airports and airlines, then insert nodes and
   relationships in batches.  From the project root run:

   ```bash
   python load_openflights.py
   ```

   The script prints progress information for each stage.  Large imports may
   take a few minutes depending on your hardware and the number of routes.

6. **Explore the graph**

   After loading completes, open the Neo4j Browser (usually at
   `http://localhost:7474` for local installations) and run queries such
   as:

   ```cypher
   // Number of airports and airlines
   MATCH (a:Airport) RETURN count(a) AS airports;
   MATCH (a:Airline) RETURN count(a) AS airlines;

   // Top hubs by number of destinations
   MATCH (src:Airport)-[:FLIGHT]->(dst:Airport)
   WITH src, count(DISTINCT dst) AS destinations
   RETURN src.iata AS code, src.city AS city, destinations
   ORDER BY destinations DESC LIMIT 10;

   // Shortest path (by number of hops) between two IATA codes
   MATCH (start:Airport {iata: 'DEL'}), (end:Airport {iata: 'JFK'})
   MATCH p = shortestPath((start)-[:FLIGHT*..4]->(end))
   RETURN [n IN nodes(p) | n.iata] AS route,
          length(p) AS hops;
   ```

   Since multiple flights may exist between the same airports, each
   `FLIGHT` relationship stores `airline_id`, `airline_code` (IATA code of
   the carrier), `codeshare` (if any), `stops` (number of stops) and
   `equipment` (aircraft type).  You can filter paths by specific airlines or
   equipment using these properties.

## Project structure

```
neo4j_openflights/
├── README.md          # This file with usage instructions
├── requirements.txt   # Python dependencies for easy installation
└── load_openflights.py# Main script to import OpenFlights data
```

Feel free to modify the script to suit your use case—for example,
associating airlines with the `FLIGHT` relationships directly as
relationships between `Airline` and `Airport` nodes.  The basic
implementation provided here should serve as a solid starting point for
experimenting with graph queries that are difficult to express in
traditional SQL (e.g. multi‑hop route searches, hub detection, community
detection, etc.).