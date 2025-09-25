import argparse
import os
import sys
import pandas as pd
from neo4j import GraphDatabase, basic_auth
from tqdm import tqdm

# NEO4J_URI="neo4j://127.0.0.1:7687"
NEO4J_URI="bolt://127.0.0.1:7687"
NEO4J_USER="neo4j"
NEO4J_PASSWORD="password"


def parse_airports(path: str) -> list:
    """Parse airports.dat into a list of dictionaries.

    Each row in airports.dat has the following fields:
        0: Airport ID (int)
        1: Name
        2: City
        3: Country
        4: IATA code (may be "\\N" to denote a missing value)
        5: ICAO code
        6: Latitude
        7: Longitude
        8: Altitude
        9: Timezone offset from UTC
        10: DST (Daylight savings) indicator
        11: Timezone name
        12: Type (e.g. airport, closed)
        13: Source

    Returns:
        A list of dictionaries suitable for parameterised Cypher queries.
    """
    # Read using pandas for convenience; specify header=None because the file
    # does not include column names, and specify quoting to handle quoted
    # strings.  Keep dtype=str so that we can normalise null values.
    cols = [
        'airport_id', 'name', 'city', 'country', 'iata', 'icao',
        'latitude', 'longitude', 'altitude', 'timezone_offset', 'dst',
        'timezone', 'type', 'source'
    ]
    df = pd.read_csv(
        path,
        header=None,
        names=cols,
        dtype=str,
        na_values=['\\N'],
        keep_default_na=True,
        quotechar='"'
    )
    # Convert numeric columns
    df['airport_id'] = df['airport_id'].astype(int)
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['altitude'] = pd.to_numeric(df['altitude'], errors='coerce')
    df['timezone_offset'] = pd.to_numeric(df['timezone_offset'], errors='coerce')
    # Replace NaN with None for Neo4j compatibility
    df = df.where(pd.notnull(df), None)
    return df.to_dict('records')


def parse_airlines(path: str) -> list:
    """Parse airlines.dat into a list of dictionaries.

    Each row in airlines.dat has the following fields:
        0: Airline ID (int)
        1: Name
        2: Alias
        3: IATA code
        4: ICAO code
        5: Callsign
        6: Country
        7: Active (Y/N)

    Returns:
        A list of dictionaries suitable for parameterised Cypher queries.
    """
    cols = [
        'airline_id', 'name', 'alias', 'iata', 'icao', 'callsign',
        'country', 'active'
    ]
    df = pd.read_csv(
        path,
        header=None,
        names=cols,
        dtype=str,
        na_values=['\\N'],
        keep_default_na=True,
        quotechar='"'
    )
    df['airline_id'] = pd.to_numeric(df['airline_id'], errors='coerce').astype('Int64')
    df = df.where(pd.notnull(df), None)
    return df.to_dict('records')


def parse_routes(path: str) -> list:
    """Parse routes.dat into a list of dictionaries.

    Each row in routes.dat has the following fields:
        0: Airline code (IATA or ICAO)
        1: Airline ID
        2: Source airport code (IATA or ICAO)
        3: Source airport ID
        4: Destination airport code (IATA or ICAO)
        5: Destination airport ID
        6: Codeshare (empty if not codeshare)
        7: Stops (number of stops on route)
        8: Equipment (aircraft types separated by space)

    Returns:
        A list of dictionaries suitable for parameterised Cypher queries.
    """
    cols = [
        'airline_code', 'airline_id', 'src_code', 'src_id',
        'dst_code', 'dst_id', 'codeshare', 'stops', 'equipment'
    ]
    df = pd.read_csv(
        path,
        header=None,
        names=cols,
        dtype=str,
        na_values=['\\N'],
        keep_default_na=True,
        # Some routes entries may not have quotes; but that's fine
    )
    # Convert numeric columns
    df['airline_id'] = pd.to_numeric(df['airline_id'], errors='coerce').astype('Int64')
    df['src_id'] = pd.to_numeric(df['src_id'], errors='coerce').astype('Int64')
    df['dst_id'] = pd.to_numeric(df['dst_id'], errors='coerce').astype('Int64')
    df['stops'] = pd.to_numeric(df['stops'], errors='coerce').fillna(0).astype(int)
    df = df.where(pd.notnull(df), None)
    # We care only about rows with valid source and destination IDs
    df = df[df['src_id'].notna() & df['dst_id'].notna()]
    return df.to_dict('records')


def create_constraints(session):
    """Create uniqueness constraints for Airport and Airline nodes."""
    session.run(
        "CREATE CONSTRAINT airport_id IF NOT EXISTS FOR (a:Airport) REQUIRE a.airport_id IS UNIQUE"
    )
    session.run(
        "CREATE CONSTRAINT airline_id IF NOT EXISTS FOR (al:Airline) REQUIRE al.airline_id IS UNIQUE"
    )


def load_airports(session, airports: list, batch_size: int = 1000):
    """Insert Airport nodes into Neo4j in batches."""
    query = """
    UNWIND $rows AS row
    MERGE (a:Airport {airport_id: row.airport_id})
      SET a.name = row.name,
          a.city = row.city,
          a.country = row.country,
          a.iata = row.iata,
          a.icao = row.icao,
          a.latitude = row.latitude,
          a.longitude = row.longitude,
          a.altitude = row.altitude,
          a.timezone_offset = row.timezone_offset,
          a.dst = row.dst,
          a.timezone = row.timezone,
          a.type = row.type,
          a.source = row.source
    """
    for i in tqdm(range(0, len(airports), batch_size), desc="Loading airports"):
        batch = airports[i:i + batch_size]
        session.run(query, rows=batch)


def load_airlines(session, airlines: list, batch_size: int = 1000):
    """Insert Airline nodes into Neo4j in batches."""
    query = """
    UNWIND $rows AS row
    MERGE (al:Airline {airline_id: row.airline_id})
      SET al.name = row.name,
          al.alias = row.alias,
          al.iata = row.iata,
          al.icao = row.icao,
          al.callsign = row.callsign,
          al.country = row.country,
          al.active = row.active
    """
    for i in tqdm(range(0, len(airlines), batch_size), desc="Loading airlines"):
        batch = airlines[i:i + batch_size]
        session.run(query, rows=batch)


def load_routes(session, routes: list, batch_size: int = 1000):
    """Create FLIGHT relationships between Airport nodes.

    Each route dictionary should have keys:
      src_id, dst_id, airline_id, airline_code, codeshare, stops, equipment
    """
    query = """
    UNWIND $rows AS row
    MATCH (src:Airport {airport_id: row.src_id})
    MATCH (dst:Airport {airport_id: row.dst_id})
    CREATE (src)-[:FLIGHT {
        airline_id: row.airline_id,
        airline_code: row.airline_code,
        codeshare: row.codeshare,
        stops: row.stops,
        equipment: row.equipment
    }]->(dst)
    """
    for i in tqdm(range(0, len(routes), batch_size), desc="Loading routes"):
        batch = routes[i:i + batch_size]
        session.run(query, rows=batch)


def main():
    parser = argparse.ArgumentParser(
        description="Load OpenFlights data into Neo4j."
    )
    parser.add_argument(
        '--uri', default=os.environ.get('NEO4J_URI',NEO4J_URI),
        help='Bolt URI of the Neo4j database (e.g. bolt://localhost:7687)'
    )
    parser.add_argument(
        '--user', default=os.environ.get('NEO4J_USER',NEO4J_USER),
        help='Neo4j username'
    )
    parser.add_argument(
        '--password', default=os.environ.get('NEO4J_PASSWORD',NEO4J_PASSWORD),
        help='Neo4j password'
    )
    parser.add_argument(
        '--airports', default='airports.dat',
        help='Path to airports.dat file'
    )
    parser.add_argument(
        '--airlines', default='airlines.dat',
        help='Path to airlines.dat file'
    )
    parser.add_argument(
        '--routes', default='routes.dat',
        help='Path to routes.dat file'
    )
    parser.add_argument(
        '--batch-size', type=int, default=1000,
        help='Number of rows per batch when inserting into Neo4j'
    )
    args = parser.parse_args()

    if not args.uri or not args.user or not args.password:
        print(
            "Error: connection details are missing. Set NEO4J_URI, NEO4J_USER "
            "and NEO4J_PASSWORD environment variables or pass --uri, --user and "
            "--password on the command line.",
            file=sys.stderr
        )
        sys.exit(1)

    # Resolve data file paths relative to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    airports_path = args.airports if os.path.isabs(args.airports) else os.path.join(script_dir, args.airports)
    airlines_path = args.airlines if os.path.isabs(args.airlines) else os.path.join(script_dir, args.airlines)
    routes_path = args.routes if os.path.isabs(args.routes) else os.path.join(script_dir, args.routes)

    if not os.path.exists(airports_path):
        print(f"Error: airports file not found at {airports_path}", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(airlines_path):
        print(f"Error: airlines file not found at {airlines_path}", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(routes_path):
        print(f"Error: routes file not found at {routes_path}", file=sys.stderr)
        sys.exit(1)

    print("Parsing data files...")
    airports = parse_airports(airports_path)
    airlines = parse_airlines(airlines_path)
    routes = parse_routes(routes_path)

    # Connect to Neo4j
    print(f"Connecting to Neo4j at {args.uri} ...")
    driver = GraphDatabase.driver(args.uri, auth=basic_auth(args.user, args.password))

    try:
        with driver.session() as session:
            # Create constraints
            print("Creating constraints...")
            create_constraints(session)

            # Load data
            load_airports(session, airports, batch_size=args.batch_size)
            load_airlines(session, airlines, batch_size=args.batch_size)
            load_routes(session, routes, batch_size=args.batch_size)

            print("Data import complete.")
    finally:
        driver.close()


if __name__ == '__main__':
    main()