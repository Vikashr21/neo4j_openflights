import os
from neo4j import GraphDatabase


class EasyQueries:
    """Convenience methods for simple analyses of the OpenFlights graph."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        """Close the underlying Neo4j driver."""
        self.driver.close()

    def count_entities(self) -> tuple:
        """Return counts of airports, airlines and flight relationships.

        Returns:
            A tuple `(num_airports, num_airlines, num_flights)`.
        """
        with self.driver.session() as session:
            airports = session.run("MATCH (a:Airport) RETURN count(a)").single()[0]
            airlines = session.run("MATCH (al:Airline) RETURN count(al)").single()[0]
            flights = session.run("MATCH ()-[f:FLIGHT]->() RETURN count(f)").single()[0]
            return airports, airlines, flights

    def airports_in_country(self, country: str):
        """Return a list of airports in the specified country.

        Args:
            country: Name of the country, e.g. 'India'.
        Returns:
            A list of dictionaries with airport details.
        """
        query = (
            "MATCH (a:Airport {country:$country}) "
            "RETURN a.airport_id AS id, a.name AS name, a.city AS city, a.iata AS iata, a.icao AS icao "
            "ORDER BY a.city"
        )
        with self.driver.session() as session:
            result = session.run(query, country=country)
            return [record.data() for record in result]

    def top_airlines_by_destinations(self, limit: int = 5):
        """Return the airlines serving the largest number of distinct destinations.

        Args:
            limit: How many airlines to return.
        Returns:
            A list of dictionaries with airline name and number of destinations.
        """
        query = (
            "MATCH (:Airport)-[f:FLIGHT]->(dst:Airport) "
            "MATCH (al:Airline {airline_id: f.airline_id}) "
            "WITH al, count(DISTINCT dst) AS dests "
            "RETURN al.name AS name, dests "
            "ORDER BY dests DESC "
            "LIMIT $limit"
        )
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return [record.data() for record in result]

    def direct_flights_from(self, iata_code: str):
        """Return all direct flights departing from the given IATA code.

        Args:
            iata_code: Three‑letter IATA airport code (e.g. 'DEL').
        Returns:
            A list of dictionaries with destination code, airline code, number of stops and equipment.
        """
        query = (
            "MATCH (src:Airport {iata:$code})-[f:FLIGHT]->(dst) "
            "RETURN dst.iata AS destination, f.airline_code AS airlineCode, f.stops AS stops, f.equipment AS equipment "
            "ORDER BY destination"
        )
        with self.driver.session() as session:
            result = session.run(query, code=iata_code)
            return [record.data() for record in result]

    def airports_without_outbound(self):
        """Return airports that have no outbound flights."""
        query = (
            "MATCH (a:Airport) "
            "WHERE NOT (a)-[:FLIGHT]->(:Airport) "
            "RETURN a.iata AS iata, a.city AS city, a.country AS country"
        )
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]


def _demo() -> None:
    """Demonstrate basic usage when run as a script."""
    uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    user = os.environ.get('NEO4J_USER', 'neo4j')
    password = os.environ.get('NEO4J_PASSWORD', 'password')
    eq = EasyQueries(uri, user, password)
    try:
        print("Counts (airports, airlines, flights):", eq.count_entities())
        print("Top 5 airlines by destinations:")
        for row in eq.top_airlines_by_destinations():
            print(f"  {row['name']} -> {row['dests']} destinations")
        print("Example airports in India:")
        for row in eq.airports_in_country('India')[:5]:
            print(f"  {row['iata']} - {row['city']}")
    finally:
        eq.close()


if __name__ == '__main__':
    _demo()