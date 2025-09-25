import os
from neo4j import GraphDatabase


class MediumQueries:
    """Queries that require a bit more traversal and aggregation."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self.driver.close()

    def two_hop_destinations(self, from_iata: str):
        """Return airports reachable in exactly two flights from the given airport.

        Args:
            from_iata: The IATA code of the origin airport.
        Returns:
            A list of dictionaries with IATA code, city and country for two‑hop destinations.
        """
        query = (
            "MATCH (src:Airport {iata:$code})-[:FLIGHT]->()-[:FLIGHT]->(two:Airport) "
            "WHERE src <> two "
            "RETURN DISTINCT two.iata AS iata, two.city AS city, two.country AS country "
            "ORDER BY two.country, two.city"
        )
        with self.driver.session() as session:
            result = session.run(query, code=from_iata)
            return [record.data() for record in result]

    def airlines_between_countries(self, origin_country: str, dest_country: str):
        """Return airlines that operate flights between two countries.

        Args:
            origin_country: The country of the origin airport (e.g. 'India').
            dest_country: The country of the destination airport (e.g. 'Germany').
        Returns:
            A list of dictionaries with airline name, IATA code and home country.
        """
        query = (
            "MATCH (src:Airport {country:$orig})-[f:FLIGHT]->(dst:Airport {country:$dest}) "
            "MATCH (al:Airline {airline_id: f.airline_id}) "
            "RETURN DISTINCT al.name AS name, al.iata AS iata, al.country AS baseCountry "
            "ORDER BY name"
        )
        with self.driver.session() as session:
            result = session.run(query, orig=origin_country, dest=dest_country)
            return [record.data() for record in result]

    def hubs_above_average(self, top_n: int = 10):
        """Return airports whose number of destinations exceeds the average hub size.

        Args:
            top_n: How many hub airports to return.
        Returns:
            A list of dictionaries with IATA code, city, country and number of destinations.
        """
        query = (
            "MATCH (a:Airport)-[:FLIGHT]->(dst:Airport) "
            "WITH a, count(DISTINCT dst) AS dests "
            "WITH avg(dests) AS avgDests "
            "MATCH (a:Airport)-[:FLIGHT]->(d:Airport) "
            "WITH a, count(DISTINCT d) AS hubDests, avgDests "
            "WHERE hubDests > avgDests "
            "RETURN a.iata AS iata, a.city AS city, a.country AS country, hubDests "
            "ORDER BY hubDests DESC "
            "LIMIT $n"
        )
        with self.driver.session() as session:
            result = session.run(query, n=top_n)
            return [record.data() for record in result]

    def routes_with_multiple_carriers(self):
        """Find origin–destination pairs served by more than one airline.

        Returns:
            A list of dictionaries with source IATA, destination IATA and list of carriers.
        """
        query = (
            "MATCH (src:Airport)-[f:FLIGHT]->(dst:Airport) "
            "WITH src, dst, collect(DISTINCT f.airline_code) AS carriers "
            "WHERE size(carriers) > 1 "
            "RETURN src.iata AS fromIATA, dst.iata AS toIATA, carriers "
            "ORDER BY size(carriers) DESC, fromIATA, toIATA"
        )
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

    def top_airline_routes(self, airline_code: str, limit: int = 5):
        """Return the busiest routes for a specific airline.

        Args:
            airline_code: The IATA or ICAO code of the airline (e.g. 'AI').
            limit: Number of routes to return.
        Returns:
            A list of dictionaries with origin, destination and flight count.
        """
        query = (
            "MATCH (src:Airport)-[f:FLIGHT]->(dst:Airport) "
            "WHERE f.airline_code = $code "
            "WITH src, dst, count(*) AS flights "
            "RETURN src.iata AS fromIATA, dst.iata AS toIATA, flights "
            "ORDER BY flights DESC "
            "LIMIT $limit"
        )
        with self.driver.session() as session:
            result = session.run(query, code=airline_code, limit=limit)
            return [record.data() for record in result]


def _demo() -> None:
    uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    user = os.environ.get('NEO4J_USER', 'neo4j')
    password = os.environ.get('NEO4J_PASSWORD', 'password')
    m = MediumQueries(uri, user, password)
    try:
        print("Two hop destinations from DEL:")
        print(m.two_hop_destinations('DEL')[:10])
        print("Airlines between India and Germany:")
        print(m.airlines_between_countries('India', 'Germany'))
        print("Top hubs:")
        print(m.hubs_above_average())
    finally:
        m.close()


if __name__ == '__main__':
    _demo()