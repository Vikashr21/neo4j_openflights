import os
from neo4j import GraphDatabase


class HardQueries:
    """Queries that demonstrate advanced graph analytics."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self.driver.close()

    def shortest_path(self, from_iata: str, to_iata: str, max_hops: int = 6):
        """Return the shortest path (fewest flights) between two airports.

        Args:
            from_iata: Origin airport IATA code.
            to_iata: Destination airport IATA code.
            max_hops: Maximum number of hops to consider.
        Returns:
            A dictionary with the path (list of IATA codes), list of
            airline codes for each leg and the number of hops.  Returns
            `None` if no path is found within the hop limit.
        """
        query = (
            "MATCH (start:Airport {iata:$start}), (end:Airport {iata:$end}) "
            f"MATCH p = shortestPath((start)-[:FLIGHT*..{max_hops}]->(end)) "
            "RETURN [n IN nodes(p) | n.iata] AS route, "
            "       [r IN relationships(p) | r.airline_code] AS carriers, "
            "       length(p) AS hops"
        )
        with self.driver.session() as session:
            record = session.run(query, start=from_iata, end=to_iata).single()
            if record:
                return {
                    'route': record['route'],
                    'carriers': record['carriers'],
                    'hops': record['hops']
                }
            return None

    def add_distance_property(self) -> int:
        """Compute great‑circle distances on FLIGHT relationships.

        This method calculates the distance (in kilometres) between the
        source and destination airports of each FLIGHT using Neo4j's
        built‑in `point.distance` function.  The distance is stored on
        the relationship under the `distance` property.  Call this once
        after loading the graph to enable weighted shortest paths.

        Returns:
            The number of relationships updated.
        """
        query = (
            "MATCH (a1:Airport)-[f:FLIGHT]->(a2:Airport) "
            "SET f.distance = point.distance( "
            "  point({latitude:a1.latitude, longitude:a1.longitude}), "
            "  point({latitude:a2.latitude, longitude:a2.longitude}) "
            ") / 1000.0 "
            "RETURN count(f) AS updated"
        )
        with self.driver.session() as session:
            updated = session.run(query).single()[0]
            return updated

    def weighted_shortest_path(self, from_iata: str, to_iata: str):
        """Compute a weighted shortest path using the GDS Dijkstra algorithm.

        Requires the Graph Data Science library to be installed.  The
        FLIGHT relationships must have a `distance` property (see
        `add_distance_property`).

        Args:
            from_iata: Origin IATA code.
            to_iata: Destination IATA code.
        Returns:
            A dictionary with the total cost (kilometres) and the list
            of IATA codes in the path.  Returns `None` if no path is
            found.
        """
        # Build the necessary projections on the fly and run Dijkstra
        query = (
            "MATCH (start:Airport {iata:$start}), (end:Airport {iata:$end}) "
            "CALL gds.shortestPath.dijkstra.stream({ "
            "  nodeProjection: 'Airport', "
            "  relationshipProjection: { "
            "    FLIGHT: { type: 'FLIGHT', properties: 'distance' } "
            "  }, "
            "  sourceNode: id(start), "
            "  targetNode: id(end), "
            "  relationshipWeightProperty: 'distance' "
            "}) "
            "YIELD totalCost, nodeIds "
            "RETURN totalCost, [nodeId IN nodeIds | gds.util.asNode(nodeId).iata] AS route"
        )
        with self.driver.session() as session:
            record = session.run(query, start=from_iata, end=to_iata).single()
            if record:
                return {
                    'distance_km': record['totalCost'],
                    'route': record['route']
                }
            return None

    def find_articulation_points(self):
        """Find airports whose removal would increase the number of components.

        Uses the GDS `articulationPoints` algorithm.  Requires the
        Graph Data Science library.

        Returns:
            A list of dictionaries with IATA code, city and country.
        """
        query = (
            "CALL gds.articulationPoints.stream({ "
            "  nodeProjection: 'Airport', "
            "  relationshipProjection: 'FLIGHT' "
            "}) "
            "YIELD articulationPointNode "
            "RETURN gds.util.asNode(articulationPointNode).iata AS iata, "
            "       gds.util.asNode(articulationPointNode).city AS city, "
            "       gds.util.asNode(articulationPointNode).country AS country"
        )
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

    def detect_communities_louvain(self, sample_limit: int = 20):
        """Detect communities of airports using Louvain modularity.

        This method streams the results of the Louvain algorithm from
        GDS.  Requires the Graph Data Science library to be installed.

        Args:
            sample_limit: Limit the number of rows returned for inspection.
        Returns:
            A list of dictionaries with IATA code, city and community ID.
        """
        query = (
            "CALL gds.louvain.stream({ "
            "  nodeProjection: 'Airport', "
            "  relationshipProjection: { "
            "    FLIGHT: { type: 'FLIGHT', orientation: 'UNDIRECTED' } "
            "  } "
            "}) "
            "YIELD nodeId, communityId "
            "RETURN gds.util.asNode(nodeId).iata AS iata, "
            "       gds.util.asNode(nodeId).city AS city, "
            "       communityId "
            "LIMIT $limit"
        )
        with self.driver.session() as session:
            result = session.run(query, limit=sample_limit)
            return [record.data() for record in result]


def _demo() -> None:
    uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
    user = os.environ.get('NEO4J_USER', 'neo4j')
    password = os.environ.get('NEO4J_PASSWORD', 'password')
    h = HardQueries(uri, user, password)
    try:
        print("Shortest path (by hops) from DEL to JFK:")
        print(h.shortest_path('DEL', 'JFK'))
        # The following calls require GDS.  Uncomment if GDS is available:
        # print("Updating distance property...")
        # print(h.add_distance_property())
        # print("Weighted shortest path (distance) from DEL to JFK:")
        # print(h.weighted_shortest_path('DEL', 'JFK'))
        # print("Articulation points:")
        # print(h.find_articulation_points()[:10])
        # print("Louvain communities sample:")
        # print(h.detect_communities_louvain())
    finally:
        h.close()


if __name__ == '__main__':
    _demo()