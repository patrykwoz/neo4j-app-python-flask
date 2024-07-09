from api.data import people, pacino
from api.exceptions.notfound import NotFoundException


class PeopleDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """

    def __init__(self, driver):
        self.driver = driver

    """
    This method should return a paginated list of People (actors or directors),
    with an optional filter on the person's name based on the `q` parameter.

    Results should be ordered by the `sort` parameter and limited to the
    number passed as `limit`.  The `skip` variable should be used to skip a
    certain number of rows.
    """
    def all(self, q, sort = 'name', order = 'ASC', limit = 6, skip = 0):
        # Get a list of people from the database
        def get_all_people(tx, q, sort, order, limit, skip):
            cypher = "MATCH (p:Person) "

            # If q is set, use it to filter on the name property
            if q is not None:
                cypher += "WHERE p.name CONTAINS $q"

            cypher += """
            RETURN p {{ .* }} AS person
            ORDER BY p.`{0}` {1}
            SKIP $skip
            LIMIT $limit
            """.format(sort, order)

            result = tx.run(cypher, q=q, sort=sort, order=order, limit=limit, skip=skip)

            return [ row.get("person") for row in result ]

        with self.driver.session() as session:
            return session.execute_read(get_all_people, q, sort, order, limit, skip)

    """
    Find a user by their ID.

    If no user is found, a NotFoundError should be thrown.
    """
    def find_by_id(self, id):
        # Get person by their ID
        def get_person(tx, id):
            row = tx.run("""
                MATCH (p:Person {tmdbId: $id})
                WITH p,
                     COUNT { (p)-[:ACTED_IN]->() } AS actedCount,
                     COUNT { (p)-[:DIRECTED]->() } AS directedCount
                RETURN p {
                    .*,
                    actedCount: actedCount,
                    directedCount: directedCount
                } AS person
            """, id=id).single()

            if row is None:
                raise NotFoundException()
            return row.get("person")

        with self.driver.session() as session:
            return session.execute_read(get_person, id)

    """
    Get a list of similar people to a Person, ordered by their similarity score
    in descending order.
    """
    def get_similar_people(self, id, limit=6, skip=0):
        # Get a list of similar people to the person by their id
        def get_similar_people(tx, id, skip, limit):
            result = tx.run("""
                MATCH (:Person {tmdbId: $id})-[:ACTED_IN|DIRECTED]->(m)<-[r:ACTED_IN|DIRECTED]-(p)
                WITH p, m, r,
                     COUNT { (p)-[:ACTED_IN]->() } AS actedCount,
                     COUNT { (p)-[:DIRECTED]->() } AS directedCount,
                     collect(m {.tmdbId, .title, type: type(r)}) AS inCommon
                RETURN p {
                    .*,
                    actedCount: actedCount,
                    directedCount: directedCount,
                    inCommon: inCommon
                } AS person
                ORDER BY size(inCommon) DESC
                SKIP $skip
                LIMIT $limit
            """, id=id, skip=skip, limit=limit)

            return [row.get("person") for row in result]

        with self.driver.session() as session:
            return session.execute_read(get_similar_people, id, skip, limit)