from data import popular
from neo4j import GraphDatabase

NEO4J_URI="provide uri here"
NEO4J_USERNAME="provide username here"
NEO4J_PASSWORD="provide password here"

def add_movies_to_neo4j(popular, driver):
    def create_movie(tx, movie):
        query = """
        MERGE (m:Movie {tmdbId: $tmdbId})
        ON CREATE SET m.title = $title, m.plot = $plot, m.year = $year, m.imdbRating = $imdbRating, m.poster = $poster
        """
        tx.run(query, tmdbId=movie["tmdbId"], title=movie["title"], plot=movie["plot"], year=movie["year"],
               imdbRating=movie["imdbRating"], poster=movie["poster"])

    def create_actor(tx, actor):
        query = """
        MERGE (a:Actor {tmdbId: $tmdbId})
        ON CREATE SET a.name = $name
        """
        tx.run(query, tmdbId=actor["tmdbId"], name=actor["name"])

    def create_director(tx, director):
        query = """
        MERGE (d:Director {tmdbId: $tmdbId})
        ON CREATE SET d.name = $name
        """
        tx.run(query, tmdbId=director["tmdbId"], name=director["name"])

    def create_genre(tx, genre):
        query = """
        MERGE (g:Genre {name: $name, link: $link})
        """
        tx.run(query, name=genre["name"], link=genre["link"])

    def create_relationships(tx, movie):
        for actor in movie["actors"]:
            tx.run("""
            MATCH (m:Movie {tmdbId: $tmdbId})
            WITH m
            MATCH (a:Actor {tmdbId: $actorTmdbId})
            MERGE (a)-[:ACTED_IN]->(m)
            """, tmdbId=movie["tmdbId"], actorTmdbId=actor["tmdbId"])
        
        for director in movie["directors"]:
            tx.run("""
            MATCH (m:Movie {tmdbId: $tmdbId})
            WITH m
            MATCH (d:Director {tmdbId: $directorTmdbId})
            MERGE (d)-[:DIRECTED]->(m)
            """, tmdbId=movie["tmdbId"], directorTmdbId=director["tmdbId"])

        for genre in movie["genres"]:
            tx.run("""
            MATCH (m:Movie {tmdbId: $tmdbId})
            WITH m
            MATCH (g:Genre {name: $genreName})
            MERGE (m)-[:HAS_GENRE]->(g)
            """, tmdbId=movie["tmdbId"], genreName=genre["name"])

    def create_language_relationships(tx, movie):
        for language in movie["languages"]:
            tx.run("""
            MERGE (l:Language {name: $language})
            WITH l
            MATCH (m:Movie {tmdbId: $tmdbId})
            MERGE (m)-[:IN_LANGUAGE]->(l)
            """, tmdbId=movie["tmdbId"], language=language.strip())

    with driver.session() as session:
        for movie in popular:
            session.execute_write(create_movie, movie)
            for actor in movie["actors"]:
                session.execute_write(create_actor, actor)
            for director in movie["directors"]:
                session.execute_write(create_director, director)
            for genre in movie["genres"]:
                session.execute_write(create_genre, genre)
            session.execute_write(create_relationships, movie)
            session.execute_write(create_language_relationships, movie)

# driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
# add_movies_to_neo4j(popular, driver)
