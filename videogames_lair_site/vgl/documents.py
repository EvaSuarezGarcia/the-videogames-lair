from elasticsearch_dsl import Document, analyzer, Date, Keyword, Text, analysis

ALIAS = ""

english_stop = analysis.token_filter("english_stop", type="stop", stopwords="_english_")
english_stemmer = analysis.token_filter("english_stemmer", type="stemmer", language="english")
english_possessive_stemmer = analysis.token_filter("english_possessive_stemmer", type="stemmer",
                                                   language="possessive_english")
my_english = analyzer('my_english', tokenizer="standard",
                      filter=[
                          english_possessive_stemmer,
                          "lowercase",
                          english_stop,
                          "asciifolding",
                          english_stemmer
                      ],
                      char_filter=["html_strip"])


class Game(Document):
    name = Text(analyzer=my_english)
    aliases = Text(analyzer=my_english)
    deck = Text(analyzer=my_english)
    description = Text(analyzer=my_english)
    release_date = Date(format="date")
    gb_id = Keyword()
    steam_ids = Keyword()
    vgl_id = Keyword()
    image = Keyword()
    site_detail_url = Keyword()
    age_ratings = Keyword()
    age_ratings_strings = Keyword()
    concepts = Keyword(fields={"text": Text(analyzer=my_english)})
    concepts_strings = Keyword()
    developers = Keyword()
    developers_strings = Keyword()
    publishers = Keyword()
    publishers_strings = Keyword()
    franchises = Keyword()
    franchises_strings = Keyword()
    genres = Keyword(fields={"text": Text(analyzer=my_english)})
    genres_strings = Keyword()
    platforms = Keyword()
    platforms_strings = Keyword()
    themes = Keyword(fields={"text": Text(analyzer=my_english)})
    themes_strings = Keyword()

    class Index:
        name = 'games'
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
