---- Final preproc tables for GB metadata ----
-- Drop tables
DROP TABLE game_steam_ids;
DROP TABLE game_aliases;
DROP TABLE game_age_ratings;
DROP TABLE game_concepts;
DROP TABLE game_developers;
DROP TABLE game_franchises;
DROP TABLE game_genres;
DROP TABLE game_publishers;
DROP TABLE game_platforms;
DROP TABLE game_themes;
DROP TABLE games;
DROP TABLE age_ratings;
DROP TABLE concepts;
DROP TABLE companies;
DROP TABLE franchises;
DROP TABLE genres;
DROP TABLE platforms;
DROP TABLE themes;

CREATE TABLE games (
    id INT UNSIGNED NOT NULL,
    main_name VARCHAR(200) NOT NULL,
    api_detail_url VARCHAR(1000),
    site_detail_url VARCHAR(1000),
    release_date_year SMALLINT UNSIGNED,
    release_date_month TINYINT UNSIGNED,
    release_date_day TINYINT UNSIGNED,
    estimated_date BOOLEAN NOT NULL,

    CONSTRAINT games_pk PRIMARY KEY (id)
);

CREATE TABLE game_steam_ids (
    gb_id INT UNSIGNED NOT NULL,
    steam_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_steam_ids PRIMARY KEY (gb_id, steam_id),
    CONSTRAINT game_steam_ids_game_fk FOREIGN KEY (gb_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE game_aliases (
    game_id INT UNSIGNED NOT NULL,
    -- We need this column to be sensitive to accents
    alias VARCHAR(200) NOT NULL COLLATE 'utf8mb4_bin',

    CONSTRAINT game_aliases_pk PRIMARY KEY (game_id, alias),
    CONSTRAINT game_aliases_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Concepts and games-to-concepts relationship
CREATE TABLE concepts (
    id INT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    api_detail_url VARCHAR(1000),
    site_detail_url VARCHAR(1000),

    CONSTRAINT concepts_pk PRIMARY KEY (id)
);

CREATE TABLE game_concepts (
    game_id INT UNSIGNED NOT NULL,
    concept_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_concepts_pk PRIMARY KEY (game_id, concept_id),
    CONSTRAINT game_concepts_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_concepts_concept_fk FOREIGN KEY (concept_id)
        REFERENCES concepts (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Companies, games-to-developers and games-to-publishers relationships
CREATE TABLE companies (
    id INT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    api_detail_url VARCHAR(1000),
    site_detail_url VARCHAR(1000),

    CONSTRAINT companies_pk PRIMARY KEY (id)
);

CREATE TABLE game_developers (
    game_id INT UNSIGNED NOT NULL,
    developer_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_developers_pk PRIMARY KEY (game_id, developer_id),
    CONSTRAINT game_developers_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_developers_developer_fk FOREIGN KEY (developer_id)
        REFERENCES companies (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE game_publishers (
    game_id INT UNSIGNED NOT NULL,
    publisher_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_publishers_pk PRIMARY KEY (game_id, publisher_id),
    CONSTRAINT game_publishers_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_publishers_publisher_fk FOREIGN KEY (publisher_id)
        REFERENCES companies (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Franchises and games-to-franchises relationship
CREATE TABLE franchises (
    id INT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    api_detail_url VARCHAR(1000),
    site_detail_url VARCHAR(1000),

    CONSTRAINT franchises_pk PRIMARY KEY (id)
);

CREATE TABLE game_franchises (
    game_id INT UNSIGNED NOT NULL,
    franchise_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_franchises_pk PRIMARY KEY (game_id, franchise_id),
    CONSTRAINT game_franchises_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_franchises_franchise_fk FOREIGN KEY (franchise_id)
        REFERENCES franchises (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Genres and games-to-genres relationship
CREATE TABLE genres (
    id INT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    api_detail_url VARCHAR(1000),
    site_detail_url VARCHAR(1000),

    CONSTRAINT genres_pk PRIMARY KEY (id)
);

CREATE TABLE game_genres (
    game_id INT UNSIGNED NOT NULL,
    genre_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_genres_pk PRIMARY KEY (game_id, genre_id),
    CONSTRAINT game_genres_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_genres_genre_fk FOREIGN KEY (genre_id)
        REFERENCES genres (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Age ratings and game-to-age-ratings relationship
CREATE TABLE age_ratings (
    id INT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    api_detail_url VARCHAR(1000),
    site_detail_url VARCHAR(1000),

    CONSTRAINT age_ratings_pk PRIMARY KEY (id)
);

CREATE TABLE game_age_ratings (
    game_id INT UNSIGNED NOT NULL,
    age_rating_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_age_ratings_pk PRIMARY KEY (game_id, age_rating_id),
    CONSTRAINT game_age_ratings_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_age_ratings_age_rating_fk FOREIGN KEY (age_rating_id)
        REFERENCES age_ratings (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Themes and games-to-themes relationship
CREATE TABLE themes (
    id INT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    api_detail_url VARCHAR(1000),
    site_detail_url VARCHAR(1000),

    CONSTRAINT themes_pk PRIMARY KEY (id)
);

CREATE TABLE game_themes (
    game_id INT UNSIGNED NOT NULL,
    theme_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_themes_pk PRIMARY KEY (game_id, theme_id),
    CONSTRAINT game_themes_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_themes_theme_fk FOREIGN KEY (theme_id)
        REFERENCES themes (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Platforms and games-to-platforms relationship
CREATE TABLE platforms (
    id INT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    abbreviation VARCHAR(20) NOT NULL,
    api_detail_url VARCHAR(1000),
    site_detail_url VARCHAR(1000),

    CONSTRAINT platforms_pk PRIMARY KEY (id)
);

CREATE TABLE game_platforms (
    game_id INT UNSIGNED NOT NULL,
    platform_id INT UNSIGNED NOT NULL,

    CONSTRAINT game_platforms_pk PRIMARY KEY (game_id, platform_id),
    CONSTRAINT game_platforms_game_fk FOREIGN KEY (game_id)
        REFERENCES games (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT game_platforms_platform_fk FOREIGN KEY (platform_id)
        REFERENCES platforms (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);