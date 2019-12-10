-- Create admin role
CREATE ROLE eva
WITH PASSWORD = 'pass' AND LOGIN = true AND SUPERUSER = true;

-- Drop default role cassandra (after login with admin user)
DROP ROLE cassandra;

-- Create keyspace
CREATE KEYSPACE videogames_lair
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

-- Create user ratings table
CREATE TABLE ratings (
    user_id int,
    game_id int,
    rating tinyint,
    estimated boolean,
    PRIMARY KEY (user_id, game_id)
);

-- Create metadata "ratings" tables
CREATE TABLE game_age_ratings (
    user_id int,
    game_id int,
    rating tinyint,
    PRIMARY KEY (user_id, game_id)
);

CREATE TABLE game_companies (
    user_id int,
    game_id int,
    rating tinyint,
    PRIMARY KEY (user_id, game_id)
);

CREATE TABLE game_concepts (
    user_id int,
    game_id int,
    rating tinyint,
    PRIMARY KEY (user_id, game_id)
);

CREATE TABLE game_franchises (
    user_id int,
    game_id int,
    rating tinyint,
    PRIMARY KEY (user_id, game_id)
);

CREATE TABLE game_genres (
    user_id int,
    game_id int,
    rating tinyint,
    PRIMARY KEY (user_id, game_id)
);

CREATE TABLE game_platforms (
    user_id int,
    game_id int,
    rating tinyint,
    PRIMARY KEY (user_id, game_id)
);

CREATE TABLE game_themes (
    user_id int,
    game_id int,
    rating tinyint,
    PRIMARY KEY (user_id, game_id)
);