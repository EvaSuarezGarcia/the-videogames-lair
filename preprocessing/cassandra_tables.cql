-- Create admin role
CREATE ROLE eva
WITH PASSWORD = 'pass' AND LOGIN = true AND SUPERUSER = true;

-- Drop default role cassandra (after login with admin user)
DROP ROLE cassandra;

-- Create keyspace
CREATE KEYSPACE videogames_lair
WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'salita' : 3 };

USE videogames_lair;

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

CREATE TABLE steam_games (
    steam_id int,
    game_name text,
    p20 float,
    p40 float,
    p60 float,
    p80 float,
    players int,
    owners int,
    PRIMARY KEY (steam_id)
);

CREATE TABLE steam_playtimes (
    user_id int,
    steam_id int,
    playtime int,
    PRIMARY KEY (user_id, steam_id)
);