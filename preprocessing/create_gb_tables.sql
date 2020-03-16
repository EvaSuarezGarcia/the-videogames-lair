USE tfm_eva_preproc;

-- Drop tables
DROP TABLE IF EXISTS gb_staff_reviews;
DROP TABLE IF EXISTS gb_staff_games;
DROP TABLE IF EXISTS gb_staff_releases;
DROP TABLE IF EXISTS gb_user_reviews;

-- GB staff reviews (from JSON files)

CREATE TABLE gb_staff_games (
    game_id VARCHAR(50) NOT NULL,
    game_name VARCHAR(200),
    game_api_detail_url VARCHAR(1000),
    game_site_detail_url VARCHAR(1000),
    
    CONSTRAINT gb_staff_games_pk PRIMARY KEY (game_id)
);

CREATE TABLE gb_staff_releases (
    release_id MEDIUMINT UNSIGNED NOT NULL,
    release_name VARCHAR(200) NOT NULL,
    release_api_detail_url VARCHAR(1000) NOT NULL,

    CONSTRAINT gb_staff_releases_pk PRIMARY KEY (release_id)
);

CREATE TABLE gb_staff_reviews (
    review_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    reviewer VARCHAR(50) NOT NULL,
    game_id VARCHAR(50),
    dlc_name VARCHAR(200),
    score TINYINT UNSIGNED NOT NULL,
    release_id MEDIUMINT UNSIGNED,

    CONSTRAINT gb_staff_reviews_pk PRIMARY KEY (review_id),
    CONSTRAINT gb_staff_reviews_game_fk FOREIGN KEY (game_id) 
        REFERENCES gb_staff_games (game_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    CONSTRAINT gb_staff_reviews_release_fk FOREIGN KEY (release_id)
        REFERENCES gb_staff_releases (release_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

-- GB user reviews (from CSV)

CREATE TABLE gb_user_reviews (
    review_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    reviewer VARCHAR(50) NOT NULL,
    game_name VARCHAR(200) NOT NULL,
    score TINYINT UNSIGNED NOT NULL,
    game_id VARCHAR(50) NOT NULL,
    review_date DATETIME NOT NULL,

    CONSTRAINT gb_user_reviews_pk PRIMARY KEY (review_id) 
);