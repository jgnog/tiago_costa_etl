--Dim players

CREATE TABLE star_schema.dim_player (
    player_id INT PRIMARY KEY,
    name VARCHAR(255),
    country_of_citizenship VARCHAR(100),
    position VARCHAR(100),
    date_of_birth DATE,
    height_in_cm NUMERIC,
    agent_name VARCHAR(255)
);

INSERT INTO star_schema.dim_player (player_id, name, country_of_citizenship, position, date_of_birth, height_in_cm, agent_name)
SELECT
    player_id,
    name,
    country_of_citizenship,
    position,
    CAST(date_of_birth AS DATE),
    height_in_cm,
    agent_name
FROM
    raw.players_t;


--Dim Clubs

CREATE TABLE star_schema.dim_clubs (
    club_id INT PRIMARY KEY,
    name VARCHAR(255),
    domestic_competition_id Varchar(200),
    total_market_value Numeric,
    stadium_name VARCHAR(255),
    stadium_seats Numeric,
    average_age NUMERIC,
    foreigners_number NUMERIC,
    national_team_players NUMERIC
);

INSERT INTO star_schema.dim_clubs (club_id, name, domestic_competition_id, total_market_value, stadium_name, stadium_seats, average_age, foreigners_number, national_team_players)
SELECT
   	club_id,
    name,
    domestic_competition_id,
    total_market_value,
    stadium_name,
    stadium_seats,
    average_age,
    foreigners_number,
    national_team_players
FROM
    raw.clubs_t;

--Dim Date
CREATE TABLE star_schema.dim_date (
    date DATE Primary Key ,
    day INT,
    month INT,
    year INT,
    day_of_week VARCHAR(15),
    is_weekend BOOLEAN
);


INSERT INTO star_schema.dim_date (date, day, month, year, day_of_week, is_weekend)
SELECT distinct
    d::DATE AS date,
    EXTRACT(DAY FROM d)::INT AS day,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(YEAR FROM d)::INT AS year,
    TO_CHAR(d, 'Day') AS day_of_week,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM
    GENERATE_SERIES('2000-01-01'::DATE, '2030-12-31'::DATE, '1 DAY'::INTERVAL) d;

--Dim Competitions
CREATE TABLE star_schema.dim_competitons(
    competition_id VARCHAR(50) primary key,
    domestic_league_code VARCHAR(50),
    name VARCHAR(50)
);

INSERT INTO star_schema.dim_competitons (competition_id, domestic_league_code, name)
SELECT DISTINCT
    competition_id,
    domestic_league_code,
    name
FROM 
    raw.competitions_t



--Fact_market
CREATE TABLE star_schema.fact_market (
    market_id SERIAL PRIMARY KEY,
    player_id INT,
    date DATE,
    market_value_in_eur BIGINT,
    highest_market_value_in_eur BIGINT,
    FOREIGN KEY (player_id) REFERENCES star_schema.dim_player(player_id),
    FOREIGN KEY (date) REFERENCES star_schema.dim_date(date)
);

INSERT INTO star_schema.fact_market (player_id, date, market_value_in_eur, highest_market_value_in_eur)
SELECT 
    pv.player_id, 
    d.date, 
    pv.market_value_in_eur, 
    p.highest_market_value_in_eur
FROM 
    raw.player_valuations_t pv
JOIN 
    Star_schema.dim_date d ON CAST(pv.date AS DATE) = d.date
JOIN 
    raw.players_t p ON pv.player_id = p.player_id


--fact_appearances
CREATE TABLE star_schema.fact_appearances (
    appearance_id SERIAL PRIMARY KEY,
    player_id INT,
    club_id INT,
    date DATE,
    competition_id VARCHAR(100),
    yellow_cards INT,
    red_cards INT,
    goals INT,
    assists INT,
    minutes_played INT,
    FOREIGN KEY (player_id) REFERENCES star_schema.dim_player(player_id),
    FOREIGN KEY (club_id) REFERENCES star_schema.dim_clubs(club_id),
    FOREIGN KEY (date) REFERENCES star_schema.dim_date(date),  
    FOREIGN KEY (competition_id) REFERENCES star_schema.dim_competitons(competition_id)
);



INSERT INTO star_schema.fact_appearances (player_id, club_id, date, competition_id, yellow_cards, red_cards, goals, assists, minutes_played)
SELECT 
    a.player_id, 
    c.club_id, 
    d.date, 
    co.competition_id,
    a.yellow_cards, 
    a.red_cards, 
    a.goals, 
    a.assists, 
    a.minutes_played
FROM 
    raw.appearances_t a
JOIN 
    star_schema.dim_clubs c ON a.player_current_club_id = c.club_id
JOIN 
    star_schema.dim_date d ON CAST(a.date AS DATE) = d.date 
left JOIN 
    star_schema.dim_competitons co ON co.competition_id = a.competition_id;




