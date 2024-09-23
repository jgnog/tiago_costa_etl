import pandas as pd
import re
import unidecode
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

class Data_Etl:
    def __init__(self, path_data, postgres_db):
        self.path_data = path_data
        self.engine = create_engine(postgres_db)
        

    def load_data(self, table_name):
        """
        Loads data from a CSV file and ensures proper data types.
        Dynamically parses any column with 'date' in its name as datetime,
        attempts to cast numeric-looking columns to floats, and all other columns to strings,
        while ignoring null values and ensuring non-numeric columns (e.g., names, mixed alphanumerics) stay as strings.
        """
        
        file_path = f'{self.path_data}/{table_name}'
        df = pd.read_csv(file_path)

        # Identify  columns with 'date' in their name and convert them to datetime
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')

        # Verify is exist any alpha in  the column, if yes, convert to string
        def contains_alpha(val):
            if isinstance(val, str):
                return bool(re.search(r'[a-zA-Z]', val))  # Verifica se há letras
            return False

        for col in df.columns:
            if col in date_columns:
                continue

            # Apply the  function to each value in the column
            if df[col].dropna().apply(contains_alpha).any():
                df[col] = df[col].astype(str)  # if is function return true, convert to string
            else:
                # if function is false, convert into numeric values
                if df[col].notnull().any():
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except ValueError:
                        pass 

            if df[col].dtype == 'object':
                df[col] = df[col].where(df[col].isnull(), df[col].astype(str))  

        return df
    def replaceNA(self, df):
        """ 
        If the dtype is numeric, fill NaN with 0. 
        If the dtype is string, fill NaN with 'don't exist'. 
        If the dtype is a date (object), fill NaN with '01/01/1990' and convert to datetime.
        """
        for column in df.columns:
            if pd.api.types.is_numeric_dtype(df[column]):
                df[column] = df[column].fillna(0.0)
            elif pd.api.types.is_string_dtype(df[column]):
                df[column] = df[column].fillna("don't exist")
            elif pd.api.types.is_object_dtype(df[column]):  
                try:
                    df[column] = pd.to_datetime(df[column], errors='coerce')  
                    df[column] = df[column].fillna(pd.to_datetime("1990-01-01"))  
                except Exception as e:
                    print(f"Error converting {column}: {e}")
        return df


    def clean_special_characters(self, df):
        """
        Cleans the string columns by removing special characters
        Keeps exceptions for numerical and date types
        """
        def replace_accented_characters(text):
            """
            Replaces accented characters
            Removes special characters and Title case
            """
            if not isinstance(text, str):
                return text
            
            # Remove accented characters 
            text = unidecode.unidecode(text)
            
            # Remove special characters (only stay alpha, numeric an symbols)
            text = re.sub(r'[^a-zA-Z0-9 ]', '', text).title().strip()
            return text

        # call the function replace_accented_characters
        for column in df.select_dtypes(include='object').columns:
            df[column] = df[column].apply(replace_accented_characters)

        return df
    
    def load_to_postgres(self, df, table_name):
        """
        Loads a pandas DataFrame to PostgreSQL.
        """
        df.to_sql(table_name, self.engine, if_exists='replace', index=False, schema='raw')
        print(f"Data loaded into {table_name} table in PostgreSQL")


path_data = r"C:\Users\tiago.costa\football-data-engineering\Data\Score_csv"

load_dotenv(r"C:\Users\tiago.costa\OneDrive - Salsajeans\Desktop\ETL\.env")

username = os.getenv('username')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')
database = os.getenv('database')

database_url = f'postgresql://{username}:{password}@{host}:{port}/{database}'
etl = Data_Etl(path_data, database_url)


try:
    df_appearances = etl.load_data('appearances.csv')  
    df_appearances = etl.clean_special_characters(df_appearances)
    df_appearances = etl.replaceNA(df_appearances)
    etl.load_to_postgres(df_appearances, 'appearances_t')
    print('df_appearances was transformed and load  to postgres')


    df_club_games = etl.load_data('club_games.csv')
    df_club_games = etl.clean_special_characters(df_club_games)
    df_club_games = etl.replaceNA(df_club_games)
    etl.load_to_postgres(df_club_games, 'club_games_t')
    print('df_club_games was transformed and load to postgres')

    df_clubs = etl.load_data('clubs.csv')
    df_clubs = etl.clean_special_characters(df_clubs)
    df_clubs = etl.replaceNA(df_clubs)
    etl.load_to_postgres(df_clubs, 'clubs_t')
    print('df_clubs was transformed and load to postgres')

    df_competitions = etl.load_data('competitions.csv')
    df_competitions = etl.clean_special_characters(df_competitions)
    df_competitions = etl.replaceNA(df_competitions)
    etl.load_to_postgres(df_competitions,  'competitions_t')
    print('df_competitions was transformed and load to postgres')

    df_game_events = etl.load_data('game_events.csv')
    df_game_events = etl.clean_special_characters(df_game_events)
    df_game_events = etl.replaceNA(df_game_events)
    etl.load_to_postgres(df_game_events, 'game_events_t')
    print('df_game_events was transformed and load to postgres')

    df_game_lineups = etl.load_data('game_lineups.csv')
    df_game_lineups = etl.clean_special_characters(df_game_lineups)
    df_game_lineups = etl.replaceNA(df_game_lineups)
    etl.load_to_postgres(df_game_lineups, 'game_lineups_t')
    print('df_game_lineups was transformed and load to postgres')

    df_games = etl.load_data('games.csv')
    df_games = etl.clean_special_characters(df_games)
    df_games = etl.replaceNA(df_games)
    etl.load_to_postgres(df_games, 'games_t')
    print('df_games was transformed and load to postgres')

    df_player_valuations = etl.load_data('player_valuations.csv')
    df_player_valuations = etl.clean_special_characters(df_player_valuations)
    df_player_valuations = etl.replaceNA(df_player_valuations)
    etl.load_to_postgres(df_player_valuations,  'player_valuations_t')
    print('df_player_valuations was transformed and load to postgres')

    df_players = etl.load_data('players.csv')
    df_players = etl.clean_special_characters(df_players)
    df_players = etl.replaceNA(df_players)
    etl.load_to_postgres(df_players, 'players_t')
    print('df_players was transformed and load to postgres')

    df_transfers = etl.load_data('transfers.csv')
    df_transfers = etl.clean_special_characters(df_transfers)
    df_transfers = etl.replaceNA(df_transfers)
    etl.load_to_postgres(df_transfers, 'transfers_t')
    print('df_transfers was transformed and load to postgres')

except Exception as e:
     print(e)



