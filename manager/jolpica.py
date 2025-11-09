import os

import pandas as pd
import requests


class JolpicaAPI:
    BASE_URL = "https://api.jolpi.ca/ergast/f1"
    DEFAULT_LIMIT = 100

    def __requests_get(self, endpoint, season: int = None, round: int = None, limit: int = DEFAULT_LIMIT, offset: int = None) -> dict:
        url = f"{self.BASE_URL}"
        if season:
            url += f"/{season}"
            if round:
                url += f"/{round}"

        url += f"/{endpoint}"
        params = {"limit": limit, "offset": offset}
        response = requests.get(url, params=params)
        return response.json()

    def constructors_standings(self, season: int, round: int = None, limit: int = DEFAULT_LIMIT, offset: int = None) -> dict:
        return self.__requests_get("/constructorstandings.json", season, round, limit, offset)

    def drivers_standings(self, season: int, round: int = None, limit: int = DEFAULT_LIMIT, offset: int = None) -> dict:
        return self.__requests_get("/driverstandings.json", season, round, limit, offset)

    def races(self, season: int = None, round: int = None, limit: int = DEFAULT_LIMIT, offset: int = None) -> dict:
        return self.__requests_get("/races.json", season, round, limit, offset)

    def drivers(self, season: int = None, round: int = None, limit: int = DEFAULT_LIMIT, offset: int = None) -> dict:
        return self.__requests_get("/drivers.json", season, round, limit, offset)

    def constructors(self, season: int = None, round: int = None, limit: int = DEFAULT_LIMIT, offset: int = None) -> dict:
        return self.__requests_get("/constructors.json", season, round, limit, offset)

    def circuits(self, season: int = None, round: int = None, limit: int = DEFAULT_LIMIT, offset: int = None) -> dict:
        return self.__requests_get("/circuits.json", season, round, limit, offset)


class JolpicaParser:
    def parser(self, json_data: dict) -> tuple[pd.DataFrame, int, int, int]:
        data = json_data["MRData"]

        limit = data["limit"]
        offset = data["offset"]
        total = data["total"]

        for table_key in ["Table", "Lists"]:
            table = [x for x in data.keys() if x.endswith(table_key)]
            if table:
                table = table[0]
                data = data[table]
            if isinstance(data, list) and len(data) > 0:
                data = data[0]

        keys = list(data.keys())

        main_key = [x for x in keys if x[0].isupper()][0]
        keys.remove(main_key)

        df = pd.DataFrame(data[main_key])
        for k in keys:
            if k in df.columns:
                continue
            df.insert(0, k, data[k])

        cols_to_drop = r"Practice|Qualifying|Sprint"
        df = df.drop(columns=df.filter(regex=cols_to_drop).columns)

        return df, int(limit), int(offset), int(total)

    def extract_other_tables(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        other_tables = {}
        df = df.copy()
        for index, column in enumerate(df.columns):
            if column[0].islower():
                continue

            new_table = pd.DataFrame(pd.json_normalize(df[column]))
            if 0 in new_table.columns:
                new_table = pd.DataFrame(pd.json_normalize(new_table[0]))
            new_table.columns = self._convert_columns(new_table.columns.tolist())
            id_columns = [x for x in new_table.columns if x.endswith("Id")]

            if id_columns:
                id_column = id_columns[0]
                df.insert(index, id_column, new_table[id_column])
                df = df.drop(columns=[column])

                new_table = new_table.drop_duplicates(subset=[id_column]).reset_index(drop=True)
                other_tables[column] = new_table
            else:
                new_table.columns = [f"{column.lower()}{col[0].upper()}{col[1:]}" for col in new_table.columns]
                df = df.drop(columns=[column])
                df = pd.concat([df, new_table], axis=1)

        return df, other_tables

    def _convert_columns(self, columns: list[str]) -> list[str]:
        new_columns = []
        for col in columns:
            if "." not in col:
                new_columns.append(col)
            else:
                table, column = col.split(".")
                new_col = f"{table[0].lower()}{table[1:]}{column[0].upper()}{column[1:]}"
                new_columns.append(new_col)
        return new_columns


class JolpicaDB:
    map_dtypes = {
        "circuits": {
            "dtypes": {
                "circuitId": str,
                "url": str,
                "circuitName": str,
                "locationLat": float,
                "locationLong": float,
                "locationLocality": str,
                "locationCountry": str,
            },
            "sort": [
                "circuitId",
            ],
            "duplicates": [
                "circuitId",
            ],
        },
        "constructors_standings": {
            "dtypes": {
                "round": int,
                "season": int,
                "position": float,
                "points": float,
                "wins": int,
                "constructorId": str,
            },
            "sort": [
                "season",
                "round",
                "constructorId",
            ],
            "duplicates": [
                "season",
                "round",
                "constructorId",
            ],
        },
        "constructors": {
            "dtypes": {
                "constructorId": str,
                "url": str,
                "name": str,
                "nationality": str,
            },
            "sort": [
                "constructorId",
            ],
            "duplicates": [
                "constructorId",
            ],
        },
        "drivers_standings": {
            "dtypes": {
                "round": int,
                "season": int,
                "position": float,
                "points": float,
                "wins": int,
                "driverId": str,
                "constructorId": str,
            },
            "sort": [
                "season",
                "round",
                "driverId",
            ],
            "duplicates": [
                "season",
                "round",
                "driverId",
            ],
        },
        "drivers": {
            "dtypes": {
                "driverId": str,
                "url": str,
                "givenName": str,
                "familyName": str,
                "dateOfBirth": str,
                "nationality": str,
                "code": str,
            },
            "sort": [
                "driverId",
            ],
            "duplicates": [
                "driverId",
            ],
        },
        "races": {
            "dtypes": {
                "season": int,
                "round": int,
                "url": str,
                "raceName": str,
                "circuitId": str,
                "date": str,
            },
            "sort": [
                "season",
                "round",
            ],
            "duplicates": [
                "season",
                "round",
            ],
        },
    }

    def __init__(self, directory: str):
        self.jolpica_api = JolpicaAPI()
        self.jolpica_parser = JolpicaParser()
        self.directory = directory
        self._loaded = False

        self.db = {key: None for key in self.map_dtypes.keys()}

    def _load_db(self):
        for table in self.db.keys():
            file_path = os.path.join(self.directory, f"{table}.csv")
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df = self._convert_dtypes(table, df)
                self.db[table] = df
            else:
                print(f"File {file_path} does not exist. Skipping load for table {table}.")
        self._loaded = True

    def _save_db(self):
        for table, df in self.db.items():
            if df is not None:
                file_path = os.path.join(self.directory, f"{table}.csv")
                df.to_csv(file_path, index=False)

    def _request_with_pagination(self, method: callable, *args, **kwargs) -> pd.DataFrame:
        limite = 100
        total = limite
        offset = 0
        all_data = []
        while offset < total:
            kwargs.update({"limit": limite, "offset": offset})
            json_data = method(*args, **kwargs)
            df, _, _, total = self.jolpica_parser.parser(json_data)
            all_data.append(df)
            offset += limite
        return pd.concat(all_data, ignore_index=True)

    def _convert_dtypes(self, table: str, df: pd.DataFrame) -> pd.DataFrame:
        dtypes = self.map_dtypes[table]["dtypes"]
        return df.astype(dtypes)[dtypes.keys()]

    def _concat_and_clean(self, table: str, new_data: pd.DataFrame):
        if self.db[table] is not None:
            combined_df = pd.concat([self.db[table], new_data], ignore_index=True)
        else:
            combined_df = new_data

        combined_df = self._convert_dtypes(table, combined_df)
        combined_df = combined_df.drop_duplicates(subset=self.map_dtypes[table]["duplicates"], keep="last").reset_index(drop=True)
        combined_df = combined_df.sort_values(by=self.map_dtypes[table]["sort"]).reset_index(drop=True)

        self.db[table] = combined_df

    # Aux functions to update tables
    def _get_races_between_races(self, start_race: tuple[int, int] = None, end_race: tuple[int, int] = None, n_backward: int = 0, n_forward: int = 0) -> pd.DataFrame:
        if start_race is None and end_race is None:
            raise ValueError("At least one of start_race or end_race must be provided.")

        races = self.db["races"]

        if start_race is not None:
            start_index = races[(races["season"] == start_race[0]) & (races["round"] == start_race[1])].index[0]

        if end_race is not None:
            end_index = races[(races["season"] == end_race[0]) & (races["round"] == end_race[1])].index[0]

        if start_race is not None and end_race is not None:
            return races.iloc[start_index - n_backward : end_index + n_forward + 1]
        elif start_race is not None:
            return races.iloc[start_index - n_backward :]
        else:
            return races.iloc[: end_index + n_forward + 1]

    # Functions to update tables
    def _update_races(self, last_race_year: int = None):
        current_year = pd.to_datetime("now").year
        if last_race_year is None:
            last_race_year = self.db["races"]["season"].max()
        else:
            last_race_year -= 2

        all_races = []
        for year in range(last_race_year, current_year + 1):
            print(f"Updating races for season {year}")
            races = self._request_with_pagination(self.jolpica_api.races, season=year)
            all_races.append(races)
        races = pd.concat(all_races, ignore_index=True)
        races, other_tables = self.jolpica_parser.extract_other_tables(races)

        self._concat_and_clean("races", races)
        self._concat_and_clean("circuits", other_tables["Circuit"])

    def _update_drivers_standings(self, last_season: int = None, last_round: int = None, n_backward: int = 0):
        if last_season is not None and last_round is not None:
            races_to_update_drivers_standings = self._get_races_between_races((last_season, last_round), n_backward=n_backward)
        else:
            df_drivers_standings = self.db["drivers_standings"]
            last_drivers_standings_season = df_drivers_standings["season"].max()
            last_drivers_standings_round = df_drivers_standings[df_drivers_standings["season"] == last_drivers_standings_season]["round"].max()

            df = self.jolpica_api.drivers_standings(self.db["races"]["season"].max())
            df = self.jolpica_parser.parser(df)[0]
            df, _ = self.jolpica_parser.extract_other_tables(df)
            df = self._convert_dtypes("drivers_standings", df)
            last_available_drivers_standings_season = df["season"].max()
            last_available_drivers_standings_round = df["round"].max()

            races_to_update_drivers_standings = self._get_races_between_races(
                (last_drivers_standings_season, last_drivers_standings_round),
                (last_available_drivers_standings_season, last_available_drivers_standings_round),
                n_backward=n_backward,
            )

        all_drivers_standings = []
        for _, race in races_to_update_drivers_standings.iterrows():
            season = race["season"]
            round = race["round"]
            print(f"Updating drivers_standings for season {season}, round {round}")
            drivers_standings = self._request_with_pagination(self.jolpica_api.drivers_standings, season, round)
            all_drivers_standings.append(drivers_standings)

        drivers_standings = pd.concat(all_drivers_standings, ignore_index=True)
        drivers_standings, other_tables = self.jolpica_parser.extract_other_tables(drivers_standings)

        self._concat_and_clean("drivers_standings", drivers_standings)
        self._concat_and_clean("drivers", other_tables["Driver"])
        self._concat_and_clean("constructors", other_tables["Constructors"])

    def _update_constructors_standings(self, last_season: int = None, last_round: int = None, n_backward: int = 0):
        if last_season is not None and last_round is not None:
            races_to_update_constructors_standings = self._get_races_between_races((last_season, last_round), n_backward=n_backward)
        else:
            df_constructors_standings = self.db["constructors_standings"]
            last_constructors_standings_season = df_constructors_standings["season"].max()
            last_constructors_standings_round = df_constructors_standings[df_constructors_standings["season"] == last_constructors_standings_season]["round"].max()

            df = self.jolpica_api.constructors_standings(self.db["races"]["season"].max())
            df = self.jolpica_parser.parser(df)[0]
            df, _ = self.jolpica_parser.extract_other_tables(df)
            df = self._convert_dtypes("constructors_standings", df)
            last_available_constructors_standings_season = df["season"].max()
            last_available_constructors_standings_round = df["round"].max()

            races_to_update_constructors_standings = self._get_races_between_races(
                (last_constructors_standings_season, last_constructors_standings_round),
                (last_available_constructors_standings_season, last_available_constructors_standings_round),
                n_backward=n_backward,
            )

        all_constructors_standings = []
        for _, race in races_to_update_constructors_standings.iterrows():
            season = race["season"]
            round = race["round"]
            print(f"Updating constructors_standings for season {season}, round {round}")
            constructors_standings = self._request_with_pagination(self.jolpica_api.constructors_standings, season, round)
            all_constructors_standings.append(constructors_standings)

        constructors_standings = pd.concat(all_constructors_standings, ignore_index=True)
        constructors_standings, other_tables = self.jolpica_parser.extract_other_tables(constructors_standings)

        self._concat_and_clean("constructors_standings", constructors_standings)
        self._concat_and_clean("constructors", other_tables["Constructor"])

    def create(self):
        raise NotImplementedError("The create method is not implemented yet.")

    def update(self):
        self._load_db()
        self._update_races()
        self._update_drivers_standings(n_backward=2)
        self._update_constructors_standings(n_backward=2)
        self._save_db()
        self._save_db()

    # Functions to retrive data
    def get_drivers(self, start_season: int = 2000) -> pd.DataFrame:
        if not self._loaded:
            self._load_db()
        df_drivers = self.db["drivers"]
        df_drivers_standings = self.db["drivers_standings"]
        seasons_with_standings = df_drivers_standings[df_drivers_standings["season"] >= start_season]["driverId"].unique()
        return df_drivers[df_drivers["driverId"].isin(seasons_with_standings)].reset_index(drop=True)
    
    def get_constructors(self, start_season: int = 2000) -> pd.DataFrame:
        if not self._loaded:
            self._load_db()
        df_constructors = self.db["constructors"]
        df_constructors_standings = self.db["constructors_standings"]
        seasons_with_standings = df_constructors_standings[df_constructors_standings["season"] >= start_season]["constructorId"].unique()
        return df_constructors[df_constructors["constructorId"].isin(seasons_with_standings)].reset_index(drop=True)