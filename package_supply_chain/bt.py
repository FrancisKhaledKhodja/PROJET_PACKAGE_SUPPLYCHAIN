import polars as pl
from package_supply_chain.excel_csv_to_dataframe import read_csv


class BT():

    def __init__(self, folder_path, file_name):
        self.df = read_csv(folder_path, file_name)
        self.clean_columns()
        self.calculate_number_non_empty_fields()
        self.sort_dataframe()
        self.remove_duplicate_rows()

    def clean_columns(self):
        self.df = self.df.with_columns(
            [
                pl.when(pl.col(col).str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
                for col in self.df.columns
                if self.df.schema[col] == pl.String
            ]
        )

    def sort_dataframe(self):
        self.df = self.df.sort(by=["numero_du_bt", "nb_non_vides"], descending=[False, True])

    def remove_duplicate_rows(self):
        self.df = self.df.unique(subset="numero_du_bt", keep="first")

    def calculate_number_non_empty_fields(self):
        self.df = self.df.with_columns(
            pl.fold(
                acc=pl.lit(0),
                function=lambda acc, x: acc + x,
                exprs=[pl.col(col).is_not_null().cast(pl.Int8) for col in self.df.columns]
                ).alias("nb_non_vides")
            )

