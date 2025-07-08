import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import polars as pl

    from package_supply_chain.constants import folder_path_output
    return folder_path_output, os, pl


@app.cell
def _(folder_path_output, os):
    last_folder = os.listdir(folder_path_output)[-1]
    last_folder
    return (last_folder,)


@app.cell
def _(folder_path_output, last_folder, os, pl):
    mvt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "mvt_oracle_and_speed_final.parquet"))
    return (mvt,)


@app.cell
def _(mvt, pl):
    mvt.filter((pl.col("magasin").str.contains("6306")) & (pl.col("code_article") == "TDF156144"))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
