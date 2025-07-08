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
    mvt = mvt.with_columns(pl.col("flag_panne_sur_stock").cast(pl.Int8))
    bt = pl.read_parquet(os.path.join(folder_path_output, last_folder, "bt.parquet"))
    return bt, mvt


@app.cell
def _(bt, mvt, pl):
    mvt_panne_sur_stock = mvt.filter(pl.col("flag_panne_sur_stock") == 1)
    mvt_panne_sur_stock = mvt_panne_sur_stock.join(bt, how="left", left_on="n_bt", right_on="numero_du_bt")
    return (mvt_panne_sur_stock,)


@app.cell
def _(mvt_panne_sur_stock):
    mvt_panne_sur_stock
    return


@app.cell
def _(mvt, pl):
    mvt.select(pl.col("lib_motif_mvt")).unique().sort("lib_motif_mvt")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
