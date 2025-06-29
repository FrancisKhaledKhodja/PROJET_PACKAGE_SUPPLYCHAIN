

from package_supply_chain.api.api_inter_prod_conso_output_stat import get_inter_prod_conso_output_statistics



def test_get_intervention_output_statistics():
    inter_output = get_inter_prod_conso_output_statistics()
    inter_output.write_excel("inter_prod_conso_output_stats.xlsx")

