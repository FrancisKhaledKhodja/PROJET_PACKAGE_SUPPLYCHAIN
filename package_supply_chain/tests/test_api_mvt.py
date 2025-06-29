

from package_supply_chain.api.api_mvt import get_movements_oracle_and_speed


def test_get_mvt_oracle():
    mvt = get_movements_oracle_and_speed()
    print(mvt.shape)