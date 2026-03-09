%load_ext line_profiler
from asset_base.manager import Manager
from asset_base.asset import ExchangeTradeFund, Asset
from sqlalchemy.orm import joinedload

manager = Manager()
asset_list = manager.session.query(ExchangeTradeFund).filter(ExchangeTradeFund.status == 'listed').all()
identity_code_list = [asset.identity_code for asset in asset_list]

ts_obj = manager.get_time_series_processor(identity_code_list)

stats = %prun -r -q ts_obj = manager.get_time_series_processor(identity_code_list)

asset = asset_list[0]
func = asset._eod_series[0].to_dict
%lprun -f func func()quit

func = manager.get_asset_dict
%lprun -f func func(identity_code_list)
