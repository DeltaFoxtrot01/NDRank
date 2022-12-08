from typing import Any, Dict, Final
import pytest
import xarray
import numpy as np
import numpy.typing as npt
from correlation_functions.correlation_statistics import CorrelationStatistics

from correlation_functions.main_structure import CorrelationFunction
from correlation_functions.implementations.implementations import EnhancedPcc, Rmsd
from repository.repository_layer import DummyRepositoryMetadata, RepositoryMetadata

PROPERTIES_PATH: Final = "./worker_node/test/test_correlation_properties/properties.yaml"

def _create_data_array(values, date_as_string: str) -> xarray.DataArray:
    data:npt.NDArray = np.ndarray(shape=(1,4,1))
    data[0] = np.array(values)

    x_values = [1,2,3,4]
    time = [np.timedelta64(0,'ns')]
    reference_time = np.datetime64(date_as_string)

    return xarray.DataArray(
        data=data,
        dims=["step","x","variable"],
        coords=dict(
            step=(["step"], time),
            x=("x",x_values),
            time=reference_time,
            variable=["z"]
        ),
        attrs=dict(
            description="Array created for testing purposes",
        ),
    )

def test_rdsm() -> None:
    corr_function: CorrelationFunction = Rmsd("rmsd")
    da1: xarray.DataArray
    da2: xarray.DataArray

    da1 = xarray.DataArray(
        np.array([1,2,3,4])
    )
    da2 = xarray.DataArray(
        np.array([1,2,3,4])
    )
    
    assert corr_function.calculate(da1, da2, DummyRepositoryMetadata({}),"v") == 0.0

    da1 = xarray.DataArray(
        np.array([1,1,1,1])
    )
    da2 = xarray.DataArray(
        np.array([2,2,2,2])
    )

    assert corr_function.calculate(da1, da2, DummyRepositoryMetadata({}),"v") == 1.0


    da1 = xarray.DataArray(
        np.array([10,4,-1,1])
    )
    da2 = xarray.DataArray(
        np.array([1,5,26,8])
    )

    assert 14.66 < corr_function.calculate(da1, da2, DummyRepositoryMetadata({}),"v") < 14.67


def test_enhanced_pcc() -> None:
    pcc: EnhancedPcc = EnhancedPcc("enhanced-pcc",PROPERTIES_PATH)
    repo_metadata: RepositoryMetadata = RepositoryMetadata({
        "step": 1.0,
        'time-variation-dim': "step",
        'time-initial-dim': "time",
        "data-vars": ["z"]
    })

    da1: xarray.DataArray = _create_data_array([[3],[2],[5],[6]],"2014-01-01T00:00:00")
    da2: xarray.DataArray = _create_data_array([[1],[4],[3],[2]],"2014-01-01T00:00:00")

    stats_da1: CorrelationStatistics = CorrelationStatistics({"mean":3, "count":4})
    stats_da2: CorrelationStatistics = CorrelationStatistics({"mean":1.5, "count":4})
    params: Dict[str, Any] = {
        "x": slice(1,2)
    }

    best_value: float
    worst_value: float
    best_value, worst_value = pcc.calculate_partial_value(da1.sel(params),da2.sel(params),stats_da1,stats_da2,params,repo_metadata,"z")

    assert round(best_value,5) == 0.73764
    assert round(worst_value,5) == 0.06202

    params = {
        "x": slice(1,3)
    }
    best_value, worst_value = pcc.calculate_partial_value(da1.sel(params),da2.sel(params),stats_da1,stats_da2,params,repo_metadata,"z")


    assert round(best_value,5) == 0.57854
    assert round(worst_value,5) == 0.0

    params = {
        "x": slice(1,4)
    }
    best_value, worst_value = pcc.calculate_partial_value(da1.sel(params),da2.sel(params),stats_da1,stats_da2,params,repo_metadata,"z")


    assert round(best_value,5) == round(worst_value,5) == -0.28284

    assert xarray.DataArray.equals(pcc._process_arrays_correctly(da1,repo_metadata,"z"), pcc._process_arrays_correctly(da1.sel(params),repo_metadata,"z",params))
    
    value = pcc.calculate(da1,da2,repo_metadata,"z")

    assert round(value,5) == -0.28284

def test_nan_replacement() -> None:
    pcc: EnhancedPcc = EnhancedPcc("enhanced-pcc",PROPERTIES_PATH)
    repo_metadata: RepositoryMetadata = RepositoryMetadata({
        "step": 1.0,
        'time-variation-dim': "step",
        'time-initial-dim': "time",
        "data-vars": ["z"]
    })

    da1: xarray.DataArray = _create_data_array([[0],[0],[0],[0]],"2014-01-01T01:00:00")

    res: xarray.DataArray = pcc._process_arrays_correctly(da1,repo_metadata,"z")


    assert not np.isnan(da1).any()
    assert not np.isnan(res).any()
    