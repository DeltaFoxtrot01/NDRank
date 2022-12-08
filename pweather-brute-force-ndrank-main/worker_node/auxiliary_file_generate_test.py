import pickle
from typing import BinaryIO, Dict, List, Union
import numpy as np
import numpy.typing as npt
import xarray

if __name__ == "__main__":
    input: List[xarray.Dataset] = [xarray.open_dataset("./testing_input_2/1980-01-03T06:00:00.000000000.nc"), 
                                   xarray.open_dataset("./testing_input_2/1980-01-03T12:00:00.000000000.nc")
                                   #xarray.open_dataset("./testing_input_2/1980-01-03T18:00:00.000000000.nc"),
                                   #xarray.open_dataset("./testing_input_2/1980-01-04T00:00:00.000000000.nc")
                                   ]

    ds1: xarray.Dataset = xarray.open_dataset("./testing_dataset/ERA5-1-1980.nc")
    ds2: xarray.Dataset = xarray.open_dataset("./testing_dataset/ERA5-2-1980.nc")
    ds3: xarray.Dataset = xarray.open_dataset("./testing_dataset/ERA5-3-1980.nc")

    res: Dict[str, float] = {}

    def aux_1(ds: xarray.Dataset, input: xarray.Dataset, difference_multiplier: int, variables: List[str], divide: bool) -> None:
        global res
        time_ds: Union[np.datetime64, npt.NDArray[np.datetime64]] = ds.coords['time'].values
        steps: npt.NDArray[np.datetime64] = ds.coords['step'].values
        difference: np.timedelta64 = steps[1] - steps[0]

        for step in steps:
            similarity_val: float = 0.0
            for var in variables:
                da: xarray.DataArray = ds.sel(step=step)[var]
                similarity_val += xarray.corr(da, input[var]).data.item()

            similarity_val /= 2
            similarity_val -= 1/2

            key: str = str(time_ds + step - difference * difference_multiplier)
            
            if key in res:
                res[key] += float(similarity_val)
                if divide:
                    res[key] /= 2
            else:
                res[key] = float(similarity_val)

    aux_1(ds1,input[0], 0, ["z"],False)
    aux_1(ds2,input[0], 0, ["z"],False)
    aux_1(ds3,input[0], 0, ["z"],False)

    aux_1(ds1,input[1], 1, ["z"],True)
    aux_1(ds2,input[1], 1, ["z"],True)
    aux_1(ds3,input[1], 1, ["z"],True)

    #aux_1(ds1,input[0], 0, ["t"],False)
    #aux_1(ds2,input[0], 0, ["t"],False)
    #aux_1(ds3,input[0], 0, ["t"],False)

    #aux_1(ds1,input[1], 1, ["t"],False)
    #aux_1(ds2,input[1], 1, ["t"],False)
    #aux_1(ds3,input[1], 1, ["t"],False)
    
    for key in res:
        print(key,end='\t'); print(res[key],end='\t'); print(type(res[key]))

    binary_file: BinaryIO = open('test/worst_param_search_res.bin', mode='wb')
    pickle.dump(res,binary_file)
    binary_file.close() 