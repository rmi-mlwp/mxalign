from .registry import register_loader
from ..properties.properties import Properties, Space, Time, Uncertainty

@register_loader(
    "anemoi-inference", 
    properties=Properties(
        space = Space.GRID, 
        time=Time.FORECAST,
        uncertainty=Uncertainty.DETERMINISTIC,
    )
)
def load(files: str | list[str]):
    import xarray as xr

    if isinstance(files, str):
        files = [files] 
 
    times = xr.open_dataset(files[0])["time"].values
    lead_times = times - times[0]    


    ds = xr.open_mfdataset(
        files, 
        preprocess=_preprocess,
    )

    ds_out = ds.\
        assign_coords({"lead_time": ("time", lead_times)}).\
        rename_dims({"values": "grid_index"}).\
        swap_dims({"time": "lead_time"})
    
    return ds_out


def _preprocess(ds):
    ds_out = ds.\
        set_coords(["longitude", "latitude"]).\
        expand_dims("reference_time").\
        assign_coords(
            {"reference_time": ("reference_time", [ds["time"].values[0]])}
        ).\
        drop_vars("time")
    
    return ds_out