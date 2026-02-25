from .registry import register_loader
from ..properties.properties import Properties, Space, Time, Uncertainty

# @register_loader("grid-forecast", properties=(Property.GRID, Property.FORECAST))
# def load(files: str | list[str]):
#     import xarray as xr

#     if isinstance(files, str):
#         files = [files] 
 
#     ds = xr.open_mfdataset(files, chunks="auto") 
    
#     return ds

# @register_loader("grid-observation", properties=(Property.GRID, Property.OBSERVATION))
# def load(files: str | list[str]):
#     import xarray as xr

#     if isinstance(files, str):
#         files = [files] 
 
#     ds = xr.open_mfdataset(files, chunks="auto") 
    
#     return ds

@register_loader(
    "point-forecast", 
    properties=Properties(
        space=Space.POINT,
        time=Time.FORECAST
    )
)
def load(files: str | list[str]):
    import xarray as xr

    if isinstance(files, str):
        files = [files] 
 
    ds = xr.open_mfdataset(files, chunks="auto") 
    
    return ds

@register_loader(
    "point-observation", 
    properties=Properties(
        space=Space.POINT,
        time=Time.OBSERVATION
    )
)
def load(files: str | list[str], **kwargs):
    import xarray as xr

    if isinstance(files, str):
        files = [files] 
 
    ds = xr.open_mfdataset(files, chunks="auto") 
    ds = ds.rename_dims({"code":"point_index"})
    for coord in ds.coords:
        ds[coord] = ds[coord].compute()
    return ds
