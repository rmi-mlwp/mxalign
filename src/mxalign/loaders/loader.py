from .registry import get_loader
from ..properties.validation import validate_dataset
from ..properties.utils import properties_to_attrs

def load(name, files, variables=None, **kwargs_loader):
    entry = get_loader(name)
    ds = entry["func"](files=files, **kwargs_loader)
    
    if variables:
        ds = ds[variables]

    # Make sure all the coordinates are loaded
    for coord in ds.coords:
        ds[coord] = ds[coord].compute()
    properties = entry["properties"]
    validate_dataset(ds, properties)

    ds.attrs["properties"] = properties_to_attrs(properties)

    # Post-processors
    grid_mapping = kwargs_loader.pop("grid_mapping", None)
    if grid_mapping and ds.space.is_point():
        raise ValueError("Can't define a grid mapping for POINT datasets")
    elif grid_mapping:
        ds = ds.space.add_crs(grid_mapping)
        ds = ds.space.add_grid_mapping(grid_mapping)
    return ds