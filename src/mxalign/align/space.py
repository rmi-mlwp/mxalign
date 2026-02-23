import xarray as xr

from ..properties.properties import Properties, Time, Space, Uncertainty
from ..properties.utils import properties_to_attrs

def align_space(datasets, reference, **kwargs):
    if isinstance(datasets, (xr.Dataset, xr.DataArray)):
        datasets = [datasets]
    if isinstance(datasets, dict):
        keys = datasets.keys()
        datasets = datasets.items()
    else:
        keys = None

    datasets = [
        ds.space.align_with(reference, **kwargs)[0]
        for ds in datasets
    ]

    if keys is None:
        return datasets
    else:
        return {key: value for (key, value) in zip(keys, datasets)}
        
