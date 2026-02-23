from .registry import get_loader
from ..properties.validation import validate_dataset
from ..properties.utils import properties_to_attrs

def load(name, *args, **kwargs):
    entry = get_loader(name)
    ds = entry["func"](*args, **kwargs)

    properties = entry["properties"]
    validate_dataset(ds, properties)

    ds.attrs["properties"] = properties_to_attrs(properties)

    return ds