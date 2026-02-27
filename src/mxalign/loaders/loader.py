from .registry import get_loader
from ..properties.validation import validate_dataset
from ..properties.utils import properties_to_attrs

def load(name, files, variables=None, grid_mapping=None, **kwargs):
    loader_cls = get_loader(name)
    loader = loader_cls(files, variables, grid_mapping, **kwargs)

    return loader.load()