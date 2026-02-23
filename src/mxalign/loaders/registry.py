_LOADER_REGISTRY = {}

def register_loader(name, properties):
    def decorator(func):
        _LOADER_REGISTRY[name] = {
            "func": func,
            "properties": properties,
        }
        return func
    return decorator


def available_loaders():
    return list(_LOADER_REGISTRY.keys())


def get_loader(name):
    try:
        return _LOADER_REGISTRY[name]
    except KeyError:
        raise ValueError(f"Unknown loader: {name}")