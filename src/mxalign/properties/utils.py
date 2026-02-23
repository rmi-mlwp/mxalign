from .properties import Properties, Space, Time, Uncertainty
from .validation import validate_dataset

def properties_to_attrs(prop: Properties) -> dict:
    return {
        "space": prop.space.value,
        "time": prop.time.value,
        "uncertainty": prop.uncertainty.value
    }

def properties_from_attrs(ds) -> Properties:
    attrs = ds.attrs.get("properties",{})
    return Properties(
        space=Space(attrs["space"]),
        time=Time(attrs["time"]),
        uncertainty=Uncertainty(attrs.get("uncertainty", Uncertainty.DETERMINISTIC)),
    )

def update_space_property(ds, prop: Space):
    old_props = properties_from_attrs(ds)
    new_props = Properties(
        space = prop,
        time= old_props.time,
        uncertainty=old_props.uncertainty,
    )
    validate_dataset(ds, new_props)
    ds.attrs["properties"] = properties_to_attrs(new_props)
    return ds

def update_time_property(ds, prop: Time):
    props = properties_from_attrs(ds)
    props.time = prop
    validate_dataset(ds, props)
    ds.attrs["properties"] = properties_to_attrs(props)
    return ds