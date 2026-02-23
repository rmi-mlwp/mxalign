import numpy as np
import dask.array as dda
import xarray as xr 

from scipy.interpolate import LinearNDInterpolator
from scipy.spatial import Delaunay

from .base import BaseInterpolator
from .registry import register_interpolator
from ..properties.properties import Space



@register_interpolator
class DelaunayInterpolator(BaseInterpolator):
    name = "delaunay"
    source_space = Space.GRID
    target_space = Space.POINT

    def __init__(self, target_dataset, **options):
        super().__init__(target_dataset, **options)
        method = self.options.get("method", "linear")
        if  method != "linear":
            raise ValueError(f"Method: {method}. Delaunay interpolation only supports linear interpolation")
        
    def _interpolate(self, source_dataset):
        if "grid_index" not in source_dataset.dims:
            raise NotImplementedError("Delaunay interpolation currently only supports stacked grids")
        
        if "latitude" in source_dataset.dims:
            lon_grid, lat_grid = np.meshgrid(
                source_dataset["longitude"].values, 
                source_dataset["latitude"].values
            )
            source_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))
        else: 
            source_points = np.column_stack(
              (source_dataset["latitude"].values, source_dataset["longitude"].values)
        )

        triangulation = Delaunay(source_points)

        target_points = np.column_stack(
            (self.target_dataset["latitude"].values, self.target_dataset["longitude"].values)
        )

        arrays_out = {}
        for var in source_dataset.data_vars:
            da = source_dataset[var]
            if da.dims[-1] != "grid_index":
                print(f"Skipping variable '{var}' - doesn't end with spatial dimension grid_index")
                continue
            else:
                arrays_out[var] = interpolate_da(da, triangulation, target_points)

        ds_out = xr.Dataset(arrays_out).assign_coords(
            latitude = self.target_dataset["latitude"],
            longitude = self.target_dataset["longitude"]
        )
        ds_out.attrs["properties"] = source_dataset.attrs["properties"]
        return ds_out


def interpolate_da(da, triangulation, target_points):
    n_target = len(target_points)
    leading_dims = da.dims[:-1]

    #Build the template
    #Get chunking info for leading dims
    shape_tmp = tuple(
        da.sizes[d] for d in leading_dims
    ) + (n_target,)
    
    if isinstance(da.data, dda.Array):
        dim_to_chunks = dict(zip(da.dims, da.chunks))
    else:
        dim_to_chunks = {dim: (da.sizes[dim],) for dim in da.dims}
    chunks_tmp = tuple(
        dim_to_chunks[dim] if dim in dim_to_chunks else (da.sizes[dim],)
        for dim in leading_dims
    ) + ((n_target,), )

    # Create a dask array template matching the chunking pattern
    tmp = dda.empty(shape=shape_tmp, chunks=chunks_tmp, dtype=da.dtype)
    tmp = xr.DataArray(
        tmp,
        dims=leading_dims + ("point_index", ),
        coords={d: da.coords[d].load() for d in leading_dims} 
        )
        # .assign_coords(
        #     point_index=np.arange(n_target),
        # )

    da_interp = da.map_blocks(
        lambda block: interpolate_block(
            block,
            triangulation,
            target_points
        ),
        template=tmp
    )

    return da_interp

def interpolate_block(block : xr.DataArray, triangulation: Delaunay, target_points: np.ndarray):
    data = block.values #data # shape = (.., npoints)
    original_shape = data.shape[:-1]
    data_flat = data.reshape(-1, data.shape[-1]) # shape = (ndim1 * ndim2 * ... , npoints)

    _interpolated = []
    for row in data_flat:
        interpolator = LinearNDInterpolator(triangulation, row)
        _interpolated.append(interpolator(target_points))

    interpolated_flat = np.stack(_interpolated)

    interpolated = interpolated_flat.reshape(*original_shape, target_points.shape[0])

    new_dims = block.dims[:-1] + ("point_index", )
    new_coords = {dim: block.coords[dim] for dim in block.dims[:-1]}
    # new_coords["point_index"] = np.arange(target_points.shape[0])
    # new_coords["latitude"] = ("point_index", target_points[:,0])
    # new_coords["longitude"] = ("point_index", target_points[:,1])

    return xr.DataArray(interpolated, dims=new_dims, coords=new_coords)
        
        


