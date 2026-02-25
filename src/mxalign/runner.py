import os
import xarray as xr

from .utils.config import Config
from .loaders.loader import load
from .transformations.transform import transform
from .align.time import align_time
from .align.space import align_space
from .utils.save import save_dataset
from .verification import Metric


class Runner():
    def __init__(self, config: str | dict):
        self.config = Config(config)
        self.datasets = {}
    
    def run(self):
        self.load_datasets()
        self.transform_datasets()
        self.align()
        self.verify()

    def load_datasets(self):
        config_datasets = self.config["datasets"]
        for key, config in config_datasets.items():
            config = config.copy()
            # Check if all the files exist
            loader = config.pop("loader")
            variables = config.pop("variables", None)
            files = []
            for file in config.pop("files"):
                if os.path.exists(file):
                    files.append(file)
                else: 
                    print(f"File: {file} is missing, skipping.")
            self.datasets[key] = load(
                name=loader,
                files=files,
                variables=variables,
                **config
            )
    
    def transform_datasets(self):
        config_transformations = self.config["transformations"]
        for transformation, config in config_transformations.items():
            config = config.copy()
            keys = config.pop("datasets", self.datasets.keys())
            for key in keys:
                ds = self.datasets[key]
                self.datasets[key] = transform(
                    name=transformation,
                    datasets=ds,
                    **config
                )

    def align(self):
        config_align = self.config["alignment"]
        reference = config_align.pop("reference")
        config_align_time = config_align.get("time",None)
        config_align_space = config_align.get("space", None)
        config_align_save = config_align.get("save", None)

        if config_align_time:
            self.align_time(config_align_time)
        else:
            print("Skipping temporal alignment")
        if config_align_space:
            self.align_space(reference=reference, config=config_align_space)
        else:
            print("Skipping spatial alignment")
        if config_align_save:
            config = config_align_save.copy()
            method = config.pop("method")
            datasets = config.pop("datasets","all")
            if datasets == "all":
                for name, ds in self.datasets.items():
                    save_dataset(method, name, ds, **config)
    
    def verify(self):
        config_verify = self.config["verification"]
        reference = self.datasets[config_verify["reference"]]
        metrics = {}
        for metric_name, config in config_verify["metrics"].items():
            config = config.copy()
            func_path = config.pop("function")
            inputs = config.pop("inputs")
            metric = Metric(
                name=metric_name,
                func_path=func_path,
                ds_ref=reference,
                inputs=inputs,
                **config
            )
            models = {}
            for ds_name, ds in self.datasets.items():
                if ds_name != config_verify["reference"]:
                    models[ds_name] = metric.compute(ds)
            models = xr.concat(
                models.values(),
                dim = xr.Variable("model", list(models.keys()))
            )
            metrics[metric.name] = models
        metrics = xr.concat(
            metrics.values(),
            dim = xr.Variable("metric", list(metrics.keys()))
        )
        self.metrics = metrics.transpose("model", "metric", ...)
    
    def align_time(self, config):
        self.datasets = align_time(self.datasets, **config)

    def align_space(self, reference, config):
        ds_ref = self.datasets[reference]
        for name, ds in self.datasets.items():
            if name != reference:
                options = config.get(get_spatial_alignment(ds, ds_ref), None)
                self.datasets[name] = align_space(ds, ds_ref, **options)
        
    
def get_spatial_alignment(ds, reference):
    if reference.space.is_point() and ds.space.is_grid():
        return "interpolation"
    if reference.space.is_grid() and ds.space.is_grid():
        return "regrid"
    return "null"

            