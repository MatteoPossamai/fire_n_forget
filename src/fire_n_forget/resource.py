from enum import StrEnum, auto
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class CPUResource:
    cores: int
    max_freq: int


@dataclass(slots=True)
class GPUResource:
    present: bool


@dataclass(slots=True)
class RAMResources:
    size: int


@dataclass(slots=True)
class DatasetResource:
    name: str
    path: str


@dataclass(slots=True)
class Cluster:
    cpu: CPUResource
    gpu: GPUResource
    ram: RAMResources
    datasets: list[DatasetResource]

    def to_json(self) -> dict[str, list]:
        return {
            "cpu": [self.cpu.cores, self.cpu.max_freq],
            "gpu": [self.gpu.present],
            "ram": [self.ram.size],
            "datasets": [[d.name, d.path] for d in self.datasets],
        }

    @staticmethod
    def from_json(data: dict[str, list]) -> Cluster:
        cpu = data.get("cpu")
        gpu = data.get("gpu")
        ram = data.get("ram")
        datasets = data.get("datasets")
        assert cpu is not None
        assert gpu is not None
        assert ram is not None
        assert datasets is not None
        return Cluster(
            cpu=CPUResource(cpu[0], cpu[1]),
            gpu=GPUResource(gpu[0]),
            ram=RAMResources(ram[0]),
            datasets=[DatasetResource(d[0], d[1]) for d in datasets],
        )
