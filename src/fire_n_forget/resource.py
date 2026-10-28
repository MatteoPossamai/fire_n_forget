from enum import StrEnum, auto
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class NetworkMetadata:
    ip: str
    port: int


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
    network: NetworkMetadata
    cpu: CPUResource
    gpu: GPUResource
    ram: RAMResources
    datasets: list[DatasetResource]

    def to_json(self) -> dict[str, list]:
        return {
            "network": [self.network.ip, self.network.port],
            "cpu": [self.cpu.cores, self.cpu.max_freq],
            "gpu": [self.gpu.present],
            "ram": [self.ram.size],
            "datasets": [[d.name, d.path] for d in self.datasets],
        }

    @staticmethod
    def from_json(data: dict[str, list]) -> Cluster:
        network = data.get("network")
        cpu = data.get("cpu")
        gpu = data.get("gpu")
        ram = data.get("ram")
        datasets = data.get("datasets")
        assert network is not None
        assert cpu is not None
        assert gpu is not None
        assert ram is not None
        assert datasets is not None
        return Cluster(
            network=NetworkMetadata(network[0], int(network[1])),
            cpu=CPUResource(int(cpu[0]), int(cpu[1])),
            gpu=GPUResource(bool(gpu[0])),
            ram=RAMResources(int(ram[0])),
            datasets=[DatasetResource(d[0], d[1]) for d in datasets],
        )
