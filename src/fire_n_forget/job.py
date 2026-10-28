from dataclasses import dataclass
from enum import StrEnum, auto

from fire_n_forget.resource import (
    CPUResource,
    Cluster,
    DatasetResource,
    GPUResource,
    RAMResources,
)


class JobStatus(StrEnum):
    RUNNING = auto()
    WAITING = auto()
    COMPLETED = auto()
    UNKNOWN = auto()


@dataclass(slots=True)
class Job:
    command: str
    cpu_req: CPUResource | None = None
    gpu_req: GPUResource | None = None
    ram_res: RAMResources | None = None
    dataset_res: DatasetResource | None = None


def satisfies(cluster: Cluster, job: Job) -> bool:
    if isinstance(job.cpu_req, CPUResource) and (
        cluster.cpu.cores < job.cpu_req.cores
        or cluster.cpu.max_freq < job.cpu_req.max_freq
    ):
        return False

    if isinstance(job.gpu_req, GPUResource) and (
        job.gpu_req.present and not cluster.gpu.present
    ):
        return False

    if isinstance(job.ram_res, RAMResources) and (cluster.ram.size < job.ram_res.size):
        return False

    if isinstance(job.dataset_res, DatasetResource) and not any(
        d.name == job.dataset_res.name for d in cluster.datasets
    ):
        return False

    return True
