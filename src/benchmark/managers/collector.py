# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The collector class.

This class runs all the collection tasks for a given result.
"""

from benchmark.core.models import SosreportCLIArgsModel
from benchmark.core.workload_base import WorkloadBase


class CollectorManager:
    """The collector manager class."""

    def __init__(
        self,
        workload: WorkloadBase,
        sosreport_config: SosreportCLIArgsModel | None = None,
    ):
        # TODO: we need a way to run "sos collect"
        # For that, we will have to manage ssh keys between the peers
        # E.G.:
        #   sudo sos collect \
        #       -i ~/.local/share/juju/ssh/juju_id_rsa --ssh-user ubuntu --no-local \
        #       --nodes "$NODES" \
        #       --only-plugins systemd,logs,juju \
        #       -k logs.all_logs=true \
        #       --batch \
        #       --clean \
        #       --tmp-dir=/tmp/sos \
        #       -z gzip -j 1
        self.workload = workload
        if not sosreport_config:
            if workload.is_running_on_k8s():
                self.sosreport_config = SosreportCLIArgsModel(
                    plugins=["systemd", "logs", "juju"],
                )
            else:
                self.sosreport_config = SosreportCLIArgsModel(
                    plugins=["logs", "juju"],
                )
        self.sosreport_config = sosreport_config

    def install(self) -> bool:
        """Installs the collector."""
        ...

    def collect_sosreport(self) -> bool:
        """Collect the sosreport."""
        self.workload.exec(
            command=["sosreport"] + str(self.sosreport_config).split(),
        )
