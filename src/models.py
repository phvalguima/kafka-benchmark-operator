# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Java Workload Path details."""

import logging
import os

from benchmark.core.workload_base import WorkloadTemplatePaths
from literals import JAVA_VERSION

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class JavaWorkloadPaths:
    """Class to manage the Java trust and keystores."""

    def __init__(self, paths: WorkloadTemplatePaths):
        self.paths = paths

    @property
    def java_home(self) -> str:
        """Return the JAVA_HOME path."""
        return f"/usr/lib/jvm/java-{JAVA_VERSION}-openjdk-amd64"

    @property
    def keytool(self) -> str:
        """Return the keytool path."""
        return os.path.join(self.java_home, "bin/keytool")

    @property
    def truststore(self) -> str:
        """Return the truststore path."""
        return os.path.join(self.paths.charm_dir, "truststore.jks")

    @property
    def ca(self) -> str:
        """Return the CA path."""
        return os.path.join(self.paths.charm_dir, "ca.pem")

    @property
    def server_certificate(self) -> str:
        """Return the CA path."""
        return os.path.join(self.paths.charm_dir, "server_certificate.pem")

    @property
    def keystore(self) -> str:
        """Return the keystore path."""
        return os.path.join(self.paths.charm_dir, "keystore.jks")
