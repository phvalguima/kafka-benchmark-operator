# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

# TODO: This file must go away once Kafka starts sharing its certificates via client relation

"""This module contains TLS handlers and managers for the trusted-ca relation."""

import json
import logging
import os
import secrets
import string
import subprocess

from charms.tls_certificates_interface.v1.tls_certificates import (
    generate_csr,
    generate_private_key,
)
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.model import ModelError, SecretNotFoundError

from benchmark.events.handler import RelationHandler
from literals import TRUSTED_CA_RELATION, TRUSTSTORE_LABEL
from models import JavaWorkloadPaths

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class JavaTlsHandler(RelationHandler):
    """Class to manage the Java truststores."""

    def __init__(
        self,
        charm: CharmBase,
    ):
        super().__init__(charm, TRUSTED_CA_RELATION)
        self.charm = charm
        self.tls_manager = JavaTlsStoreManager(charm)

        self.framework.observe(
            self.charm.on[TRUSTED_CA_RELATION].relation_joined,
            self._trusted_relation_joined,
        )
        self.framework.observe(
            self.charm.on[TRUSTED_CA_RELATION].relation_changed,
            self._trusted_relation_changed,
        )
        self.framework.observe(
            self.charm.on[TRUSTED_CA_RELATION].relation_broken,
            self._tls_relation_broken,
        )

    def _tls_relation_broken(self, event: EventBase) -> None:
        """Handler for `certificates_relation_broken` event."""
        event.relation.data[self.model.unit]["certificate_signing_requests"] = json.dumps({
            "csr": "",
            "certificate": "",
            "ca": "",
            "ca-cert": "",
        })

    def _trusted_relation_joined(self, event: EventBase) -> None:
        """Generate a CSR so the tls-certificates operator works as expected.

        We do not need the CSR or the following certificate. Therefore, we will not use the
        signed certificate nor the generated key.
        """
        if self.charm.unit.is_leader():
            # Leader does not defer this event, so we can generate the private key and publish
            # it across the entire
            self.tls_manager.truststore_pwd = self.tls_manager.generate_password()

        alias = f"{event.app.name}-{event.relation.id}"
        subject = os.uname().nodename
        csr = (
            generate_csr(
                add_unique_id_to_subject_name=bool(alias),
                private_key=generate_private_key(),
                subject=subject,
            )
            .decode()
            .strip()
        )

        csr_dict = [{"certificate_signing_request": csr}]
        event.relation.data[self.model.unit]["certificate_signing_requests"] = json.dumps(csr_dict)

    def _load_relation_data(self, raw_relation_data):
        """Loads relation data from the relation data bag.

        Json loads all data.

        Args:
            raw_relation_data: Relation data from the databag

        Returns:
            dict: Relation data in dict format.
        """
        certificate_data = {}
        for key in raw_relation_data:
            try:
                certificate_data[key] = json.loads(raw_relation_data[key])
            except (json.decoder.JSONDecodeError, TypeError):
                certificate_data[key] = raw_relation_data[key]
        return certificate_data

    def _trusted_relation_changed(self, event: EventBase) -> None:
        """Overrides the requirer logic of TLSInterface."""
        if not event.relation or not event.relation.app:
            # This is a relation broken event, we may not have this information available.
            return

        relation_data = self._load_relation_data(event.relation.data[event.relation.app])

        if not (provider_certificates := relation_data.get("certificates", [])):
            logger.warning("No certificates on provider side")
            return

        if provider_certificates and "certificate" in provider_certificates[0]:
            self.tls_manager.set_certificate(
                certificate=provider_certificates[0]["certificate"],
                path=self.tls_manager.java_paths.server_certificate,
            )
        if provider_certificates and "ca" in provider_certificates[0]:
            self.tls_manager.set_certificate(
                certificate=provider_certificates[0]["ca"],
                path=self.tls_manager.java_paths.ca,
            )

        # For now, let's keep all TLS logic here, even if it trigger wider charm changes
        # If we want to later abandon the entire TLS, we can then just remove this file.
        self.charm._on_config_changed(event)


class JavaTlsStoreManager:
    """Class to manage the Java trust and keystores."""

    def __init__(
        self,
        charm: CharmBase,
    ):
        self.workload = charm.workload
        self.java_paths = JavaWorkloadPaths(self.workload.paths)
        self.charm = charm
        self.database = charm.database
        self.relation_name = self.database.relation_name
        self.ca_alias = "ca"
        self.cert_alias = "server_certificate"

    def set(self) -> bool:
        """Sets the truststore and imports the certificates."""
        if not self.truststore_pwd:
            return False
        return (
            self.set_truststore()
            and self.import_cert(self.ca_alias, self.java_paths.ca)
            and self.import_cert(self.cert_alias, self.java_paths.server_certificate)
        )

    @property
    def truststore_pwd(self) -> str | None:
        """Returns the truststore password."""
        try:
            return self.charm.model.get_secret(label=TRUSTSTORE_LABEL).get_content(refresh=True)[
                "pwd"
            ]
        except (SecretNotFoundError, KeyError):
            return None
        except ModelError as e:
            logger.error(f"Error fetching secret: {e}")
            return None

    @truststore_pwd.setter
    def truststore_pwd(self, pwd: str) -> None:
        """Returns the truststore password."""
        if not self.charm.unit.is_leader():
            # Nothing to do, we manage a single password for the entire application
            return

        if not self.truststore_pwd:
            self.charm.app.add_secret({"pwd": pwd}, label=TRUSTSTORE_LABEL)
            return

        self.charm.model.get_secret(label=TRUSTSTORE_LABEL).set_content({"pwd": pwd})

    def set_certificate(self, certificate: str, path: str) -> None:
        """Sets the unit certificate."""
        self.workload.write(content=certificate, path=path)

    def set_truststore(self) -> bool:
        """Adds CA to JKS truststore."""
        if not self.workload.paths.exists(self.java_paths.ca):
            return False

        command = f"{self.java_paths.keytool} \
            -import -v -alias {self.ca_alias} \
            -file {self.java_paths.ca} \
            -keystore {self.java_paths.truststore} \
            -storepass {self.truststore_pwd} \
            -noprompt"
        return (
            self._exec(
                command=command.split(),
                working_dir=os.path.dirname(os.path.realpath(self.java_paths.truststore)),
            )
            and self._exec(
                f"chown {self.workload.user}:{self.workload.group} {self.java_paths.truststore}".split()
            )
            and self._exec(f"chmod 770 {self.java_paths.truststore}".split())
        )

    def generate_password(self) -> str:
        """Creates randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])

    def import_cert(self, alias: str, filename: str) -> bool:
        """Add a certificate to the truststore."""
        command = f"{self.java_paths.keytool} \
            -import -v \
            -alias {alias} \
            -file {filename} \
            -keystore {self.java_paths.truststore} \
            -storepass {self.truststore_pwd} -noprompt"
        return self._exec(command.split())

    def _exec(self, command: list[str], working_dir: str | None = None) -> bool:
        """Executes a command in the workload and manages the exceptions.

        Returns True if the command was successful.
        """
        try:
            self.workload.exec(command=" ".join(command), working_dir=working_dir)
        except subprocess.CalledProcessError as e:
            logger.error(e.stdout)
            return e.stdout and "already exists" in e.stdout
        return True
