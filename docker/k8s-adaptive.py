# -*- coding: utf-8 -*-
"""
This module implements the dask class required to provide adaptive functionality
using Kubernetes.
"""

import logging, os, json, urlparse

import distributed.deploy
from kubernetes import client, config


class KubeCluster(object):
    """
    Implementation of dask adaptive deployment where workers are spawned as
    Kubernetes pods.

    See http://distributed.readthedocs.io/en/latest/adaptive.html.
    """
    def __init__(self, **kwargs):
        self.log = logging.getLogger("distributed.deploy.adaptive")
        # Configure ourselves using environment variables
        if 'KUBECONFIG' in os.environ:
            config.load_kube_config()
        else:
            config.load_incluster_config()
        # Switch off SSL host name verification for now (the JAP Python is old... :-()
        client.configuration.assert_hostname = False
        self.api = client.CoreV1Api()
        # Read the environment variables for configuration
        self.namespace = os.environ.get('NAMESPACE', 'dask')
        self.worker_labels = os.environ.get('WORKER_LABELS', 'app=dask,component=worker')
        dask_scheduler_service = os.environ.get('DASK_SCHEDULER_SERVICE', 'dask-scheduler')
        worker_name_prefix = os.environ.get('WORKER_NAME_PREFIX', 'dask-worker-')
        worker_image = os.environ.get('WORKER_IMAGE', 'daskdev/dask:latest')
        worker_image_pull_policy = os.environ.get('WORKER_IMAGE_PULL_POLICY', '')
        # Worker resources should be given as a JSON string, e.g.
        # "{'requests': {'cpu':'100m','memory':'1Gi'}}"
        # We need them as a dict
        worker_resources = json.loads(os.environ.get('WORKER_RESOURCES', '\{\}'))
        # Build the pod template once for use later
        # Note that because we use generate_name rather than name, this is reusable
        self.pod_template = client.V1Pod(
            metadata = client.V1ObjectMeta(
                generate_name = worker_name_prefix,
                # Convert comma-separated 'k=v' pairs to a dict
                labels = dict(
                    l.strip().split('=')
                    for l in self.worker_labels.split(',')
                    if l.strip()
                )
            ),
            spec = client.V1PodSpec(
                # Don't attempt to restart failed workers as this causes funny things
                # to happen when dask kills workers legitimately
                restart_policy = 'Never',
                containers = [
                    client.V1Container(
                        name = 'dask-worker',
                        image = worker_image,
                        image_pull_policy = worker_image_pull_policy,
                        env = [
                            client.V1EnvVar(
                                name = 'POD_IP',
                                value_from = client.V1EnvVarSource(
                                    field_ref = client.V1ObjectFieldSelector(
                                        field_path = 'status.podIP'
                                    )
                                )
                            ),
                            client.V1EnvVar(
                                name = 'POD_NAME',
                                value_from = client.V1EnvVarSource(
                                    field_ref = client.V1ObjectFieldSelector(
                                        field_path = 'metadata.name'
                                    )
                                )
                            ),
                        ],
                        args = [
                            'dask-worker',
                            dask_scheduler_service,
                            '--nprocs', '1',
                            '--nthreads', '1',
                            '--host', '$(POD_IP)',
                            '--name', '$(POD_NAME)',
                        ]
                    )
                ]
            )
        )
        # If resource requests were given, add them to the pod template
        if worker_resources:
            resources = client.V1ResourceRequirements(**worker_resources)
            self.pod_template.spec.containers[0].resources = resources

    def scale_up(self, n):
        """
        Creates worker pods so that the total number of worker pods is ``n``.
        """
        # Find the number of pods that match the specified labels
        try:
            pods = self.api.list_namespaced_pod(
                self.namespace,
                label_selector = self.worker_labels
            )
        except client.rest.ApiException:
            self.log.exception('Error fetching existing pods. No scaling attempted.')
            return
        self.log.info('Scaling worker pods from %s to %s', len(pods.items), n)
        for _ in xrange(n - len(pods.items)):
            # Create the pod
            try:
                created = self.api.create_namespaced_pod(self.namespace, self.pod_template)
            except client.rest.ApiException:
                self.log.exception('Error creating pod')
            else:
                self.log.info('Created pod: %s', created.metadata.name)

    def scale_down(self, workers):
        """
        When the worker process exits, Kubernetes leaves the pods in a completed
        state. Kill them when we are asked to.
        """
        # Get the existing worker pods
        try:
            pods = self.api.list_namespaced_pod(self.namespace, label_selector = self.worker_labels)
        except client.rest.ApiException:
            self.log.exception('Error fetching existing pods. No scaling attempted.')
            return
        # Work out pods that we are going to delete
        # Each worker to delete is given in the form "tcp://<worker ip>:<port>"
        # Convert this to a set of IPs
        ips = set(urlparse.urlparse(worker).hostname for worker in workers)
        to_delete = set(
            p for p in pods.items
            # Every time we run, purge any completed pods as well as the specified ones
            if p.status.phase == 'Succeeded' or p.status.pod_ip in ips
        )
        if not to_delete: return
        self.log.info("Deleting %s of %s pods", len(to_delete), len(pods.items))
        for pod in to_delete:
            try:
                self.api.delete_namespaced_pod(
                    pod.metadata.name,
                    self.namespace,
                    client.V1DeleteOptions()
                )
            except client.rest.ApiException as e:
                # If a pod has already been removed, just ignore the error
                if e.status != 404:
                    self.log.exception('Error deleting pod: %s', pod.metadata.name)
            else:
                self.log.info('Deleted pod: %s', pod.metadata.name)


def dask_setup(scheduler):
    """
    Configures the dask scheduler to use the adaptive implementation.
    """
    cluster = KubeCluster()
    adapative_cluster = distributed.deploy.Adaptive(scheduler, cluster)
